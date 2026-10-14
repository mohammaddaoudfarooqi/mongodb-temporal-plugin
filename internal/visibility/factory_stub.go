// Package visibility owns store.VisibilityStore in both Aggregation and
// Atlas Search modes. The Factory connects a
// CustomDatastoreConfig to a live Store.
package visibility

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	visfactory "go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"

	internalconfig "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/config"
	pluginmetrics "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metrics"
	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

// Factory implements visibility.VisibilityStoreFactory. Wires the
// plugin's configured Mongo URI into a Store honoring the caller's
// visibilityMode option.
type Factory struct{}

// NewFactory constructs a visibility factory. Stateless by design;
// every NewVisibilityStore call opens its own ClientProvider (so
// default + visibility datastores may point at different clusters).
func NewFactory() *Factory { return &Factory{} }

// NewVisibilityStore satisfies visibility.VisibilityStoreFactory.
func (f *Factory) NewVisibilityStore(
	cfg config.CustomDatastoreConfig,
	saProvider searchattribute.Provider,
	mapperProvider searchattribute.MapperProvider,
	_ namespace.Registry,
	_ *chasm.Registry,
	_ resolver.ServiceResolver,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (store.VisibilityStore, error) {
	parsed, warnings, err := internalconfig.ParseConfig(cfg.Options)
	if err != nil {
		return nil, fmt.Errorf("visibility: %w", err)
	}
	for _, w := range warnings {
		if logger != nil {
			logger.Warn(w)
		}
	}

	mode := ModeAggregation
	switch parsed.VisibilityMode {
	case "atlas-search":
		mode = ModeAtlasSearch
	case "aggregation", "":
		mode = ModeAggregation
	default:
		return nil, fmt.Errorf("visibility: unknown visibilityMode %q", parsed.VisibilityMode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	provider, err := internalmongo.NewClientProvider(ctx, internalmongo.FromConfig(parsed))
	if err != nil {
		return nil, fmt.Errorf("visibility: %w", err)
	}
	// The returned store owns the client; every failure below releases it
	//.
	closeClient := func() { _ = provider.Close(context.Background()) }

	// Fail startup when Atlas Search is requested on a
	// deployment that can't support it. The probe helper's error
	// message points the operator at either an Atlas M10+ cluster or
	// visibilityMode=aggregation.
	if mode == ModeAtlasSearch {
		if err := ProbeAtlasSearchAvailable(ctx, provider.Database()); err != nil {
			closeClient()
			return nil, err
		}
	}

	opts := []Option{
		WithMetrics(pluginmetrics.New(metricsHandler)),
		WithTemporalObservability(metricsHandler, logger),
		withClientCloser(closeClient),
		WithIndexName(cfg.IndexName),
	}
	if saProvider != nil {
		opts = append(opts, WithSearchAttributeProvider(saProvider))
	}
	if mapperProvider != nil {
		opts = append(opts, WithSearchAttributeMapperProvider(mapperProvider))
	}
	// In Atlas Search mode, spin up an index tracker that
	// polls status and gates reads until READY. Writes are not gated
	// so records land even while the index is still building.
	if mode == ModeAtlasSearch {
		indexName := parsed.AtlasSearchIndexName
		if indexName == "" {
			indexName = "visibility_executions_search"
		}
		tracker := NewIndexTracker(provider.Database(), indexName, 5*time.Second)
		tracker.Start(context.Background())
		opts = append(opts, WithIndexTracker(tracker))
	}
	return New(provider.Database(), mode, parsed.AtlasSearchIndexName, opts...), nil
}

// Static interface guard.
var (
	_ visfactory.VisibilityStoreFactory = (*Factory)(nil)
	_ store.VisibilityStore             = (*Store)(nil)
)

// Placeholder references keep upstream imports resolved in later tasks.
var (
	_ = visibilityservice.ListChasmExecutionsRequest{}
	_ = manager.GetWorkflowExecutionRequest{}
)
