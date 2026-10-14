// Package plugin hosts the internal factory that vends store instances.
// Consumers never import this package directly — they go through
// github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/pkg/plugin, which
// wraps the internal types to keep the public API surface minimal.
package plugin

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistenceclient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/clustermetastore"
	internalconfig "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/config"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metadatastore"
	pluginmetrics "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metrics"
	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/nexusstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/queuestore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/schema"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/security"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/shardstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/taskstore"
)

// AbstractFactory implements persistenceclient.AbstractDataStoreFactory.
// Construction is cheap and pre-network: AbstractFactory.NewFactory
// returns a DataStoreFactory immediately; the MongoDB connection is
// established lazily on the first store call, which returns the
// connect error rather than panicking.
type AbstractFactory struct{}

// NewAbstract constructs the internal abstract factory.
func NewAbstract() *AbstractFactory { return &AbstractFactory{} }

// NewFactory satisfies persistenceclient.AbstractDataStoreFactory. Temporal
// server's DI calls this once per process per datastore (default +
// visibility). The returned DataStoreFactory is held for the lifetime
// of the server.
func (f *AbstractFactory) NewFactory(
	cfg config.CustomDatastoreConfig,
	_ resolver.ServiceResolver,
	clusterName string,
	logger log.Logger,
	mh metrics.Handler,
	_ serialization.Serializer,
) persistence.DataStoreFactory {
	if logger == nil {
		logger = log.NewNoopLogger()
	}
	if mh == nil {
		mh = metrics.NoopMetricsHandler
	}
	return &DataStoreFactory{
		cfgOptions:  cfg.Options,
		clusterName: clusterName,
		logger:      logger,
		metrics:     mh,
	}
}

// DataStoreFactory vends every persistence sub-store and owns the
// single mongo.Client every store reuses. The client is
// created lazily behind a sync.Once so callers of NewFactory do not
// block on network I/O — that belongs on the first store call.
type DataStoreFactory struct {
	// Configuration captured from Temporal's DI; parsed lazily.
	cfgOptions  map[string]any
	clusterName string
	logger      log.Logger
	metrics     metrics.Handler

	// initOnce guards the provider initialisation. initErr captures a
	// failed construction so subsequent store calls return the same
	// error rather than retrying connect on every call.
	initOnce sync.Once
	provider *internalmongo.ClientProvider
	initErr  error

	// clientBuilds counts how many distinct ClientProviders have been
	// built. A well-wired factory only ever reports 1 (or 0 if no
	// store was requested); tests use this to catch shared-client
	// regressions.
	clientBuilds atomic.Int32

	closeOnce sync.Once
}

// Close satisfies persistence.DataStoreFactory. Idempotent: callers
// may invoke it more than once without panicking. Budgets the Mongo
// disconnect to 10 seconds.
func (f *DataStoreFactory) Close() {
	f.closeOnce.Do(func() {
		if f.provider == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := f.provider.Close(ctx); err != nil {
			f.logger.Warn("plugin: provider close returned error",
				tag.Error(err))
		}
	})
}

// ensureProvider initialises the shared ClientProvider on demand. Every
// store constructor calls this before touching the database.
func (f *DataStoreFactory) ensureProvider() (*internalmongo.ClientProvider, error) {
	f.initOnce.Do(func() {
		parsed, warnings, err := internalconfig.ParseConfig(f.cfgOptions)
		for _, w := range warnings {
			f.logger.Warn(w)
		}
		if err != nil {
			f.initErr = err
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		clientCfg := internalmongo.FromConfig(parsed)
		p, err := internalmongo.NewClientProvider(ctx, clientCfg)
		if err != nil {
			// Driver init errors sometimes embed
			// the connection URI (with credentials). Redact before
			// storing so subsequent f.initErr reads are safe to log.
			f.initErr = security.RedactError(err)
			return
		}

		// Schema-version startup check.
		// The plugin refuses to start if:
		//   - _schema_version is absent (cluster not initialised)
		//   - state is not "ready" (upgrading or failed)
		// The check is skipped when cfgOptions includes
		// skipSchemaCheck: true, so tests that stand up a fresh cluster
		// without setup-schema can still exercise the store layer.
		if !f.skipSchemaCheck() {
			if err := f.verifySchemaMetadata(ctx, p.Database()); err != nil {
				_ = p.Close(context.Background())
				f.initErr = err
				return
			}
		}

		f.provider = p
		f.clientBuilds.Add(1)
	})
	if f.initErr != nil {
		return nil, f.initErr
	}
	if f.provider == nil {
		return nil, errors.New("plugin: client provider not available")
	}
	return f.provider, nil
}

// --- store constructors. Each one first ensures the provider exists.
// The provider's *mongo.Database handle is passed into the store, so
// future milestones will replace Stub with a real impl that takes a
// *mongo.Database argument.

func (f *DataStoreFactory) NewTaskStore() (persistence.TaskStore, error) {
	p, err := f.ensureProvider()
	if err != nil {
		return nil, err
	}
	return taskstore.NewWithMetrics(p.Database(), pluginmetrics.New(f.metrics)), nil
}

func (f *DataStoreFactory) NewFairTaskStore() (persistence.TaskStore, error) {
	// Matching runs a queue's classic and fair backlogs side by side,
	// each with its own RangeID lease, so the fair store has its own
	// collections.
	p, err := f.ensureProvider()
	if err != nil {
		return nil, err
	}
	return taskstore.NewFairWithMetrics(p.Database(), pluginmetrics.New(f.metrics)), nil
}

func (f *DataStoreFactory) NewShardStore() (persistence.ShardStore, error) {
	p, err := f.ensureProvider()
	if err != nil {
		return nil, err
	}
	return shardstore.NewWithMetrics(p.Database(), f.clusterName, pluginmetrics.New(f.metrics)), nil
}

func (f *DataStoreFactory) NewMetadataStore() (persistence.MetadataStore, error) {
	p, err := f.ensureProvider()
	if err != nil {
		return nil, err
	}
	return metadatastore.NewWithMetrics(p.Database(), pluginmetrics.New(f.metrics)), nil
}

func (f *DataStoreFactory) NewExecutionStore() (persistence.ExecutionStore, error) {
	p, err := f.ensureProvider()
	if err != nil {
		return nil, err
	}
	// Thread the metrics emitter into the hot execution
	// path. pluginmetrics.New is nil-safe — if the host passed the
	// noop handler (tests, toys), the emitter's methods are no-ops
	// and the store sees no extra overhead.
	return executionstore.NewWithMetrics(p.Database(), f.clusterName,
		pluginmetrics.New(f.metrics)), nil
}

func (f *DataStoreFactory) NewQueue(queueType persistence.QueueType) (persistence.Queue, error) {
	p, err := f.ensureProvider()
	if err != nil {
		return nil, err
	}
	return queuestore.NewWithMetrics(p.Database(), queueType, pluginmetrics.New(f.metrics)), nil
}

func (f *DataStoreFactory) NewQueueV2() (persistence.QueueV2, error) {
	p, err := f.ensureProvider()
	if err != nil {
		return nil, err
	}
	return queuestore.NewV2WithMetrics(p.Database(), pluginmetrics.New(f.metrics)), nil
}

func (f *DataStoreFactory) NewClusterMetadataStore() (persistence.ClusterMetadataStore, error) {
	p, err := f.ensureProvider()
	if err != nil {
		return nil, err
	}
	return clustermetastore.NewWithMetrics(p.Database(), pluginmetrics.New(f.metrics)), nil
}

func (f *DataStoreFactory) NewNexusEndpointStore() (persistence.NexusEndpointStore, error) {
	p, err := f.ensureProvider()
	if err != nil {
		return nil, err
	}
	return nexusstore.NewWithMetrics(p.Database(), pluginmetrics.New(f.metrics)), nil
}

// skipSchemaCheck returns true when the caller explicitly opts out of
// the _schema_version startup check via cfgOptions["skipSchemaCheck"].
// Tests that spin up a fresh cluster without running setup-schema use
// this; production deployments never should.
func (f *DataStoreFactory) skipSchemaCheck() bool {
	v, ok := f.cfgOptions["skipSchemaCheck"].(bool)
	return ok && v
}

// verifySchemaMetadata enforces the schema-version contract at startup:
//   - _schema_version must exist (cluster initialised by setup-schema)
//   - state must be "ready" (refuse on upgrading-* or failed-*)
//   - numHistoryShards, if configured, must match the recorded value
func (f *DataStoreFactory) verifySchemaMetadata(ctx context.Context, db *mongo.Database) error {
	meta, err := schema.ReadMetadata(ctx, db)
	if err != nil {
		return fmt.Errorf("plugin: read schema metadata: %w", err)
	}
	if meta == nil {
		return errors.New("plugin: _schema_version absent; run temporal-mongodb-tool setup-schema before starting the server")
	}
	if meta.State != string(schema.StateReady) {
		return fmt.Errorf("plugin: refusing to start — _schema_version.state=%q is not \"ready\"; run temporal-mongodb-tool verify", meta.State)
	}
	// numHistoryShards check: if the caller supplied one via
	// cfgOptions["numHistoryShards"], it must match what setup-schema
	// recorded. Mismatch is a full-data-migration scenario and
	// refuses startup.
	if configured, ok := asInt32(f.cfgOptions["numHistoryShards"]); ok && configured > 0 {
		if meta.NumHistoryShards != 0 && configured != meta.NumHistoryShards {
			return fmt.Errorf(
				"plugin: refusing to start — configured numHistoryShards=%d but _schema_version recorded %d; resharding is a full data migration, not a config change",
				configured, meta.NumHistoryShards)
		}
	}
	return nil
}

// asInt32 reads a map[string]any value as an int32 if it was supplied
// as any numeric form YAML-to-Go tends to produce.
func asInt32(v any) (int32, bool) {
	switch t := v.(type) {
	case int:
		return int32(t), true
	case int32:
		return t, true
	case int64:
		return int32(t), true
	case float64:
		return int32(t), true
	}
	return 0, false
}

// Static interface guards.
var (
	_ persistenceclient.AbstractDataStoreFactory = (*AbstractFactory)(nil)
	_ persistence.DataStoreFactory               = (*DataStoreFactory)(nil)
)
