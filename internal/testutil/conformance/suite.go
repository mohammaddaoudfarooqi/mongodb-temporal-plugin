package conformance

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/clustermetastore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metadatastore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/queuestore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/shardstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/taskstore"
)

// RunAll orchestrates every store's suite against the fixture.
// Each top-level t.Run names the scenario group so `-run` filters
// stay intuitive. Scenarios that require a capability the fixture
// lacks (transactions, atlas-search) skip with an explanatory
// message rather than failing.
func RunAll(t *testing.T, f Fixture) {
	t.Helper()
	caps := f.Capabilities()

	// Indexes + schema bootstrap happen once per top-level run; the
	// first per-subtest DB the fixture hands back carries the
	// ensured indexes. Drivers may short-circuit repeat ensure calls.
	t.Run("ShardStore", func(t *testing.T) { runShardStore(t, f) })
	t.Run("MetadataStore", func(t *testing.T) { runMetadataStore(t, f) })
	t.Run("ClusterMetadataStore", func(t *testing.T) { runClusterMetadataStore(t, f) })
	t.Run("ExecutionStore", func(t *testing.T) {
		if !caps.Transactions {
			t.Skip("fixture lacks Transactions capability")
		}
		runExecutionStore(t, f)
	})
	t.Run("HistoryStore", func(t *testing.T) { runHistoryStore(t, f) })
	t.Run("TaskStore", func(t *testing.T) { runTaskStore(t, f) })
	t.Run("NexusEndpointStore", func(t *testing.T) { runNexusStore(t, f) })
	t.Run("QueueStore", func(t *testing.T) { runQueueStore(t, f) })
	t.Run("Visibility_Aggregation", func(t *testing.T) { runVisibilityAggregation(t, f) })
	t.Run("Visibility_AtlasSearch", func(t *testing.T) {
		if !caps.AtlasSearch {
			t.Skip("fixture lacks AtlasSearch capability")
		}
		runVisibilityAtlasSearch(t, f)
	})
	t.Run("Invariants", func(t *testing.T) { runInvariants(t, f) })
	t.Run("ErrorMatrix", func(t *testing.T) { runErrorMatrix(t, f) })
}

// EnsureAllIndexes brings every store's collection + index into
// existence on the given database. Idempotent; safe to call on a
// fresh or reused DB. Exported so drivers can pre-seed indexes
// once per top-level run instead of on every subtest.
func EnsureAllIndexes(ctx context.Context, db *mongo.Database) error {
	for _, fn := range []func(context.Context, *mongo.Database) error{
		shardstore.EnsureIndexes,
		metadatastore.EnsureIndexes,
		clustermetastore.EnsureIndexes,
		executionstore.EnsureIndexes,
		executionstore.EnsureHistoryIndexes,
		executionstore.EnsureTaskIndexes,
		executionstore.EnsureDLQIndexes,
		taskstore.EnsureIndexes,
		queuestore.EnsureIndexes,
	} {
		if err := fn(ctx, db); err != nil {
			return err
		}
	}
	return nil
}
