//go:build integration

// Integration tests for the internal factory. Unlike the unit-level
// wiring tests, these bring up a real MongoDB replica set via
// testcontainers-go so that NewShardStore/NewMetadataStore/etc.
// actually return a usable (non-nil) store bound to a live database.
// Runs only under `make integration`.
package plugin_test

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"

	iplugin "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/plugin"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil"
)

// startReplicaSet brings up a single-node replica set (image selected
// by MONGO_VERSION, default mongo:6) and returns a URI ready for the
// plugin to consume.
func startReplicaSet(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, err := mongodb.Run(ctx, testutil.MongoImage(), mongodb.WithReplicaSet("rs0"))
	if err != nil {
		t.Fatalf("start mongo: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	raw, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	u, _ := url.Parse(raw)
	q := u.Query()
	q.Del("replicaSet")
	u.RawQuery = q.Encode()
	return strings.TrimRight(u.String(), "?") + "?directConnection=true"
}

// buildFactory constructs a DataStoreFactory wired to the given URI
// and database name. All other plugin options use defaults.
func buildFactory(t *testing.T) persistence.DataStoreFactory {
	t.Helper()
	uri := startReplicaSet(t)
	return iplugin.NewAbstract().NewFactory(
		config.CustomDatastoreConfig{
			Name:      "mongodb",
			IndexName: "temporal",
			Options: map[string]any{
				"uri":             uri,
				"database":        "temporal",
				"skipSchemaCheck": true,
			},
		},
		resolver.NewNoopResolver(),
		"test-cluster",
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		serialization.NewSerializer(),
	)
}

// TestDataStoreFactory_VendsAllNineStores verifies that the
// factory vends ShardStore, TaskStore, FairTaskStore, MetadataStore,
// ExecutionStore, ClusterMetadataStore, NexusEndpointStore, Queue, and
// QueueV2 — nine entry points. Each returned store must be non-nil and
// must have a working `GetName()` (or behave cleanly on Close).
func TestDataStoreFactory_VendsAllNineStores(t *testing.T) {
	f := buildFactory(t)
	defer f.Close()

	// ShardStore
	shard, err := f.NewShardStore()
	if err != nil || shard == nil {
		t.Fatalf("NewShardStore: %v, %v", shard, err)
	}
	if shard.GetName() == "" {
		t.Error("ShardStore.GetName() returned empty")
	}

	// TaskStore + FairTaskStore
	ts, err := f.NewTaskStore()
	if err != nil || ts == nil {
		t.Fatalf("NewTaskStore: %v, %v", ts, err)
	}
	fairTS, err := f.NewFairTaskStore()
	if err != nil || fairTS == nil {
		t.Fatalf("NewFairTaskStore: %v, %v", fairTS, err)
	}

	// MetadataStore, ClusterMetadataStore
	md, err := f.NewMetadataStore()
	if err != nil || md == nil {
		t.Fatalf("NewMetadataStore: %v, %v", md, err)
	}
	cmd, err := f.NewClusterMetadataStore()
	if err != nil || cmd == nil {
		t.Fatalf("NewClusterMetadataStore: %v, %v", cmd, err)
	}

	// ExecutionStore
	es, err := f.NewExecutionStore()
	if err != nil || es == nil {
		t.Fatalf("NewExecutionStore: %v, %v", es, err)
	}

	// NexusEndpointStore
	nx, err := f.NewNexusEndpointStore()
	if err != nil || nx == nil {
		t.Fatalf("NewNexusEndpointStore: %v, %v", nx, err)
	}

	// Queue
	q, err := f.NewQueue(persistence.NamespaceReplicationQueueType)
	if err != nil || q == nil {
		t.Fatalf("NewQueue: %v, %v", q, err)
	}

	// QueueV2
	qv2, err := f.NewQueueV2()
	if err != nil || qv2 == nil {
		t.Fatalf("NewQueueV2: %v, %v", qv2, err)
	}
}

// TestExecutionStore_HistoryBranchUtilIsReady verifies that branch-token
// parsing delegates to the upstream util. We don't exercise wire compat
// here; the golden-corpus tests cover that. This test guards against a later refactor that drops the
// delegation.
func TestExecutionStore_HistoryBranchUtilIsReady(t *testing.T) {
	f := buildFactory(t)
	defer f.Close()
	es, err := f.NewExecutionStore()
	if err != nil {
		t.Fatalf("NewExecutionStore: %v", err)
	}
	if es.GetHistoryBranchUtil() == nil {
		t.Fatal("GetHistoryBranchUtil() returned nil; must delegate to upstream")
	}
}
