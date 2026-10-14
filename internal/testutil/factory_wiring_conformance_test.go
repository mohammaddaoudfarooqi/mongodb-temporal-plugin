//go:build conformance

// Baseline conformance test. This test tracks what — if anything — in
// the plugin's factory path still returns serviceerror.Unimplemented.
// As of M3 every store is real; the test exists as a regression
// guardrail that all store constructors return non-nil working impls
// through the upstream DI path.
//
// Runs only under `make conformance`.
package testutil_test

import (
	"testing"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistenceclient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/pkg/plugin"
)

// buildStoreFactory runs the plugin's factory through the same path
// Temporal's server uses (DataStoreFactoryProvider), so this test
// catches any wiring drift between our NewFactory and the upstream DI.
func buildStoreFactory(t *testing.T) persistence.DataStoreFactory {
	t.Helper()
	cluster := testutil.NewConformanceCluster(t)
	cfg := cluster.Config()
	custom := cfg.DataStores[cfg.DefaultStore].CustomDataStoreConfig

	var absFactory persistenceclient.AbstractDataStoreFactory = plugin.NewFactory()
	return absFactory.NewFactory(
		*custom,
		resolver.NewNoopResolver(),
		"mongo-conformance",
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		serialization.NewSerializer(),
	)
}

// TestConformance_Baseline_AllStoresConstructible validates the factory
// wiring at M3: every store constructor returns a non-nil, real
// implementation (no remaining Unimplemented stubs). An upstream
// interface addition that the plugin hasn't implemented would fail
// compile; a behavioral miss is caught by each store's own integration
// tests, not here.
func TestConformance_Baseline_AllStoresConstructible(t *testing.T) {
	f := buildStoreFactory(t)
	t.Cleanup(f.Close)

	if s, err := f.NewShardStore(); err != nil || s == nil {
		t.Errorf("NewShardStore: s=%v err=%v", s, err)
	}
	if s, err := f.NewExecutionStore(); err != nil || s == nil {
		t.Errorf("NewExecutionStore: s=%v err=%v", s, err)
	}
	if s, err := f.NewTaskStore(); err != nil || s == nil {
		t.Errorf("NewTaskStore: s=%v err=%v", s, err)
	}
	if s, err := f.NewFairTaskStore(); err != nil || s == nil {
		t.Errorf("NewFairTaskStore: s=%v err=%v", s, err)
	}
	if s, err := f.NewMetadataStore(); err != nil || s == nil {
		t.Errorf("NewMetadataStore: s=%v err=%v", s, err)
	}
	if s, err := f.NewClusterMetadataStore(); err != nil || s == nil {
		t.Errorf("NewClusterMetadataStore: s=%v err=%v", s, err)
	}
	if s, err := f.NewNexusEndpointStore(); err != nil || s == nil {
		t.Errorf("NewNexusEndpointStore: s=%v err=%v", s, err)
	}
	if s, err := f.NewQueue(persistence.NamespaceReplicationQueueType); err != nil || s == nil {
		t.Errorf("NewQueue: s=%v err=%v", s, err)
	}
	if s, err := f.NewQueueV2(); err != nil || s == nil {
		t.Errorf("NewQueueV2: s=%v err=%v", s, err)
	}
}
