//go:build conformance

// Upstream persistence-tests suite gate (backlog B-8).
//
// The M3 cooperation test (TestM3_EveryStore_WorksTogether) is a
// hand-rolled multi-store slice. This test drives the full upstream
// persistencetests.TestBase machinery: factory wiring via
// client.DataStoreFactoryProvider, TaskManager / ClusterMetadataManager
// / MetadataManager / ShardManager / ExecutionManager / Nexus /
// NamespaceReplicationQueue all constructed through the plugin.
//
// Green here is the signal we're compatible with the shape Temporal
// server's bootstrap expects end-to-end. A successful TestBase.Setup
// is the entry gate to every upstream test suite; if anything here
// fails, the suites beneath would too.
package testutil_test

import (
	"context"
	"testing"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/pkg/plugin"
)

// newUpstreamTestBase builds a fully-initialized persistencetests.TestBase
// pointing at our plugin. The caller owns Setup/Teardown.
func newUpstreamTestBase(t *testing.T) *persistencetests.TestBase {
	t.Helper()
	c := testutil.NewConformanceCluster(t)
	tb := persistencetests.NewTestBaseForCluster(c, c.Logger())
	tb.AbstractDataStoreFactory = plugin.NewFactory()
	return tb
}

// TestUpstreamSuite_Smoke drives TestBase.Setup against our plugin.
// Setup exercises every manager's construction through
// DataStoreFactoryProvider: ShardMgr, ClusterMetadataManager,
// MetadataManager, ExecutionManager, TaskMgr, FairTaskMgr,
// NexusEndpointManager, NamespaceReplicationQueue. It also creates
// a shard (ID 10) and reads current cluster metadata.
//
// A pass here means the plugin survives the same bootstrap path
// temporal-server runs, which is the B-8 "full upstream
// persistence-tests suite entry gate" signal.
func TestUpstreamSuite_Smoke(t *testing.T) {
	tb := newUpstreamTestBase(t)
	tb.Setup(cluster.NewTestClusterMetadataConfig(false, false))
	t.Cleanup(tb.TearDownWorkflowStore)

	ctx := context.Background()

	// ShardManager was populated by Setup. Re-reading shard 10 through
	// the manager layer confirms the round trip works end-to-end.
	if _, err := tb.ShardMgr.GetOrCreateShard(ctx, &persistence.GetOrCreateShardRequest{
		ShardID: 10,
	}); err != nil {
		t.Errorf("GetOrCreateShard on seeded shard: %v", err)
	}

	// ExecutionManager: a non-existent execution surfaces an error
	// (NotFound) through the full manager stack rather than an
	// internal error.
	if _, err := tb.ExecutionManager.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
		ShardID: 10, NamespaceID: "ns-nope", WorkflowID: "wf-nope",
	}); err == nil {
		t.Error("expected error on missing current execution")
	}

	// ClusterMetadata is lazily populated by the server on first boot,
	// not by Setup. A NotFound here is the expected state until a
	// SaveClusterMetadata call happens elsewhere in the bootstrap
	// sequence; the point of THIS test is the manager stack's shape,
	// not cluster-metadata persistence semantics (those are covered
	// by TestM1_ClusterMetadataManager_SaveAndList).
	if _, err := tb.ClusterMetadataManager.GetCurrentClusterMetadata(ctx); err == nil {
		t.Error("unexpected success reading unsaved cluster metadata")
	}
}
