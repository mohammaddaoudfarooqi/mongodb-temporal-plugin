package conformance

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/clustermetastore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/shardstore"
)

// runInvariants hosts backend-wide invariants: every hot read path
// chooses IXSCAN, TTL indexes carry expireAfterSeconds=0, etc. These
// ride the live backend (not a unit fake) so a planner regression or
// wrong index option flip is caught on the actual server version the
// fixture targets.
func runInvariants(t *testing.T, f Fixture) {
	t.Run("IXSCAN_Executions_ByShardNamespace", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure shard idx", shardstore.EnsureIndexes(tc.Ctx, tc.DB))
		fatalOnErr(t, "ensure exec idx", executionstore.EnsureIndexes(tc.Ctx, tc.DB))

		// Seed a couple of execution docs so the planner has data.
		shards := shardstore.New(tc.DB, "cluster0")
		_, err := shards.GetOrCreateShard(tc.Ctx, &persistence.InternalGetOrCreateShardRequest{
			ShardID: 51,
			CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
				return 1, dataBlob("init"), nil
			},
		})
		fatalOnErr(t, "seed shard", err)
		es := executionstore.New(tc.DB, "cluster0")
		for i := 0; i < 3; i++ {
			snap := makeSnapshot(uniqStr("ns-x"), "wf", uniqStr("run"))
			_, err := es.CreateWorkflowExecution(tc.Ctx, &persistence.InternalCreateWorkflowExecutionRequest{
				ShardID: 51, RangeID: 1, Mode: persistence.CreateWorkflowModeBrandNew,
				NewWorkflowSnapshot: snap,
			})
			fatalOnErr(t, "create", err)
		}

		assertIXSCAN(t, tc.Ctx, tc.DB, "executions", bson.M{"shard_id": int32(51)})
	})

	t.Run("IXSCAN_HistoryNodes_ByBranch", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure history idx", executionstore.EnsureHistoryIndexes(tc.Ctx, tc.DB))
		es := executionstore.New(tc.DB, "cluster0")
		tree, branch := uniqStr("tree"), uniqStr("branch")
		for nid := int64(1); nid <= 3; nid++ {
			err := es.AppendHistoryNodes(tc.Ctx, &persistence.InternalAppendHistoryNodesRequest{
				ShardID:     1,
				BranchInfo:  &persistencespb.HistoryBranch{TreeId: tree, BranchId: branch},
				IsNewBranch: nid == 1,
				TreeInfo:    dataBlob("ti"),
				Node: persistence.InternalHistoryNode{
					NodeID: nid, TransactionID: nid, Events: dataBlob("evt"),
				},
			})
			fatalOnErr(t, "append", err)
		}
		assertIXSCAN(t, tc.Ctx, tc.DB, "history_nodes", bson.M{
			"tree_id": tree, "branch_id": branch,
			"node_id": bson.M{"$gte": int64(1), "$lt": int64(100)},
		})
	})

	t.Run("TTL_ClusterMembership_ExpireAfterSecondsIsZero", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", clustermetastore.EnsureIndexes(tc.Ctx, tc.DB))
		cursor, err := tc.DB.Collection("cluster_membership").Indexes().List(tc.Ctx)
		fatalOnErr(t, "list indexes", err)
		var indexes []bson.M
		fatalOnErr(t, "decode indexes", cursor.All(tc.Ctx, &indexes))
		found := false
		for _, idx := range indexes {
			if idx["name"] == "membership_record_expiry_ttl" {
				found = true
				if v := idx["expireAfterSeconds"]; v != int32(0) {
					t.Errorf("expireAfterSeconds=%v (%T); want 0", v, v)
				}
			}
		}
		if !found {
			t.Errorf("membership_record_expiry_ttl index missing: %+v", indexes)
		}
	})
}

// assertIXSCAN runs explain(executionStats) and fails unless the
// winning plan contains an IXSCAN stage. Copied from the existing
// explain_integration_test.go to keep suite code self-contained.
func assertIXSCAN(t *testing.T, ctx context.Context, db *mongo.Database, collection string, filter bson.M) {
	t.Helper()
	cmd := bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: collection},
			{Key: "filter", Value: filter},
		}},
		{Key: "verbosity", Value: "executionStats"},
	}
	var out bson.M
	if err := db.RunCommand(ctx, cmd).Decode(&out); err != nil {
		t.Fatalf("explain %s: %v", collection, err)
	}
	stages := collectStages(out)
	hasIXSCAN := false
	hasCOLLSCAN := false
	for _, s := range stages {
		if s == "IXSCAN" {
			hasIXSCAN = true
		}
		if s == "COLLSCAN" {
			hasCOLLSCAN = true
		}
	}
	if !hasIXSCAN {
		t.Fatalf("expected IXSCAN on %s; stages seen: %v", collection, stages)
	}
	if hasCOLLSCAN {
		t.Errorf("plan for %s contains a COLLSCAN — index is not picked: %v", collection, stages)
	}
}

func collectStages(node any) []string {
	out := []string{}
	switch v := node.(type) {
	case bson.M:
		for k, child := range v {
			if k == "stage" {
				if s, ok := child.(string); ok {
					out = append(out, s)
				}
			}
			out = append(out, collectStages(child)...)
		}
	case bson.D:
		for _, e := range v {
			if e.Key == "stage" {
				if s, ok := e.Value.(string); ok {
					out = append(out, s)
				}
			}
			out = append(out, collectStages(e.Value)...)
		}
	case bson.A:
		for _, e := range v {
			out = append(out, collectStages(e)...)
		}
	case map[string]any:
		for k, child := range v {
			if k == "stage" {
				if s, ok := child.(string); ok {
					out = append(out, s)
				}
			}
			out = append(out, collectStages(child)...)
		}
	case []any:
		for _, e := range v {
			out = append(out, collectStages(e)...)
		}
	}
	return out
}
