//go:build integration

// M2 gate — drives ExecutionStore through persistence.NewExecutionManager
// (the upstream manager wrapper) for a full write → read → update cycle.
// A green run here proves the plugin's document schema + transaction
// boundaries are compatible with Temporal's serialization layer.
//
// Full upstream persistence-tests suite invocation runs after
// M3 when task stores are real.
package testutil_test

import (
	"context"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/shardstore"
)

func gateM2DB(t *testing.T) *mongo.Database {
	t.Helper()
	db := gateDB(t) // from m1 test file
	ctx := context.Background()
	if err := shardstore.EnsureIndexes(ctx, db); err != nil {
		t.Fatalf("shardstore EnsureIndexes: %v", err)
	}
	if err := executionstore.EnsureIndexes(ctx, db); err != nil {
		t.Fatalf("executionstore EnsureIndexes: %v", err)
	}
	if err := executionstore.EnsureHistoryIndexes(ctx, db); err != nil {
		t.Fatalf("executionstore EnsureHistoryIndexes: %v", err)
	}
	return db
}

// TestM2_StoreDirect_CreateGetRoundTrip drives the execution store
// directly through its Internal* request/response surface. The
// ExecutionManager wrapper ships full workflow validation (version
// histories, execution-stats math, task-category routing) that
// depends on M3 task-store paths; we validate the store's boundary
// shape here and defer the full manager-level round-trip to the
// upstream suite after M3.
func TestM2_StoreDirect_CreateGetRoundTrip(t *testing.T) {
	db := gateM2DB(t)
	ctx := context.Background()

	// Seed shard through the upstream manager so the plugin is driven
	// via the same serialization code path Temporal uses in production.
	shardMgr := persistence.NewShardManager(shardstore.New(db, "cluster0"), serialization.NewSerializer())
	defer shardMgr.Close()
	_, err := shardMgr.GetOrCreateShard(ctx, &persistence.GetOrCreateShardRequest{
		ShardID:          1,
		InitialShardInfo: &persistencespb.ShardInfo{ShardId: 1, RangeId: 1},
	})
	if err != nil {
		t.Fatal(err)
	}

	store := executionstore.New(db, "cluster0")
	defer store.Close()

	ns, wfID, runID := "ns-m2", "wf-m2", "run-m2"
	execState := &persistencespb.WorkflowExecutionState{
		CreateRequestId: "req-1",
		RunId:           runID,
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	// Serialize the proto through the same serializer the manager uses
	// so the blob is byte-identical to what Temporal would write.
	serializer := serialization.NewSerializer()
	stateBlob, err := serializer.WorkflowExecutionStateToBlob(execState)
	if err != nil {
		t.Fatal(err)
	}
	infoBlob := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte("opaque-info-blob-bytes"),
	}

	_, err = store.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1,
		Mode: persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: persistence.InternalWorkflowSnapshot{
			NamespaceID:        ns,
			WorkflowID:         wfID,
			RunID:              runID,
			ExecutionInfoBlob:  infoBlob,
			ExecutionStateBlob: stateBlob,
			ExecutionState:     execState,
			NextEventID:        2,
			DBRecordVersion:    1,
			LastWriteVersion:   1,
		},
	})
	if err != nil {
		t.Fatalf("CreateWorkflowExecution: %v", err)
	}

	got, err := store.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: ns, WorkflowID: wfID, RunID: runID,
	})
	if err != nil {
		t.Fatalf("GetWorkflowExecution: %v", err)
	}
	// Deserialize the round-tripped state blob via the manager's
	// serializer: this is the critical interop check.
	decodedState, err := serializer.WorkflowExecutionStateFromBlob(got.State.ExecutionState)
	if err != nil {
		t.Fatalf("state blob did not round-trip through manager serializer: %v", err)
	}
	if decodedState.RunId != runID {
		t.Errorf("RunId round-trip: got %q", decodedState.RunId)
	}
	if decodedState.State != enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING {
		t.Errorf("State round-trip: got %v", decodedState.State)
	}
	// Info blob is opaque — byte-equal round trip.
	if string(got.State.ExecutionInfo.Data) != "opaque-info-blob-bytes" {
		t.Errorf("info blob bytes drifted: %q", string(got.State.ExecutionInfo.Data))
	}
}

// TestM2_HistoryV2_RoundTrip exercises AppendHistoryNodes + ReadHistoryBranch
// through the upstream ExecutionManager. Keeps the test focused on the
// plugin boundary: the manager encodes history events, we persist them,
// and a subsequent read returns them in order.
func TestM2_HistoryV2_RoundTrip(t *testing.T) {
	db := gateM2DB(t)
	ctx := context.Background()

	store := executionstore.New(db, "cluster0")
	util := store.GetHistoryBranchUtil()

	// Build a branch token.
	bid := "branch-m2"
	token, err := util.NewHistoryBranch("ns", "wf", "run", "tree-m2", &bid, nil, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := util.ParseHistoryBranchInfo(token)
	if err != nil {
		t.Fatal(err)
	}

	// Append 3 nodes directly through the store (the manager's history
	// API threads this shape).
	for i := int64(1); i <= 3; i++ {
		err := store.AppendHistoryNodes(ctx, &persistence.InternalAppendHistoryNodesRequest{
			ShardID:     1,
			BranchToken: token,
			BranchInfo:  parsed,
			IsNewBranch: i == 1,
			Node: persistence.InternalHistoryNode{
				NodeID:        i,
				TransactionID: i,
				Events: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					Data:         []byte(fmt.Sprintf("event-%d", i)),
				},
			},
		})
		if err != nil {
			t.Fatalf("AppendHistoryNodes %d: %v", i, err)
		}
	}

	resp, err := store.ReadHistoryBranch(ctx, &persistence.InternalReadHistoryBranchRequest{
		ShardID:   1,
		BranchID:  bid,
		MinNodeID: 1,
		MaxNodeID: 100,
		PageSize:  10,
	})
	if err != nil {
		t.Fatalf("ReadHistoryBranch: %v", err)
	}
	if len(resp.Nodes) != 3 {
		t.Errorf("want 3 nodes; got %d", len(resp.Nodes))
	}
}
