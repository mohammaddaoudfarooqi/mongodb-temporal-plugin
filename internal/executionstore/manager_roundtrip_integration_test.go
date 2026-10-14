//go:build integration

// Integration test for the ExecutionManager round-trip (backlog B-9).
//
// The M2 gate exercised CreateWorkflowExecution via the internal
// store surface; the full manager wraps the store with a serializer +
// cache + transaction-size gate. This test drives a round-trip via
// persistence.NewExecutionManager so a real Temporal server that
// owned the manager would see the same behavior.
package executionstore_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
)

// TestM2_ExecutionManager_Roundtrip covers the path manager→store→Mongo
// for CreateWorkflowExecution + GetWorkflowExecution. This is the
// scenario that previously failed with "version histories index is
// out of range" (B-9); the incremental-update path (B-3) + history
// events in transaction (B-1) are the prerequisites that let it pass.
func TestM2_ExecutionManager_Roundtrip(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	if err := executionstore.EnsureHistoryIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if err := executionstore.EnsureTaskIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	store := executionstore.New(db, "cluster0")
	serializer := serialization.NewSerializer()
	mgr := persistence.NewExecutionManager(
		store, serializer, nil,
		log.NewTestLogger(),
		dynamicconfig.GetIntPropertyFn(12*1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	)
	defer mgr.Close()

	// Craft a snapshot that will pass the manager's
	// version-histories projection. The manager requires a non-empty
	// version_histories entry with a branch token + one item.
	execInfo := &persistencespb.WorkflowExecutionInfo{
		NamespaceId:   "ns-mgr",
		WorkflowId:    "wf-mgr",
		StartTime:     ptrToTime(time.Now().UTC()),
		ExecutionTime: ptrToTime(time.Now().UTC()),
		VersionHistories: &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{{
				BranchToken: []byte("branch-token-mgr"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 1, Version: 1},
				},
			}},
		},
		ExecutionStats: &persistencespb.ExecutionStats{},
	}
	execState := &persistencespb.WorkflowExecutionState{
		CreateRequestId: "req-mgr",
		RunId:           "run-mgr",
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}

	ctx := context.Background()
	_, err := mgr.CreateWorkflowExecution(ctx, &persistence.CreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: persistence.WorkflowSnapshot{
			ExecutionInfo:   execInfo,
			ExecutionState:  execState,
			NextEventID:     2,
			DBRecordVersion: 1,
		},
	})
	if err != nil {
		t.Fatalf("CreateWorkflowExecution via ExecutionManager: %v", err)
	}

	// Round-trip read: GetWorkflowExecution via the manager should
	// deserialize through the same blob codec and return a
	// WorkflowExecutionInfo that matches the one we wrote.
	resp, err := mgr.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID:     1,
		NamespaceID: "ns-mgr",
		WorkflowID:  "wf-mgr",
		RunID:       "run-mgr",
	})
	if err != nil {
		t.Fatalf("GetWorkflowExecution via ExecutionManager: %v", err)
	}
	if resp.State.ExecutionInfo == nil {
		t.Fatal("ExecutionInfo nil after round-trip")
	}
	if resp.State.ExecutionInfo.WorkflowId != "wf-mgr" {
		t.Errorf("WorkflowID drift: %q", resp.State.ExecutionInfo.WorkflowId)
	}
	if resp.State.ExecutionState == nil || resp.State.ExecutionState.RunId != "run-mgr" {
		t.Errorf("ExecutionState drift: %+v", resp.State.ExecutionState)
	}
	if resp.DBRecordVersion != 1 {
		t.Errorf("DBRecordVersion drift: got %d", resp.DBRecordVersion)
	}
}

// ptrToTime returns a protobuf timestamp pointer for the given time,
// which is the wire shape the manager expects for StartTime /
// ExecutionTime.
func ptrToTime(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}
