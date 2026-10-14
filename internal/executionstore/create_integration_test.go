//go:build integration

// Integration tests for CreateWorkflowExecution.
// Exercises the four creation modes + CAS paths against a live replica
// set with transactions enabled.
package executionstore_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/mongo"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	txTasks "go.temporal.io/server/service/history/tasks"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/shardstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil"
)

func startDB(t *testing.T) *mongo.Database {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// enableTestCommands lets tests arm failCommand fail points.
	container, err := mongodb.Run(ctx, testutil.MongoImage(), mongodb.WithReplicaSet("rs0"),
		testcontainers.WithCmdArgs("--setParameter", "enableTestCommands=1"))
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
	uri := strings.TrimRight(u.String(), "?") + "?directConnection=true"
	provider, err := internalmongo.NewClientProvider(ctx, internalmongo.ClientConfig{
		URI: uri, Database: "temporal_exec_test", AppName: "test",
	})
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close(context.Background()) })
	db := provider.Database()
	if err := testutil.WaitForPrimary(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := shardstore.EnsureIndexes(ctx, db); err != nil {
		t.Fatalf("shardstore EnsureIndexes: %v", err)
	}
	if err := executionstore.EnsureIndexes(ctx, db); err != nil {
		t.Fatalf("executionstore EnsureIndexes: %v", err)
	}
	return db
}

// seedShard inserts a shard doc so the RangeID check can succeed.
func seedShard(t *testing.T, db *mongo.Database, shardID int32, rangeID int64) {
	t.Helper()
	shards := shardstore.New(db, "cluster0")
	_, err := shards.GetOrCreateShard(context.Background(), &persistence.InternalGetOrCreateShardRequest{
		ShardID: shardID,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return rangeID, blobBytes("shard-info"), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func blobBytes(s string) *commonpb.DataBlob {
	return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte(s)}
}

func makeSnapshot(ns, wf, run string) persistence.InternalWorkflowSnapshot {
	return persistence.InternalWorkflowSnapshot{
		NamespaceID:        ns,
		WorkflowID:         wf,
		RunID:              run,
		ExecutionInfoBlob:  blobBytes(fmt.Sprintf("info:%s:%s:%s", ns, wf, run)),
		ExecutionStateBlob: blobBytes(fmt.Sprintf("state:%s", run)),
		ExecutionState: &persistencespb.WorkflowExecutionState{
			CreateRequestId: "req-" + run,
			RunId:           run,
			State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventID:      2,
		DBRecordVersion:  1,
		StartVersion:     1,
		LastWriteVersion: 1,
	}
}

// TestCreate_BrandNew_FreshWorkflow happy path: a
// brand-new workflow on a shard with no current execution succeeds.
func TestCreate_BrandNew_FreshWorkflow(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")

	req := &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID:             1,
		RangeID:             1,
		Mode:                persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: makeSnapshot("ns-a", "wf-1", "run-1"),
	}
	_, err := s.CreateWorkflowExecution(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateWorkflowExecution: %v", err)
	}

	// Read-back through GetCurrentExecution to prove the pointer
	// landed atomically.
	got, err := s.GetCurrentExecution(context.Background(), &persistence.GetCurrentExecutionRequest{
		ShardID: 1, NamespaceID: "ns-a", WorkflowID: "wf-1",
	})
	if err != nil {
		t.Fatalf("GetCurrentExecution: %v", err)
	}
	if got.RunID != "run-1" {
		t.Errorf("current run_id = %q; want run-1", got.RunID)
	}
}

// : BrandNew mode must fail when a current execution
// already exists for the (namespace, workflow) pair.
func TestCreate_BrandNew_WithExisting_ReturnsCurrentWorkflowConditionFailed(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()

	_, err := s.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: makeSnapshot("ns-b", "wf-2", "run-1"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Second BrandNew on same (ns, wf) must fail.
	_, err = s.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: makeSnapshot("ns-b", "wf-2", "run-2"),
	})
	if err == nil {
		t.Fatal("expected CurrentWorkflowConditionFailedError; got nil")
	}
	var cwcf *persistence.CurrentWorkflowConditionFailedError
	if !errors.As(err, &cwcf) {
		t.Fatalf("expected *CurrentWorkflowConditionFailedError; got %T: %v", err, err)
	}
	if cwcf.RunID != "run-1" {
		t.Errorf("error should carry current RunID run-1; got %q", cwcf.RunID)
	}
}

func TestCreate_ShardRangeMismatch_ReturnsShardOwnershipLost(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 5) // stored RangeID = 5
	s := executionstore.New(db, "cluster0")

	_, err := s.CreateWorkflowExecution(context.Background(), &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 99, // stale
		Mode:                persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: makeSnapshot("ns-c", "wf-3", "run-1"),
	})
	if err == nil {
		t.Fatal("expected ShardOwnershipLostError; got nil")
	}
	var shardLost *persistence.ShardOwnershipLostError
	if !errors.As(err, &shardLost) {
		t.Fatalf("expected *ShardOwnershipLostError; got %T: %v", err, err)
	}
}

// TestCreate_BypassCurrent_DoesNotCreatePointer covers the bypass mode:
// the execution is inserted but the current_executions pointer is not
// touched. Useful for zombie-state recovery.
func TestCreate_BypassCurrent_DoesNotCreatePointer(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()

	_, err := s.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.CreateWorkflowModeBypassCurrent,
		NewWorkflowSnapshot: makeSnapshot("ns-d", "wf-4", "run-1"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// No current pointer → GetCurrentExecution returns NotFound.
	_, err = s.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
		ShardID: 1, NamespaceID: "ns-d", WorkflowID: "wf-4",
	})
	if err == nil {
		t.Error("expected NotFound on bypassed current pointer; got nil")
	}
}

// TestCreate_UpdateCurrent_WhenPreviousClosed covers the
// UpdateCurrent mode with an existing closed workflow: the new run
// takes over as current.
func TestCreate_UpdateCurrent_WhenPreviousClosed(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()

	// First run: completes (State=Completed).
	prev := makeSnapshot("ns-e", "wf-5", "run-1")
	prev.ExecutionState = &persistencespb.WorkflowExecutionState{
		CreateRequestId: "req-1",
		RunId:           "run-1",
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}
	_, err := s.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: prev,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Second run: UpdateCurrent with PreviousRunID=run-1.
	next := makeSnapshot("ns-e", "wf-5", "run-2")
	_, err = s.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1,
		Mode:                     persistence.CreateWorkflowModeUpdateCurrent,
		PreviousRunID:            "run-1",
		PreviousLastWriteVersion: 1,
		NewWorkflowSnapshot:      next,
	})
	if err != nil {
		t.Fatalf("UpdateCurrent create: %v", err)
	}
	got, err := s.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
		ShardID: 1, NamespaceID: "ns-e", WorkflowID: "wf-5",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.RunID != "run-2" {
		t.Errorf("current run_id after update: %q; want run-2", got.RunID)
	}
}

// TestCreate_WithHistoryAndTasks_CommitAtomically covers backlog B-1 +
// B-2: a CreateWorkflowExecution request carrying NewWorkflowNewEvents
// (history-node batch) AND snapshot.Tasks (transfer/timer/etc. tasks)
// commits all three — execution doc, history nodes, history tasks — in
// one transaction. A read-back confirms each surface landed.
func TestCreate_WithHistoryAndTasks_CommitAtomically(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 2, 1)
	if err := executionstore.EnsureHistoryIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if err := executionstore.EnsureTaskIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()

	snap := makeSnapshot("ns-txn", "wf-txn", "run-txn")
	// Attach a transfer task that should commit atomically.
	snap.Tasks = map[txTasks.Category][]persistence.InternalHistoryTask{
		txTasks.CategoryTransfer: {
			{Key: txTasks.NewImmediateKey(500), Blob: blobBytes("txn-transfer-500")},
		},
	}

	// Attach a history node batch.
	branch := &persistencespb.HistoryBranch{TreeId: "tree-txn", BranchId: "branch-txn"}
	treeInfoBlob := blobBytes("tree-info-txn")
	req := &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 2, RangeID: 1,
		Mode:                persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: snap,
		NewWorkflowNewEvents: []*persistence.InternalAppendHistoryNodesRequest{
			{
				BranchInfo: branch,
				TreeInfo:   treeInfoBlob,
				Node: persistence.InternalHistoryNode{
					NodeID:        1,
					TransactionID: 10,
					Events:        blobBytes("events-batch-1"),
				},
				IsNewBranch: true,
			},
		},
	}
	if _, err := s.CreateWorkflowExecution(ctx, req); err != nil {
		t.Fatalf("CreateWorkflowExecution: %v", err)
	}

	// Execution doc: should exist.
	if _, err := s.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID: 2, NamespaceID: "ns-txn", WorkflowID: "wf-txn", RunID: "run-txn",
	}); err != nil {
		t.Errorf("GetWorkflowExecution: %v", err)
	}

	// History node: should exist under the branch.
	histResp, err := s.ReadHistoryBranch(ctx, &persistence.InternalReadHistoryBranchRequest{
		ShardID:   2,
		BranchID:  "branch-txn",
		MinNodeID: 1,
		MaxNodeID: 100,
	})
	if err != nil {
		t.Fatalf("ReadHistoryBranch: %v", err)
	}
	if len(histResp.Nodes) != 1 || histResp.Nodes[0].TransactionID != 10 {
		t.Errorf("expected 1 node txn=10; got %+v", histResp.Nodes)
	}

	// Transfer task: should exist in history_tasks.
	taskResp, err := s.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             2,
		TaskCategory:        txTasks.CategoryTransfer,
		InclusiveMinTaskKey: txTasks.NewImmediateKey(0),
		ExclusiveMaxTaskKey: txTasks.NewImmediateKey(1000),
		BatchSize:           10,
	})
	if err != nil {
		t.Fatalf("GetHistoryTasks: %v", err)
	}
	if len(taskResp.Tasks) != 1 || taskResp.Tasks[0].Key.TaskID != 500 {
		t.Errorf("expected 1 transfer task id=500; got %+v", taskResp.Tasks)
	}

	// Current-execution pointer must still land (UpdateCurrent default).
	if _, err := s.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
		ShardID: 2, NamespaceID: "ns-txn", WorkflowID: "wf-txn",
	}); err != nil {
		t.Errorf("GetCurrentExecution: %v", err)
	}
}
