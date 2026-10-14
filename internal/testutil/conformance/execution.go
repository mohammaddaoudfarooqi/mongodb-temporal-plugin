package conformance

import (
	"errors"
	"fmt"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	txTasks "go.temporal.io/server/service/history/tasks"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/shardstore"
)

// runExecutionStore exercises the execution store + the atomicity promises
// that B-1/B-2/B-3 closed: history events + tasks commit together with
// the execution doc; incremental updates touch only the fields the
// caller changed.
func runExecutionStore(t *testing.T, f Fixture) {
	seed := func(t *testing.T, shardID int32, rangeID int64) (executionStoreHandle, int32, int64) {
		t.Helper()
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure shard idx", shardstore.EnsureIndexes(tc.Ctx, tc.DB))
		fatalOnErr(t, "ensure exec idx", executionstore.EnsureIndexes(tc.Ctx, tc.DB))
		fatalOnErr(t, "ensure history idx", executionstore.EnsureHistoryIndexes(tc.Ctx, tc.DB))
		fatalOnErr(t, "ensure task idx", executionstore.EnsureTaskIndexes(tc.Ctx, tc.DB))
		shards := shardstore.New(tc.DB, "cluster0")
		_, err := shards.GetOrCreateShard(tc.Ctx, &persistence.InternalGetOrCreateShardRequest{
			ShardID: shardID,
			CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
				return rangeID, dataBlob("init"), nil
			},
		})
		fatalOnErr(t, "seed shard", err)
		return executionStoreHandle{tc: tc, s: executionstore.New(tc.DB, "cluster0")}, shardID, rangeID
	}

	t.Run("Create_BrandNew_Succeeds", func(t *testing.T) {
		h, shardID, rangeID := seed(t, 10, 1)
		snap := makeSnapshot(uniqStr("ns"), "wf", "run-1")
		_, err := h.s.CreateWorkflowExecution(h.tc.Ctx, &persistence.InternalCreateWorkflowExecutionRequest{
			ShardID: shardID, RangeID: rangeID, Mode: persistence.CreateWorkflowModeBrandNew,
			NewWorkflowSnapshot: snap,
		})
		fatalOnErr(t, "create", err)

		resp, err := h.s.GetWorkflowExecution(h.tc.Ctx, &persistence.GetWorkflowExecutionRequest{
			ShardID: shardID, NamespaceID: snap.NamespaceID, WorkflowID: snap.WorkflowID, RunID: snap.RunID,
		})
		fatalOnErr(t, "get", err)
		if resp.DBRecordVersion != 1 {
			t.Errorf("DBRecordVersion drift: got %d", resp.DBRecordVersion)
		}
	})

	t.Run("Create_ShardRangeMismatch_ReturnsShardOwnershipLost", func(t *testing.T) {
		h, shardID, _ := seed(t, 11, 1)
		snap := makeSnapshot(uniqStr("ns"), "wf", "run-1")
		_, err := h.s.CreateWorkflowExecution(h.tc.Ctx, &persistence.InternalCreateWorkflowExecutionRequest{
			ShardID: shardID, RangeID: 999, Mode: persistence.CreateWorkflowModeBrandNew,
			NewWorkflowSnapshot: snap,
		})
		if err == nil {
			t.Fatal("expected shard-ownership error on stale range; got nil")
		}
		var sol *persistence.ShardOwnershipLostError
		if !errors.As(err, &sol) {
			t.Errorf("expected *ShardOwnershipLostError; got %T: %v", err, err)
		}
	})

	t.Run("Create_Duplicate_ReturnsWorkflowConditionFailed", func(t *testing.T) {
		h, shardID, rangeID := seed(t, 12, 1)
		snap := makeSnapshot(uniqStr("ns"), "wf", "run-1")
		req := &persistence.InternalCreateWorkflowExecutionRequest{
			ShardID: shardID, RangeID: rangeID, Mode: persistence.CreateWorkflowModeBrandNew,
			NewWorkflowSnapshot: snap,
		}
		_, err := h.s.CreateWorkflowExecution(h.tc.Ctx, req)
		fatalOnErr(t, "first create", err)

		_, err = h.s.CreateWorkflowExecution(h.tc.Ctx, req)
		if err == nil {
			t.Fatal("expected duplicate-create to fail; got nil")
		}
		var wcfe *persistence.CurrentWorkflowConditionFailedError
		var wcf *persistence.WorkflowConditionFailedError
		if !errors.As(err, &wcfe) && !errors.As(err, &wcf) {
			t.Errorf("expected condition-failed; got %T: %v", err, err)
		}
	})

	t.Run("Update_IncrementalSubCollectionMutations", func(t *testing.T) {
		// Backlog B-3 regression guard: upsert + delete on activity_infos
		// must leave adjacent fields (ExecutionInfo) untouched.
		h, shardID, rangeID := seed(t, 13, 1)
		snap := makeSnapshot(uniqStr("ns"), "wf", "run-1")
		snap.ActivityInfos = map[int64]*commonpb.DataBlob{
			1: dataBlob("activity-1"),
			2: dataBlob("activity-2"),
		}
		_, err := h.s.CreateWorkflowExecution(h.tc.Ctx, &persistence.InternalCreateWorkflowExecutionRequest{
			ShardID: shardID, RangeID: rangeID, Mode: persistence.CreateWorkflowModeBrandNew,
			NewWorkflowSnapshot: snap,
		})
		fatalOnErr(t, "create", err)

		pre, err := h.s.GetWorkflowExecution(h.tc.Ctx, &persistence.GetWorkflowExecutionRequest{
			ShardID: shardID, NamespaceID: snap.NamespaceID, WorkflowID: snap.WorkflowID, RunID: snap.RunID,
		})
		fatalOnErr(t, "get pre", err)
		preExec := append([]byte(nil), pre.State.ExecutionInfo.Data...)

		mut := snapshotToMutation(snap)
		// CAS: Condition = stored next_event_id (from Create; snapshot
		// sets NextEventID=2); DBRecordVersion = new version (pre-update
		// must be DBRecordVersion - 1, matching stored 1).
		mut.Condition = 2
		mut.DBRecordVersion = 2
		mut.ExecutionInfoBlob = nil // don't touch the blob
		mut.UpsertActivityInfos = map[int64]*commonpb.DataBlob{3: dataBlob("activity-3")}
		mut.DeleteActivityInfos = map[int64]struct{}{1: {}}

		err = h.s.UpdateWorkflowExecution(h.tc.Ctx, &persistence.InternalUpdateWorkflowExecutionRequest{
			ShardID: shardID, RangeID: rangeID,
			Mode:                   persistence.UpdateWorkflowModeUpdateCurrent,
			UpdateWorkflowMutation: mut,
		})
		fatalOnErr(t, "update", err)

		post, err := h.s.GetWorkflowExecution(h.tc.Ctx, &persistence.GetWorkflowExecutionRequest{
			ShardID: shardID, NamespaceID: snap.NamespaceID, WorkflowID: snap.WorkflowID, RunID: snap.RunID,
		})
		fatalOnErr(t, "get post", err)

		if _, ok := post.State.ActivityInfos[1]; ok {
			t.Error("activity 1 should have been deleted")
		}
		if _, ok := post.State.ActivityInfos[3]; !ok {
			t.Error("activity 3 should have been added")
		}
		if string(post.State.ExecutionInfo.Data) != string(preExec) {
			t.Errorf("ExecutionInfo blob drifted under incremental update:\n pre=%q\npost=%q",
				preExec, post.State.ExecutionInfo.Data)
		}
		if post.DBRecordVersion != 2 {
			t.Errorf("DBRecordVersion not bumped: %d", post.DBRecordVersion)
		}
	})

	t.Run("Update_StaleCAS_ReturnsWorkflowConditionFailed", func(t *testing.T) {
		h, shardID, rangeID := seed(t, 14, 1)
		snap := makeSnapshot(uniqStr("ns"), "wf", "run-1")
		_, err := h.s.CreateWorkflowExecution(h.tc.Ctx, &persistence.InternalCreateWorkflowExecutionRequest{
			ShardID: shardID, RangeID: rangeID, Mode: persistence.CreateWorkflowModeBrandNew,
			NewWorkflowSnapshot: snap,
		})
		fatalOnErr(t, "create", err)

		mut := snapshotToMutation(snap)
		mut.DBRecordVersion = 99
		mut.Condition = 42 // wrong
		err = h.s.UpdateWorkflowExecution(h.tc.Ctx, &persistence.InternalUpdateWorkflowExecutionRequest{
			ShardID: shardID, RangeID: rangeID,
			Mode:                   persistence.UpdateWorkflowModeUpdateCurrent,
			UpdateWorkflowMutation: mut,
		})
		if err == nil {
			t.Fatal("expected WorkflowConditionFailedError; got nil")
		}
		var wcfe *persistence.WorkflowConditionFailedError
		if !errors.As(err, &wcfe) {
			t.Errorf("expected *WorkflowConditionFailedError; got %T: %v", err, err)
		}
	})

	t.Run("Create_WithHistoryAndTasks_CommitAtomically", func(t *testing.T) {
		// B-1 + B-2 regression: events + tasks must land inside the
		// same transaction as the execution doc.
		h, shardID, rangeID := seed(t, 15, 1)
		snap := makeSnapshot(uniqStr("ns"), "wf", "run-1")
		snap.Tasks = map[txTasks.Category][]persistence.InternalHistoryTask{
			txTasks.CategoryTransfer: {
				{Key: txTasks.NewImmediateKey(500), Blob: dataBlob("txn-transfer-500")},
			},
		}
		branch := &persistencespb.HistoryBranch{TreeId: uniqStr("tree"), BranchId: uniqStr("branch")}
		_, err := h.s.CreateWorkflowExecution(h.tc.Ctx, &persistence.InternalCreateWorkflowExecutionRequest{
			ShardID: shardID, RangeID: rangeID, Mode: persistence.CreateWorkflowModeBrandNew,
			NewWorkflowSnapshot: snap,
			NewWorkflowNewEvents: []*persistence.InternalAppendHistoryNodesRequest{
				{
					BranchInfo: branch,
					TreeInfo:   dataBlob("tree-info"),
					Node: persistence.InternalHistoryNode{
						NodeID: 1, TransactionID: 10, Events: dataBlob("events-1"),
					},
					IsNewBranch: true,
				},
			},
		})
		fatalOnErr(t, "create with events+tasks", err)

		// History node readable.
		hist, err := h.s.ReadHistoryBranch(h.tc.Ctx, &persistence.InternalReadHistoryBranchRequest{
			ShardID: shardID, BranchID: branch.BranchId, MinNodeID: 1, MaxNodeID: 100,
		})
		fatalOnErr(t, "read history", err)
		if len(hist.Nodes) != 1 || hist.Nodes[0].TransactionID != 10 {
			t.Errorf("expected 1 node txn=10; got %+v", hist.Nodes)
		}

		// Task readable in the same shard+category.
		taskResp, err := h.s.GetHistoryTasks(h.tc.Ctx, &persistence.GetHistoryTasksRequest{
			ShardID:             shardID,
			TaskCategory:        txTasks.CategoryTransfer,
			InclusiveMinTaskKey: txTasks.NewImmediateKey(0),
			ExclusiveMaxTaskKey: txTasks.NewImmediateKey(1000),
			BatchSize:           10,
		})
		fatalOnErr(t, "get tasks", err)
		if len(taskResp.Tasks) != 1 || taskResp.Tasks[0].Key.TaskID != 500 {
			t.Errorf("expected 1 transfer task id=500; got %+v", taskResp.Tasks)
		}
	})

	// Previously the suite covered Create / Update / atomicity.
	// The scenarios below fill in the remaining public surface.

	t.Run("GetCurrentExecution_ReturnsLatestRun", func(t *testing.T) {
		h, shardID, rangeID := seed(t, 17, 1)
		snap := makeSnapshot(uniqStr("ns"), "wf", "run-1")
		_, err := h.s.CreateWorkflowExecution(h.tc.Ctx, &persistence.InternalCreateWorkflowExecutionRequest{
			ShardID: shardID, RangeID: rangeID, Mode: persistence.CreateWorkflowModeBrandNew,
			NewWorkflowSnapshot: snap,
		})
		fatalOnErr(t, "create", err)

		resp, err := h.s.GetCurrentExecution(h.tc.Ctx, &persistence.GetCurrentExecutionRequest{
			ShardID: shardID, NamespaceID: snap.NamespaceID, WorkflowID: snap.WorkflowID,
		})
		fatalOnErr(t, "get current", err)
		if resp.RunID != "run-1" {
			t.Errorf("RunID drift: got %q want run-1", resp.RunID)
		}
	})

	t.Run("GetCurrentExecution_MissingWorkflow_ReturnsError", func(t *testing.T) {
		h, shardID, _ := seed(t, 18, 1)
		_, err := h.s.GetCurrentExecution(h.tc.Ctx, &persistence.GetCurrentExecutionRequest{
			ShardID: shardID, NamespaceID: uniqStr("ns"), WorkflowID: "wf-missing",
		})
		if err == nil {
			t.Fatal("expected error on missing current; got nil")
		}
	})

	t.Run("SetWorkflowExecution_ReplacesDoc", func(t *testing.T) {
		// Set is the admin-reset path: stamps a fresh snapshot over an
		// existing doc under the snapshot CAS. Seed with Create first,
		// then exercise Set with a mutated ExecutionInfoBlob so the
		// follow-up read confirms Set actually overwrote.
		h, shardID, rangeID := seed(t, 19, 1)
		snap := makeSnapshot(uniqStr("ns"), "wf", "run-set")
		_, err := h.s.CreateWorkflowExecution(h.tc.Ctx, &persistence.InternalCreateWorkflowExecutionRequest{
			ShardID: shardID, RangeID: rangeID, Mode: persistence.CreateWorkflowModeBrandNew,
			NewWorkflowSnapshot: snap,
		})
		fatalOnErr(t, "seed before Set", err)

		// Mutate the snapshot and call Set — the doc must now carry
		// the new ExecutionInfo bytes. Set is CAS-guarded like every
		// snapshot write: send the bumped version.
		snap.ExecutionInfoBlob = dataBlob("info-after-set")
		snap.DBRecordVersion++
		err = h.s.SetWorkflowExecution(h.tc.Ctx, &persistence.InternalSetWorkflowExecutionRequest{
			ShardID: shardID, RangeID: rangeID,
			SetWorkflowSnapshot: snap,
		})
		fatalOnErr(t, "SetWorkflowExecution", err)

		resp, err := h.s.GetWorkflowExecution(h.tc.Ctx, &persistence.GetWorkflowExecutionRequest{
			ShardID: shardID, NamespaceID: snap.NamespaceID,
			WorkflowID: snap.WorkflowID, RunID: snap.RunID,
		})
		fatalOnErr(t, "get after set", err)
		// State is blob-typed; byte-equality on the state blob isn't
		// guaranteed after the serializer round-trip. Compare the
		// ExecutionInfo blob bytes instead — that IS invariant.
		if resp.State == nil || resp.State.ExecutionInfo == nil {
			t.Fatal("SetWorkflowExecution did not leave a readable state")
		}
		if string(resp.State.ExecutionInfo.Data) != string(snap.ExecutionInfoBlob.Data) {
			t.Errorf("ExecutionInfo blob drift after Set")
		}
	})

	t.Run("ListConcreteExecutions_ReturnsShardExecutions", func(t *testing.T) {
		h, shardID, rangeID := seed(t, 20, 1)
		for i := 0; i < 3; i++ {
			snap := makeSnapshot(uniqStr("ns-list"), "wf", uniqStr("run"))
			_, err := h.s.CreateWorkflowExecution(h.tc.Ctx, &persistence.InternalCreateWorkflowExecutionRequest{
				ShardID: shardID, RangeID: rangeID, Mode: persistence.CreateWorkflowModeBrandNew,
				NewWorkflowSnapshot: snap,
			})
			fatalOnErr(t, "create", err)
		}
		resp, err := h.s.ListConcreteExecutions(h.tc.Ctx, &persistence.ListConcreteExecutionsRequest{
			ShardID: shardID, PageSize: 10,
		})
		fatalOnErr(t, "list concrete", err)
		if len(resp.States) < 3 {
			t.Errorf("expected >=3 executions on shard %d; got %d", shardID, len(resp.States))
		}
	})

	t.Run("ConflictResolve_UpdateCurrent_ResetsThenPointsAtNewRun", func(t *testing.T) {
		// Reset the original run; concurrently start a fresh run and
		// point the current-execution pointer at it.
		h, shardID, rangeID := seed(t, 22, 1)
		reset := makeSnapshot(uniqStr("ns"), "wf-cr", "run-orig")
		_, err := h.s.CreateWorkflowExecution(h.tc.Ctx, &persistence.InternalCreateWorkflowExecutionRequest{
			ShardID: shardID, RangeID: rangeID, Mode: persistence.CreateWorkflowModeBrandNew,
			NewWorkflowSnapshot: reset,
		})
		fatalOnErr(t, "seed original", err)

		resetMut := reset
		resetMut.DBRecordVersion = 2
		resetMut.Condition = 1
		newRun := makeSnapshot(reset.NamespaceID, reset.WorkflowID, "run-new")
		err = h.s.ConflictResolveWorkflowExecution(h.tc.Ctx, &persistence.InternalConflictResolveWorkflowExecutionRequest{
			ShardID: shardID, RangeID: rangeID,
			Mode:                  persistence.ConflictResolveWorkflowModeUpdateCurrent,
			ResetWorkflowSnapshot: resetMut,
			NewWorkflowSnapshot:   &newRun,
		})
		fatalOnErr(t, "conflict resolve", err)

		cur, err := h.s.GetCurrentExecution(h.tc.Ctx, &persistence.GetCurrentExecutionRequest{
			ShardID: shardID, NamespaceID: reset.NamespaceID, WorkflowID: reset.WorkflowID,
		})
		fatalOnErr(t, "get current after resolve", err)
		if cur.RunID != "run-new" {
			t.Errorf("current run after ConflictResolve: got %q want run-new", cur.RunID)
		}
	})

	t.Run("Delete_Idempotent", func(t *testing.T) {
		h, shardID, rangeID := seed(t, 16, 1)
		snap := makeSnapshot(uniqStr("ns"), "wf", "run-1")
		_, err := h.s.CreateWorkflowExecution(h.tc.Ctx, &persistence.InternalCreateWorkflowExecutionRequest{
			ShardID: shardID, RangeID: rangeID, Mode: persistence.CreateWorkflowModeBrandNew,
			NewWorkflowSnapshot: snap,
		})
		fatalOnErr(t, "create", err)

		req := &persistence.DeleteWorkflowExecutionRequest{
			ShardID: shardID, NamespaceID: snap.NamespaceID, WorkflowID: snap.WorkflowID, RunID: snap.RunID,
		}
		if err := h.s.DeleteWorkflowExecution(h.tc.Ctx, req); err != nil {
			t.Errorf("first delete: %v", err)
		}
		if err := h.s.DeleteWorkflowExecution(h.tc.Ctx, req); err != nil {
			t.Errorf("idempotent re-delete: %v", err)
		}

		// Follow-up Get now returns NotFound.
		_, err = h.s.GetWorkflowExecution(h.tc.Ctx, &persistence.GetWorkflowExecutionRequest{
			ShardID: shardID, NamespaceID: snap.NamespaceID, WorkflowID: snap.WorkflowID, RunID: snap.RunID,
		})
		if err == nil {
			t.Fatal("expected NotFound after delete; got nil")
		}
		var nf *serviceerror.NotFound
		if !errors.As(err, &nf) {
			t.Errorf("expected *NotFound; got %T: %v", err, err)
		}
	})

	// Keep the import bound tight for future scenario additions.
	_ = fmt.Sprintf
	_ = enumspb.ENCODING_TYPE_PROTO3
}

// executionStoreHandle bundles the per-subtest context + store handle.
type executionStoreHandle struct {
	tc TestContext
	s  *executionstore.Stub
}

// snapshotToMutation mirrors the handful of fields the suite needs to
// carry forward from the create snapshot onto the update mutation.
// Kept local so the helper file doesn't gain an execution-specific
// dependency.
func snapshotToMutation(s persistence.InternalWorkflowSnapshot) persistence.InternalWorkflowMutation {
	return persistence.InternalWorkflowMutation{
		NamespaceID:        s.NamespaceID,
		WorkflowID:         s.WorkflowID,
		RunID:              s.RunID,
		ExecutionInfoBlob:  s.ExecutionInfoBlob,
		ExecutionStateBlob: s.ExecutionStateBlob,
		ExecutionState:     s.ExecutionState,
		NextEventID:        s.NextEventID,
		DBRecordVersion:    s.DBRecordVersion,
		Condition:          s.DBRecordVersion,
		LastWriteVersion:   s.LastWriteVersion,
		StartVersion:       s.StartVersion,
	}
}
