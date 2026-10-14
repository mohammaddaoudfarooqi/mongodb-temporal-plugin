//go:build integration

// Execution-mutation regression tests.
package executionstore_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	txTasks "go.temporal.io/server/service/history/tasks"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/shardstore"
)

func mustCreate(t *testing.T, s *executionstore.Stub, mode persistence.CreateWorkflowMode, snap persistence.InternalWorkflowSnapshot) {
	t.Helper()
	if _, err := s.CreateWorkflowExecution(context.Background(), &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: mode, NewWorkflowSnapshot: snap,
	}); err != nil {
		t.Fatalf("create %s: %v", snap.RunID, err)
	}
}

// nextMutation returns a mutation of snap that passes the update CAS
// against a freshly created execution (next_event_id=2, version=1).
func nextMutation(snap persistence.InternalWorkflowSnapshot) persistence.InternalWorkflowMutation {
	mut := snapshotToMutation(snap)
	mut.Condition = 2
	mut.DBRecordVersion = 2
	mut.NextEventID = 5
	return mut
}

func currentRun(t *testing.T, s *executionstore.Stub, ns, wf string) string {
	t.Helper()
	cur, err := s.GetCurrentExecution(context.Background(), &persistence.GetCurrentExecutionRequest{
		ShardID: 1, NamespaceID: ns, WorkflowID: wf,
	})
	if err != nil {
		t.Fatalf("GetCurrentExecution: %v", err)
	}
	return cur.RunID
}

func requireCurrentConditionFailed(t *testing.T, err error) {
	t.Helper()
	var cf *persistence.CurrentWorkflowConditionFailedError
	if !errors.As(err, &cf) {
		t.Fatalf("want *CurrentWorkflowConditionFailedError; got %T %v", err, err)
	}
}

func TestUpdate_ContinueAsNew_CurrentPointsToNewRun(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	old := makeSnapshot("ns", "wf-can", "old")
	mustCreate(t, s, persistence.CreateWorkflowModeBrandNew, old)

	newRun := makeSnapshot("ns", "wf-can", "new")
	err := s.UpdateWorkflowExecution(context.Background(), &persistence.InternalUpdateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowMutation: nextMutation(old), NewWorkflowSnapshot: &newRun,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := currentRun(t, s, "ns", "wf-can"); got != "new" {
		t.Fatalf("current run after continue-as-new = %q; want new", got)
	}
}

func TestUpdate_UpdateCurrentOnNonCurrentRun_CurrentConditionFailed(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	cur := makeSnapshot("ns", "wf-nc", "current")
	zombie := makeSnapshot("ns", "wf-nc", "zombie")
	mustCreate(t, s, persistence.CreateWorkflowModeBrandNew, cur)
	mustCreate(t, s, persistence.CreateWorkflowModeBypassCurrent, zombie)

	err := s.UpdateWorkflowExecution(context.Background(), &persistence.InternalUpdateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowMutation: nextMutation(zombie),
	})
	requireCurrentConditionFailed(t, err)
	if got := currentRun(t, s, "ns", "wf-nc"); got != "current" {
		t.Fatalf("pointer moved to %q", got)
	}
}

func TestUpdate_BypassCurrentOnCurrentRun_CurrentConditionFailed(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	cur := makeSnapshot("ns", "wf-bp", "current")
	mustCreate(t, s, persistence.CreateWorkflowModeBrandNew, cur)

	err := s.UpdateWorkflowExecution(context.Background(), &persistence.InternalUpdateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.UpdateWorkflowModeBypassCurrent,
		UpdateWorkflowMutation: nextMutation(cur),
	})
	requireCurrentConditionFailed(t, err)
}

func TestConflictResolve_CurrentMutation_UntouchedActivitySurvives(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()

	cur := makeSnapshot("ns", "wf-cr", "run-a")
	cur.ActivityInfos = map[int64]*commonpb.DataBlob{1: blobBytes("act-1"), 2: blobBytes("act-2")}
	mustCreate(t, s, persistence.CreateWorkflowModeBrandNew, cur)
	reset := makeSnapshot("ns", "wf-cr", "run-b")
	mustCreate(t, s, persistence.CreateWorkflowModeBypassCurrent, reset)

	resetSnap := reset
	resetSnap.DBRecordVersion = 2
	cm := nextMutation(cur)
	cm.UpsertActivityInfos = map[int64]*commonpb.DataBlob{3: blobBytes("act-3")}
	err := s.ConflictResolveWorkflowExecution(ctx, &persistence.InternalConflictResolveWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.ConflictResolveWorkflowModeUpdateCurrent,
		ResetWorkflowSnapshot: resetSnap, CurrentWorkflowMutation: &cm,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "ns", WorkflowID: "wf-cr", RunID: "run-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.State.ActivityInfos) != 3 {
		t.Fatalf("activities after conflict resolve = %d; want 3 (1,2 untouched + 3 upserted)", len(got.State.ActivityInfos))
	}
	if string(got.State.ExecutionInfo.Data) == "" {
		t.Fatal("execution info lost")
	}
	if run := currentRun(t, s, "ns", "wf-cr"); run != "run-b" {
		t.Fatalf("current = %q; want run-b", run)
	}
}

func TestConflictResolve_StaleReset_WorkflowConditionFailed(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()
	orig := makeSnapshot("ns", "wf-sr", "run-1")
	mustCreate(t, s, persistence.CreateWorkflowModeBrandNew, orig)

	stale := orig
	stale.DBRecordVersion = 5 // expects stored version 4; stored is 1
	stale.NextEventID = 99
	err := s.ConflictResolveWorkflowExecution(ctx, &persistence.InternalConflictResolveWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.ConflictResolveWorkflowModeUpdateCurrent,
		ResetWorkflowSnapshot: stale,
	})
	var wcf *persistence.WorkflowConditionFailedError
	if !errors.As(err, &wcf) {
		t.Fatalf("want WorkflowConditionFailedError; got %T %v", err, err)
	}
	got, err := s.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "ns", WorkflowID: "wf-sr", RunID: "run-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.State.NextEventID != 2 {
		t.Fatalf("stale reset was written: next_event_id=%d", got.State.NextEventID)
	}
}

func TestConflictResolve_UpdateCurrentWrongRun_CurrentConditionFailed(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	cur := makeSnapshot("ns", "wf-cw", "current")
	other := makeSnapshot("ns", "wf-cw", "other")
	mustCreate(t, s, persistence.CreateWorkflowModeBrandNew, cur)
	mustCreate(t, s, persistence.CreateWorkflowModeBypassCurrent, other)

	reset := other
	reset.DBRecordVersion = 2
	err := s.ConflictResolveWorkflowExecution(context.Background(), &persistence.InternalConflictResolveWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.ConflictResolveWorkflowModeUpdateCurrent,
		ResetWorkflowSnapshot: reset,
	})
	requireCurrentConditionFailed(t, err)
}

// TestUpdate_ShardStolenMidTxn_ShardOwnershipLost. The
// update transaction is paused after its shard check; meanwhile another
// owner takes the shard. The stale owner must not commit after the new
// owner's UpdateShard has returned.
func TestUpdate_ShardStolenMidTxn_ShardOwnershipLost(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	shards := shardstore.New(db, "cluster0")
	ctx := context.Background()
	snap := makeSnapshot("ns", "wf-steal", "run-1")
	mustCreate(t, s, persistence.CreateWorkflowModeBrandNew, snap)

	mut := nextMutation(snap)
	mut.Tasks = map[txTasks.Category][]persistence.InternalHistoryTask{
		txTasks.CategoryTransfer: {{Key: txTasks.NewImmediateKey(900), Blob: blobBytes("t")}},
	}
	cmd := bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: bson.D{{Key: "times", Value: 1}}},
		{Key: "data", Value: bson.D{
			{Key: "failCommands", Value: bson.A{"insert"}},
			{Key: "blockConnection", Value: true},
			{Key: "blockTimeMS", Value: 2500},
		}},
	}
	if err := db.Client().Database("admin").RunCommand(ctx, cmd).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Client().Database("admin").RunCommand(ctx, bson.D{
			{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: "off"}}).Err()
	})

	var wg sync.WaitGroup
	var errA error
	var doneA time.Time
	wg.Add(1)
	go func() {
		defer wg.Done()
		errA = s.UpdateWorkflowExecution(ctx, &persistence.InternalUpdateWorkflowExecutionRequest{
			ShardID: 1, RangeID: 1, Mode: persistence.UpdateWorkflowModeUpdateCurrent,
			UpdateWorkflowMutation: mut,
		})
		doneA = time.Now()
	}()
	time.Sleep(700 * time.Millisecond) // A is now blocked inside its transaction
	if err := shards.UpdateShard(ctx, &persistence.InternalUpdateShardRequest{
		ShardID: 1, RangeID: 2, PreviousRangeID: 1, ShardInfo: blobBytes("stolen"),
	}); err != nil {
		t.Fatalf("UpdateShard: %v", err)
	}
	doneB := time.Now()
	wg.Wait()

	if errA == nil && doneA.After(doneB) {
		t.Fatalf("stale owner committed %v after the shard was taken over", doneA.Sub(doneB))
	}
	if errA != nil {
		var lost *persistence.ShardOwnershipLostError
		if !errors.As(errA, &lost) {
			t.Fatalf("stale owner failed with %T %v; want ShardOwnershipLostError", errA, errA)
		}
	}
}

func TestUpdate_OverSizeBudget_TransactionSizeLimit(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()
	snap := makeSnapshot("ns", "wf-big", "run-1")
	snap.ActivityInfos = map[int64]*commonpb.DataBlob{1: {EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: bytes.Repeat([]byte("a"), 7<<20)}}
	mustCreate(t, s, persistence.CreateWorkflowModeBrandNew, snap)

	mut := nextMutation(snap)
	mut.UpsertActivityInfos = map[int64]*commonpb.DataBlob{2: {EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: bytes.Repeat([]byte("b"), 6<<20)}}
	err := s.UpdateWorkflowExecution(ctx, &persistence.InternalUpdateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowMutation: mut,
	})
	var tooBig *persistence.TransactionSizeLimitError
	if !errors.As(err, &tooBig) {
		t.Fatalf("want TransactionSizeLimitError; got %T %v", err, err)
	}
	got, err := s.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "ns", WorkflowID: "wf-big", RunID: "run-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.State.ActivityInfos) != 1 || got.State.NextEventID != 2 {
		t.Fatal("oversized update was partially written")
	}
}

func TestUpdate_DollarPrefixedTimerID_StoredVerbatim(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()
	snap := makeSnapshot("ns", "wf-dollar", "run-1")
	mustCreate(t, s, persistence.CreateWorkflowModeBrandNew, snap)

	mut := nextMutation(snap)
	mut.UpsertTimerInfos = map[string]*commonpb.DataBlob{"$timer": blobBytes("t1"), "$$NOW": blobBytes("t2")}
	mut.UpsertSignalRequestedIDs = map[string]struct{}{"$signal": {}}
	if err := s.UpdateWorkflowExecution(ctx, &persistence.InternalUpdateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.UpdateWorkflowModeUpdateCurrent, UpdateWorkflowMutation: mut,
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "ns", WorkflowID: "wf-dollar", RunID: "run-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.State.TimerInfos["$timer"] == nil || got.State.TimerInfos["$$NOW"] == nil {
		t.Fatalf("timer keys not stored verbatim: %v", keys(got.State.TimerInfos))
	}
	if len(got.State.SignalRequestedIDs) != 1 || got.State.SignalRequestedIDs[0] != "$signal" {
		t.Fatalf("signal requested ids = %v", got.State.SignalRequestedIDs)
	}
}

func keys(m map[string]*commonpb.DataBlob) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestInsertTasks_DuplicateInTxn_ReturnsError(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	req := &persistence.InternalAddHistoryTasksRequest{
		ShardID: 1, RangeID: 1, NamespaceID: "ns", WorkflowID: "wf",
		Tasks: map[txTasks.Category][]persistence.InternalHistoryTask{
			txTasks.CategoryTransfer: {{Key: txTasks.NewImmediateKey(77), Blob: blobBytes("t")}},
		},
	}
	if err := s.AddHistoryTasks(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err := s.AddHistoryTasks(context.Background(), req)
	if err == nil {
		t.Fatal("duplicate task ID was silently accepted")
	}
	// The duplicate must fail fast and non-retryably, not spin in the
	// driver's transient-retry loop until the transaction times out.
	var unavailable *serviceerror.Unavailable
	if errors.As(err, &unavailable) || time.Since(start) > 10*time.Second {
		t.Fatalf("duplicate surfaced as retryable after %v: %T %v", time.Since(start), err, err)
	}
}

func TestCreate_UpdateCurrentPreviousRunning_CurrentConditionFailed(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	prev := makeSnapshot("ns", "wf-run", "run-1") // RUNNING
	mustCreate(t, s, persistence.CreateWorkflowModeBrandNew, prev)

	_, err := s.CreateWorkflowExecution(context.Background(), &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.CreateWorkflowModeUpdateCurrent,
		PreviousRunID: "run-1", PreviousLastWriteVersion: 1,
		NewWorkflowSnapshot: makeSnapshot("ns", "wf-run", "run-2"),
	})
	requireCurrentConditionFailed(t, err)
}
