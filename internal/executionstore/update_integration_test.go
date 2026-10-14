//go:build integration

// Integration tests for UpdateWorkflowExecution + GetWorkflowExecution
// + Set/Delete/List.
package executionstore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
)

// happy path: an UpdateCurrent with the matching DBRecordVersion CAS.
func TestUpdate_WithMatchingDBRecordVersion_Succeeds(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()

	snap := makeSnapshot("ns", "wf", "run-1")
	_, err := s.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: snap,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Update with matching CAS → bump to DBRecordVersion=2.
	// Per upstream (mutable_state_impl.CloseTransactionAsMutation):
	//   Condition       = the stored next_event_id the server expects.
	//   DBRecordVersion = the NEW db_record_version. The plugin CAS
	//                     matches pre-update db_record_version = DBRecordVersion - 1.
	// The snapshot was created with next_event_id=2, db_record_version=1.
	mut := snapshotToMutation(snap)
	mut.Condition = 2       // matches stored next_event_id
	mut.DBRecordVersion = 2 // bump to 2 (pre-update 1 matches)
	mut.NextEventID = 10
	err = s.UpdateWorkflowExecution(ctx, &persistence.InternalUpdateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1,
		Mode:                   persistence.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowMutation: mut,
	})
	if err != nil {
		t.Fatalf("UpdateWorkflowExecution: %v", err)
	}

	// Read-back via GetWorkflowExecution.
	resp, err := s.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "ns", WorkflowID: "wf", RunID: "run-1",
	})
	if err != nil {
		t.Fatalf("GetWorkflowExecution: %v", err)
	}
	if resp.State.NextEventID != 10 {
		t.Errorf("NextEventID not updated: got %d", resp.State.NextEventID)
	}
	if resp.DBRecordVersion != 2 {
		t.Errorf("DBRecordVersion not updated: got %d", resp.DBRecordVersion)
	}
}

// .
func TestUpdate_WithStaleDBRecordVersion_ReturnsWorkflowConditionFailed(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()

	snap := makeSnapshot("ns-s", "wf-s", "run-s")
	_, err := s.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: snap,
	})
	if err != nil {
		t.Fatal(err)
	}

	mut := snapshotToMutation(snap)
	mut.DBRecordVersion = 99
	mut.Condition = 42 // wrong: stored is 1
	err = s.UpdateWorkflowExecution(ctx, &persistence.InternalUpdateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1,
		Mode:                   persistence.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowMutation: mut,
	})
	if err == nil {
		t.Fatal("expected WorkflowConditionFailedError; got nil")
	}
	var wcfe *persistence.WorkflowConditionFailedError
	if !errors.As(err, &wcfe) {
		t.Fatalf("expected *WorkflowConditionFailedError; got %T: %v", err, err)
	}
}

// TestGetWorkflowExecution_NotFound miss path.
func TestGetWorkflowExecution_NotFound(t *testing.T) {
	db := startDB(t)
	s := executionstore.New(db, "cluster0")
	_, err := s.GetWorkflowExecution(context.Background(), &persistence.GetWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "x", WorkflowID: "y", RunID: "z",
	})
	if err == nil {
		t.Fatal("expected NotFound; got nil")
	}
	var nf *serviceerror.NotFound
	if !errors.As(err, &nf) {
		t.Fatalf("expected *NotFound; got %T: %v", err, err)
	}
}

func TestDeleteWorkflowExecution_Idempotent(t *testing.T) {
	db := startDB(t)
	s := executionstore.New(db, "cluster0")
	err := s.DeleteWorkflowExecution(context.Background(), &persistence.DeleteWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "ghost", WorkflowID: "gone", RunID: "poof",
	})
	if err != nil {
		t.Errorf("delete of non-existent execution should be nil; got %v", err)
	}
}

func TestDeleteCurrentWorkflowExecution_MatchingRunID(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()

	_, err := s.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: makeSnapshot("ns-d", "wf-d", "run-d"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Mismatching RunID is a no-op (per spec).
	err = s.DeleteCurrentWorkflowExecution(ctx, &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "ns-d", WorkflowID: "wf-d", RunID: "wrong-run",
	})
	if err != nil {
		t.Errorf("non-matching run RunID delete should be nil; got %v", err)
	}
	// Pointer still exists.
	if _, err := s.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
		ShardID: 1, NamespaceID: "ns-d", WorkflowID: "wf-d",
	}); err != nil {
		t.Errorf("current pointer deleted incorrectly: %v", err)
	}
	// Matching RunID deletes.
	err = s.DeleteCurrentWorkflowExecution(ctx, &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "ns-d", WorkflowID: "wf-d", RunID: "run-d",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
		ShardID: 1, NamespaceID: "ns-d", WorkflowID: "wf-d",
	}); err == nil {
		t.Error("current pointer still exists after matching-RunID delete")
	}
}

func TestListConcreteExecutions_PaginatedWithinShard(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()

	// Create 5 executions across two workflows on shard 1.
	for i := 0; i < 5; i++ {
		snap := makeSnapshot("ns-l", fmt.Sprintf("wf-%d", i), fmt.Sprintf("run-%d", i))
		_, err := s.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
			ShardID: 1, RangeID: 1, Mode: persistence.CreateWorkflowModeBrandNew,
			NewWorkflowSnapshot: snap,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// First page: PageSize=3.
	resp, err := s.ListConcreteExecutions(ctx, &persistence.ListConcreteExecutionsRequest{
		ShardID:  1,
		PageSize: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.States) != 3 {
		t.Errorf("page 1 size: got %d, want 3", len(resp.States))
	}
	if len(resp.NextPageToken) == 0 {
		t.Fatal("expected NextPageToken after first page")
	}

	// Second page: pass token, expect remaining 2.
	resp2, err := s.ListConcreteExecutions(ctx, &persistence.ListConcreteExecutionsRequest{
		ShardID: 1, PageSize: 3, PageToken: resp.NextPageToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp2.States) != 2 {
		t.Errorf("page 2 size: got %d, want 2", len(resp2.States))
	}
	if len(resp2.NextPageToken) != 0 {
		t.Errorf("NextPageToken should be empty after last page; got %q", string(resp2.NextPageToken))
	}
}

// snapshotToMutation converts the helper's snapshot into a Mutation
// suitable for UpdateWorkflowExecution.
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
		LastWriteVersion:   s.LastWriteVersion,
		StartVersion:       s.StartVersion,
	}
}

// TestUpdate_IncrementalActivityInfos covers backlog B-3: the
// UpdateWorkflowExecution path should apply sub-collection mutations
// (upsert + delete) without touching fields the caller didn't mention.
// Seeds a workflow with two activity infos, issues an update that adds
// activity #3 and deletes activity #1, and asserts the surviving set
// matches {#2, #3} plus the pre-existing ExecutionInfo blob (which the
// incremental path must preserve).
func TestUpdate_IncrementalActivityInfos(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()

	snap := makeSnapshot("ns-b3", "wf-b3", "run-b3")
	snap.ActivityInfos = map[int64]*commonpb.DataBlob{
		1: blobBytes("activity-1"),
		2: blobBytes("activity-2"),
	}
	if _, err := s.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: snap,
	}); err != nil {
		t.Fatal(err)
	}

	// Capture the ExecutionInfo blob bytes so we can confirm they are
	// unchanged after the incremental update.
	pre, err := s.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "ns-b3", WorkflowID: "wf-b3", RunID: "run-b3",
	})
	if err != nil {
		t.Fatalf("GetWorkflowExecution pre: %v", err)
	}
	preInfoBytes := append([]byte(nil), pre.State.ExecutionInfo.Data...)

	mut := snapshotToMutation(snap)
	mut.Condition = 2       // stored next_event_id from Create
	mut.DBRecordVersion = 2 // bump to 2 (pre-update 1 matches)
	// Don't re-send ExecutionInfoBlob — the incremental path must keep
	// the pre-existing blob intact.
	mut.ExecutionInfoBlob = nil
	mut.UpsertActivityInfos = map[int64]*commonpb.DataBlob{
		3: blobBytes("activity-3"),
	}
	mut.DeleteActivityInfos = map[int64]struct{}{
		1: {},
	}
	if err := s.UpdateWorkflowExecution(ctx, &persistence.InternalUpdateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1,
		Mode:                   persistence.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowMutation: mut,
	}); err != nil {
		t.Fatalf("UpdateWorkflowExecution: %v", err)
	}

	post, err := s.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "ns-b3", WorkflowID: "wf-b3", RunID: "run-b3",
	})
	if err != nil {
		t.Fatalf("GetWorkflowExecution post: %v", err)
	}

	if _, has := post.State.ActivityInfos[1]; has {
		t.Error("expected activity 1 to be deleted")
	}
	if _, has := post.State.ActivityInfos[2]; !has {
		t.Error("expected activity 2 to survive untouched")
	}
	if _, has := post.State.ActivityInfos[3]; !has {
		t.Error("expected activity 3 to be added")
	}
	if len(post.State.ActivityInfos) != 2 {
		t.Errorf("expected exactly 2 activities; got %d", len(post.State.ActivityInfos))
	}

	// ExecutionInfo blob must be unchanged — the incremental update
	// did not rewrite it.
	if string(post.State.ExecutionInfo.Data) != string(preInfoBytes) {
		t.Errorf("ExecutionInfo blob drifted under incremental update;\n pre=%q\npost=%q",
			preInfoBytes, post.State.ExecutionInfo.Data)
	}

	if post.DBRecordVersion != 2 {
		t.Errorf("DBRecordVersion not bumped: got %d", post.DBRecordVersion)
	}
}
