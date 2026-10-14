//go:build integration

// Integration tests for ConflictResolveWorkflowExecution.
package executionstore_test

import (
	"context"
	"testing"

	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
)

// TestConflictResolve_ResetExisting_NoNew simplest
// case: reset the workflow's mutable state in place, no new workflow.
func TestConflictResolve_ResetExisting_NoNew(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()

	orig := makeSnapshot("ns", "wf", "run-1")
	_, err := s.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: orig,
	})
	if err != nil {
		t.Fatal(err)
	}

	reset := makeSnapshot("ns", "wf", "run-1") // same run, reset state
	reset.NextEventID = 100
	// Upstream sends the bumped version; the store CASes on version-1.
	reset.DBRecordVersion = 2
	// run-1 is the current run, so the upstream contract requires
	// UpdateCurrent (BypassCurrent forbids the pointer naming the reset run).
	err = s.ConflictResolveWorkflowExecution(ctx, &persistence.InternalConflictResolveWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1,
		Mode:                  persistence.ConflictResolveWorkflowModeUpdateCurrent,
		ResetWorkflowSnapshot: reset,
	})
	if err != nil {
		t.Fatalf("ConflictResolveWorkflowExecution: %v", err)
	}

	got, err := s.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "ns", WorkflowID: "wf", RunID: "run-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.State.NextEventID != 100 {
		t.Errorf("reset did not persist NextEventID: got %d", got.State.NextEventID)
	}
}

// TestConflictResolve_WithNewWorkflow with a new
// workflow inserted alongside the reset.
func TestConflictResolve_WithNewWorkflow(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()

	_, err := s.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: makeSnapshot("ns", "wf2", "run-a"),
	})
	if err != nil {
		t.Fatal(err)
	}

	reset := makeSnapshot("ns", "wf2", "run-a")
	reset.DBRecordVersion = 2 // bumped version; CAS expects stored 1
	newWF := makeSnapshot("ns", "wf2", "run-b")
	err = s.ConflictResolveWorkflowExecution(ctx, &persistence.InternalConflictResolveWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1,
		Mode:                  persistence.ConflictResolveWorkflowModeUpdateCurrent,
		ResetWorkflowSnapshot: reset,
		NewWorkflowSnapshot:   &newWF,
	})
	if err != nil {
		t.Fatalf("ConflictResolveWorkflowExecution with new wf: %v", err)
	}

	// Both executions exist.
	if _, err := s.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "ns", WorkflowID: "wf2", RunID: "run-a",
	}); err != nil {
		t.Errorf("reset workflow missing: %v", err)
	}
	if _, err := s.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "ns", WorkflowID: "wf2", RunID: "run-b",
	}); err != nil {
		t.Errorf("new workflow missing: %v", err)
	}
	// Current pointer points at run-b.
	curr, err := s.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
		ShardID: 1, NamespaceID: "ns", WorkflowID: "wf2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if curr.RunID != "run-b" {
		t.Errorf("current pointer: got %q, want run-b", curr.RunID)
	}
}
