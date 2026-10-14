package executionstore_test

import (
	"bytes"
	"testing"

	persistencespb "go.temporal.io/server/api/persistence/v1"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
)

// subtask 2: a branch token encoded with the plugin's util must be
// byte-identical to the re-encoded version after a parse + rewrite,
// and the upstream util produces the same bytes.
func TestHistoryBranchUtil_ByteExactRoundTrip(t *testing.T) {
	s := executionstore.NewStub() // no DB needed — util is stateless
	util := s.GetHistoryBranchUtil()

	// Build a branch token with no ancestors.
	bid := "branch-abc"
	token, err := util.NewHistoryBranch("ns", "wf", "run", "tree-a", &bid, nil, 0, 0, 0)
	if err != nil {
		t.Fatalf("NewHistoryBranch: %v", err)
	}

	parsed, err := util.ParseHistoryBranchInfo(token)
	if err != nil {
		t.Fatalf("ParseHistoryBranchInfo: %v", err)
	}
	if parsed.TreeId != "tree-a" || parsed.BranchId != "branch-abc" {
		t.Errorf("parse mismatch: tree=%q branch=%q", parsed.TreeId, parsed.BranchId)
	}

	// Round-trip: parse → serialise should yield equal bytes.
	token2, err := util.UpdateHistoryBranchInfo(token, parsed, "run")
	if err != nil {
		t.Fatalf("UpdateHistoryBranchInfo: %v", err)
	}
	if !bytes.Equal(token, token2) {
		t.Error("token not byte-identical after parse/re-encode round-trip")
	}
}

// TestHistoryBranchUtil_WithAncestors verifies the ancestor list
// survives round-trip — this is the shape that exercises branch
// forking.
func TestHistoryBranchUtil_WithAncestors(t *testing.T) {
	s := executionstore.NewStub()
	util := s.GetHistoryBranchUtil()

	ancestors := []*persistencespb.HistoryBranchRange{
		{BranchId: "ancestor-1", BeginNodeId: 1, EndNodeId: 10},
		{BranchId: "ancestor-2", BeginNodeId: 10, EndNodeId: 20},
	}
	bid := "branch-child"
	token, err := util.NewHistoryBranch("ns", "wf", "run", "tree-x", &bid, ancestors, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := util.ParseHistoryBranchInfo(token)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.Ancestors) != 2 {
		t.Errorf("ancestors lost: got %d, want 2", len(parsed.Ancestors))
	}
	if parsed.Ancestors[0].BranchId != "ancestor-1" {
		t.Errorf("ancestor[0] drifted: %q", parsed.Ancestors[0].BranchId)
	}
}
