//go:build integration

// Integration tests for history branches + nodes.
package executionstore_test

import (
	"context"
	"fmt"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
)

func branchInfo(treeID, branchID string) *persistencespb.HistoryBranch {
	return &persistencespb.HistoryBranch{TreeId: treeID, BranchId: branchID}
}

func historyBlob(s string) *commonpb.DataBlob {
	return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte(s)}
}

func appendNode(t *testing.T, s *executionstore.Stub, tree, branch string, nodeID, txnID int64, payload string) {
	t.Helper()
	err := s.AppendHistoryNodes(context.Background(), &persistence.InternalAppendHistoryNodesRequest{
		ShardID:     1,
		BranchInfo:  branchInfo(tree, branch),
		IsNewBranch: nodeID == 1,
		TreeInfo:    historyBlob("tree-info"),
		Node: persistence.InternalHistoryNode{
			NodeID:        nodeID,
			TransactionID: txnID,
			Events:        historyBlob(payload),
		},
	})
	if err != nil {
		t.Fatalf("AppendHistoryNodes nodeID=%d txnID=%d: %v", nodeID, txnID, err)
	}
}

// TestAppendAndRead_ForwardOrder (append)
// (forward read) (index).
func TestAppendAndRead_ForwardOrder(t *testing.T) {
	db := startDB(t)
	s := executionstore.New(db, "cluster0")
	if err := executionstore.EnsureHistoryIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	for i := int64(1); i <= 5; i++ {
		appendNode(t, s, "tree-a", "branch-a", i, i, fmt.Sprintf("events-%d", i))
	}

	resp, err := s.ReadHistoryBranch(context.Background(), &persistence.InternalReadHistoryBranchRequest{
		ShardID:   1,
		BranchID:  "branch-a",
		MinNodeID: 1,
		MaxNodeID: 10,
		PageSize:  10,
	})
	if err != nil {
		t.Fatalf("ReadHistoryBranch: %v", err)
	}
	if len(resp.Nodes) != 5 {
		t.Fatalf("want 5 nodes; got %d", len(resp.Nodes))
	}
	for i, n := range resp.Nodes {
		if n.NodeID != int64(i+1) {
			t.Errorf("node %d out of order: got NodeID=%d", i, n.NodeID)
		}
	}
}

// TestAppend_NodeVersionsCoexist: appends of the same node with
// different transaction IDs
// are kept side by side, returned with the highest transaction first,
// because upstream's history manager picks the valid version by
// following the PrevTransactionID chain — and the valid one is not
// always the newest. Re-appending an existing (node, txn) is a no-op.
func TestAppend_NodeVersionsCoexist(t *testing.T) {
	db := startDB(t)
	s := executionstore.New(db, "cluster0")
	if err := executionstore.EnsureHistoryIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	appendNode(t, s, "tree-b", "branch-b", 1, 10, "first")
	appendNode(t, s, "tree-b", "branch-b", 1, 20, "higher")
	appendNode(t, s, "tree-b", "branch-b", 1, 5, "lower")
	appendNode(t, s, "tree-b", "branch-b", 1, 10, "replayed") // same (node, txn): no-op

	resp, err := s.ReadHistoryBranch(context.Background(), &persistence.InternalReadHistoryBranchRequest{
		ShardID: 1, BranchID: "branch-b", MinNodeID: 1, MaxNodeID: 10, PageSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, n := range resp.Nodes {
		got = append(got, fmt.Sprintf("%d:%s", n.TransactionID, n.Events.Data))
	}
	if fmt.Sprint(got) != "[20:higher 10:first 5:lower]" {
		t.Fatalf("node versions = %v; want [20:higher 10:first 5:lower]", got)
	}
}

// TestDeleteBranch_RemovesBranchAndNodes/061:
// DeleteHistoryBranch removes both the branch doc and all its nodes.
func TestDeleteBranch_RemovesBranchAndNodes(t *testing.T) {
	db := startDB(t)
	s := executionstore.New(db, "cluster0")
	if err := executionstore.EnsureHistoryIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	appendNode(t, s, "tree-c", "branch-c", 1, 1, "e1")
	appendNode(t, s, "tree-c", "branch-c", 2, 2, "e2")

	err := s.DeleteHistoryBranch(context.Background(), &persistence.InternalDeleteHistoryBranchRequest{
		ShardID:    1,
		BranchInfo: branchInfo("tree-c", "branch-c"),
	})
	if err != nil {
		t.Fatalf("DeleteHistoryBranch: %v", err)
	}

	// Nodes gone.
	resp, _ := s.ReadHistoryBranch(context.Background(), &persistence.InternalReadHistoryBranchRequest{
		ShardID: 1, BranchID: "branch-c", MinNodeID: 1, MaxNodeID: 100, PageSize: 10,
	})
	if resp != nil && len(resp.Nodes) != 0 {
		t.Errorf("want 0 nodes after delete; got %d", len(resp.Nodes))
	}
}

// TestForkBranch_NewBranchDocument: fork creates a
// new branch document without copying event data.
func TestForkBranch_NewBranchDocument(t *testing.T) {
	db := startDB(t)
	s := executionstore.New(db, "cluster0")
	if err := executionstore.EnsureHistoryIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	appendNode(t, s, "tree-f", "branch-parent", 1, 1, "e1")

	err := s.ForkHistoryBranch(context.Background(), &persistence.InternalForkHistoryBranchRequest{
		ShardID:        1,
		ForkBranchInfo: branchInfo("tree-f", "branch-parent"),
		NewBranchID:    "branch-child",
		ForkNodeID:     2,
		TreeInfo:       historyBlob("child-tree-info"),
	})
	if err != nil {
		t.Fatalf("ForkHistoryBranch: %v", err)
	}
	// The child branch is queryable via GetHistoryTreeContainingBranch.
	// (That method lands together.)
}

func TestGetHistoryTreeContainingBranch(t *testing.T) {
	db := startDB(t)
	s := executionstore.New(db, "cluster0")
	if err := executionstore.EnsureHistoryIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	// Seed two branches of the same tree.
	appendNode(t, s, "tree-g", "branch-g1", 1, 1, "e1")
	appendNode(t, s, "tree-g", "branch-g2", 1, 1, "e1-g2")

	// Build a branch token matching branch-g1.
	util := s.GetHistoryBranchUtil()
	token, err := util.NewHistoryBranch("ns", "wf", "run", "tree-g", pstr("branch-g1"), nil, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := s.GetHistoryTreeContainingBranch(context.Background(), &persistence.InternalGetHistoryTreeContainingBranchRequest{
		ShardID:     1,
		BranchToken: token,
	})
	if err != nil {
		t.Fatalf("GetHistoryTreeContainingBranch: %v", err)
	}
	if len(resp.TreeInfos) != 2 {
		t.Errorf("want 2 tree infos; got %d", len(resp.TreeInfos))
	}
}

func pstr(s string) *string { return &s }
