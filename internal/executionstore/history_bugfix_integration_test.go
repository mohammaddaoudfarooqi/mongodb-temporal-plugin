//go:build integration

// History-store regression tests.
package executionstore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
)

func newHistoryStore(t *testing.T) (*executionstore.Stub, any) {
	t.Helper()
	db := startDB(t)
	if err := executionstore.EnsureHistoryIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	s := executionstore.New(db, "cluster0")
	return s, nil
}

func branchToken(t *testing.T, s *executionstore.Stub, tree, branch string, ancestors []*persistencespb.HistoryBranchRange) []byte {
	t.Helper()
	tok, err := s.GetHistoryBranchUtil().NewHistoryBranch("ns", "wf", "run", tree, pstr(branch), ancestors, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// readAll pages through a branch and returns node IDs in returned order
// plus the number of pages consumed.
func readAll(t *testing.T, s *executionstore.Stub, token []byte, branch string, min, max int64, pageSize int, reverse bool) ([]int64, int) {
	t.Helper()
	var ids []int64
	var next []byte
	pages := 0
	for {
		resp, err := s.ReadHistoryBranch(context.Background(), &persistence.InternalReadHistoryBranchRequest{
			ShardID: 1, BranchToken: token, BranchID: branch,
			MinNodeID: min, MaxNodeID: max, PageSize: pageSize,
			NextPageToken: next, ReverseOrder: reverse,
		})
		if err != nil {
			t.Fatalf("ReadHistoryBranch page %d: %v", pages, err)
		}
		pages++
		for _, n := range resp.Nodes {
			ids = append(ids, n.NodeID)
		}
		if len(resp.NextPageToken) == 0 {
			return ids, pages
		}
		if pages > 50 {
			t.Fatal("pagination did not terminate")
		}
		next = resp.NextPageToken
	}
}

func seq(from, to int64) []int64 {
	var out []int64
	if from <= to {
		for i := from; i <= to; i++ {
			out = append(out, i)
		}
	} else {
		for i := from; i >= to; i-- {
			out = append(out, i)
		}
	}
	return out
}

func equalIDs(a, b []int64) bool {
	return fmt.Sprint(a) == fmt.Sprint(b)
}

func TestReadHistoryBranch_MultiPage_ReturnsAllNodes(t *testing.T) {
	s, _ := newHistoryStore(t)
	for i := int64(1); i <= 7; i++ {
		appendNode(t, s, "tree-p", "branch-p", i, i, fmt.Sprintf("e%d", i))
	}
	tok := branchToken(t, s, "tree-p", "branch-p", nil)
	ids, pages := readAll(t, s, tok, "branch-p", 1, 100, 3, false)
	if !equalIDs(ids, seq(1, 7)) || pages < 3 {
		t.Fatalf("forward paging returned %v in %d pages; want 1..7 in >=3 pages", ids, pages)
	}
}

func TestReadHistoryBranch_ReverseMultiPage_ReturnsAllNodes(t *testing.T) {
	s, _ := newHistoryStore(t)
	for i := int64(1); i <= 7; i++ {
		appendNode(t, s, "tree-r", "branch-r", i, i, fmt.Sprintf("e%d", i))
	}
	tok := branchToken(t, s, "tree-r", "branch-r", nil)
	ids, pages := readAll(t, s, tok, "branch-r", 2, 7, 2, true)
	if !equalIDs(ids, seq(6, 2)) || pages < 3 {
		t.Fatalf("reverse paging returned %v in %d pages; want 6..2 in >=3 pages", ids, pages)
	}
}

func TestReadHistoryBranch_BadToken_InvalidArgument(t *testing.T) {
	s, _ := newHistoryStore(t)
	appendNode(t, s, "tree-b", "branch-b", 1, 1, "e1")
	_, err := s.ReadHistoryBranch(context.Background(), &persistence.InternalReadHistoryBranchRequest{
		ShardID: 1, BranchID: "branch-b", MinNodeID: 1, MaxNodeID: 10, PageSize: 5,
		NextPageToken: []byte("garbage"),
	})
	var ia *serviceerror.InvalidArgument
	if !errors.As(err, &ia) {
		t.Fatalf("want InvalidArgument for corrupt token; got %T %v", err, err)
	}
}

// Upstream sends TreeInfo only with IsNewBranch, so later
// appends must not overwrite the stored tree info.
func TestAppendHistoryNodes_NotNewBranch_PreservesTreeInfo(t *testing.T) {
	s, _ := newHistoryStore(t)
	ctx := context.Background()
	if err := s.AppendHistoryNodes(ctx, &persistence.InternalAppendHistoryNodesRequest{
		ShardID: 1, BranchInfo: branchInfo("tree-t", "branch-t"), IsNewBranch: true,
		TreeInfo: historyBlob("original-tree-info"),
		Node:     persistence.InternalHistoryNode{NodeID: 1, TransactionID: 1, Events: historyBlob("e1")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendHistoryNodes(ctx, &persistence.InternalAppendHistoryNodesRequest{
		ShardID: 1, BranchInfo: branchInfo("tree-t", "branch-t"), IsNewBranch: false,
		Node: persistence.InternalHistoryNode{NodeID: 2, TransactionID: 2, Events: historyBlob("e2")},
	}); err != nil {
		t.Fatal(err)
	}
	resp, err := s.GetHistoryTreeContainingBranch(ctx, &persistence.InternalGetHistoryTreeContainingBranchRequest{
		ShardID: 1, BranchToken: branchToken(t, s, "tree-t", "branch-t", nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.TreeInfos) != 1 || resp.TreeInfos[0] == nil || string(resp.TreeInfos[0].Data) != "original-tree-info" {
		t.Fatalf("tree info overwritten: %+v", resp.TreeInfos)
	}
}

// Deleting a parent branch removes only the ranges the
// manager hands down, so nodes the forked child still references survive.
func TestDeleteHistoryBranch_ForkedChild_ChildHistoryReadable(t *testing.T) {
	s, _ := newHistoryStore(t)
	ctx := context.Background()
	for i := int64(1); i <= 5; i++ {
		appendNode(t, s, "tree-f", "parent", i, i, fmt.Sprintf("p%d", i))
	}
	// Child forked at node 3: it references parent nodes [1,3).
	appendNode(t, s, "tree-f", "child", 3, 10, "c3")
	appendNode(t, s, "tree-f", "child", 4, 11, "c4")

	// The manager computes: child uses parent up to EndNodeId 3, so only
	// parent nodes >= 3 are deletable.
	if err := s.DeleteHistoryBranch(ctx, &persistence.InternalDeleteHistoryBranchRequest{
		ShardID: 1, BranchInfo: branchInfo("tree-f", "parent"),
		BranchRanges: []persistence.InternalDeleteHistoryBranchRange{{BranchId: "parent", BeginNodeId: 3}},
	}); err != nil {
		t.Fatal(err)
	}
	parentTok := branchToken(t, s, "tree-f", "parent", nil)
	if ids, _ := readAll(t, s, parentTok, "parent", 1, 100, 10, false); !equalIDs(ids, seq(1, 2)) {
		t.Fatalf("parent nodes after delete = %v; want [1 2] retained for the child", ids)
	}

	// Deleting the child with its ancestor range removes the rest.
	childTok := branchToken(t, s, "tree-f", "child", []*persistencespb.HistoryBranchRange{{BranchId: "parent", BeginNodeId: 1, EndNodeId: 3}})
	if err := s.DeleteHistoryBranch(ctx, &persistence.InternalDeleteHistoryBranchRequest{
		ShardID: 1, BranchToken: childTok, BranchInfo: branchInfo("tree-f", "child"),
		BranchRanges: []persistence.InternalDeleteHistoryBranchRange{
			{BranchId: "child", BeginNodeId: 3}, {BranchId: "parent", BeginNodeId: 1},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if ids, _ := readAll(t, s, parentTok, "parent", 1, 100, 10, false); len(ids) != 0 {
		t.Fatalf("ancestor range not deleted: %v", ids)
	}
	if ids, _ := readAll(t, s, childTok, "child", 1, 100, 10, false); len(ids) != 0 {
		t.Fatalf("child nodes not deleted: %v", ids)
	}
}

func TestDeleteHistoryNodes_NewerTxnID_Preserved(t *testing.T) {
	s, _ := newHistoryStore(t)
	ctx := context.Background()
	appendNode(t, s, "tree-d", "branch-d", 1, 5, "e1")
	del := func(txn int64) {
		if err := s.DeleteHistoryNodes(ctx, &persistence.InternalDeleteHistoryNodesRequest{
			ShardID: 1, BranchInfo: branchInfo("tree-d", "branch-d"), NodeID: 1, TransactionID: txn,
		}); err != nil {
			t.Fatal(err)
		}
	}
	tok := branchToken(t, s, "tree-d", "branch-d", nil)
	del(3)
	if ids, _ := readAll(t, s, tok, "branch-d", 1, 10, 10, false); len(ids) != 1 {
		t.Fatalf("node with newer txn_id was deleted by a stale request")
	}
	del(5)
	if ids, _ := readAll(t, s, tok, "branch-d", 1, 10, 10, false); len(ids) != 0 {
		t.Fatalf("matching txn_id was not deleted")
	}
}

// TestAppendHistoryNodes_InTxnStaleTxnID_NoAbort: a
// replayed history append inside UpdateWorkflowExecution (same node,
// same txn_id) must not abort the transaction.
func TestAppendHistoryNodes_InTxnStaleTxnID_NoAbort(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()

	snap := makeSnapshot("ns", "wf", "run-h")
	if _, err := s.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.CreateWorkflowModeBrandNew, NewWorkflowSnapshot: snap,
	}); err != nil {
		t.Fatal(err)
	}
	appendNode(t, s, "tree-h", "branch-h", 2, 7, "already-written")

	mut := snapshotToMutation(snap)
	mut.Condition, mut.DBRecordVersion, mut.NextEventID = 2, 2, 3
	err := s.UpdateWorkflowExecution(ctx, &persistence.InternalUpdateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowMutation: mut,
		UpdateWorkflowNewEvents: []*persistence.InternalAppendHistoryNodesRequest{{
			ShardID: 1, BranchInfo: branchInfo("tree-h", "branch-h"),
			Node: persistence.InternalHistoryNode{NodeID: 2, TransactionID: 7, Events: historyBlob("replayed")},
		}},
	})
	if err != nil {
		t.Fatalf("replayed append aborted the update transaction: %T %v", err, err)
	}
	var node bson.M
	if err := db.Collection("history_nodes").FindOne(ctx, bson.M{"tree_id": "tree-h", "node_id": 2}).Decode(&node); err != nil {
		t.Fatal(err)
	}
	if node["txn_id"] != int64(7) {
		t.Fatalf("stored node changed: %v", node)
	}
}
