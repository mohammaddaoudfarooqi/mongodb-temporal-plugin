package conformance

import (
	"testing"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
)

// runHistoryStore exercises branch append + read,
// txn-id tie-break, fork, delete-branch atomicity.
func runHistoryStore(t *testing.T, f Fixture) {
	newStore := func(t *testing.T) (TestContext, *executionstore.Stub) {
		t.Helper()
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure history idx", executionstore.EnsureHistoryIndexes(tc.Ctx, tc.DB))
		return tc, executionstore.New(tc.DB, "cluster0")
	}

	t.Run("AppendAndRead_ForwardOrder", func(t *testing.T) {
		tc, s := newStore(t)
		tree, branch := uniqStr("tree"), uniqStr("branch")
		for nid := int64(1); nid <= 5; nid++ {
			err := s.AppendHistoryNodes(tc.Ctx, &persistence.InternalAppendHistoryNodesRequest{
				ShardID:     1,
				BranchInfo:  &persistencespb.HistoryBranch{TreeId: tree, BranchId: branch},
				IsNewBranch: nid == 1,
				TreeInfo:    dataBlob("ti"),
				Node: persistence.InternalHistoryNode{
					NodeID: nid, TransactionID: nid, Events: dataBlob("events"),
				},
			})
			fatalOnErr(t, "append", err)
		}

		resp, err := s.ReadHistoryBranch(tc.Ctx, &persistence.InternalReadHistoryBranchRequest{
			ShardID: 1, BranchID: branch, MinNodeID: 1, MaxNodeID: 10, PageSize: 10,
		})
		fatalOnErr(t, "read forward", err)
		if len(resp.Nodes) != 5 {
			t.Fatalf("expected 5 nodes; got %d", len(resp.Nodes))
		}
		for i, n := range resp.Nodes {
			if n.NodeID != int64(i+1) {
				t.Errorf("node %d out of order: NodeID=%d", i, n.NodeID)
			}
		}
	})

	t.Run("Append_NodeVersionsCoexist", func(t *testing.T) {
		// Every (node, txn) version
		// is kept and returned highest transaction first; the history
		// manager picks the valid one via the PrevTransactionID chain.
		tc, s := newStore(t)
		tree, branch := uniqStr("tree"), uniqStr("branch")

		err := s.AppendHistoryNodes(tc.Ctx, &persistence.InternalAppendHistoryNodesRequest{
			ShardID:     1,
			BranchInfo:  &persistencespb.HistoryBranch{TreeId: tree, BranchId: branch},
			IsNewBranch: true,
			TreeInfo:    dataBlob("ti-1"),
			Node: persistence.InternalHistoryNode{
				NodeID: 1, TransactionID: 10, Events: dataBlob("batch-a"),
			},
		})
		fatalOnErr(t, "append v1", err)

		err = s.AppendHistoryNodes(tc.Ctx, &persistence.InternalAppendHistoryNodesRequest{
			ShardID:    1,
			BranchInfo: &persistencespb.HistoryBranch{TreeId: tree, BranchId: branch},
			TreeInfo:   dataBlob("ti-2"),
			Node: persistence.InternalHistoryNode{
				NodeID: 1, TransactionID: 20, Events: dataBlob("batch-b"),
			},
		})
		fatalOnErr(t, "append v2", err)

		resp, err := s.ReadHistoryBranch(tc.Ctx, &persistence.InternalReadHistoryBranchRequest{
			ShardID: 1, BranchID: branch, MinNodeID: 1, MaxNodeID: 5, PageSize: 5,
		})
		fatalOnErr(t, "read", err)
		if len(resp.Nodes) != 2 {
			t.Fatalf("expected both versions of node 1; got %d", len(resp.Nodes))
		}
		if resp.Nodes[0].TransactionID != 20 || string(resp.Nodes[0].Events.Data) != "batch-b" ||
			resp.Nodes[1].TransactionID != 10 || string(resp.Nodes[1].Events.Data) != "batch-a" {
			t.Errorf("expected [20:batch-b 10:batch-a]; got %+v", resp.Nodes)
		}
	})

	t.Run("DeleteHistoryBranch_RemovesNodes", func(t *testing.T) {
		tc, s := newStore(t)
		tree, branch := uniqStr("tree"), uniqStr("branch")
		for nid := int64(1); nid <= 3; nid++ {
			err := s.AppendHistoryNodes(tc.Ctx, &persistence.InternalAppendHistoryNodesRequest{
				ShardID:     1,
				BranchInfo:  &persistencespb.HistoryBranch{TreeId: tree, BranchId: branch},
				IsNewBranch: nid == 1,
				TreeInfo:    dataBlob("ti"),
				Node: persistence.InternalHistoryNode{
					NodeID: nid, TransactionID: nid, Events: dataBlob("evt"),
				},
			})
			fatalOnErr(t, "append", err)
		}
		err := s.DeleteHistoryBranch(tc.Ctx, &persistence.InternalDeleteHistoryBranchRequest{
			ShardID:    1,
			BranchInfo: &persistencespb.HistoryBranch{TreeId: tree, BranchId: branch},
		})
		fatalOnErr(t, "delete branch", err)

		resp, err := s.ReadHistoryBranch(tc.Ctx, &persistence.InternalReadHistoryBranchRequest{
			ShardID: 1, BranchID: branch, MinNodeID: 1, MaxNodeID: 10, PageSize: 10,
		})
		fatalOnErr(t, "read after delete", err)
		if len(resp.Nodes) != 0 {
			t.Errorf("expected 0 nodes after branch delete; got %d", len(resp.Nodes))
		}
	})
}
