package executionstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

const (
	historyNodesColl = "history_nodes"
	historyTreesColl = "history_trees"
)

// historyNodeDoc is the BSON projection of one version of a history node.
// Keyed on "<tree_id>:<branch_id>:<node_id>:<txn_id>": every transaction's
// write of a node is kept, because upstream's history manager picks the
// valid version by following the PrevTransactionID chain.
// Documents written before this change use "<tree_id>:<branch_id>:<node_id>";
// all reads and deletes therefore match on fields, never on _id.
type historyNodeDoc struct {
	ID        string              `bson:"_id"`
	TreeID    string              `bson:"tree_id"`
	BranchID  string              `bson:"branch_id"`
	NodeID    int64               `bson:"node_id"`
	TxnID     int64               `bson:"txn_id"`
	PrevTxnID int64               `bson:"prev_txn_id,omitempty"`
	Events    *internalmongo.Blob `bson:"events"`
}

// historyTreeDoc represents one branch of a history tree. Keyed on
// "<tree_id>:<branch_id>".
type historyTreeDoc struct {
	ID         string              `bson:"_id"`
	TreeID     string              `bson:"tree_id"`
	BranchID   string              `bson:"branch_id"`
	TreeInfo   *internalmongo.Blob `bson:"tree_info"`
	UpdateTime time.Time           `bson:"update_time"`
}

func historyNodeID(tree, branch string, nodeID, txnID int64) string {
	return fmt.Sprintf("%s:%s:%d:%d", tree, branch, nodeID, txnID)
}
func historyTreeID(tree, branch string) string {
	return fmt.Sprintf("%s:%s", tree, branch)
}

// AppendHistoryNodes inserts a history node with upsert-if-newer
// semantics. The branch document is upserted on every append
// as the plugin's authoritative record of branches in the tree.
func (s *Stub) AppendHistoryNodes(
	ctx context.Context,
	req *persistence.InternalAppendHistoryNodesRequest,
) (retErr error) {
	defer s.observeOp("AppendHistoryNodes")(&retErr)
	return internalmongo.TranslateDriverError(s.appendHistoryNodesIn(ctx, req))
}

// appendHistoryNodesIn is the shared implementation for AppendHistoryNodes
// and for the in-transaction callers (B-1). The ctx passed here may be a
// session context, in which case the underlying driver routes the
// operation through the transaction; otherwise the two collection writes
// are independent. Callers that need atomicity must pass a session ctx.
func (s *Stub) appendHistoryNodesIn(
	ctx context.Context,
	req *persistence.InternalAppendHistoryNodesRequest,
) error {
	if req == nil || req.BranchInfo == nil {
		return errors.New("executionstore: invalid AppendHistoryNodes request")
	}
	// Temporal bounds a batch at system.transactionSizeLimit (4 MB by
	// default). If that is raised past what one MongoDB document holds,
	// refuse the batch with the error Temporal answers by terminating the
	// workflow, instead of a BSONObjectTooLarge it would retry for ever.
	if n := len(req.Node.Events.GetData()); n > payloadBudget {
		return &persistence.TransactionSizeLimitError{
			Msg: fmt.Sprintf("history batch of %d bytes for branch %s exceeds the %d-byte budget", n, req.BranchInfo.BranchId, payloadBudget),
		}
	}
	if s.db == nil {
		return errors.New("executionstore: uninitialised store")
	}
	branch := req.BranchInfo
	nodeDoc := historyNodeDoc{
		ID:        historyNodeID(branch.TreeId, branch.BranchId, req.Node.NodeID, req.Node.TransactionID),
		TreeID:    branch.TreeId,
		BranchID:  branch.BranchId,
		NodeID:    req.Node.NodeID,
		TxnID:     req.Node.TransactionID,
		PrevTxnID: req.Node.PrevTransactionID,
		Events:    internalmongo.BlobFromDataBlob(req.Node.Events),
	}

	// Insert this (node, txn) version if absent. A replay of the same
	// version is a no-op rather than a failing write, so it can never
	// abort the surrounding transaction.
	if _, err := s.db.Collection(historyNodesColl).UpdateOne(ctx,
		bson.M{
			"tree_id":   nodeDoc.TreeID,
			"branch_id": nodeDoc.BranchID,
			"node_id":   nodeDoc.NodeID,
			"txn_id":    nodeDoc.TxnID,
		},
		bson.M{"$setOnInsert": nodeDoc},
		options.UpdateOne().SetUpsert(true)); err != nil {
		return internalmongo.Wrap(err, "executionstore.AppendHistoryNodes.node")
	}

	// The tree/branch record carries branch ancestry. Upstream sends
	// TreeInfo only with the first append to a branch, so later appends
	// must leave the stored record alone.
	if !req.IsNewBranch {
		return nil
	}
	treeDoc := historyTreeDoc{
		ID:         historyTreeID(branch.TreeId, branch.BranchId),
		TreeID:     branch.TreeId,
		BranchID:   branch.BranchId,
		TreeInfo:   internalmongo.BlobFromDataBlob(req.TreeInfo),
		UpdateTime: time.Now().UTC(),
	}
	if _, err := s.db.Collection(historyTreesColl).ReplaceOne(ctx,
		bson.M{"_id": treeDoc.ID}, treeDoc, options.Replace().SetUpsert(true)); err != nil {
		return internalmongo.Wrap(err, "executionstore.AppendHistoryNodes.tree")
	}
	return nil
}

// appendHistoryEventsBatchIn iterates a slice of
// InternalAppendHistoryNodesRequest and appends each inside the caller's
// session context. A per-request nil entry is skipped quietly — upstream
// sometimes sends sparse lists.
func (s *Stub) appendHistoryEventsBatchIn(
	ctx context.Context,
	batch []*persistence.InternalAppendHistoryNodesRequest,
) error {
	for _, ev := range batch {
		if ev == nil {
			continue
		}
		if err := s.appendHistoryNodesIn(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

// ReadHistoryBranch returns history nodes in the requested range
// (forward, reverse, or raw). Pagination
// uses the last-returned node_id as the resume cursor.
func (s *Stub) ReadHistoryBranch(
	ctx context.Context,
	req *persistence.InternalReadHistoryBranchRequest,
) (_ *persistence.InternalReadHistoryBranchResponse, retErr error) {
	defer s.observeOp("ReadHistoryBranch")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("executionstore: uninitialised or nil request")
	}
	// Versions are ordered node_id ascending with the highest txn_id
	// first (what upstream's filterHistoryNodes expects); reverse reads
	// walk the same index backwards.
	filter := bson.M{
		"branch_id": req.BranchID,
		"node_id":   bson.M{"$gte": req.MinNodeID, "$lt": req.MaxNodeID},
	}
	if len(req.NextPageToken) > 0 {
		last, err := decodeHistoryPageToken(req.NextPageToken)
		if err != nil {
			return nil, err
		}
		// Resume strictly after the last (node, txn) returned.
		if req.ReverseOrder {
			filter["$or"] = bson.A{
				bson.M{"node_id": bson.M{"$lt": last.NodeID}},
				bson.M{"node_id": last.NodeID, "txn_id": bson.M{"$gt": last.TxnID}},
			}
		} else {
			filter["$or"] = bson.A{
				bson.M{"node_id": bson.M{"$gt": last.NodeID}},
				bson.M{"node_id": last.NodeID, "txn_id": bson.M{"$lt": last.TxnID}},
			}
		}
	}
	// Scope by tree so the (tree_id, branch_id, node_id) index serves the
	// read. Upstream always sends the branch token.
	if len(req.BranchToken) > 0 {
		branch, err := s.branchUtil.ParseHistoryBranchInfo(req.BranchToken)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("executionstore.ReadHistoryBranch: branch token: %v", err))
		}
		filter["tree_id"] = branch.TreeId
	}
	sort := bson.D{{Key: "node_id", Value: 1}, {Key: "txn_id", Value: -1}}
	if req.ReverseOrder {
		sort = bson.D{{Key: "node_id", Value: -1}, {Key: "txn_id", Value: 1}}
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 256
	}
	opts := options.Find().
		SetSort(sort).
		SetLimit(int64(pageSize) + 1)

	cursor, err := s.db.Collection(historyNodesColl).Find(ctx, filter, opts)
	if err != nil {
		return nil, internalmongo.Translate(err, "executionstore.ReadHistoryBranch")
	}
	defer cursor.Close(ctx)

	var docs []historyNodeDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, internalmongo.Translate(err, "executionstore.ReadHistoryBranch.decode")
	}
	resp := &persistence.InternalReadHistoryBranchResponse{}
	for i, d := range docs {
		if i == pageSize {
			token, err := encodeHistoryPageToken(docs[pageSize-1].NodeID, docs[pageSize-1].TxnID)
			if err != nil {
				return nil, err
			}
			resp.NextPageToken = token
			break
		}
		resp.Nodes = append(resp.Nodes, persistence.InternalHistoryNode{
			NodeID:            d.NodeID,
			TransactionID:     d.TxnID,
			PrevTransactionID: d.PrevTxnID,
			Events:            internalmongo.DataBlobFromBlob(d.Events),
		})
	}
	return resp, nil
}

// ForkHistoryBranch creates a branch document for the new branch.
func (s *Stub) ForkHistoryBranch(
	ctx context.Context,
	req *persistence.InternalForkHistoryBranchRequest,
) (retErr error) {
	defer s.observeOp("ForkHistoryBranch")(&retErr)
	if s.db == nil || req == nil || req.ForkBranchInfo == nil {
		return errors.New("executionstore: uninitialised or invalid ForkHistoryBranch request")
	}
	doc := historyTreeDoc{
		ID:         historyTreeID(req.ForkBranchInfo.TreeId, req.NewBranchID),
		TreeID:     req.ForkBranchInfo.TreeId,
		BranchID:   req.NewBranchID,
		TreeInfo:   internalmongo.BlobFromDataBlob(req.TreeInfo),
		UpdateTime: time.Now().UTC(),
	}
	_, err := s.db.Collection(historyTreesColl).ReplaceOne(ctx,
		bson.M{"_id": doc.ID}, doc, options.Replace().SetUpsert(true))
	if err != nil {
		return internalmongo.Translate(err, "executionstore.ForkHistoryBranch")
	}
	return nil
}

// DeleteHistoryBranch removes the branch document and the node ranges
// listed in BranchRanges within a single transaction.
func (s *Stub) DeleteHistoryBranch(
	ctx context.Context,
	req *persistence.InternalDeleteHistoryBranchRequest,
) (retErr error) {
	defer s.observeOp("DeleteHistoryBranch")(&retErr)
	if s.db == nil || req == nil || req.BranchInfo == nil {
		return errors.New("executionstore: uninitialised or invalid DeleteHistoryBranch request")
	}
	branch := req.BranchInfo
	// The manager computes which ranges of the branch and its ancestors
	// no other branch references; only those are deleted.
	// A request without ranges deletes the target branch outright.
	ranges := req.BranchRanges
	if len(ranges) == 0 {
		ranges = []persistence.InternalDeleteHistoryBranchRange{{BranchId: branch.BranchId}}
	}
	sess, err := s.db.Client().StartSession()
	if err != nil {
		return internalmongo.Translate(err, "executionstore.DeleteHistoryBranch.session")
	}
	defer sess.EndSession(ctx)

	_, err = internalmongo.WithTxn[struct{}](ctx, s.txnRunner(sess, "DeleteHistoryBranch"),
		func(sctx context.Context) (struct{}, error) {
			for _, r := range ranges {
				if _, err := s.db.Collection(historyNodesColl).DeleteMany(sctx, bson.M{
					"tree_id":   branch.TreeId,
					"branch_id": r.BranchId,
					"node_id":   bson.M{"$gte": r.BeginNodeId},
				}); err != nil {
					return struct{}{}, internalmongo.Wrap(err, "executionstore.DeleteHistoryBranch.nodes")
				}
			}
			if _, err := s.db.Collection(historyTreesColl).DeleteOne(sctx,
				bson.M{"_id": historyTreeID(branch.TreeId, branch.BranchId)}); err != nil {
				return struct{}{}, internalmongo.Wrap(err, "executionstore.DeleteHistoryBranch.tree")
			}
			return struct{}{}, nil
		})
	return err
}

// DeleteHistoryNodes removes a single history node (admin surface,
// rarely used). The upstream history-scavenger workflow uses
// DeleteHistoryBranch for retention; this method exists only to match
// the interface surface.
func (s *Stub) DeleteHistoryNodes(
	ctx context.Context,
	req *persistence.InternalDeleteHistoryNodesRequest,
) (retErr error) {
	defer s.observeOp("DeleteHistoryNodes")(&retErr)
	if s.db == nil || req == nil || req.BranchInfo == nil {
		return errors.New("executionstore: uninitialised or invalid DeleteHistoryNodes request")
	}
	// Match the transaction too: a newer write of the same node must
	// survive a delete aimed at the older one.
	_, err := s.db.Collection(historyNodesColl).DeleteOne(ctx, bson.M{
		"tree_id":   req.BranchInfo.TreeId,
		"branch_id": req.BranchInfo.BranchId,
		"node_id":   req.NodeID,
		"txn_id":    req.TransactionID,
	})
	if err != nil {
		return internalmongo.Translate(err, "executionstore.DeleteHistoryNodes")
	}
	return nil
}

// GetHistoryTreeContainingBranch returns every TreeInfo blob for the
// tree containing the branch referenced by BranchToken.
func (s *Stub) GetHistoryTreeContainingBranch(
	ctx context.Context,
	req *persistence.InternalGetHistoryTreeContainingBranchRequest,
) (_ *persistence.InternalGetHistoryTreeContainingBranchResponse, retErr error) {
	defer s.observeOp("GetHistoryTreeContainingBranch")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("executionstore: uninitialised or nil request")
	}
	branch, err := s.branchUtil.ParseHistoryBranchInfo(req.BranchToken)
	if err != nil {
		return nil, internalmongo.Translate(err, "executionstore.GetHistoryTreeContainingBranch.parse")
	}
	cursor, err := s.db.Collection(historyTreesColl).Find(ctx, bson.M{"tree_id": branch.TreeId})
	if err != nil {
		return nil, internalmongo.Translate(err, "executionstore.GetHistoryTreeContainingBranch")
	}
	defer cursor.Close(ctx)

	var docs []historyTreeDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, internalmongo.Translate(err, "executionstore.GetHistoryTreeContainingBranch.decode")
	}
	resp := &persistence.InternalGetHistoryTreeContainingBranchResponse{}
	for _, d := range docs {
		resp.TreeInfos = append(resp.TreeInfos, internalmongo.DataBlobFromBlob(d.TreeInfo))
	}
	return resp, nil
}

// GetAllHistoryTreeBranches returns a paginated list of every branch
// across every tree. Admin-only.
func (s *Stub) GetAllHistoryTreeBranches(
	ctx context.Context,
	req *persistence.GetAllHistoryTreeBranchesRequest,
) (_ *persistence.InternalGetAllHistoryTreeBranchesResponse, retErr error) {
	defer s.observeOp("GetAllHistoryTreeBranches")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("executionstore: uninitialised or nil request")
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	filter := bson.M{}
	if len(req.NextPageToken) > 0 {
		filter["_id"] = bson.M{"$gt": string(req.NextPageToken)}
	}
	cursor, err := s.db.Collection(historyTreesColl).Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(pageSize)+1))
	if err != nil {
		return nil, internalmongo.Translate(err, "executionstore.GetAllHistoryTreeBranches")
	}
	defer cursor.Close(ctx)

	var docs []historyTreeDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, internalmongo.Translate(err, "executionstore.GetAllHistoryTreeBranches.decode")
	}
	resp := &persistence.InternalGetAllHistoryTreeBranchesResponse{}
	for i, d := range docs {
		if i == pageSize {
			resp.NextPageToken = []byte(docs[pageSize-1].ID)
			break
		}
		var encoding string
		var data []byte
		if d.TreeInfo != nil {
			encoding = d.TreeInfo.Encoding
			data = d.TreeInfo.Data
		}
		resp.Branches = append(resp.Branches, persistence.InternalHistoryBranchDetail{
			TreeID:   d.TreeID,
			BranchID: d.BranchID,
			Encoding: encoding,
			Data:     data,
		})
	}
	return resp, nil
}

// EnsureHistoryIndexes creates the compound indexes needed for branch
// pagination. history_nodes deliberately carries no TTL index.
func EnsureHistoryIndexes(ctx context.Context, db *mongo.Database) error {
	_, err := db.Collection(historyNodesColl).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "tree_id", Value: 1},
			{Key: "branch_id", Value: 1},
			{Key: "node_id", Value: 1},
		},
		Options: options.Index().SetName("history_nodes_tree_branch_node"),
	})
	if err != nil {
		return internalmongo.Translate(err, "executionstore.EnsureHistoryIndexes")
	}
	// Serves forward (node asc, txn desc) and reverse reads over every
	// version of a node.
	_, err = db.Collection(historyNodesColl).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "tree_id", Value: 1},
			{Key: "branch_id", Value: 1},
			{Key: "node_id", Value: 1},
			{Key: "txn_id", Value: -1},
		},
		Options: options.Index().SetName("history_nodes_tree_branch_node_txn"),
	})
	if err != nil {
		return internalmongo.Translate(err, "executionstore.EnsureHistoryIndexes")
	}
	// GetHistoryTreeContainingBranch lists every branch of a tree.
	_, err = db.Collection(historyTreesColl).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "tree_id", Value: 1}},
		Options: options.Index().SetName("history_trees_tree"),
	})
	if err != nil {
		return internalmongo.Translate(err, "executionstore.EnsureHistoryIndexes")
	}
	return nil
}

// historyPageToken is the resume cursor for ReadHistoryBranch: the last
// (node_id, txn_id) returned on the previous page.
type historyPageToken struct {
	NodeID int64 `bson:"n"`
	TxnID  int64 `bson:"t"`
}

func encodeHistoryPageToken(nodeID, txnID int64) ([]byte, error) {
	raw, err := bson.Marshal(historyPageToken{NodeID: nodeID, TxnID: txnID})
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("executionstore.ReadHistoryBranch: encode page token: %v", err))
	}
	return raw, nil
}

func decodeHistoryPageToken(raw []byte) (historyPageToken, error) {
	var tok historyPageToken
	if err := bson.Unmarshal(raw, &tok); err != nil {
		return tok, serviceerror.NewInvalidArgument(fmt.Sprintf("executionstore.ReadHistoryBranch: invalid page token: %v", err))
	}
	return tok, nil
}
