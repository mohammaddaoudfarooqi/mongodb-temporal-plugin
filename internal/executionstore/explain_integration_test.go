//go:build integration

// Explain-plan assertions (backlog B-10). For each hot query the
// plugin issues, we assert MongoDB's planner chooses IXSCAN (not
// COLLSCAN). A regression here — fields reordered, index renamed,
// predicate shape drift — would flip the winning plan to COLLSCAN
// and this test would catch it.
//
// Indexes covered:
//   - shard-id leading predicate on executions/history_tasks.
//   - history_nodes (tree_id, branch_id, node_id).
//   - history_tasks (shard_id, category_id, fire_time, task_id).
package executionstore_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
)

// TestExplain_ExecutionsByShard covers the primary execution-doc
// read path (shard_id-leading predicate).
func TestExplain_ExecutionsByShard(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 7, 1)
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		ns := "ns-x"
		if i%2 == 0 {
			ns = "ns-y"
		}
		_, err := s.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
			ShardID: 7, RangeID: 1, Mode: persistence.CreateWorkflowModeBrandNew,
			NewWorkflowSnapshot: makeSnapshot(ns, "wf-"+itoaLocal(i), tokenI(i)),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	assertIXSCAN(t, ctx, db, "executions",
		bson.M{"shard_id": int32(7), "namespace_id": "ns-x"})
}

func TestExplain_HistoryNodesByBranch(t *testing.T) {
	db := startDB(t)
	if err := executionstore.EnsureHistoryIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()

	for nid := int64(1); nid <= 3; nid++ {
		err := s.AppendHistoryNodes(ctx, &persistence.InternalAppendHistoryNodesRequest{
			ShardID: 1,
			BranchInfo: &persistencespb.HistoryBranch{
				TreeId: "tree-exp", BranchId: "branch-exp",
			},
			IsNewBranch: nid == 1,
			TreeInfo: &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("ti"),
			},
			Node: persistence.InternalHistoryNode{
				NodeID: nid, TransactionID: nid,
				Events: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					Data:         []byte(tokenI(int(nid))),
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	assertIXSCAN(t, ctx, db, "history_nodes",
		bson.M{
			"tree_id":   "tree-exp",
			"branch_id": "branch-exp",
			"node_id":   bson.M{"$gte": int64(1), "$lt": int64(100)},
		})
}

func TestExplain_HistoryTasksByShardCategory(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 2, 1)
	if err := executionstore.EnsureTaskIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()

	err := s.AddHistoryTasks(ctx, &persistence.InternalAddHistoryTasksRequest{
		ShardID: 2, RangeID: 1,
		Tasks: map[tasks.Category][]persistence.InternalHistoryTask{
			tasks.CategoryTransfer: {
				{Key: tasks.NewImmediateKey(100), Blob: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("t"),
				}},
				{Key: tasks.NewImmediateKey(200), Blob: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("t"),
				}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	assertIXSCAN(t, ctx, db, "history_tasks",
		bson.M{"shard_id": int32(2), "category_id": int32(tasks.CategoryTransfer.ID())})
}

// assertIXSCAN runs the `explain` admin command with executionStats
// verbosity and walks the returned plan tree looking for an IXSCAN
// stage. Fails the test if the winning plan is a COLLSCAN (or if
// the explain shape drifts beyond recognition). This guards
// the promise that every shard-owned query leads with shard_id and
// stays index-eligible.
func assertIXSCAN(t *testing.T, ctx context.Context, db *mongo.Database, collection string, filter bson.M) {
	t.Helper()
	cmd := bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: collection},
			{Key: "filter", Value: filter},
		}},
		{Key: "verbosity", Value: "executionStats"},
	}
	var out bson.M
	if err := db.RunCommand(ctx, cmd).Decode(&out); err != nil {
		t.Fatalf("explain %s: %v", collection, err)
	}
	stages := collectStages(out)
	hasIXSCAN := false
	hasCOLLSCAN := false
	for _, s := range stages {
		if s == "IXSCAN" {
			hasIXSCAN = true
		}
		if s == "COLLSCAN" {
			hasCOLLSCAN = true
		}
	}
	if !hasIXSCAN {
		t.Fatalf("expected an IXSCAN stage in plan for %s; stages seen: %v", collection, stages)
	}
	if hasCOLLSCAN {
		t.Errorf("plan for %s contains a COLLSCAN — index is not picked: %v", collection, stages)
	}
}

// collectStages walks the explain output and returns every "stage"
// string it finds. The plan tree shape varies across Mongo versions
// (winningPlan, queryPlanner, executionStages) AND the driver may
// decode nested docs as bson.D instead of bson.M. This walker
// handles every shape driver v2 emits.
func collectStages(node any) []string {
	out := []string{}
	switch v := node.(type) {
	case bson.M:
		for k, child := range v {
			if k == "stage" {
				if s, ok := child.(string); ok {
					out = append(out, s)
				}
			}
			out = append(out, collectStages(child)...)
		}
	case bson.D:
		for _, e := range v {
			if e.Key == "stage" {
				if s, ok := e.Value.(string); ok {
					out = append(out, s)
				}
			}
			out = append(out, collectStages(e.Value)...)
		}
	case bson.A:
		for _, e := range v {
			out = append(out, collectStages(e)...)
		}
	case map[string]any:
		for k, child := range v {
			if k == "stage" {
				if s, ok := child.(string); ok {
					out = append(out, s)
				}
			}
			out = append(out, collectStages(child)...)
		}
	case []any:
		for _, e := range v {
			out = append(out, collectStages(e)...)
		}
	case []bson.M:
		for _, e := range v {
			out = append(out, collectStages(e)...)
		}
	case []bson.D:
		for _, e := range v {
			out = append(out, collectStages(e)...)
		}
	}
	return out
}

func tokenI(i int) string {
	return "run-" + strings.ReplaceAll(time.Now().UTC().Format("150405.000000"), ".", "") + "-" + itoaLocal(i)
}

func itoaLocal(i int) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	buf := []byte{}
	for i > 0 {
		buf = append([]byte{byte('0' + i%10)}, buf...)
		i /= 10
	}
	if neg {
		buf = append([]byte{'-'}, buf...)
	}
	return string(buf)
}

// TestExplain_HistoryTreesByTree:
// GetHistoryTreeContainingBranch filters history_trees by tree_id.
func TestExplain_HistoryTreesByTree(t *testing.T) {
	ctx := context.Background()
	db := startDB(t)
	if err := executionstore.EnsureHistoryIndexes(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("history_trees").InsertOne(ctx, bson.M{"_id": "t1:b1", "tree_id": "t1", "branch_id": "b1"}); err != nil {
		t.Fatal(err)
	}
	assertIXSCAN(t, ctx, db, "history_trees", bson.M{"tree_id": "t1"})
}

// TestExplain_ListConcreteExecutions_ShardScoped /
// Listing one shard must not walk other shards' keys. Without
// a shard-leading index the planner scans the _id index and filters
// shard_id afterwards, or uses the namespace index and sorts the whole
// shard in memory.
func TestExplain_ListConcreteExecutions_ShardScoped(t *testing.T) {
	ctx := context.Background()
	db := startDB(t)
	// "10:…".."18:…" sort before "1:…" (':' > '0'), so an _id-index plan
	// must walk every other shard's keys before reaching shard 1.
	var docs []any
	for i := 0; i < 300; i++ {
		docs = append(docs,
			bson.M{"_id": fmt.Sprintf("1:ns:wf-%03d:r", i), "shard_id": int32(1), "namespace_id": "ns", "workflow_id": fmt.Sprintf("wf-%03d", i), "run_id": "r"},
			bson.M{"_id": fmt.Sprintf("1%d:ns:wf-%03d:r", i%9, i), "shard_id": int32(10 + i%9), "namespace_id": "ns", "workflow_id": fmt.Sprintf("wf-%03d", i), "run_id": "r"})
	}
	if _, err := db.Collection("executions").InsertMany(ctx, docs); err != nil {
		t.Fatal(err)
	}
	var out struct {
		Planner struct {
			Winning bson.Raw `bson:"winningPlan"`
		} `bson:"queryPlanner"`
		Stats struct {
			KeysExamined int64 `bson:"totalKeysExamined"`
			Returned     int64 `bson:"nReturned"`
		} `bson:"executionStats"`
	}
	err := db.RunCommand(ctx, bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: "executions"},
			{Key: "filter", Value: bson.M{"shard_id": int32(1)}},
			{Key: "sort", Value: bson.D{{Key: "_id", Value: 1}}},
			{Key: "limit", Value: 101},
		}},
		{Key: "verbosity", Value: "executionStats"},
	}).Decode(&out)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.Planner.Winning.String(), `"SORT"`) {
		t.Fatalf("shard-scoped list sorts in memory: %s", out.Planner.Winning)
	}
	if out.Stats.Returned != 101 || out.Stats.KeysExamined > out.Stats.Returned+1 {
		t.Fatalf("shard-scoped list examined %d keys to return %d docs", out.Stats.KeysExamined, out.Stats.Returned)
	}
}
