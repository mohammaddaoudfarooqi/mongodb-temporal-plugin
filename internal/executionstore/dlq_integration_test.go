//go:build integration

// Integration tests for the replication DLQ.
package executionstore_test

import (
	"context"
	"testing"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
)

func putDLQ(t *testing.T, s *executionstore.Stub, shardID int32, src string, taskID int64, nsID string) {
	t.Helper()
	err := s.PutReplicationTaskToDLQ(context.Background(), &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           shardID,
		SourceClusterName: src,
		TaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId: nsID,
			TaskId:      taskID,
		},
	})
	if err != nil {
		t.Fatalf("PutReplicationTaskToDLQ: %v", err)
	}
}

// TestPutAndGetReplicationDLQ_RoundTrip:
// put a batch of DLQ tasks, read them back ordered by TaskId.
func TestPutAndGetReplicationDLQ_RoundTrip(t *testing.T) {
	db := startDB(t)
	s := executionstore.New(db, "cluster0")
	if err := executionstore.EnsureDLQIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	putDLQ(t, s, 1, "src-a", 100, "ns-1")
	putDLQ(t, s, 1, "src-a", 101, "ns-1")
	putDLQ(t, s, 1, "src-a", 102, "ns-1")

	resp, err := s.GetReplicationTasksFromDLQ(ctx, &persistence.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{
			ShardID:             1,
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.NewImmediateKey(0),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(1000),
			BatchSize:           100,
		},
		SourceClusterName: "src-a",
	})
	if err != nil {
		t.Fatalf("GetReplicationTasksFromDLQ: %v", err)
	}
	if len(resp.Tasks) != 3 {
		t.Fatalf("want 3 DLQ tasks; got %d", len(resp.Tasks))
	}
	for i, task := range resp.Tasks {
		wantID := int64(100 + i)
		if task.Key.TaskID != wantID {
			t.Errorf("DLQ[%d] TaskID: got %d want %d", i, task.Key.TaskID, wantID)
		}
	}
}

// TestReplicationDLQ_ScopedBySourceCluster covers the source-cluster
// filter: putting to "src-a" and "src-b" returns only matching entries.
func TestReplicationDLQ_ScopedBySourceCluster(t *testing.T) {
	db := startDB(t)
	s := executionstore.New(db, "cluster0")
	if err := executionstore.EnsureDLQIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	putDLQ(t, s, 1, "src-a", 1, "n")
	putDLQ(t, s, 1, "src-b", 1, "n")
	putDLQ(t, s, 1, "src-a", 2, "n")

	resp, err := s.GetReplicationTasksFromDLQ(context.Background(), &persistence.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{
			ShardID:             1,
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.NewImmediateKey(0),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(10),
			BatchSize:           10,
		},
		SourceClusterName: "src-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Tasks) != 2 {
		t.Errorf("want 2 src-a tasks; got %d", len(resp.Tasks))
	}
}

func TestRangeDeleteReplicationTaskFromDLQ(t *testing.T) {
	db := startDB(t)
	s := executionstore.New(db, "cluster0")
	if err := executionstore.EnsureDLQIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 5; i++ {
		putDLQ(t, s, 1, "src", i, "n")
	}
	err := s.RangeDeleteReplicationTaskFromDLQ(context.Background(), &persistence.RangeDeleteReplicationTaskFromDLQRequest{
		RangeCompleteHistoryTasksRequest: persistence.RangeCompleteHistoryTasksRequest{
			ShardID:             1,
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.NewImmediateKey(2),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(5),
		},
		SourceClusterName: "src",
	})
	if err != nil {
		t.Fatalf("RangeDeleteReplicationTaskFromDLQ: %v", err)
	}
	resp, _ := s.GetReplicationTasksFromDLQ(context.Background(), &persistence.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{
			ShardID:             1,
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.NewImmediateKey(0),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(100),
			BatchSize:           100,
		},
		SourceClusterName: "src",
	})
	if len(resp.Tasks) != 2 {
		t.Fatalf("want 2 remaining (1, 5); got %d", len(resp.Tasks))
	}
}

// TestIsReplicationDLQEmpty covers the helper used by reconciliation
// to short-circuit empty DLQs.
func TestIsReplicationDLQEmpty(t *testing.T) {
	db := startDB(t)
	s := executionstore.New(db, "cluster0")
	if err := executionstore.EnsureDLQIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	empty, err := s.IsReplicationDLQEmpty(context.Background(), &persistence.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{
			ShardID:      1,
			TaskCategory: tasks.CategoryReplication,
		},
		SourceClusterName: "empty-src",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !empty {
		t.Error("empty DLQ should report empty=true")
	}
	putDLQ(t, s, 1, "nonempty-src", 42, "n")
	notEmpty, _ := s.IsReplicationDLQEmpty(context.Background(), &persistence.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{
			ShardID:      1,
			TaskCategory: tasks.CategoryReplication,
		},
		SourceClusterName: "nonempty-src",
	})
	if notEmpty {
		t.Error("non-empty DLQ should report empty=false")
	}
}
