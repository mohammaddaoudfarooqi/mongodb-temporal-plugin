//go:build integration

// Integration tests for history tasks. Keyed on shard + category,
// scanned ordered by fire_time + task_id. Immediate tasks use epoch 0
// for fire_time so they sort purely by task_id.
package executionstore_test

import (
	"context"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
)

func makeTaskBlob(label string) *commonpb.DataBlob {
	return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte(label)}
}

// TestAddHistoryTasks_TransferImmediate_Roundtrip/066:
// inserting a batch of transfer tasks (immediate category) and reading
// them back in TaskID order.
func TestAddHistoryTasks_TransferImmediate_Roundtrip(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	if err := executionstore.EnsureTaskIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	req := &persistence.InternalAddHistoryTasksRequest{
		ShardID:     1,
		RangeID:     1,
		NamespaceID: "ns",
		WorkflowID:  "wf",
		Tasks: map[tasks.Category][]persistence.InternalHistoryTask{
			tasks.CategoryTransfer: {
				{Key: tasks.NewImmediateKey(100), Blob: makeTaskBlob("transfer-100")},
				{Key: tasks.NewImmediateKey(101), Blob: makeTaskBlob("transfer-101")},
				{Key: tasks.NewImmediateKey(102), Blob: makeTaskBlob("transfer-102")},
			},
		},
	}
	if err := s.AddHistoryTasks(ctx, req); err != nil {
		t.Fatalf("AddHistoryTasks: %v", err)
	}

	resp, err := s.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             1,
		TaskCategory:        tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.NewImmediateKey(0),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(1000),
		BatchSize:           100,
	})
	if err != nil {
		t.Fatalf("GetHistoryTasks: %v", err)
	}
	if len(resp.Tasks) != 3 {
		t.Fatalf("want 3; got %d", len(resp.Tasks))
	}
	for i, task := range resp.Tasks {
		wantID := int64(100 + i)
		if task.Key.TaskID != wantID {
			t.Errorf("task[%d] TaskID: got %d want %d", i, task.Key.TaskID, wantID)
		}
	}
}

// TestAddHistoryTasks_TimerScheduled_OrderedByFireTime covers the
// scheduled-category path: timer tasks carry a fire_time, and the
// compound index sorts by fire_time THEN task_id.
func TestAddHistoryTasks_TimerScheduled_OrderedByFireTime(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	if err := executionstore.EnsureTaskIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	err := s.AddHistoryTasks(ctx, &persistence.InternalAddHistoryTasksRequest{
		ShardID: 1, RangeID: 1, NamespaceID: "ns", WorkflowID: "wf",
		Tasks: map[tasks.Category][]persistence.InternalHistoryTask{
			tasks.CategoryTimer: {
				{Key: tasks.NewKey(t0.Add(10*time.Second), 3), Blob: makeTaskBlob("late")},
				{Key: tasks.NewKey(t0.Add(1*time.Second), 2), Blob: makeTaskBlob("early")},
				{Key: tasks.NewKey(t0.Add(5*time.Second), 1), Blob: makeTaskBlob("middle")},
			},
		},
	})
	if err != nil {
		t.Fatalf("AddHistoryTasks: %v", err)
	}

	resp, err := s.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             1,
		TaskCategory:        tasks.CategoryTimer,
		InclusiveMinTaskKey: tasks.NewKey(t0, 0),
		ExclusiveMaxTaskKey: tasks.NewKey(t0.Add(time.Minute), 0),
		BatchSize:           100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Tasks) != 3 {
		t.Fatalf("want 3 timer tasks; got %d", len(resp.Tasks))
	}
	wantBlobs := []string{"early", "middle", "late"}
	for i, task := range resp.Tasks {
		if string(task.Blob.Data) != wantBlobs[i] {
			t.Errorf("timer[%d] ordering: got %q want %q", i, string(task.Blob.Data), wantBlobs[i])
		}
	}
}

func TestCompleteHistoryTask_Deletes(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	if err := executionstore.EnsureTaskIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_ = s.AddHistoryTasks(ctx, &persistence.InternalAddHistoryTasksRequest{
		ShardID: 1, RangeID: 1, NamespaceID: "ns", WorkflowID: "wf",
		Tasks: map[tasks.Category][]persistence.InternalHistoryTask{
			tasks.CategoryTransfer: {
				{Key: tasks.NewImmediateKey(200), Blob: makeTaskBlob("t1")},
				{Key: tasks.NewImmediateKey(201), Blob: makeTaskBlob("t2")},
			},
		},
	})
	err := s.CompleteHistoryTask(ctx, &persistence.CompleteHistoryTaskRequest{
		ShardID:      1,
		TaskCategory: tasks.CategoryTransfer,
		TaskKey:      tasks.NewImmediateKey(200),
	})
	if err != nil {
		t.Fatalf("CompleteHistoryTask: %v", err)
	}
	// 200 gone, 201 remains.
	resp, _ := s.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID: 1, TaskCategory: tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.NewImmediateKey(0),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(1000),
		BatchSize:           10,
	})
	if len(resp.Tasks) != 1 || resp.Tasks[0].Key.TaskID != 201 {
		t.Errorf("complete did not delete task 200; remaining: %v", resp.Tasks)
	}
	// Non-existent task → nil.
	err = s.CompleteHistoryTask(ctx, &persistence.CompleteHistoryTaskRequest{
		ShardID:      1,
		TaskCategory: tasks.CategoryTransfer,
		TaskKey:      tasks.NewImmediateKey(999),
	})
	if err != nil {
		t.Errorf("delete of non-existent task should be nil; got %v", err)
	}
}

func TestRangeCompleteHistoryTasks_RangeDelete(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	if err := executionstore.EnsureTaskIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	batch := []persistence.InternalHistoryTask{}
	for i := int64(1); i <= 10; i++ {
		batch = append(batch, persistence.InternalHistoryTask{
			Key:  tasks.NewImmediateKey(i),
			Blob: makeTaskBlob("batch"),
		})
	}
	_ = s.AddHistoryTasks(ctx, &persistence.InternalAddHistoryTasksRequest{
		ShardID: 1, RangeID: 1, NamespaceID: "ns", WorkflowID: "wf",
		Tasks: map[tasks.Category][]persistence.InternalHistoryTask{
			tasks.CategoryTransfer: batch,
		},
	})

	// Delete [3, 7).
	err := s.RangeCompleteHistoryTasks(ctx, &persistence.RangeCompleteHistoryTasksRequest{
		ShardID:             1,
		TaskCategory:        tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.NewImmediateKey(3),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(7),
	})
	if err != nil {
		t.Fatalf("RangeCompleteHistoryTasks: %v", err)
	}

	resp, _ := s.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID: 1, TaskCategory: tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.NewImmediateKey(0),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(100),
		BatchSize:           100,
	})
	if len(resp.Tasks) != 6 {
		t.Fatalf("want 6 remaining (1,2,7,8,9,10); got %d", len(resp.Tasks))
	}
	wantIDs := []int64{1, 2, 7, 8, 9, 10}
	for i, task := range resp.Tasks {
		if task.Key.TaskID != wantIDs[i] {
			t.Errorf("remaining[%d] TaskID: got %d want %d", i, task.Key.TaskID, wantIDs[i])
		}
	}
}

// TestGetHistoryTasks_Pagination's token path.
func TestGetHistoryTasks_Pagination(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	if err := executionstore.EnsureTaskIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	batch := []persistence.InternalHistoryTask{}
	for i := int64(1); i <= 10; i++ {
		batch = append(batch, persistence.InternalHistoryTask{
			Key: tasks.NewImmediateKey(i), Blob: makeTaskBlob("x"),
		})
	}
	_ = s.AddHistoryTasks(ctx, &persistence.InternalAddHistoryTasksRequest{
		ShardID: 1, RangeID: 1, NamespaceID: "ns", WorkflowID: "wf",
		Tasks: map[tasks.Category][]persistence.InternalHistoryTask{
			tasks.CategoryTransfer: batch,
		},
	})

	// Page 1: 4 tasks.
	p1, err := s.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID: 1, TaskCategory: tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.NewImmediateKey(0),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(100),
		BatchSize:           4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(p1.Tasks) != 4 {
		t.Fatalf("page 1 size: got %d want 4", len(p1.Tasks))
	}
	if len(p1.NextPageToken) == 0 {
		t.Fatal("expected NextPageToken after first page")
	}

	// Page 2: another 4.
	p2, err := s.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID: 1, TaskCategory: tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.NewImmediateKey(0),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(100),
		BatchSize:           4,
		NextPageToken:       p1.NextPageToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.Tasks) != 4 {
		t.Errorf("page 2 size: got %d want 4", len(p2.Tasks))
	}
	if p2.Tasks[0].Key.TaskID != 5 {
		t.Errorf("page 2 first TaskID: got %d want 5", p2.Tasks[0].Key.TaskID)
	}
}
