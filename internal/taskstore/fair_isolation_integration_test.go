//go:build integration

package taskstore_test

import (
	"context"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/taskstore"
)

// TestFairStore_IsolatedFromClassic: matching runs
// the classic and fair backlogs of one task queue side by side, each
// with its own RangeID lease, so their rows must not collide.
func TestFairStore_IsolatedFromClassic(t *testing.T) {
	db := startDB(t)
	ctx := context.Background()
	classic, fair := taskstore.New(db), taskstore.NewFair(db)
	key := func(rangeID int64) *persistence.InternalCreateTaskQueueRequest {
		return &persistence.InternalCreateTaskQueueRequest{
			NamespaceID: "ns", TaskQueue: "tq", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			RangeID: rangeID, TaskQueueInfo: blob("info"),
		}
	}
	if err := classic.CreateTaskQueue(ctx, key(1)); err != nil {
		t.Fatalf("classic create: %v", err)
	}
	if err := fair.CreateTaskQueue(ctx, key(7)); err != nil {
		t.Fatalf("fair create collided with the classic queue: %v", err)
	}
	update := func(s persistence.TaskStore, prev int64) error {
		_, err := s.UpdateTaskQueue(ctx, &persistence.InternalUpdateTaskQueueRequest{
			NamespaceID: "ns", TaskQueue: "tq", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			RangeID: prev + 1, PrevRangeID: prev, TaskQueueInfo: blob("info"),
		})
		return err
	}
	if err := update(fair, 7); err != nil {
		t.Fatalf("fair lease renewal fenced by the classic queue: %v", err)
	}
	if err := update(classic, 1); err != nil {
		t.Fatalf("classic lease renewal fenced by the fair queue: %v", err)
	}
	if _, err := fair.CreateTasks(ctx, &persistence.InternalCreateTasksRequest{
		NamespaceID: "ns", TaskQueue: "tq", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID: 8, TaskQueueInfo: blob("info"),
		Tasks: []*persistence.InternalCreateTask{{TaskId: 100, Task: blob("t")}},
	}); err != nil {
		t.Fatalf("fair create tasks: %v", err)
	}
	got, err := classic.GetTasks(ctx, &persistence.GetTasksRequest{
		NamespaceID: "ns", TaskQueue: "tq", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinTaskID: 0, ExclusiveMaxTaskID: 1 << 40, PageSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Tasks) != 0 {
		t.Fatalf("classic backlog sees %d fair tasks, want 0", len(got.Tasks))
	}
}
