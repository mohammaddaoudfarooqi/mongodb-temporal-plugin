//go:build integration

// Task-store regression tests.
package taskstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/taskstore"
)

func createQueue(t *testing.T, s *taskstore.Stub, rangeID int64) {
	t.Helper()
	if err := s.CreateTaskQueue(context.Background(), &persistence.InternalCreateTaskQueueRequest{
		NamespaceID: "ns", TaskQueue: "q", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, RangeID: rangeID,
		TaskQueueInfo: blob("info-v1"),
	}); err != nil {
		t.Fatal(err)
	}
}

func createTasksReq(rangeID int64, info string, ids ...int64) *persistence.InternalCreateTasksRequest {
	req := &persistence.InternalCreateTasksRequest{
		NamespaceID: "ns", TaskQueue: "q", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID: rangeID, TaskQueueInfo: blob(info),
	}
	for _, id := range ids {
		req.Tasks = append(req.Tasks, &persistence.InternalCreateTask{TaskId: id, Task: blob("t")})
	}
	return req
}

func countTasks(t *testing.T, s *taskstore.Stub) int {
	t.Helper()
	resp, err := s.GetTasks(context.Background(), &persistence.GetTasksRequest{
		NamespaceID: "ns", TaskQueue: "q", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinTaskID: 0, ExclusiveMaxTaskID: 1 << 40, PageSize: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	return len(resp.Tasks)
}

func TestCreateTasks_StaleRangeID_ConditionFailed(t *testing.T) {
	db := startDB(t)
	s := taskstore.New(db)
	ctx := context.Background()
	createQueue(t, s, 1)
	// A new owner bumps the range to 2.
	if _, err := s.UpdateTaskQueue(ctx, &persistence.InternalUpdateTaskQueueRequest{
		NamespaceID: "ns", TaskQueue: "q", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID: 2, PrevRangeID: 1, TaskQueueInfo: blob("info-v2"),
	}); err != nil {
		t.Fatal(err)
	}
	_, err := s.CreateTasks(ctx, createTasksReq(1, "stale-info", 100, 101))
	var cf *persistence.ConditionFailedError
	if !errors.As(err, &cf) {
		t.Fatalf("stale owner CreateTasks: want ConditionFailedError; got %T %v", err, err)
	}
	if n := countTasks(t, s); n != 0 {
		t.Fatalf("stale owner inserted %d tasks", n)
	}
	q, err := s.GetTaskQueue(ctx, &persistence.InternalGetTaskQueueRequest{
		NamespaceID: "ns", TaskQueue: "q", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(q.TaskQueueInfo.Data) != "info-v2" {
		t.Fatalf("stale owner overwrote queue info: %q", q.TaskQueueInfo.Data)
	}
}

func TestCreateTasks_CurrentOwner_UpdatesQueueInfo(t *testing.T) {
	db := startDB(t)
	s := taskstore.New(db)
	ctx := context.Background()
	createQueue(t, s, 1)
	if _, err := s.CreateTasks(ctx, createTasksReq(1, "info-after-create", 5)); err != nil {
		t.Fatal(err)
	}
	q, err := s.GetTaskQueue(ctx, &persistence.InternalGetTaskQueueRequest{
		NamespaceID: "ns", TaskQueue: "q", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(q.TaskQueueInfo.Data) != "info-after-create" || countTasks(t, s) != 1 {
		t.Fatalf("queue info=%q tasks=%d", q.TaskQueueInfo.Data, countTasks(t, s))
	}
}

func TestCreateTasks_DuplicateTaskID_Errors(t *testing.T) {
	db := startDB(t)
	s := taskstore.New(db)
	ctx := context.Background()
	createQueue(t, s, 1)
	if _, err := s.CreateTasks(ctx, createTasksReq(1, "i", 7)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err := s.CreateTasks(ctx, createTasksReq(1, "i", 7))
	if err == nil {
		t.Fatal("duplicate task ID was silently accepted")
	}
	var unavailable *serviceerror.Unavailable
	if errors.As(err, &unavailable) || time.Since(start) > 10*time.Second {
		t.Fatalf("duplicate surfaced as retryable after %v: %T %v", time.Since(start), err, err)
	}
}

func userDataUpdate(ns string, updates map[string]*persistence.InternalSingleTaskQueueUserDataUpdate) *persistence.InternalUpdateTaskQueueUserDataRequest {
	return &persistence.InternalUpdateTaskQueueUserDataRequest{NamespaceID: ns, Updates: updates}
}

func getVersion(t *testing.T, s *taskstore.Stub, tq string) int64 {
	t.Helper()
	resp, err := s.GetTaskQueueUserData(context.Background(), &persistence.GetTaskQueueUserDataRequest{NamespaceID: "ns", TaskQueue: tq})
	if err != nil {
		t.Fatal(err)
	}
	return resp.Version
}

func TestUpdateUserData_Success_IncrementsVersion(t *testing.T) {
	db := startDB(t)
	s := taskstore.New(db)
	ctx := context.Background()
	for expected := int64(0); expected < 3; expected++ {
		applied := false
		if err := s.UpdateTaskQueueUserData(ctx, userDataUpdate("ns", map[string]*persistence.InternalSingleTaskQueueUserDataUpdate{
			"tq": {Version: expected, UserData: blob("d"), Applied: &applied},
		})); err != nil {
			t.Fatalf("update at version %d: %v", expected, err)
		}
		if !applied {
			t.Fatal("Applied not set after a committed update")
		}
		if got := getVersion(t, s, "tq"); got != expected+1 {
			t.Fatalf("stored version = %d; want %d", got, expected+1)
		}
	}
}

// One conflicting entry fails the whole batch atomically.
func TestUpdateUserData_StaleVersion_ConditionFailedAndConflicting(t *testing.T) {
	db := startDB(t)
	s := taskstore.New(db)
	ctx := context.Background()
	for _, tq := range []string{"a", "b"} {
		if err := s.UpdateTaskQueueUserData(ctx, userDataUpdate("ns", map[string]*persistence.InternalSingleTaskQueueUserDataUpdate{
			tq: {Version: 0, UserData: blob("v1")},
		})); err != nil {
			t.Fatal(err)
		}
	}
	appliedA, appliedB := true, true // stale values from an earlier call
	conflictA, conflictB := false, false
	err := s.UpdateTaskQueueUserData(ctx, userDataUpdate("ns", map[string]*persistence.InternalSingleTaskQueueUserDataUpdate{
		"a": {Version: 1, UserData: blob("v2"), Applied: &appliedA, Conflicting: &conflictA},
		"b": {Version: 0, UserData: blob("v2"), Applied: &appliedB, Conflicting: &conflictB}, // stale
	}))
	var cf *persistence.ConditionFailedError
	if !errors.As(err, &cf) {
		t.Fatalf("want ConditionFailedError; got %T %v", err, err)
	}
	if appliedA || appliedB {
		t.Fatalf("Applied left true after rollback: a=%v b=%v", appliedA, appliedB)
	}
	if conflictA || !conflictB {
		t.Fatalf("Conflicting flags: a=%v b=%v; want a=false b=true", conflictA, conflictB)
	}
	if getVersion(t, s, "a") != 1 || getVersion(t, s, "b") != 1 {
		t.Fatal("conflicting batch was partially applied")
	}
}
