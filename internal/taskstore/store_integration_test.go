//go:build integration

// Integration tests for TaskStore.
package taskstore_test

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/mongo"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/taskstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil"
)

func startDB(t *testing.T) *mongo.Database {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, err := mongodb.Run(ctx, testutil.MongoImage(), mongodb.WithReplicaSet("rs0"))
	if err != nil {
		t.Fatalf("start mongo: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	raw, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	u, _ := url.Parse(raw)
	q := u.Query()
	q.Del("replicaSet")
	u.RawQuery = q.Encode()
	uri := strings.TrimRight(u.String(), "?") + "?directConnection=true"
	provider, err := internalmongo.NewClientProvider(ctx, internalmongo.ClientConfig{
		URI: uri, Database: "temporal_task_test", AppName: "test",
	})
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close(context.Background()) })
	db := provider.Database()
	if err := testutil.WaitForPrimary(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := taskstore.EnsureIndexes(ctx, db); err != nil {
		t.Fatalf("taskstore EnsureIndexes: %v", err)
	}
	return db
}

func blob(s string) *commonpb.DataBlob {
	return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte(s)}
}

func TestCreateTaskQueue_FreshAndDuplicate(t *testing.T) {
	db := startDB(t)
	s := taskstore.New(db)
	ctx := context.Background()

	err := s.CreateTaskQueue(ctx, &persistence.InternalCreateTaskQueueRequest{
		NamespaceID:   "ns-1",
		TaskQueue:     "tq-1",
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       1,
		TaskQueueInfo: blob("q1"),
	})
	if err != nil {
		t.Fatalf("CreateTaskQueue: %v", err)
	}
	// Duplicate → ConditionFailedError.
	err = s.CreateTaskQueue(ctx, &persistence.InternalCreateTaskQueueRequest{
		NamespaceID: "ns-1", TaskQueue: "tq-1", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, RangeID: 1,
		TaskQueueInfo: blob("q1"),
	})
	if err == nil {
		t.Fatal("expected ConditionFailedError on duplicate create")
	}
	var cfe *persistence.ConditionFailedError
	if !errors.As(err, &cfe) {
		t.Fatalf("expected ConditionFailedError; got %T: %v", err, err)
	}
}

// TestUpdateTaskQueue_CAS: RangeID is the CAS guard.
func TestUpdateTaskQueue_CAS(t *testing.T) {
	db := startDB(t)
	s := taskstore.New(db)
	ctx := context.Background()

	if err := s.CreateTaskQueue(ctx, &persistence.InternalCreateTaskQueueRequest{
		NamespaceID: "ns", TaskQueue: "q", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, RangeID: 5,
		TaskQueueInfo: blob("v1"),
	}); err != nil {
		t.Fatal(err)
	}
	// Matching PrevRangeID succeeds.
	_, err := s.UpdateTaskQueue(ctx, &persistence.InternalUpdateTaskQueueRequest{
		NamespaceID: "ns", TaskQueue: "q", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID: 6, PrevRangeID: 5,
		TaskQueueInfo: blob("v2"),
	})
	if err != nil {
		t.Fatalf("UpdateTaskQueue (matching prev): %v", err)
	}
	// Stale PrevRangeID fails.
	_, err = s.UpdateTaskQueue(ctx, &persistence.InternalUpdateTaskQueueRequest{
		NamespaceID: "ns", TaskQueue: "q", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID: 7, PrevRangeID: 5, // stored is 6 now
		TaskQueueInfo: blob("v3"),
	})
	if err == nil {
		t.Fatal("expected stale-PrevRangeID error")
	}
}

func TestGetTaskQueue_NotFound(t *testing.T) {
	db := startDB(t)
	s := taskstore.New(db)
	_, err := s.GetTaskQueue(context.Background(), &persistence.InternalGetTaskQueueRequest{
		NamespaceID: "nope", TaskQueue: "nope", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	})
	var nf *serviceerror.NotFound
	if !errors.As(err, &nf) {
		t.Fatalf("expected NotFound; got %T: %v", err, err)
	}
}

func TestDeleteTaskQueue_RemovesQueueAndTasks(t *testing.T) {
	db := startDB(t)
	s := taskstore.New(db)
	ctx := context.Background()

	if err := s.CreateTaskQueue(ctx, &persistence.InternalCreateTaskQueueRequest{
		NamespaceID: "ns", TaskQueue: "todelete", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, RangeID: 1,
		TaskQueueInfo: blob("q"),
	}); err != nil {
		t.Fatal(err)
	}
	_, err := s.CreateTasks(ctx, &persistence.InternalCreateTasksRequest{
		NamespaceID: "ns", TaskQueue: "todelete", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, RangeID: 1,
		TaskQueueInfo: blob("q"),
		Tasks: []*persistence.InternalCreateTask{
			{TaskId: 1, Task: blob("t1")},
			{TaskId: 2, Task: blob("t2")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Delete queue.
	err = s.DeleteTaskQueue(ctx, &persistence.DeleteTaskQueueRequest{
		TaskQueue: &persistence.TaskQueueKey{
			NamespaceID:   "ns",
			TaskQueueName: "todelete",
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		},
		RangeID: 1,
	})
	if err != nil {
		t.Fatalf("DeleteTaskQueue: %v", err)
	}

	// Queue gone → NotFound.
	if _, err := s.GetTaskQueue(ctx, &persistence.InternalGetTaskQueueRequest{
		NamespaceID: "ns", TaskQueue: "todelete", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	}); err == nil {
		t.Error("queue should be gone after DeleteTaskQueue")
	}
	// Tasks gone — GetTasks returns empty.
	resp, _ := s.GetTasks(ctx, &persistence.GetTasksRequest{
		NamespaceID: "ns", TaskQueue: "todelete", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinTaskID: 0, ExclusiveMaxTaskID: 1000, PageSize: 100,
	})
	if resp != nil && len(resp.Tasks) != 0 {
		t.Errorf("tasks not deleted; got %d remaining", len(resp.Tasks))
	}
}

// TestCreateAndGetTasks.
func TestCreateAndGetTasks(t *testing.T) {
	db := startDB(t)
	s := taskstore.New(db)
	ctx := context.Background()

	_ = s.CreateTaskQueue(ctx, &persistence.InternalCreateTaskQueueRequest{
		NamespaceID: "ns", TaskQueue: "q", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, RangeID: 1,
		TaskQueueInfo: blob("q"),
	})
	_, err := s.CreateTasks(ctx, &persistence.InternalCreateTasksRequest{
		NamespaceID: "ns", TaskQueue: "q", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, RangeID: 1,
		TaskQueueInfo: blob("q"),
		Tasks: []*persistence.InternalCreateTask{
			{TaskId: 10, Task: blob("ten")},
			{TaskId: 20, Task: blob("twenty")},
			{TaskId: 30, Task: blob("thirty")},
		},
	})
	if err != nil {
		t.Fatalf("CreateTasks: %v", err)
	}

	resp, err := s.GetTasks(ctx, &persistence.GetTasksRequest{
		NamespaceID: "ns", TaskQueue: "q", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinTaskID: 0, ExclusiveMaxTaskID: 100, PageSize: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Tasks) != 3 {
		t.Fatalf("want 3 tasks; got %d", len(resp.Tasks))
	}
	// Tasks are returned as blob slices in ascending TaskID order.
	for i, taskBlob := range resp.Tasks {
		if taskBlob == nil {
			t.Errorf("task[%d] blob nil", i)
		}
	}
}

func TestCompleteTasksLessThan(t *testing.T) {
	db := startDB(t)
	s := taskstore.New(db)
	ctx := context.Background()

	_ = s.CreateTaskQueue(ctx, &persistence.InternalCreateTaskQueueRequest{
		NamespaceID: "ns", TaskQueue: "q", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, RangeID: 1,
		TaskQueueInfo: blob("q"),
	})
	tasks := []*persistence.InternalCreateTask{}
	for i := int64(1); i <= 10; i++ {
		tasks = append(tasks, &persistence.InternalCreateTask{TaskId: i, Task: blob("x")})
	}
	_, _ = s.CreateTasks(ctx, &persistence.InternalCreateTasksRequest{
		NamespaceID: "ns", TaskQueue: "q", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, RangeID: 1,
		TaskQueueInfo: blob("q"), Tasks: tasks,
	})

	n, err := s.CompleteTasksLessThan(ctx, &persistence.CompleteTasksLessThanRequest{
		NamespaceID: "ns", TaskQueueName: "q", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		ExclusiveMaxTaskID: 6, // delete task_id < 6
		Limit:              100,
	})
	if err != nil {
		t.Fatalf("CompleteTasksLessThan: %v", err)
	}
	if n != 5 {
		t.Errorf("want 5 deleted; got %d", n)
	}
}

// TestUserData_RoundTrip minimally.
func TestUserData_RoundTrip(t *testing.T) {
	db := startDB(t)
	s := taskstore.New(db)
	ctx := context.Background()

	err := s.UpdateTaskQueueUserData(ctx, &persistence.InternalUpdateTaskQueueUserDataRequest{
		NamespaceID: "ns",
		Updates: map[string]*persistence.InternalSingleTaskQueueUserDataUpdate{
			// Upstream contract: Version is the expected current version
			// (0 for a new queue); the store persists Version+1.
			"q-1": {Version: 0, UserData: blob("data-v1")},
		},
	})
	if err != nil {
		t.Fatalf("UpdateTaskQueueUserData: %v", err)
	}
	resp, err := s.GetTaskQueueUserData(ctx, &persistence.GetTaskQueueUserDataRequest{
		NamespaceID: "ns", TaskQueue: "q-1",
	})
	if err != nil {
		t.Fatalf("GetTaskQueueUserData: %v", err)
	}
	if resp.Version != 1 {
		t.Errorf("version round-trip: got %d", resp.Version)
	}
	if string(resp.UserData.Data) != "data-v1" {
		t.Errorf("user data round-trip: got %q", string(resp.UserData.Data))
	}
}
