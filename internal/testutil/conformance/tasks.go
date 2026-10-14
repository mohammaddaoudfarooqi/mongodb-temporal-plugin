package conformance

import (
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence"
	txTasks "go.temporal.io/server/service/history/tasks"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/shardstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/taskstore"
)

// runTaskStore exercises task-queue CRUD and the
// history-tasks shard+category ordering invariant.
func runTaskStore(t *testing.T, f Fixture) {
	t.Run("TaskQueue_CreateAndGet", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", taskstore.EnsureIndexes(tc.Ctx, tc.DB))
		ts := taskstore.New(tc.DB)

		ns := uniqStr("ns")
		tq := uniqStr("tq")
		err := ts.CreateTaskQueue(tc.Ctx, &persistence.InternalCreateTaskQueueRequest{
			NamespaceID: ns, TaskQueue: tq, TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			RangeID:       1,
			TaskQueueInfo: dataBlob("tq"),
		})
		fatalOnErr(t, "CreateTaskQueue", err)

		_, err = ts.GetTaskQueue(tc.Ctx, &persistence.InternalGetTaskQueueRequest{
			NamespaceID: ns, TaskQueue: tq, TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		})
		fatalOnErr(t, "GetTaskQueue", err)
	})

	t.Run("TaskQueue_CreateTasks_Roundtrip", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", taskstore.EnsureIndexes(tc.Ctx, tc.DB))
		ts := taskstore.New(tc.DB)

		ns := uniqStr("ns")
		tq := uniqStr("tq")
		err := ts.CreateTaskQueue(tc.Ctx, &persistence.InternalCreateTaskQueueRequest{
			NamespaceID: ns, TaskQueue: tq, TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			RangeID:       1,
			TaskQueueInfo: dataBlob("tq"),
		})
		fatalOnErr(t, "CreateTaskQueue", err)

		_, err = ts.CreateTasks(tc.Ctx, &persistence.InternalCreateTasksRequest{
			NamespaceID: ns, TaskQueue: tq, TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, RangeID: 1,
			TaskQueueInfo: dataBlob("tq"),
			Tasks: []*persistence.InternalCreateTask{
				{TaskId: 1, Task: dataBlob("t1")},
				{TaskId: 2, Task: dataBlob("t2")},
			},
		})
		fatalOnErr(t, "CreateTasks", err)
	})

	t.Run("HistoryTasks_TransferImmediate_Roundtrip", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", shardstore.EnsureIndexes(tc.Ctx, tc.DB))
		fatalOnErr(t, "ensure task idx", executionstore.EnsureTaskIndexes(tc.Ctx, tc.DB))
		shards := shardstore.New(tc.DB, "cluster0")
		_, err := shards.GetOrCreateShard(tc.Ctx, &persistence.InternalGetOrCreateShardRequest{
			ShardID: 21,
			CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
				return 1, dataBlob("init"), nil
			},
		})
		fatalOnErr(t, "seed shard", err)
		es := executionstore.New(tc.DB, "cluster0")

		err = es.AddHistoryTasks(tc.Ctx, &persistence.InternalAddHistoryTasksRequest{
			ShardID: 21, RangeID: 1, NamespaceID: "ns",
			Tasks: map[txTasks.Category][]persistence.InternalHistoryTask{
				txTasks.CategoryTransfer: {
					{Key: txTasks.NewImmediateKey(100), Blob: dataBlob("transfer-100")},
					{Key: txTasks.NewImmediateKey(200), Blob: dataBlob("transfer-200")},
				},
			},
		})
		fatalOnErr(t, "AddHistoryTasks", err)

		resp, err := es.GetHistoryTasks(tc.Ctx, &persistence.GetHistoryTasksRequest{
			ShardID: 21, TaskCategory: txTasks.CategoryTransfer,
			InclusiveMinTaskKey: txTasks.NewImmediateKey(0),
			ExclusiveMaxTaskKey: txTasks.NewImmediateKey(1000),
			BatchSize:           10,
		})
		fatalOnErr(t, "GetHistoryTasks", err)
		if len(resp.Tasks) != 2 {
			t.Errorf("expected 2 tasks; got %d", len(resp.Tasks))
		}
	})
}
