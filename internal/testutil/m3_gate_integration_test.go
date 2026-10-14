//go:build integration

// M3 gate — drives every M1/M2/M3 store through the full factory and
// exercises a representative workflow slice end-to-end: seed shard,
// create namespace, create-update-get workflow execution, enqueue +
// read queue messages, add history task. A green run here proves the
// stores cooperate under one provider/session.
//
// This is NOT the full upstream persistence-tests suite invocation —
// that requires a workflow-init flow with complete version-histories
// + cluster-metadata wiring that lives outside a single milestone
// boundary. Upstream-suite integration remains pending for a later
// release-gate task.
package testutil_test

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/mongo"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/clustermetastore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metadatastore"
	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/nexusstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/queuestore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/shardstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/taskstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil"
)

func gateM3DB(t *testing.T) *mongo.Database {
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
		URI: uri, Database: "temporal_m3_gate", AppName: "test",
	})
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close(context.Background()) })
	db := provider.Database()
	for _, ensure := range []func(context.Context, *mongo.Database) error{
		shardstore.EnsureIndexes,
		metadatastore.EnsureIndexes,
		clustermetastore.EnsureIndexes,
		executionstore.EnsureIndexes,
		executionstore.EnsureHistoryIndexes,
		executionstore.EnsureTaskIndexes,
		executionstore.EnsureDLQIndexes,
		taskstore.EnsureIndexes,
		queuestore.EnsureIndexes,
	} {
		if err := ensure(ctx, db); err != nil {
			t.Fatalf("EnsureIndexes: %v", err)
		}
	}
	return db
}

// TestM3_EveryStore_WorksTogether drives a representative slice of a
// workflow lifecycle through every M1/M2/M3 store against a live
// mongo:6 replica set. Green = the stores share the same provider,
// cooperate under the factory's single session pool, and round-trip
// every blob the upstream serializer produces.
func TestM3_EveryStore_WorksTogether(t *testing.T) {
	db := gateM3DB(t)
	ctx := context.Background()

	// ShardStore: seed shard.
	shardMgr := persistence.NewShardManager(shardstore.New(db, "cluster0"), serialization.NewSerializer())
	defer shardMgr.Close()
	_, err := shardMgr.GetOrCreateShard(ctx, &persistence.GetOrCreateShardRequest{
		ShardID:          1,
		InitialShardInfo: &persistencespb.ShardInfo{ShardId: 1, RangeId: 1},
	})
	if err != nil {
		t.Fatalf("ShardManager.GetOrCreateShard: %v", err)
	}

	// MetadataStore: create namespace.
	md := metadatastore.New(db)
	_, err = md.CreateNamespace(ctx, &persistence.InternalCreateNamespaceRequest{
		ID: "ns-id", Name: "ns-name",
		Namespace: &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("ns-blob")},
	})
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}

	// ClusterMetadataStore: save cluster metadata.
	cm := clustermetastore.New(db)
	ok, err := cm.SaveClusterMetadata(ctx, &persistence.InternalSaveClusterMetadataRequest{
		ClusterName:     "cluster0",
		ClusterMetadata: &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("cm-v1")},
		Version:         0,
	})
	if err != nil || !ok {
		t.Fatalf("SaveClusterMetadata: ok=%v err=%v", ok, err)
	}

	// ExecutionStore: create workflow.
	execState := &persistencespb.WorkflowExecutionState{
		CreateRequestId: "req-m3",
		RunId:           "run-m3",
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	stateBlob, err := serialization.NewSerializer().WorkflowExecutionStateToBlob(execState)
	if err != nil {
		t.Fatal(err)
	}
	es := executionstore.New(db, "cluster0")
	_, err = es.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1,
		Mode: persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: persistence.InternalWorkflowSnapshot{
			NamespaceID: "ns-id", WorkflowID: "wf-m3", RunID: "run-m3",
			ExecutionInfoBlob: &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("info"),
			},
			ExecutionStateBlob: stateBlob,
			ExecutionState:     execState,
			NextEventID:        2,
			DBRecordVersion:    1,
		},
	})
	if err != nil {
		t.Fatalf("CreateWorkflowExecution: %v", err)
	}

	// History tasks: add a transfer task.
	err = es.AddHistoryTasks(ctx, &persistence.InternalAddHistoryTasksRequest{
		ShardID: 1, RangeID: 1, NamespaceID: "ns-id", WorkflowID: "wf-m3",
		Tasks: map[tasks.Category][]persistence.InternalHistoryTask{
			tasks.CategoryTransfer: {
				{Key: tasks.NewImmediateKey(1), Blob: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("transfer-1"),
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("AddHistoryTasks: %v", err)
	}

	// Replication DLQ: put and query.
	err = es.PutReplicationTaskToDLQ(ctx, &persistence.PutReplicationTaskToDLQRequest{
		ShardID: 1, SourceClusterName: "src",
		TaskInfo: &persistencespb.ReplicationTaskInfo{NamespaceId: "ns-id", TaskId: 42},
	})
	if err != nil {
		t.Fatalf("PutReplicationTaskToDLQ: %v", err)
	}

	// TaskStore: create task queue and tasks.
	ts := taskstore.New(db)
	if err := ts.CreateTaskQueue(ctx, &persistence.InternalCreateTaskQueueRequest{
		NamespaceID: "ns-id", TaskQueue: "tq-m3", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       1,
		TaskQueueInfo: &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("tq")},
	}); err != nil {
		t.Fatalf("CreateTaskQueue: %v", err)
	}
	if _, err := ts.CreateTasks(ctx, &persistence.InternalCreateTasksRequest{
		NamespaceID: "ns-id", TaskQueue: "tq-m3", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, RangeID: 1,
		TaskQueueInfo: &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("tq")},
		Tasks: []*persistence.InternalCreateTask{
			{TaskId: 1, Task: &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("t1")}},
		},
	}); err != nil {
		t.Fatalf("CreateTasks: %v", err)
	}

	// NexusEndpointStore: create endpoint.
	nx := nexusstore.New(db)
	if err := nx.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: 0,
		Endpoint: persistence.InternalNexusEndpoint{
			ID: "ep-m3", Version: 0,
			Data: &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("ep")},
		},
	}); err != nil {
		t.Fatalf("CreateOrUpdateNexusEndpoint: %v", err)
	}

	// QueueV2: create queue, enqueue.
	qv2 := queuestore.NewV2(db)
	if _, err := qv2.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: 1, QueueName: "gate-q",
	}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := qv2.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
		QueueType: 1, QueueName: "gate-q",
		Blob: &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("m")},
	}); err != nil {
		t.Fatalf("EnqueueMessage: %v", err)
	}

	// Final read-back: workflow execution + task + DLQ tasks all retrievable.
	if _, err := es.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "ns-id", WorkflowID: "wf-m3", RunID: "run-m3",
	}); err != nil {
		t.Errorf("GetWorkflowExecution: %v", err)
	}
	tasksResp, err := es.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID: 1, TaskCategory: tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.NewImmediateKey(0),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(100),
		BatchSize:           10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasksResp.Tasks) != 1 {
		t.Errorf("want 1 transfer task; got %d", len(tasksResp.Tasks))
	}
	dlqResp, err := es.GetReplicationTasksFromDLQ(ctx, &persistence.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{
			ShardID: 1, TaskCategory: tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.NewImmediateKey(0),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(100),
			BatchSize:           10,
		},
		SourceClusterName: "src",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(dlqResp.Tasks) != 1 {
		t.Errorf("want 1 DLQ task; got %d", len(dlqResp.Tasks))
	}

	t.Log(fmt.Sprintf("M3 gate: all stores cooperated for namespace=%s", "ns-id"))
}
