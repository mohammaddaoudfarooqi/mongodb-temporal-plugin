//go:build integration

// Integration tests for Queue (legacy) + QueueV2.
package queuestore_test

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
	"go.temporal.io/server/common/persistence"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/queuestore"
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
		URI: uri, Database: "temporal_queue_test", AppName: "test",
	})
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close(context.Background()) })
	db := provider.Database()
	if err := testutil.WaitForPrimary(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := queuestore.EnsureIndexes(ctx, db); err != nil {
		t.Fatalf("queuestore EnsureIndexes: %v", err)
	}
	return db
}

func blob(s string) *commonpb.DataBlob {
	return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte(s)}
}

// TestQueue_EnqueueAndRead (legacy Queue).
func TestQueue_EnqueueAndRead(t *testing.T) {
	db := startDB(t)
	q := queuestore.New(db, persistence.NamespaceReplicationQueueType)
	ctx := context.Background()

	for i, s := range []string{"a", "b", "c"} {
		if err := q.EnqueueMessage(ctx, blob(s)); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	msgs, err := q.ReadMessages(ctx, -1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("want 3 messages; got %d", len(msgs))
	}
	for i, m := range msgs {
		if m.ID != int64(i+1) {
			t.Errorf("msg[%d] ID: got %d want %d", i, m.ID, i+1)
		}
	}
}

// TestQueue_DeleteMessagesBefore covers the ack-plus-delete pattern.
func TestQueue_DeleteMessagesBefore(t *testing.T) {
	db := startDB(t)
	q := queuestore.New(db, persistence.NamespaceReplicationQueueType)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_ = q.EnqueueMessage(ctx, blob("x"))
	}
	if err := q.DeleteMessagesBefore(ctx, 3); err != nil {
		t.Fatalf("DeleteMessagesBefore: %v", err)
	}
	msgs, _ := q.ReadMessages(ctx, -1, 10)
	// The bound is exclusive (upstream exclusiveMessageID):
	// messages with ID < 3 are deleted; 3, 4 and 5 remain.
	if len(msgs) != 3 {
		t.Errorf("want 3 remaining; got %d", len(msgs))
	}
}

// TestQueueV2_CreateAndEnqueueAndRead covers create, enqueue and read-back.
func TestQueueV2_CreateAndEnqueueAndRead(t *testing.T) {
	db := startDB(t)
	q := queuestore.NewV2(db)
	ctx := context.Background()

	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: persistence.QueueV2Type(1),
		QueueName: "test-queue",
	})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	for i := 0; i < 3; i++ {
		_, err := q.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
			QueueType: persistence.QueueV2Type(1),
			QueueName: "test-queue",
			Blob:      blob("msg"),
		})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	resp, err := q.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
		QueueType: persistence.QueueV2Type(1),
		QueueName: "test-queue",
		PageSize:  10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Messages) != 3 {
		t.Fatalf("want 3; got %d", len(resp.Messages))
	}
	for i, m := range resp.Messages {
		if m.MetaData.ID != int64(i) {
			t.Errorf("msg[%d] ID: got %d want %d", i, m.MetaData.ID, i)
		}
	}
}

func TestQueueV2_CreateConflict(t *testing.T) {
	db := startDB(t)
	q := queuestore.NewV2(db)
	ctx := context.Background()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: 2, QueueName: "dup",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: 2, QueueName: "dup",
	})
	if err == nil {
		t.Fatal("expected ErrQueueAlreadyExists; got nil")
	}
	if !errors.Is(err, persistence.ErrQueueAlreadyExists) {
		// Accept ConditionFailedError too — test suite may check either.
		var cfe *persistence.ConditionFailedError
		if !errors.As(err, &cfe) {
			t.Errorf("expected QueueAlreadyExists or ConditionFailedError; got %T: %v", err, err)
		}
	}
}

func TestQueueV2_ListQueues(t *testing.T) {
	db := startDB(t)
	q := queuestore.NewV2(db)
	ctx := context.Background()
	for _, name := range []string{"qA", "qB", "qC"} {
		if _, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
			QueueType: 3, QueueName: name,
		}); err != nil {
			t.Fatal(err)
		}
	}
	resp, err := q.ListQueues(ctx, &persistence.InternalListQueuesRequest{
		QueueType: 3, PageSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Queues) != 3 {
		t.Errorf("want 3 queues; got %d", len(resp.Queues))
	}
}

func TestQueueV2_RangeDeleteMessages(t *testing.T) {
	db := startDB(t)
	q := queuestore.NewV2(db)
	ctx := context.Background()
	_, _ = q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{QueueType: 4, QueueName: "q"})
	for i := 0; i < 5; i++ {
		_, _ = q.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
			QueueType: 4, QueueName: "q", Blob: blob("x"),
		})
	}
	res, err := q.RangeDeleteMessages(ctx, &persistence.InternalRangeDeleteMessagesRequest{
		QueueType:                   4,
		QueueName:                   "q",
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{ID: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.MessagesDeleted != 3 {
		t.Errorf("want 3 deleted (IDs 0..2); got %d", res.MessagesDeleted)
	}
}
