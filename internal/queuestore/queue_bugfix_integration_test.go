//go:build integration

package queuestore_test

import (
	"context"
	"errors"
	"testing"

	"go.temporal.io/server/common/persistence"
	upstream "go.temporal.io/server/common/persistence/tests"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/queuestore"
)

// TestUpstream_QueueV2Suite with the upstream suite,
// which also runs the history-task-queue manager suite on top of QueueV2.
func TestUpstream_QueueV2Suite(t *testing.T) {
	upstream.RunQueueV2TestSuite(t, queuestore.NewV2(startDB(t)))
}

func TestQueue_UpdateAckLevelStaleVersion_ConditionFailed(t *testing.T) {
	db := startDB(t)
	ctx := context.Background()
	for name, ops := range map[string]struct {
		get func(*queuestore.Stub) (*persistence.InternalQueueMetadata, error)
		set func(*queuestore.Stub, *persistence.InternalQueueMetadata) error
	}{
		"ack": {
			get: func(q *queuestore.Stub) (*persistence.InternalQueueMetadata, error) { return q.GetAckLevels(ctx) },
			set: func(q *queuestore.Stub, md *persistence.InternalQueueMetadata) error {
				return q.UpdateAckLevel(ctx, md)
			},
		},
		"dlq": {
			get: func(q *queuestore.Stub) (*persistence.InternalQueueMetadata, error) { return q.GetDLQAckLevels(ctx) },
			set: func(q *queuestore.Stub, md *persistence.InternalQueueMetadata) error {
				return q.UpdateDLQAckLevel(ctx, md)
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			q := queuestore.New(db, persistence.NamespaceReplicationQueueType)
			if err := q.Init(ctx, blob("init")); err != nil {
				t.Fatal(err)
			}
			md, err := ops.get(q)
			if err != nil {
				t.Fatal(err)
			}
			if err := ops.set(q, &persistence.InternalQueueMetadata{Blob: blob("a1"), Version: md.Version}); err != nil {
				t.Fatalf("first update: %v", err)
			}
			after, err := ops.get(q)
			if err != nil {
				t.Fatal(err)
			}
			if after.Version != md.Version+1 {
				t.Fatalf("version = %d; want %d", after.Version, md.Version+1)
			}
			err = ops.set(q, &persistence.InternalQueueMetadata{Blob: blob("stale"), Version: md.Version})
			var cf *persistence.ConditionFailedError
			if !errors.As(err, &cf) {
				t.Fatalf("stale update: want ConditionFailedError; got %T %v", err, err)
			}
		})
	}
}
