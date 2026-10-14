package conformance

import (
	"testing"

	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/queuestore"
)

// runQueueStore exercises queue v2 create + enqueue
// + read-back.
func runQueueStore(t *testing.T, f Fixture) {
	t.Run("V2_CreateAndEnqueue", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", queuestore.EnsureIndexes(tc.Ctx, tc.DB))
		qv2 := queuestore.NewV2(tc.DB)

		name := uniqStr("q")
		_, err := qv2.CreateQueue(tc.Ctx, &persistence.InternalCreateQueueRequest{
			QueueType: persistence.QueueTypeHistoryNormal, QueueName: name,
		})
		fatalOnErr(t, "CreateQueue", err)

		enqResp, err := qv2.EnqueueMessage(tc.Ctx, &persistence.InternalEnqueueMessageRequest{
			QueueType: persistence.QueueTypeHistoryNormal, QueueName: name, Blob: dataBlob("m1"),
		})
		fatalOnErr(t, "EnqueueMessage", err)
		if enqResp.Metadata.ID < 0 {
			t.Errorf("expected non-negative message id; got %d", enqResp.Metadata.ID)
		}

		read, err := qv2.ReadMessages(tc.Ctx, &persistence.InternalReadMessagesRequest{
			QueueType: persistence.QueueTypeHistoryNormal, QueueName: name, PageSize: 10,
		})
		fatalOnErr(t, "ReadMessages", err)
		if len(read.Messages) != 1 {
			t.Errorf("expected 1 message; got %d", len(read.Messages))
		}
	})
}
