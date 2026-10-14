//go:build integration

// Transaction error-boundary tests. A failCommand fail
// point injects a TransientTransactionError into a write inside the
// UpdateWorkflowExecution transaction; the driver's WithTransaction loop
// must see the label and retry, so the update commits.
package executionstore_test

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
)

// armFailCommand makes the next `times` executions of the named commands
// fail with a WriteConflict carrying the TransientTransactionError label.
func armFailCommand(t *testing.T, db *mongo.Database, times int, commands ...string) {
	t.Helper()
	cmd := bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: bson.D{{Key: "times", Value: times}}},
		{Key: "data", Value: bson.D{
			{Key: "failCommands", Value: commands},
			{Key: "errorCode", Value: 112},
			{Key: "errorLabels", Value: bson.A{"TransientTransactionError"}},
		}},
	}
	if err := db.Client().Database("admin").RunCommand(context.Background(), cmd).Err(); err != nil {
		t.Fatalf("configureFailPoint: %v", err)
	}
	t.Cleanup(func() {
		off := bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: "off"}}
		_ = db.Client().Database("admin").RunCommand(context.Background(), off).Err()
	})
}

func TestUpdate_TransientTxnErrorOnWrite_DriverRetriesAndCommits(t *testing.T) {
	db := startDB(t)
	seedShard(t, db, 1, 1)
	s := executionstore.New(db, "cluster0")
	ctx := context.Background()

	snap := makeSnapshot("ns", "wf", "run-1")
	if _, err := s.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1, Mode: persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: snap,
	}); err != nil {
		t.Fatal(err)
	}

	mut := snapshotToMutation(snap)
	mut.Condition = 2
	mut.DBRecordVersion = 2
	mut.NextEventID = 10

	armFailCommand(t, db, 1, "update")
	err := s.UpdateWorkflowExecution(ctx, &persistence.InternalUpdateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 1,
		Mode:                   persistence.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowMutation: mut,
	})
	if err != nil {
		t.Fatalf("transient error should be retried by the driver; got %T %v", err, err)
	}

	resp, err := s.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: "ns", WorkflowID: "wf", RunID: "run-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.State.NextEventID != 10 {
		t.Fatalf("update not committed: next_event_id=%d want 10", resp.State.NextEventID)
	}
}
