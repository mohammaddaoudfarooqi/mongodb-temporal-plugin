//go:build integration

// Integration tests for Record/Upsert/Delete/Get.
package visibility_test

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
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility"
)

func startDB(t *testing.T) *mongo.Database {
	t.Helper()
	db, _ := startDBWithURI(t)
	return db
}

// startDBWithURI also returns the connection URI so factory-level tests
// can build their own client.
func startDBWithURI(t *testing.T) (*mongo.Database, string) {
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
		URI: uri, Database: "temporal_vis_test", AppName: "test",
	})
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close(context.Background()) })
	db := provider.Database()
	if err := testutil.WaitForPrimary(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := visibility.EnsureIndexes(ctx, db); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
	return db, uri
}

func startedReq(ns, wfID, runID string, status enumspb.WorkflowExecutionStatus) *store.InternalRecordWorkflowExecutionStartedRequest {
	return &store.InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
			NamespaceID:      ns,
			WorkflowID:       wfID,
			RunID:            runID,
			WorkflowTypeName: "MyWF",
			StartTime:        time.Now().UTC(),
			ExecutionTime:    time.Now().UTC(),
			Status:           status,
			TaskQueue:        "tq",
			TaskID:           1,
		},
	}
}

func TestRecordStarted_Upserts(t *testing.T) {
	db := startDB(t)
	s := visibility.New(db, visibility.ModeAggregation, "")
	ctx := context.Background()

	if err := s.RecordWorkflowExecutionStarted(ctx, startedReq("ns", "wf-1", "run-1", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)); err != nil {
		t.Fatalf("RecordWorkflowExecutionStarted: %v", err)
	}
	// Upsert: second call for same (ns, run) overwrites.
	if err := s.RecordWorkflowExecutionStarted(ctx, startedReq("ns", "wf-1", "run-1", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)); err != nil {
		t.Fatalf("idempotent re-record: %v", err)
	}

	got, err := s.GetWorkflowExecution(ctx, &manager.GetWorkflowExecutionRequest{
		NamespaceID: namespace.ID("ns"),
		RunID:       "run-1",
		WorkflowID:  "wf-1",
	})
	if err != nil {
		t.Fatalf("GetWorkflowExecution: %v", err)
	}
	if got.Execution.WorkflowID != "wf-1" {
		t.Errorf("workflow_id round-trip: %q", got.Execution.WorkflowID)
	}
}

func TestRecordClosed_UpdatesTerminalFields(t *testing.T) {
	db := startDB(t)
	s := visibility.New(db, visibility.ModeAggregation, "")
	ctx := context.Background()

	_ = s.RecordWorkflowExecutionStarted(ctx, startedReq("ns", "wf-c", "run-c", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))
	err := s.RecordWorkflowExecutionClosed(ctx, &store.InternalRecordWorkflowExecutionClosedRequest{
		InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
			NamespaceID: "ns",
			WorkflowID:  "wf-c",
			RunID:       "run-c",
			Status:      enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		CloseTime:        time.Now().UTC(),
		HistoryLength:    123,
		HistorySizeBytes: 4567,
	})
	if err != nil {
		t.Fatalf("RecordWorkflowExecutionClosed: %v", err)
	}
	got, _ := s.GetWorkflowExecution(ctx, &manager.GetWorkflowExecutionRequest{
		NamespaceID: namespace.ID("ns"), RunID: "run-c", WorkflowID: "wf-c",
	})
	if got.Execution.Status != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		t.Errorf("status not closed: %v", got.Execution.Status)
	}
	if got.Execution.HistoryLength != 123 {
		t.Errorf("history_length: got %d", got.Execution.HistoryLength)
	}
}

func TestUpsertWorkflowExecution_Idempotent(t *testing.T) {
	db := startDB(t)
	s := visibility.New(db, visibility.ModeAggregation, "")
	ctx := context.Background()

	upsert := &store.InternalUpsertWorkflowExecutionRequest{
		InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
			NamespaceID: "ns", WorkflowID: "wf", RunID: "run",
			WorkflowTypeName: "T", StartTime: time.Now().UTC(),
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
	}
	for i := 0; i < 3; i++ {
		if err := s.UpsertWorkflowExecution(ctx, upsert); err != nil {
			t.Fatalf("upsert iter %d: %v", i, err)
		}
	}
}

func TestDeleteWorkflowExecution_Idempotent(t *testing.T) {
	db := startDB(t)
	s := visibility.New(db, visibility.ModeAggregation, "")
	ctx := context.Background()
	err := s.DeleteWorkflowExecution(ctx, &manager.VisibilityDeleteWorkflowExecutionRequest{
		NamespaceID: namespace.ID("ns"), RunID: "ghost", WorkflowID: "ghost",
	})
	if err != nil {
		t.Errorf("delete of non-existent execution should be nil; got %v", err)
	}
}

// TestGetWorkflowExecution_NotFound miss path.
func TestGetWorkflowExecution_NotFound(t *testing.T) {
	db := startDB(t)
	s := visibility.New(db, visibility.ModeAggregation, "")
	_, err := s.GetWorkflowExecution(context.Background(), &manager.GetWorkflowExecutionRequest{
		NamespaceID: namespace.ID("x"), RunID: "y", WorkflowID: "z",
	})
	var nf *serviceerror.NotFound
	if !errors.As(err, &nf) {
		t.Fatalf("expected NotFound; got %T: %v", err, err)
	}
}
