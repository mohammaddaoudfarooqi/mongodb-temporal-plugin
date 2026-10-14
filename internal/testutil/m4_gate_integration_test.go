//go:build integration

// M4 gate — drives the visibility store through a representative slice
// of its public surface end-to-end: record started → list → count →
// record closed → re-list → delete. A green run here proves the
// Aggregation-mode path (no DSL query string) cooperates with the
// upstream visibility-manager expectations for bootstrap/smoke
// scenarios.
//
// Full upstream visibility-manager conformance requires a
// searchattribute.Provider + NameTypeMap; integration with that
// machinery lands in a follow-up when the full end-to-end
// manager-driven flow is wired (tests typically live in the
// visibility-manager suite upstream). Until then this gate exercises
// the store's Internal* surface directly.
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
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility"
)

func gateM4DB(t *testing.T) *mongo.Database {
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
		URI: uri, Database: "temporal_m4_gate", AppName: "test",
	})
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close(context.Background()) })
	db := provider.Database()
	if err := visibility.EnsureIndexes(ctx, db); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
	return db
}

// TestM4_VisibilityLifecycle drives a 3-workflow lifecycle through the
// visibility store: two are Running, one is Completed. Validates list,
// count, pagination, and delete all compose correctly.
func TestM4_VisibilityLifecycle(t *testing.T) {
	db := gateM4DB(t)
	s := visibility.New(db, visibility.ModeAggregation, "")
	ctx := context.Background()

	// Record 3 started; close 1.
	for i := 0; i < 3; i++ {
		runID := fmt.Sprintf("run-%d", i)
		err := s.RecordWorkflowExecutionStarted(ctx, &store.InternalRecordWorkflowExecutionStartedRequest{
			InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
				NamespaceID:      "ns",
				WorkflowID:       "wf",
				RunID:            runID,
				WorkflowTypeName: "T",
				StartTime:        time.Now().UTC(),
				Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				TaskQueue:        "tq",
			},
		})
		if err != nil {
			t.Fatalf("Record[%d]: %v", i, err)
		}
	}
	err := s.RecordWorkflowExecutionClosed(ctx, &store.InternalRecordWorkflowExecutionClosedRequest{
		InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
			NamespaceID: "ns", WorkflowID: "wf", RunID: "run-0",
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		CloseTime: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Count: 3.
	resp, err := s.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespace.ID("ns"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Count != 3 {
		t.Errorf("count: got %d want 3", resp.Count)
	}

	// List: all 3, paginated at pageSize=2.
	p1, err := s.ListWorkflowExecutions(ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: namespace.ID("ns"), PageSize: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(p1.Executions) != 2 || len(p1.NextPageToken) == 0 {
		t.Fatalf("page 1: %+v, token=%q", p1.Executions, string(p1.NextPageToken))
	}
	p2, err := s.ListWorkflowExecutions(ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: namespace.ID("ns"), PageSize: 2, NextPageToken: p1.NextPageToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.Executions) != 1 {
		t.Errorf("page 2 size: got %d want 1", len(p2.Executions))
	}

	// Delete the closed one.
	err = s.DeleteWorkflowExecution(ctx, &manager.VisibilityDeleteWorkflowExecutionRequest{
		NamespaceID: namespace.ID("ns"), RunID: "run-0", WorkflowID: "wf",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Count now 2.
	resp2, _ := s.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespace.ID("ns"),
	})
	if resp2.Count != 2 {
		t.Errorf("count after delete: got %d want 2", resp2.Count)
	}
}
