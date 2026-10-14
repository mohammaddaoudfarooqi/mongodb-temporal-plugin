//go:build integration

package visibility_test

import (
	"context"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/common/persistence/visibility/store"
)

// TestRecord_UntypedPredefinedSA_IsQueryable: the
// scheduler stamps TemporalScheduledById without "type" metadata, so
// the store must decode it by the provider's registered type.
func TestRecord_UntypedPredefinedSA_IsQueryable(t *testing.T) {
	s := saStore(t)
	p, err := converter.GetDefaultDataConverter().ToPayload("sched-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, typed := p.Metadata["type"]; typed {
		t.Fatal("fixture must carry no type metadata")
	}
	err = s.RecordWorkflowExecutionStarted(context.Background(), &store.InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
			NamespaceID: testNS, WorkflowID: "wf-sched", RunID: "run-sched", WorkflowTypeName: "type",
			StartTime: base, ExecutionTime: base, Status: 1, TaskQueue: "tq",
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				"TemporalScheduledById": p,
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp := list(t, s, "TemporalScheduledById = 'sched-1'", 10, nil)
	if len(resp.Executions) != 1 {
		t.Fatalf("got %d executions, want 1", len(resp.Executions))
	}
}
