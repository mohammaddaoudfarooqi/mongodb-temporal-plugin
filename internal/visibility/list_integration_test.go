//go:build integration

// Integration tests for ListWorkflowExecutions + CountWorkflowExecutions
// + AddSearchAttributes.
package visibility_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	visibilityservice "go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility"
)

// TestList_EmptyQueryPaginates covers the no-query-string list path +
// pagination. Seeds 5 executions, lists in 2-page chunks,
// confirms the resume token round-trips.
func TestList_EmptyQueryPaginates(t *testing.T) {
	db := startDB(t)
	s := visibility.New(db, visibility.ModeAggregation, "")
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		req := startedReq("ns", "wf", runID(i), enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
		if err := s.RecordWorkflowExecutionStarted(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	// Page 1: 3.
	p1, err := s.ListWorkflowExecutions(ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: namespace.ID("ns"),
		PageSize:    3,
	})
	if err != nil {
		t.Fatalf("Page 1: %v", err)
	}
	if len(p1.Executions) != 3 {
		t.Fatalf("page 1 size: got %d want 3", len(p1.Executions))
	}
	if len(p1.NextPageToken) == 0 {
		t.Fatal("expected NextPageToken")
	}
	// Page 2: 2.
	p2, err := s.ListWorkflowExecutions(ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   namespace.ID("ns"),
		PageSize:      3,
		NextPageToken: p1.NextPageToken,
	})
	if err != nil {
		t.Fatalf("Page 2: %v", err)
	}
	if len(p2.Executions) != 2 {
		t.Errorf("page 2 size: got %d want 2", len(p2.Executions))
	}
	if len(p2.NextPageToken) != 0 {
		t.Errorf("page 2 should be last; got token %q", string(p2.NextPageToken))
	}
}

// TestCount_EmptyQuery covers the no-query count path.
func TestCount_EmptyQuery(t *testing.T) {
	db := startDB(t)
	s := visibility.New(db, visibility.ModeAggregation, "")
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		_ = s.RecordWorkflowExecutionStarted(ctx, startedReq("ns", "wf", runID(i), enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))
	}
	resp, err := s.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespace.ID("ns"),
	})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if resp.Count != 4 {
		t.Errorf("count: got %d want 4", resp.Count)
	}
}

func TestList_InvalidToken(t *testing.T) {
	db := startDB(t)
	s := visibility.New(db, visibility.ModeAggregation, "")
	_, err := s.ListWorkflowExecutions(context.Background(), &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   namespace.ID("ns"),
		NextPageToken: []byte("not-a-valid-token"),
	})
	if err == nil {
		t.Fatal("expected InvalidArgument; got nil")
	}
}

func TestAddSearchAttributes_Registers(t *testing.T) {
	db := startDB(t)
	s := visibility.New(db, visibility.ModeAggregation, "")
	err := s.AddSearchAttributes(context.Background(), &manager.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomString": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"CustomInt":    enumspb.INDEXED_VALUE_TYPE_INT,
		},
	})
	if err != nil {
		t.Fatalf("AddSearchAttributes: %v", err)
	}
	// Idempotent on duplicate names.
	err = s.AddSearchAttributes(context.Background(), &manager.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomString": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	if err != nil {
		t.Errorf("duplicate AddSearchAttributes: %v", err)
	}
}

// TestListChasm_NilRequest_ReturnsError: a nil request is an
// invariant violation; the store surfaces a "uninitialised or nil
// request" error rather than panicking.
func TestListChasm_NilRequest_ReturnsError(t *testing.T) {
	db := startDB(t)
	s := visibility.New(db, visibility.ModeAggregation, "")
	_, err := s.ListChasmExecutions(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error on nil request; got nil")
	}
}

// TestListChasm_FiltersByArchetype verifies the CHASM list path's core
// contract (backlog B-5): records stamped with an
// ArchetypeId search attribute filter through to the caller's
// archetype_id; records without the stamp do not leak in.
func TestListChasm_FiltersByArchetype(t *testing.T) {
	db := startDB(t)
	s := visibility.New(db, visibility.ModeAggregation, "")
	ctx := context.Background()

	// Seed one archetype-42 execution, one archetype-7 execution, and
	// one plain workflow execution. Only archetype-42 should appear
	// when we list with ArchetypeId=42.
	mustRecord(t, s, chasmStartedReq("ns", "wf-42", "run-42", 42))
	mustRecord(t, s, chasmStartedReq("ns", "wf-7", "run-7", 7))
	mustRecord(t, s, startedReq("ns", "wf-plain", "run-plain", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))

	resp, err := s.ListChasmExecutions(ctx, &visibilityservice.ListChasmExecutionsRequest{
		NamespaceId: "ns",
		ArchetypeId: 42,
		PageSize:    10,
	})
	if err != nil {
		t.Fatalf("ListChasmExecutions: %v", err)
	}
	if got, want := len(resp.Executions), 1; got != want {
		t.Fatalf("expected %d executions; got %d", want, got)
	}
	if got := resp.Executions[0].WorkflowID; got != "wf-42" {
		t.Errorf("expected wf-42; got %q", got)
	}

	countResp, err := s.CountChasmExecutions(ctx, &visibilityservice.CountChasmExecutionsRequest{
		NamespaceId: "ns",
		ArchetypeId: 42,
	})
	if err != nil {
		t.Fatalf("CountChasmExecutions: %v", err)
	}
	if countResp.Count != 1 {
		t.Errorf("expected count=1 for archetype=42; got %d", countResp.Count)
	}
}

// mustRecord records a visibility started request or fails the test.
func mustRecord(t *testing.T, s *visibility.Store, req *store.InternalRecordWorkflowExecutionStartedRequest) {
	t.Helper()
	if err := s.RecordWorkflowExecutionStarted(context.Background(), req); err != nil {
		t.Fatalf("RecordWorkflowExecutionStarted: %v", err)
	}
}

// chasmStartedReq builds a visibility-started request stamped with
// the reserved ArchetypeId search attribute. The flatten path reads
// the payload's Data bytes, so we encode the archetype id as a 4-byte
// big-endian blob and rely on archetypeIDFromSAValue's []byte decode.
func chasmStartedReq(ns, wfID, runID string, archetypeID uint32) *store.InternalRecordWorkflowExecutionStartedRequest {
	req := startedReq(ns, wfID, runID, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, archetypeID)
	req.SearchAttributes = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"ArchetypeId": {Data: buf},
		},
	}
	return req
}

// helpers

func runID(i int) string {
	return time.Now().Format("150405") + "-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	buf := []byte{}
	for i > 0 {
		buf = append([]byte{byte('0' + i%10)}, buf...)
		i /= 10
	}
	return string(buf)
}

// Ensure store.VisibilityStore interface matches the methods we call.
var _ store.VisibilityStore = (*visibility.Store)(nil)
