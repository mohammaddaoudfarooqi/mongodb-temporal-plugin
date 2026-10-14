package visibility_test

import (
	"encoding/binary"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/store"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility"
)

// TestVisibilityDoc_FromStartedRequest (system SA
// fields) (open-execution upsert shape).
func TestVisibilityDoc_FromStartedRequest(t *testing.T) {
	req := visibility.BaseFromStartedRequest(&store.InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
			NamespaceID:      "ns-a",
			WorkflowID:       "wf-1",
			RunID:            "run-1",
			WorkflowTypeName: "MyWorkflow",
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			TaskQueue:        "tq-a",
		},
	})
	if req.ID != "ns-a:run-1" {
		t.Errorf("_id shape drift: %q", req.ID)
	}
	if req.NamespaceID != "ns-a" || req.RunID != "run-1" || req.WorkflowID != "wf-1" {
		t.Errorf("required fields missing: %+v", req)
	}
	if req.Status != int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING) {
		t.Errorf("status field not preserved: %d", req.Status)
	}
}

// TestVisibilityDoc_BSONRoundTrip checks that the document shape
// marshals and unmarshals without field loss.
func TestVisibilityDoc_BSONRoundTrip(t *testing.T) {
	doc := visibility.Doc{
		ID:               "ns:run",
		NamespaceID:      "ns",
		RunID:            "run",
		WorkflowID:       "wf",
		WorkflowTypeName: "Type",
		Status:           int32(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED),
		TaskQueue:        "tq",
		HistoryLength:    42,
		SearchAttributes: map[string]any{
			"CustomKeyword": "value",
			"CustomInt":     int64(100),
		},
	}
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	var decoded visibility.Doc
	if err := bson.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ID != doc.ID {
		t.Errorf("_id lost: %q vs %q", decoded.ID, doc.ID)
	}
	if decoded.HistoryLength != 42 {
		t.Errorf("HistoryLength lost: %d", decoded.HistoryLength)
	}
	if v, ok := decoded.SearchAttributes["CustomKeyword"]; !ok || v != "value" {
		t.Errorf("custom SA lost: %+v", decoded.SearchAttributes)
	}
	if _, ok := decoded.SearchAttributes["CustomInt"]; !ok {
		t.Error("CustomInt missing")
	}
}

// TestVisibilityDoc_CustomSearchAttributes: SA types
// (Keyword, Int, Double, Bool, Datetime) survive BSON round-trip.
func TestVisibilityDoc_CustomSearchAttributes(t *testing.T) {
	doc := visibility.Doc{
		ID:          "ns:run",
		NamespaceID: "ns",
		SearchAttributes: map[string]any{
			"CustomKeyword":     "k",
			"CustomKeywordList": []any{"a", "b"},
			"CustomText":        "some text",
			"CustomInt":         int64(7),
			"CustomDouble":      3.14,
			"CustomBool":        true,
		},
	}
	raw, _ := bson.Marshal(doc)
	var decoded visibility.Doc
	_ = bson.Unmarshal(raw, &decoded)
	if v := decoded.SearchAttributes["CustomBool"]; v != true {
		t.Errorf("bool SA: got %v", v)
	}
	if v := decoded.SearchAttributes["CustomDouble"]; v != 3.14 {
		t.Errorf("double SA: got %v", v)
	}
	if decoded.SearchAttributes["CustomKeyword"] != "k" {
		t.Errorf("keyword SA: %v", decoded.SearchAttributes["CustomKeyword"])
	}
}

// TestDoc_ArchetypeID_StampedFromSA covers backlog B-5: a
// started-request carrying the reserved "ArchetypeId" search
// attribute surfaces into the Doc's first-class archetype_id field
// so ListChasmExecutions can filter on an indexed column rather
// than descending into the SA map.
func TestDoc_ArchetypeID_StampedFromSA(t *testing.T) {
	cases := []struct {
		name string
		val  *commonpb.Payload
		want uint32
	}{
		{
			name: "four-byte-big-endian-42",
			val:  &commonpb.Payload{Data: bigEndian4(42)},
			want: 42,
		},
		{
			name: "four-byte-big-endian-max",
			val:  &commonpb.Payload{Data: bigEndian4(^uint32(0))},
			want: ^uint32(0),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := visibility.BaseFromStartedRequest(&store.InternalRecordWorkflowExecutionStartedRequest{
				InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
					NamespaceID: "ns",
					WorkflowID:  "wf",
					RunID:       "run",
					Status:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					SearchAttributes: &commonpb.SearchAttributes{
						IndexedFields: map[string]*commonpb.Payload{
							"ArchetypeId": tc.val,
						},
					},
				},
			})
			if doc.ArchetypeID != tc.want {
				t.Errorf("expected ArchetypeID=%d; got %d", tc.want, doc.ArchetypeID)
			}
		})
	}
}

// TestDoc_ArchetypeID_UnstampedDefaultsZero: a started-request with no
// ArchetypeId SA leaves the Doc's archetype_id at zero. This is the
// expected shape for non-CHASM workflows.
func TestDoc_ArchetypeID_UnstampedDefaultsZero(t *testing.T) {
	doc := visibility.BaseFromStartedRequest(&store.InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
			NamespaceID: "ns", WorkflowID: "wf", RunID: "run",
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
	})
	if doc.ArchetypeID != 0 {
		t.Errorf("expected zero archetype on unstamped doc; got %d", doc.ArchetypeID)
	}
}

func bigEndian4(n uint32) []byte {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, n)
	return out
}
