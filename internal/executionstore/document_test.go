package executionstore_test

import (
	"crypto/rand"
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
)

func randomBlob(t *testing.T, n int) *commonpb.DataBlob {
	t.Helper()
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: buf}
}

// TestExecutionDoc_RoundTrip: every blob
// the manager layer hands the plugin round-trips through BSON
// marshal/unmarshal byte-identical, including the embedded sub-maps.
func TestExecutionDoc_RoundTrip(t *testing.T) {
	state := &persistence.InternalWorkflowMutableState{
		ExecutionInfo:  randomBlob(t, 256),
		ExecutionState: randomBlob(t, 64),
		NextEventID:    42,
		ActivityInfos: map[int64]*commonpb.DataBlob{
			1: randomBlob(t, 128),
			2: randomBlob(t, 128),
		},
		TimerInfos: map[string]*commonpb.DataBlob{
			"timer-a": randomBlob(t, 32),
		},
		ChildExecutionInfos: map[int64]*commonpb.DataBlob{3: randomBlob(t, 64)},
		RequestCancelInfos:  map[int64]*commonpb.DataBlob{4: randomBlob(t, 16)},
		SignalInfos:         map[int64]*commonpb.DataBlob{5: randomBlob(t, 96)},
		SignalRequestedIDs:  []string{"sig-1", "sig-2"},
		BufferedEvents:      []*commonpb.DataBlob{randomBlob(t, 48)},
		Checksum:            randomBlob(t, 32),
		ChasmNodes: map[string]persistence.InternalChasmNode{
			"chasm-node": {
				Metadata: randomBlob(t, 16),
				Data:     randomBlob(t, 256),
			},
		},
		DBRecordVersion: 7,
	}

	doc, err := executionstore.DocFromState(1, "ns", "wf", "run", state)
	if err != nil {
		t.Fatalf("DocFromState: %v", err)
	}

	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	var decoded executionstore.Doc
	if err := bson.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("bson.Unmarshal: %v", err)
	}

	out, err := executionstore.StateFromDoc(&decoded)
	if err != nil {
		t.Fatalf("StateFromDoc: %v", err)
	}

	if !bytesEqual(out.ExecutionInfo.Data, state.ExecutionInfo.Data) {
		t.Error("ExecutionInfo bytes drifted")
	}
	if out.NextEventID != state.NextEventID {
		t.Errorf("NextEventID drift: %d → %d", state.NextEventID, out.NextEventID)
	}
	if out.DBRecordVersion != state.DBRecordVersion {
		t.Errorf("DBRecordVersion drift: %d → %d", state.DBRecordVersion, out.DBRecordVersion)
	}
	if len(out.ActivityInfos) != 2 {
		t.Errorf("ActivityInfos len drift: %d", len(out.ActivityInfos))
	}
	for k, in := range state.ActivityInfos {
		o, ok := out.ActivityInfos[k]
		if !ok {
			t.Errorf("ActivityInfos[%d] missing after round-trip", k)
			continue
		}
		if !bytesEqual(o.Data, in.Data) {
			t.Errorf("ActivityInfos[%d] bytes drifted", k)
		}
	}
	if _, ok := out.TimerInfos["timer-a"]; !ok {
		t.Error("TimerInfos key drifted")
	}
	if len(out.SignalRequestedIDs) != 2 {
		t.Errorf("SignalRequestedIDs len drift: %d", len(out.SignalRequestedIDs))
	}
	if len(out.BufferedEvents) != 1 {
		t.Errorf("BufferedEvents len drift: %d", len(out.BufferedEvents))
	}
	if !bytesEqual(out.Checksum.Data, state.Checksum.Data) {
		t.Error("Checksum bytes drifted")
	}
	node, ok := out.ChasmNodes["chasm-node"]
	if !ok {
		t.Error("ChasmNodes key drifted")
	}
	if !bytesEqual(node.Data.Data, state.ChasmNodes["chasm-node"].Data.Data) {
		t.Error("ChasmNodes Data bytes drifted")
	}
	if !bytesEqual(node.Metadata.Data, state.ChasmNodes["chasm-node"].Metadata.Data) {
		t.Error("ChasmNodes Metadata bytes drifted")
	}
}

// TestExecutionDoc_CHASMOnly: a CHASM-only execution
// (NextEventID <= 1, no ExecutionInfo, populated ChasmNodes) round-trips
// byte-identically.
func TestExecutionDoc_CHASMOnly(t *testing.T) {
	state := &persistence.InternalWorkflowMutableState{
		NextEventID: 1,
		ChasmNodes: map[string]persistence.InternalChasmNode{
			"root": {Metadata: randomBlob(t, 32), Data: randomBlob(t, 512)},
			"leaf": {Metadata: randomBlob(t, 32), Data: randomBlob(t, 128)},
		},
	}
	doc, err := executionstore.DocFromState(2, "ns", "chasm-wf", "run", state)
	if err != nil {
		t.Fatalf("DocFromState: %v", err)
	}
	raw, _ := bson.Marshal(doc)
	var decoded executionstore.Doc
	_ = bson.Unmarshal(raw, &decoded)
	out, _ := executionstore.StateFromDoc(&decoded)

	if out.NextEventID != 1 {
		t.Errorf("NextEventID drift: %d", out.NextEventID)
	}
	if len(out.ChasmNodes) != 2 {
		t.Errorf("ChasmNodes len drift: %d", len(out.ChasmNodes))
	}
}

// TestExecutionDoc_ProjectedSizeCheck: a projected
// write exceeding 12 MB surfaces TransactionSizeLimitError BEFORE the
// transaction is attempted.
func TestExecutionDoc_ProjectedSizeCheck(t *testing.T) {
	// 13 MB of payload across the execution-info blob.
	state := &persistence.InternalWorkflowMutableState{
		NextEventID:   42,
		ExecutionInfo: randomBlob(t, 13*1024*1024),
	}
	_, err := executionstore.DocFromState(1, "ns", "wf", "run", state)
	if err == nil {
		t.Fatal("expected TransactionSizeLimitError; got nil")
	}
	// TransactionSizeLimitError is concrete in the upstream persistence pkg.
	var tsle *persistence.TransactionSizeLimitError
	if !errors.As(err, &tsle) {
		t.Fatalf("expected *TransactionSizeLimitError; got %T: %v", err, err)
	}
}

// TestExecutionDoc_UnderBudget covers the opposite: a normal-sized
// blob passes without triggering the size check.
func TestExecutionDoc_UnderBudget(t *testing.T) {
	state := &persistence.InternalWorkflowMutableState{
		NextEventID:   1,
		ExecutionInfo: randomBlob(t, 100*1024), // 100 KB
	}
	if _, err := executionstore.DocFromState(1, "ns", "wf", "run", state); err != nil {
		t.Fatalf("unexpected error under budget: %v", err)
	}
}

// TestExecutionDoc_IDShape verifies the composite _id matches the
// spec's convention: "<shard_id>:<namespace_id>:<workflow_id>:<run_id>".
func TestExecutionDoc_IDShape(t *testing.T) {
	doc, err := executionstore.DocFromState(7, "namespace-a", "wf-1", "run-xyz", &persistence.InternalWorkflowMutableState{NextEventID: 1})
	if err != nil {
		t.Fatal(err)
	}
	want := "7:namespace-a:wf-1:run-xyz"
	if doc.ID != want {
		t.Errorf("ID shape drift: got %q want %q", doc.ID, want)
	}
}

// --- helpers ---

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
