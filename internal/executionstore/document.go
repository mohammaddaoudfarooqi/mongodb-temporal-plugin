// Package executionstore hosts the MongoDB-backed implementation of
// persistence.ExecutionStore; upstream's persistence test suites are the
// behavioural contract.
//
// Storage model:
//
//	collection: executions
//	  _id:            "<shard_id>:<namespace_id>:<workflow_id>:<run_id>"
//	  shard_id:       <int32>
//	  namespace_id:   <string>
//	  workflow_id:    <string>
//	  run_id:         <string>
//	  next_event_id:  <int64>
//	  db_record_version: <int64>
//	  execution_info:  { data, encoding }     // opaque blob
//	  execution_state: { data, encoding }     // opaque blob
//	  checksum:        { data, encoding }     // opaque blob
//	  activity_infos:        [{key:<int64>, blob:{…}}]
//	  timer_infos:           [{key:<string>, blob:{…}}]
//	  child_execution_infos: [{key:<int64>, blob:{…}}]
//	  request_cancel_infos:  [{key:<int64>, blob:{…}}]
//	  signal_infos:          [{key:<int64>, blob:{…}}]
//	  signal_requested_ids:  [<string>]
//	  buffered_events:       [{data, encoding}]
//	  chasm_nodes:           [{key:<string>, metadata:{…}, data:{…}}]
//	  update_time:     <DateTime>
//
// The embedded-array layout lets CreateWorkflowExecution and
// UpdateWorkflowExecution mutate any sub-collection with a single
// document update. Concurrent updates contend on the shared document,
// which the CAS on db_record_version mediates.
//
// blob contents are never deserialised; only the
// adjacent indexed scalars (NextEventID, DBRecordVersion, …) are read
// for CAS and filtering.
package executionstore

import (
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/persistence"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

// payloadBudget is the 12 MB soft cap on execution-document payload
// . 80% of BSON's 16 MB per-document limit. Callers that
// exceed it see *persistence.TransactionSizeLimitError before the
// transaction is attempted so the upstream manager layer can trigger
// history-size remediation.
const payloadBudget = 12 * 1024 * 1024

// Doc is the BSON projection of an execution's mutable state.
// Field tags use snake_case per docs/developer/conventions.md.
type Doc struct {
	ID                  string               `bson:"_id"`
	ShardID             int32                `bson:"shard_id"`
	NamespaceID         string               `bson:"namespace_id"`
	WorkflowID          string               `bson:"workflow_id"`
	RunID               string               `bson:"run_id"`
	NextEventID         int64                `bson:"next_event_id"`
	DBRecordVersion     int64                `bson:"db_record_version"`
	ExecutionInfo       *internalmongo.Blob  `bson:"execution_info,omitempty"`
	ExecutionState      *internalmongo.Blob  `bson:"execution_state,omitempty"`
	Checksum            *internalmongo.Blob  `bson:"checksum,omitempty"`
	ActivityInfos       []Int64KeyedBlob     `bson:"activity_infos,omitempty"`
	TimerInfos          []StringKeyedBlob    `bson:"timer_infos,omitempty"`
	ChildExecutionInfos []Int64KeyedBlob     `bson:"child_execution_infos,omitempty"`
	RequestCancelInfos  []Int64KeyedBlob     `bson:"request_cancel_infos,omitempty"`
	SignalInfos         []Int64KeyedBlob     `bson:"signal_infos,omitempty"`
	SignalRequestedIDs  []string             `bson:"signal_requested_ids,omitempty"`
	BufferedEvents      []internalmongo.Blob `bson:"buffered_events,omitempty"`
	ChasmNodes          []ChasmNodeEntry     `bson:"chasm_nodes,omitempty"`
	UpdateTime          time.Time            `bson:"update_time"`
}

// Int64KeyedBlob is the wire shape for sub-maps keyed by int64
// (ActivityInfos, ChildExecutionInfos, RequestCancelInfos, SignalInfos).
type Int64KeyedBlob struct {
	Key  int64               `bson:"key"`
	Blob *internalmongo.Blob `bson:"blob"`
}

// StringKeyedBlob mirrors Int64KeyedBlob with a string key (TimerInfos).
type StringKeyedBlob struct {
	Key  string              `bson:"key"`
	Blob *internalmongo.Blob `bson:"blob"`
}

// ChasmNodeEntry represents one CHASM node entry. Both Metadata and
// Data are opaque blobs.
type ChasmNodeEntry struct {
	Key      string              `bson:"key"`
	Metadata *internalmongo.Blob `bson:"metadata,omitempty"`
	Data     *internalmongo.Blob `bson:"data,omitempty"`
}

// DocID constructs the composite primary key. Keeping this in one place
// lets every caller agree on the shape.
func DocID(shardID int32, namespaceID, workflowID, runID string) string {
	return fmt.Sprintf("%d:%s:%s:%s", shardID, namespaceID, workflowID, runID)
}

// DocFromState projects an InternalWorkflowMutableState into a Doc.
// Returns *persistence.TransactionSizeLimitError when the projected
// BSON encoding exceeds the 12 MB payload budget.
func DocFromState(
	shardID int32,
	namespaceID, workflowID, runID string,
	state *persistence.InternalWorkflowMutableState,
) (*Doc, error) {
	if state == nil {
		return nil, fmt.Errorf("executionstore: nil mutable state")
	}
	doc := &Doc{
		ID:                 DocID(shardID, namespaceID, workflowID, runID),
		ShardID:            shardID,
		NamespaceID:        namespaceID,
		WorkflowID:         workflowID,
		RunID:              runID,
		NextEventID:        state.NextEventID,
		DBRecordVersion:    state.DBRecordVersion,
		ExecutionInfo:      internalmongo.BlobFromDataBlob(state.ExecutionInfo),
		ExecutionState:     internalmongo.BlobFromDataBlob(state.ExecutionState),
		Checksum:           internalmongo.BlobFromDataBlob(state.Checksum),
		SignalRequestedIDs: state.SignalRequestedIDs,
		UpdateTime:         time.Now().UTC(),
	}

	if len(state.ActivityInfos) > 0 {
		doc.ActivityInfos = int64BlobsOrdered(state.ActivityInfos)
	}
	if len(state.TimerInfos) > 0 {
		doc.TimerInfos = stringBlobsOrdered(state.TimerInfos)
	}
	if len(state.ChildExecutionInfos) > 0 {
		doc.ChildExecutionInfos = int64BlobsOrdered(state.ChildExecutionInfos)
	}
	if len(state.RequestCancelInfos) > 0 {
		doc.RequestCancelInfos = int64BlobsOrdered(state.RequestCancelInfos)
	}
	if len(state.SignalInfos) > 0 {
		doc.SignalInfos = int64BlobsOrdered(state.SignalInfos)
	}
	if len(state.BufferedEvents) > 0 {
		doc.BufferedEvents = make([]internalmongo.Blob, 0, len(state.BufferedEvents))
		for _, b := range state.BufferedEvents {
			if bb := internalmongo.BlobFromDataBlob(b); bb != nil {
				doc.BufferedEvents = append(doc.BufferedEvents, *bb)
			}
		}
	}
	if len(state.ChasmNodes) > 0 {
		doc.ChasmNodes = chasmNodesOrdered(state.ChasmNodes)
	}

	// Size check. MarshalBSON is authoritative because
	// BSON adds type bytes per field; a raw byte-count would underestimate.
	raw, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("executionstore: bson.Marshal: %w", err)
	}
	if len(raw) > payloadBudget {
		return nil, &persistence.TransactionSizeLimitError{
			Msg: fmt.Sprintf(
				"executionstore: projected document %d bytes exceeds %d-byte budget",
				len(raw), payloadBudget),
		}
	}
	return doc, nil
}

// StateFromDoc reconstructs the InternalWorkflowMutableState from a Doc.
// This is the read-back path and the inverse of DocFromState.
func StateFromDoc(d *Doc) (*persistence.InternalWorkflowMutableState, error) {
	if d == nil {
		return nil, fmt.Errorf("executionstore: nil doc")
	}
	state := &persistence.InternalWorkflowMutableState{
		NextEventID:        d.NextEventID,
		DBRecordVersion:    d.DBRecordVersion,
		ExecutionInfo:      internalmongo.DataBlobFromBlob(d.ExecutionInfo),
		ExecutionState:     internalmongo.DataBlobFromBlob(d.ExecutionState),
		Checksum:           internalmongo.DataBlobFromBlob(d.Checksum),
		SignalRequestedIDs: d.SignalRequestedIDs,
	}
	if len(d.ActivityInfos) > 0 {
		state.ActivityInfos = make(map[int64]*commonpb.DataBlob, len(d.ActivityInfos))
		for _, kv := range d.ActivityInfos {
			state.ActivityInfos[kv.Key] = internalmongo.DataBlobFromBlob(kv.Blob)
		}
	}
	if len(d.TimerInfos) > 0 {
		state.TimerInfos = make(map[string]*commonpb.DataBlob, len(d.TimerInfos))
		for _, kv := range d.TimerInfos {
			state.TimerInfos[kv.Key] = internalmongo.DataBlobFromBlob(kv.Blob)
		}
	}
	if len(d.ChildExecutionInfos) > 0 {
		state.ChildExecutionInfos = make(map[int64]*commonpb.DataBlob, len(d.ChildExecutionInfos))
		for _, kv := range d.ChildExecutionInfos {
			state.ChildExecutionInfos[kv.Key] = internalmongo.DataBlobFromBlob(kv.Blob)
		}
	}
	if len(d.RequestCancelInfos) > 0 {
		state.RequestCancelInfos = make(map[int64]*commonpb.DataBlob, len(d.RequestCancelInfos))
		for _, kv := range d.RequestCancelInfos {
			state.RequestCancelInfos[kv.Key] = internalmongo.DataBlobFromBlob(kv.Blob)
		}
	}
	if len(d.SignalInfos) > 0 {
		state.SignalInfos = make(map[int64]*commonpb.DataBlob, len(d.SignalInfos))
		for _, kv := range d.SignalInfos {
			state.SignalInfos[kv.Key] = internalmongo.DataBlobFromBlob(kv.Blob)
		}
	}
	if len(d.BufferedEvents) > 0 {
		state.BufferedEvents = make([]*commonpb.DataBlob, 0, len(d.BufferedEvents))
		for i := range d.BufferedEvents {
			state.BufferedEvents = append(state.BufferedEvents, internalmongo.DataBlobFromBlob(&d.BufferedEvents[i]))
		}
	}
	if len(d.ChasmNodes) > 0 {
		state.ChasmNodes = make(map[string]persistence.InternalChasmNode, len(d.ChasmNodes))
		for _, e := range d.ChasmNodes {
			state.ChasmNodes[e.Key] = persistence.InternalChasmNode{
				Metadata: internalmongo.DataBlobFromBlob(e.Metadata),
				Data:     internalmongo.DataBlobFromBlob(e.Data),
			}
		}
	}
	return state, nil
}

// int64BlobsOrdered + stringBlobsOrdered + chasmNodesOrdered produce a
// deterministic slice from a map. Sorted order makes BSON documents
// reproducible across writes — useful for tests and for cluster-replay
// debugging (two identical writes produce identical documents).

func int64BlobsOrdered(m map[int64]*commonpb.DataBlob) []Int64KeyedBlob {
	keys := make([]int64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([]Int64KeyedBlob, 0, len(keys))
	for _, k := range keys {
		out = append(out, Int64KeyedBlob{Key: k, Blob: internalmongo.BlobFromDataBlob(m[k])})
	}
	return out
}

func stringBlobsOrdered(m map[string]*commonpb.DataBlob) []StringKeyedBlob {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]StringKeyedBlob, 0, len(keys))
	for _, k := range keys {
		out = append(out, StringKeyedBlob{Key: k, Blob: internalmongo.BlobFromDataBlob(m[k])})
	}
	return out
}

func chasmNodesOrdered(m map[string]persistence.InternalChasmNode) []ChasmNodeEntry {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]ChasmNodeEntry, 0, len(keys))
	for _, k := range keys {
		n := m[k]
		out = append(out, ChasmNodeEntry{
			Key:      k,
			Metadata: internalmongo.BlobFromDataBlob(n.Metadata),
			Data:     internalmongo.BlobFromDataBlob(n.Data),
		})
	}
	return out
}
