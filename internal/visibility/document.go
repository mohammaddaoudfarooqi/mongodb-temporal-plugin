package visibility

import (
	"fmt"
	"strconv"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"google.golang.org/protobuf/proto"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

// Doc is the BSON projection of one workflow execution's visibility
// record. System search attributes are top-level fields for
// query-friendly indexing; custom search attributes live in a sub-map
// under `search_attributes`, preserving BSON-native type fidelity for
// Keyword / Int / Double / Bool / Datetime / KeywordList.
//
// Keyed on "<namespace_id>:<run_id>".
type Doc struct {
	ID                        string              `bson:"_id"`
	NamespaceID               string              `bson:"namespace_id"`
	RunID                     string              `bson:"run_id"`
	WorkflowID                string              `bson:"workflow_id"`
	WorkflowTypeName          string              `bson:"workflow_type_name"`
	TaskQueue                 string              `bson:"task_queue"`
	StartTime                 time.Time           `bson:"start_time,omitempty"`
	ExecutionTime             time.Time           `bson:"execution_time,omitempty"`
	CloseTime                 time.Time           `bson:"close_time,omitempty"`
	Status                    int32               `bson:"status"` // WorkflowExecutionStatus enum
	HistoryLength             int64               `bson:"history_length,omitempty"`
	HistorySizeBytes          int64               `bson:"history_size_bytes,omitempty"`
	StateTransitionCount      int64               `bson:"state_transition_count,omitempty"`
	ExecutionDuration         int64               `bson:"execution_duration_ns,omitempty"`
	ParentWorkflowID          string              `bson:"parent_workflow_id,omitempty"`
	ParentRunID               string              `bson:"parent_run_id,omitempty"`
	RootWorkflowID            string              `bson:"root_workflow_id,omitempty"`
	RootRunID                 string              `bson:"root_run_id,omitempty"`
	TemporalNamespaceDivision string              `bson:"temporal_namespace_division,omitempty"`
	ArchetypeID               uint32              `bson:"archetype_id,omitempty"`
	Memo                      *internalmongo.Blob `bson:"memo,omitempty"`
	SearchAttributesEncoded   *internalmongo.Blob `bson:"search_attributes_encoded,omitempty"`
	SearchAttributes          map[string]any      `bson:"search_attributes,omitempty"`
	TaskID                    int64               `bson:"task_id,omitempty"`
	ShardID                   int32               `bson:"shard_id,omitempty"`
	UpdateTime                time.Time           `bson:"update_time"`
}

// DocID composites the primary key shape. Single source of truth so
// every write and read agrees on the _id format.
func DocID(namespaceID, runID string) string {
	return fmt.Sprintf("%s:%s", namespaceID, runID)
}

// BaseFromStartedRequest projects the VisibilityRequestBase fields of a
// RecordWorkflowExecutionStarted request into a Doc, leaving the
// write-time fields (close_time, history_length, etc.) zero-valued for
// the caller to decide.
func BaseFromStartedRequest(req *store.InternalRecordWorkflowExecutionStartedRequest) *Doc {
	if req == nil || req.InternalVisibilityRequestBase == nil {
		return nil
	}
	return baseFromVisibilityRequestBase(req.InternalVisibilityRequestBase)
}

// BaseFromClosedRequest extends BaseFromStartedRequest with close-time
// fields required by RecordWorkflowExecutionClosed.
func BaseFromClosedRequest(req *store.InternalRecordWorkflowExecutionClosedRequest) *Doc {
	if req == nil || req.InternalVisibilityRequestBase == nil {
		return nil
	}
	doc := baseFromVisibilityRequestBase(req.InternalVisibilityRequestBase)
	doc.CloseTime = req.CloseTime.UTC()
	doc.HistoryLength = req.HistoryLength
	doc.HistorySizeBytes = req.HistorySizeBytes
	doc.StateTransitionCount = req.StateTransitionCount
	doc.ExecutionDuration = int64(req.ExecutionDuration)
	return doc
}

// BaseFromUpsertRequest handles UpsertWorkflowExecution. Identical to
// BaseFromStartedRequest today; carved as a separate entry point so a
// future divergence (e.g., partial upsert semantics) has a natural seam.
func BaseFromUpsertRequest(req *store.InternalUpsertWorkflowExecutionRequest) *Doc {
	if req == nil || req.InternalVisibilityRequestBase == nil {
		return nil
	}
	return baseFromVisibilityRequestBase(req.InternalVisibilityRequestBase)
}

func baseFromVisibilityRequestBase(base *store.InternalVisibilityRequestBase) *Doc {
	doc := &Doc{
		ID:               DocID(base.NamespaceID, base.RunID),
		NamespaceID:      base.NamespaceID,
		RunID:            base.RunID,
		WorkflowID:       base.WorkflowID,
		WorkflowTypeName: base.WorkflowTypeName,
		TaskQueue:        base.TaskQueue,
		StartTime:        base.StartTime.UTC(),
		ExecutionTime:    base.ExecutionTime.UTC(),
		Status:           int32(base.Status),
		TaskID:           base.TaskID,
		ShardID:          base.ShardID,
		RootWorkflowID:   base.RootWorkflowID,
		RootRunID:        base.RootRunID,
		UpdateTime:       time.Now().UTC(),
	}
	if base.ParentWorkflowID != nil {
		doc.ParentWorkflowID = *base.ParentWorkflowID
	}
	if base.ParentRunID != nil {
		doc.ParentRunID = *base.ParentRunID
	}
	if base.Memo != nil {
		doc.Memo = internalmongo.BlobFromDataBlob(base.Memo)
	}
	doc.SearchAttributes = flattenSearchAttributes(base.SearchAttributes)
	doc.SearchAttributesEncoded = encodeSearchAttributes(base.SearchAttributes)
	// The namespace division (e.g. TemporalScheduler, or a CHASM
	// archetype ID) is a top-level field so the default
	// "division IS NULL" filter hides internal executions.
	if div, ok := doc.SearchAttributes[sadefs.TemporalNamespaceDivision].(string); ok {
		doc.TemporalNamespaceDivision = div
	}
	// Archetype stamp: CHASM writers stamp the reserved "ArchetypeId"
	// search attribute. Surface it into a first-class indexed field so
	// ListChasmExecutions can filter without descending into the SA map
	//. If the SA is absent, ArchetypeID stays
	// zero — list requests with a zero filter are then a no-op, which
	// is the safe default for non-CHASM workflows.
	if v, ok := doc.SearchAttributes["ArchetypeId"]; ok {
		if id, ok := archetypeIDFromSAValue(v); ok {
			doc.ArchetypeID = id
		}
	}
	// Upstream CHASM writers store the archetype ID as the namespace
	// division string.
	if doc.ArchetypeID == 0 && doc.TemporalNamespaceDivision != "" {
		if id, err := strconv.ParseUint(doc.TemporalNamespaceDivision, 10, 32); err == nil {
			doc.ArchetypeID = uint32(id)
		}
	}
	return doc
}

// archetypeIDFromSAValue decodes the ArchetypeId search attribute.
// Upstream writers can stamp it as:
//   - a Go integer (uint32/int/int32/int64) via a custom codec, or
//   - a 4-byte big-endian payload — which is what flattenSearchAttributes
//     produces when the raw commonpb.Payload.Data is ferried through.
//
// Both paths decode here; other shapes are dropped silently (the raw
// value stays in the SA map so a caller debugging by hand can see it).
func archetypeIDFromSAValue(v any) (uint32, bool) {
	switch x := v.(type) {
	case uint32:
		return x, true
	case int:
		if x >= 0 {
			return uint32(x), true
		}
	case int32:
		if x >= 0 {
			return uint32(x), true
		}
	case int64:
		if x >= 0 && x <= int64(^uint32(0)) {
			return uint32(x), true
		}
	case []byte:
		if len(x) == 4 {
			return uint32(x[0])<<24 | uint32(x[1])<<16 | uint32(x[2])<<8 | uint32(x[3]), true
		}
	}
	return 0, false
}

// flattenSearchAttributes decodes each search attribute into its typed
// value (Keyword/Text → string, KeywordList → []string, Int → int64,
// Double → float64, Bool → bool, Datetime → time.Time) using the type
// metadata upstream's searchattribute.Encode stamps on every payload, so
// queries compare BSON values of the right type. A payload
// that cannot be decoded keeps its raw bytes, which preserves the
// ArchetypeId fixture shape older writers used.
func flattenSearchAttributes(sa *commonpb.SearchAttributes) map[string]any {
	if sa == nil || len(sa.IndexedFields) == 0 {
		return nil
	}
	out := make(map[string]any, len(sa.IndexedFields))
	for name, p := range sa.IndexedFields {
		if p == nil {
			continue
		}
		v, err := sadefs.DecodeValue(p, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, true)
		if err != nil || v == nil {
			out[name] = p.Data
			continue
		}
		if tm, ok := v.(time.Time); ok {
			v = tm.UTC()
		}
		out[name] = v
	}
	return out
}

// typeUntypedSearchAttributes re-decodes, by the provider's registered
// type, every attribute flattenSearchAttributes had to keep as raw bytes
// because its payload carried no "type" metadata — the scheduler writes
// TemporalScheduledById that way. Names the provider does
// not know keep their raw bytes.
func (s *Store) typeUntypedSearchAttributes(doc *Doc, sa *commonpb.SearchAttributes) {
	if s.saProvider == nil || sa == nil {
		return
	}
	var typeMap *searchattribute.NameTypeMap
	for name, v := range doc.SearchAttributes {
		if _, raw := v.([]byte); !raw {
			continue
		}
		if typeMap == nil {
			m, err := s.saProvider.GetSearchAttributes(s.indexName, false)
			if err != nil {
				return
			}
			typeMap = &m
		}
		t, err := typeMap.GetType(name)
		if err != nil {
			continue
		}
		typed, err := sadefs.DecodeValue(sa.IndexedFields[name], t, true)
		if err != nil || typed == nil {
			continue
		}
		if tm, ok := typed.(time.Time); ok {
			typed = tm.UTC()
		}
		doc.SearchAttributes[name] = typed
	}
}

// encodeSearchAttributes keeps the original payloads (proto-encoded) so
// reads can return exactly what was written.
func encodeSearchAttributes(sa *commonpb.SearchAttributes) *internalmongo.Blob {
	if sa == nil || len(sa.IndexedFields) == 0 {
		return nil
	}
	raw, err := proto.Marshal(sa)
	if err != nil {
		return nil
	}
	return &internalmongo.Blob{Data: raw, Encoding: "proto3"}
}

// decodeSearchAttributes is the read-side counterpart of
// encodeSearchAttributes.
func decodeSearchAttributes(b *internalmongo.Blob) *commonpb.SearchAttributes {
	if b == nil || len(b.Data) == 0 {
		return nil
	}
	var sa commonpb.SearchAttributes
	if err := proto.Unmarshal(b.Data, &sa); err != nil {
		return nil
	}
	return &sa
}
