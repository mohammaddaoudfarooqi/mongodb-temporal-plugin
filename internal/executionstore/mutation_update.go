package executionstore

import (
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/persistence"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

// mutationToUpdatePipeline builds an aggregation-pipeline update that
// applies the mutation's Upsert/Delete maps to the embedded sub-arrays
// on the execution document WITHOUT rewriting the entire doc
// (backlog B-3).
//
// For each sub-collection the pipeline runs one $set stage shaped like:
//
//	{$set: {
//	  <field>: {
//	    $concatArrays: [
//	      {$filter: {input: "$<field>", as: "x",
//	                 cond: {$not: {$in: ["$$x.key", [<keys to purge>]]}}}},
//	      [<fresh upsert entries>]
//	    ]
//	  }
//	}}
//
// Purge keys = Union(Delete keys, Upsert keys). This is correct for both
// "add new" (no prior entry) and "replace existing" (prior entry removed,
// new entry re-appended) without needing arrayFilters.
//
// Top-level scalar fields (NextEventID, DBRecordVersion, ExecutionInfo,
// ExecutionState, Checksum, etc.) are set via a second $set stage.
// NewBufferedEvents / ClearBufferedEvents produce a dedicated $set on
// buffered_events.
//
// Returns the pipeline slice suitable for UpdateOne. Empty mutation
// collapses to an empty pipeline — callers treat that as a no-op.
func mutationToUpdatePipeline(mut persistence.InternalWorkflowMutation) mongo.Pipeline {
	stages := mongo.Pipeline{}

	// Top-level scalar + blob updates.
	topSet := bson.M{
		"next_event_id":     mut.NextEventID,
		"db_record_version": mut.DBRecordVersion + 0, // ensure Int64 encoding
		"update_time":       bson.M{"$toDate": "$$NOW"},
	}
	if mut.ExecutionInfoBlob != nil {
		topSet["execution_info"] = internalmongo.BlobFromDataBlob(mut.ExecutionInfoBlob)
	}
	if mut.ExecutionStateBlob != nil {
		topSet["execution_state"] = internalmongo.BlobFromDataBlob(mut.ExecutionStateBlob)
	}
	if mut.Checksum != nil {
		topSet["checksum"] = internalmongo.BlobFromDataBlob(mut.Checksum)
	}
	stages = append(stages, bson.D{{Key: "$set", Value: topSet}})

	// Embedded sub-collection maps.
	stages = appendInt64MapStage(stages, "activity_infos", mut.UpsertActivityInfos, mut.DeleteActivityInfos)
	stages = appendStringMapStage(stages, "timer_infos", toDataBlobMap(mut.UpsertTimerInfos), mut.DeleteTimerInfos)
	stages = appendInt64MapStage(stages, "child_execution_infos", mut.UpsertChildExecutionInfos, mut.DeleteChildExecutionInfos)
	stages = appendInt64MapStage(stages, "request_cancel_infos", mut.UpsertRequestCancelInfos, mut.DeleteRequestCancelInfos)
	stages = appendInt64MapStage(stages, "signal_infos", mut.UpsertSignalInfos, mut.DeleteSignalInfos)

	// SignalRequestedIDs is a set of strings, no keyed blob.
	stages = appendStringSetStage(stages, "signal_requested_ids",
		mut.UpsertSignalRequestedIDs, mut.DeleteSignalRequestedIDs)

	// Buffered events: either clear or append. Both are full-array
	// semantics — there's no key-level merge.
	if mut.ClearBufferedEvents {
		stages = append(stages, bson.D{{Key: "$set", Value: bson.M{"buffered_events": bson.A{}}}})
	}
	if mut.NewBufferedEvents != nil {
		if b := internalmongo.BlobFromDataBlob(mut.NewBufferedEvents); b != nil {
			stages = append(stages, bson.D{{Key: "$set", Value: bson.M{
				"buffered_events": bson.M{"$concatArrays": bson.A{
					bson.M{"$ifNull": bson.A{"$buffered_events", bson.A{}}},
					bson.A{*b},
				}},
			}}})
		}
	}

	// CHASM nodes. Upsert map + delete map, keyed by string.
	if len(mut.UpsertChasmNodes) > 0 || len(mut.DeleteChasmNodes) > 0 {
		stages = appendChasmStage(stages, mut.UpsertChasmNodes, mut.DeleteChasmNodes)
	}

	return stages
}

// toDataBlobMap converts commonpb.DataBlob map to the signature
// appendStringMapStage expects.
func toDataBlobMap(in map[string]*commonpb.DataBlob) map[string]*commonpb.DataBlob {
	return in
}

func appendInt64MapStage(
	stages mongo.Pipeline,
	field string,
	upserts map[int64]*commonpb.DataBlob,
	deletes map[int64]struct{},
) mongo.Pipeline {
	if len(upserts) == 0 && len(deletes) == 0 {
		return stages
	}
	purge := make([]int64, 0, len(upserts)+len(deletes))
	for k := range upserts {
		purge = append(purge, k)
	}
	for k := range deletes {
		purge = append(purge, k)
	}
	fresh := make(bson.A, 0, len(upserts))
	for k, v := range upserts {
		fresh = append(fresh, Int64KeyedBlob{Key: k, Blob: internalmongo.BlobFromDataBlob(v)})
	}
	return append(stages, bson.D{{Key: "$set", Value: bson.M{
		field: bson.M{"$concatArrays": bson.A{
			filterOutByKey(field, toAnySlice(purge)),
			literal(fresh),
		}},
	}}})
}

func appendStringMapStage(
	stages mongo.Pipeline,
	field string,
	upserts map[string]*commonpb.DataBlob,
	deletes map[string]struct{},
) mongo.Pipeline {
	if len(upserts) == 0 && len(deletes) == 0 {
		return stages
	}
	purge := make([]string, 0, len(upserts)+len(deletes))
	for k := range upserts {
		purge = append(purge, k)
	}
	for k := range deletes {
		purge = append(purge, k)
	}
	fresh := make(bson.A, 0, len(upserts))
	for k, v := range upserts {
		fresh = append(fresh, StringKeyedBlob{Key: k, Blob: internalmongo.BlobFromDataBlob(v)})
	}
	return append(stages, bson.D{{Key: "$set", Value: bson.M{
		field: bson.M{"$concatArrays": bson.A{
			filterOutByKey(field, toAnySlice(purge)),
			literal(fresh),
		}},
	}}})
}

func appendStringSetStage(
	stages mongo.Pipeline,
	field string,
	upserts map[string]struct{},
	deletes map[string]struct{},
) mongo.Pipeline {
	if len(upserts) == 0 && len(deletes) == 0 {
		return stages
	}
	purge := make([]string, 0, len(upserts)+len(deletes))
	for k := range upserts {
		purge = append(purge, k)
	}
	for k := range deletes {
		purge = append(purge, k)
	}
	fresh := make(bson.A, 0, len(upserts))
	for k := range upserts {
		fresh = append(fresh, k)
	}
	return append(stages, bson.D{{Key: "$set", Value: bson.M{
		field: bson.M{"$setUnion": bson.A{
			bson.M{"$filter": bson.M{
				"input": bson.M{"$ifNull": bson.A{"$" + field, bson.A{}}},
				"as":    "x",
				"cond":  bson.M{"$not": bson.M{"$in": bson.A{"$$x", literal(toAnySlice(purge))}}},
			}},
			literal(fresh),
		}},
	}}})
}

func appendChasmStage(
	stages mongo.Pipeline,
	upserts map[string]persistence.InternalChasmNode,
	deletes map[string]struct{},
) mongo.Pipeline {
	purge := make([]string, 0, len(upserts)+len(deletes))
	for k := range upserts {
		purge = append(purge, k)
	}
	for k := range deletes {
		purge = append(purge, k)
	}
	fresh := make(bson.A, 0, len(upserts))
	for k, v := range upserts {
		fresh = append(fresh, ChasmNodeEntry{
			Key:      k,
			Metadata: internalmongo.BlobFromDataBlob(v.Metadata),
			Data:     internalmongo.BlobFromDataBlob(v.Data),
		})
	}
	return append(stages, bson.D{{Key: "$set", Value: bson.M{
		"chasm_nodes": bson.M{"$concatArrays": bson.A{
			filterOutByKey("chasm_nodes", toAnySlice(purge)),
			literal(fresh),
		}},
	}}})
}

// filterOutByKey returns an aggregation expression that yields the
// input array with any element whose `.key` field is present in the
// given purge list removed. $ifNull handles the initial-write case
// where the field is absent.
func filterOutByKey(field string, purge bson.A) bson.M {
	return bson.M{
		"$filter": bson.M{
			"input": bson.M{"$ifNull": bson.A{"$" + field, bson.A{}}},
			"as":    "x",
			"cond":  bson.M{"$not": bson.M{"$in": bson.A{"$$x.key", literal(purge)}}},
		},
	}
}

// toAnySlice converts a typed slice to bson.A so it's legal inside
// a BSON document. Generics keep the int64/string variants in one spot.
func toAnySlice[T any](in []T) bson.A {
	out := make(bson.A, len(in))
	for i, v := range in {
		out[i] = v
	}
	return out
}

// literal wraps a value so the aggregation engine never interprets it.
// Map keys such as timer IDs are user-controlled; without $literal a key
// like "$foo" is read as a field path and "$$NOW" as a variable
// .
func literal(v any) bson.M {
	return bson.M{"$literal": v}
}
