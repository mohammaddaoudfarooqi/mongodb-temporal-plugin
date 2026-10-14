package executionstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

const replicationDLQColl = "replication_dlq"

// replicationDLQDoc stores one DLQ'd replication task. Keyed on
// "<shard>:<source_cluster>:<task_id>" so the compound index can serve
// per-cluster queries with a single range scan.
type replicationDLQDoc struct {
	ID                string              `bson:"_id"`
	ShardID           int32               `bson:"shard_id"`
	SourceClusterName string              `bson:"source_cluster_name"`
	TaskID            int64               `bson:"task_id"`
	Info              *internalmongo.Blob `bson:"info"`
}

func replicationDLQDocID(shardID int32, source string, taskID int64) string {
	return fmt.Sprintf("%d:%s:%020d", shardID, source, taskID)
}

// PutReplicationTaskToDLQ serializes the ReplicationTaskInfo proto and
// inserts it into the DLQ collection. The plugin uses the
// same serializer the upstream manager layer uses on the read side so
// the blob is format-identical.
func (s *Stub) PutReplicationTaskToDLQ(
	ctx context.Context,
	req *persistence.PutReplicationTaskToDLQRequest,
) (retErr error) {
	defer s.observeOp("PutReplicationTaskToDLQ")(&retErr)
	if s.db == nil || req == nil || req.TaskInfo == nil {
		return errors.New("executionstore: uninitialised or nil PutReplicationTaskToDLQ request")
	}
	blob, err := serialization.NewSerializer().ReplicationTaskInfoToBlob(req.TaskInfo)
	if err != nil {
		return internalmongo.Translate(err, "executionstore.PutReplicationTaskToDLQ.serialize")
	}

	doc := replicationDLQDoc{
		ID:                replicationDLQDocID(req.ShardID, req.SourceClusterName, req.TaskInfo.TaskId),
		ShardID:           req.ShardID,
		SourceClusterName: req.SourceClusterName,
		TaskID:            req.TaskInfo.TaskId,
		Info:              internalmongo.BlobFromDataBlob(blob),
	}
	// Upsert for idempotency under retries.
	_, err = s.db.Collection(replicationDLQColl).ReplaceOne(ctx,
		bson.M{"_id": doc.ID}, doc,
		options.Replace().SetUpsert(true))
	if err != nil {
		return internalmongo.Translate(err, "executionstore.PutReplicationTaskToDLQ.insert")
	}
	return nil
}

// GetReplicationTasksFromDLQ returns a paginated slice of DLQ tasks for
// a given source cluster, ordered by task_id.
func (s *Stub) GetReplicationTasksFromDLQ(
	ctx context.Context,
	req *persistence.GetReplicationTasksFromDLQRequest,
) (_ *persistence.InternalGetReplicationTasksFromDLQResponse, retErr error) {
	defer s.observeOp("GetReplicationTasksFromDLQ")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("executionstore: uninitialised or nil GetReplicationTasksFromDLQ request")
	}
	pageSize := req.BatchSize
	if pageSize <= 0 {
		pageSize = 100
	}

	minKey := req.InclusiveMinTaskKey.TaskID
	maxKey := req.ExclusiveMaxTaskKey.TaskID
	filter := bson.M{
		"shard_id":            req.ShardID,
		"source_cluster_name": req.SourceClusterName,
		"task_id":             bson.M{"$gte": minKey, "$lt": maxKey},
	}
	if len(req.NextPageToken) > 0 {
		// Page token is a decimal-string task_id for simplicity.
		var lastID int64
		fmt.Sscanf(string(req.NextPageToken), "%d", &lastID)
		filter["task_id"] = bson.M{"$gt": lastID, "$lt": maxKey}
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "task_id", Value: 1}}).
		SetLimit(int64(pageSize) + 1)

	cursor, err := s.db.Collection(replicationDLQColl).Find(ctx, filter, opts)
	if err != nil {
		return nil, internalmongo.Translate(err, "executionstore.GetReplicationTasksFromDLQ")
	}
	defer cursor.Close(ctx)

	var docs []replicationDLQDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, internalmongo.Translate(err, "executionstore.GetReplicationTasksFromDLQ.decode")
	}

	resp := &persistence.InternalGetReplicationTasksFromDLQResponse{}
	for i, d := range docs {
		if i == pageSize {
			resp.NextPageToken = []byte(fmt.Sprintf("%d", docs[pageSize-1].TaskID))
			break
		}
		resp.Tasks = append(resp.Tasks, persistence.InternalHistoryTask{
			Key:  tasks.NewImmediateKey(d.TaskID),
			Blob: internalmongo.DataBlobFromBlob(d.Info),
		})
	}
	return resp, nil
}

// DeleteReplicationTaskFromDLQ removes one DLQ entry.
func (s *Stub) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	req *persistence.DeleteReplicationTaskFromDLQRequest,
) (retErr error) {
	defer s.observeOp("DeleteReplicationTaskFromDLQ")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("executionstore: uninitialised or nil DeleteReplicationTaskFromDLQ request")
	}
	_, err := s.db.Collection(replicationDLQColl).DeleteOne(ctx,
		bson.M{"_id": replicationDLQDocID(req.ShardID, req.SourceClusterName, req.TaskKey.TaskID)})
	if err != nil {
		return internalmongo.Translate(err, "executionstore.DeleteReplicationTaskFromDLQ")
	}
	return nil
}

// RangeDeleteReplicationTaskFromDLQ removes DLQ entries for a source
// cluster in [min, max).
func (s *Stub) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	req *persistence.RangeDeleteReplicationTaskFromDLQRequest,
) (retErr error) {
	defer s.observeOp("RangeDeleteReplicationTaskFromDLQ")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("executionstore: uninitialised or nil RangeDeleteReplicationTaskFromDLQ request")
	}
	_, err := s.db.Collection(replicationDLQColl).DeleteMany(ctx, bson.M{
		"shard_id":            req.ShardID,
		"source_cluster_name": req.SourceClusterName,
		"task_id": bson.M{
			"$gte": req.InclusiveMinTaskKey.TaskID,
			"$lt":  req.ExclusiveMaxTaskKey.TaskID,
		},
	})
	if err != nil {
		return internalmongo.Translate(err, "executionstore.RangeDeleteReplicationTaskFromDLQ")
	}
	return nil
}

// IsReplicationDLQEmpty reports whether any DLQ entry exists for the
// given (shard, source cluster).
func (s *Stub) IsReplicationDLQEmpty(
	ctx context.Context,
	req *persistence.GetReplicationTasksFromDLQRequest,
) (_ bool, retErr error) {
	defer s.observeOp("IsReplicationDLQEmpty")(&retErr)
	if s.db == nil || req == nil {
		return false, errors.New("executionstore: uninitialised or nil IsReplicationDLQEmpty request")
	}
	var doc replicationDLQDoc
	err := s.db.Collection(replicationDLQColl).FindOne(ctx, bson.M{
		"shard_id":            req.ShardID,
		"source_cluster_name": req.SourceClusterName,
	}).Decode(&doc)
	if err == nil {
		return false, nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return true, nil
	}
	return false, internalmongo.Translate(err, "executionstore.IsReplicationDLQEmpty")
}

// EnsureDLQIndexes creates the compound index used by DLQ range queries.
func EnsureDLQIndexes(ctx context.Context, db *mongo.Database) error {
	_, err := db.Collection(replicationDLQColl).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "shard_id", Value: 1},
			{Key: "source_cluster_name", Value: 1},
			{Key: "task_id", Value: 1},
		},
		Options: options.Index().SetName("replication_dlq_shard_src_task"),
	})
	if err != nil {
		return internalmongo.Translate(err, "executionstore.EnsureDLQIndexes")
	}
	return nil
}

// silence an unused-import lint if time is not otherwise used in this
// file via the serializer; keeping the reference avoids churn when we
// extend to per-entry TTL later.
var _ = time.Time{}
