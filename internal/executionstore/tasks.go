package executionstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

// historyTaskDoc is the BSON projection of a single history task. Keyed
// on "<shard>:<cat>:<fire_ns>:<task_id>". Immediate categories store
// fire_time = DefaultFireTime (epoch 0) so the single compound index
// serves both category types.
//
// Upstream names a collection per category
// (history_tasks_transfer, history_tasks_timer, …); we pack all four
// into one collection with a category_id discriminator. This keeps the
// index count down and preserves the (shard_id, category_id,
// fire_time, task_id) ordering.
type historyTaskDoc struct {
	ID         string              `bson:"_id"`
	ShardID    int32               `bson:"shard_id"`
	CategoryID int32               `bson:"category_id"`
	FireTime   time.Time           `bson:"fire_time"`
	TaskID     int64               `bson:"task_id"`
	Blob       *internalmongo.Blob `bson:"blob"`
}

const historyTasksColl = "history_tasks"

func historyTaskID(shardID int32, cat tasks.Category, key tasks.Key) string {
	// fire_ns is time-unix-nano zero-padded so lexical _id ordering
	// still matches chronological ordering across scheduled tasks.
	return fmt.Sprintf("%d:%d:%020d:%020d", shardID, cat.ID(), key.FireTime.UnixNano(), key.TaskID)
}

// AddHistoryTasks inserts a batch of tasks for a shard atomically.
// Each task belongs to a category; we store them in one
// collection with a category_id discriminator and a leading shard_id
// in the compound index.
//
// Standalone AddHistoryTasks is used by Temporal's reconciliation paths.
// Tasks carried on the Create/Update/ConflictResolve requests land via
// insertTasksIn inside the same transaction as the execution mutation
// (backlog B-2).
func (s *Stub) AddHistoryTasks(
	ctx context.Context,
	req *persistence.InternalAddHistoryTasksRequest,
) (retErr error) {
	defer s.observeOp("AddHistoryTasks")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("executionstore: uninitialised or nil AddHistoryTasks request")
	}

	docs := s.tasksToHistoryTaskDocs(req.ShardID, req.Tasks)
	if len(docs) == 0 {
		return nil
	}

	sess, err := s.db.Client().StartSession()
	if err != nil {
		return internalmongo.Translate(err, "executionstore.AddHistoryTasks.session")
	}
	defer sess.EndSession(ctx)

	_, err = internalmongo.WithTxn[struct{}](ctx, internalmongo.NewSessionRunner(sess),
		func(sctx context.Context) (struct{}, error) {
			if err := s.assertShardRange(sctx, req.ShardID, req.RangeID); err != nil {
				return struct{}{}, err
			}
			return struct{}{}, s.insertTasksIn(sctx, docs)
		})
	return err
}

// tasksToHistoryTaskDocs projects a per-category task map into the flat
// BSON document list used by both the standalone AddHistoryTasks path
// and the in-transaction B-2 callers.
func (s *Stub) tasksToHistoryTaskDocs(shardID int32, perCat map[tasks.Category][]persistence.InternalHistoryTask) []interface{} {
	var docs []interface{}
	for cat, list := range perCat {
		for _, task := range list {
			docs = append(docs, historyTaskDoc{
				ID:         historyTaskID(shardID, cat, task.Key),
				ShardID:    shardID,
				CategoryID: int32(cat.ID()),
				FireTime:   task.Key.FireTime.UTC(),
				TaskID:     task.Key.TaskID,
				Blob:       internalmongo.BlobFromDataBlob(task.Blob),
			})
		}
	}
	return docs
}

// insertTasksIn bulk-inserts history_task docs in the caller's session.
// A duplicate task ID is an error: any write error aborts the server-side
// transaction, so it cannot be ignored, and task IDs are allocated
// uniquely per shard range.
func (s *Stub) insertTasksIn(ctx context.Context, docs []interface{}) error {
	if len(docs) == 0 {
		return nil
	}
	_, err := s.db.Collection(historyTasksColl).InsertMany(ctx, docs,
		options.InsertMany().SetOrdered(false))
	if err != nil {
		return internalmongo.Wrap(err, "executionstore.insertTasksIn")
	}
	return nil
}

// getHistoryTasksPageToken encodes the resume cursor for
// GetHistoryTasks. Using JSON keeps the token self-describing for
// easier debugging; the encoding is opaque to callers.
type getHistoryTasksPageToken struct {
	FireNS int64 `json:"fire_ns"`
	TaskID int64 `json:"task_id"`
}

// GetHistoryTasks returns tasks for (shard, category) in ascending
// (fire_time, task_id) order, bounded by the inclusive-min/
// exclusive-max range.
func (s *Stub) GetHistoryTasks(
	ctx context.Context,
	req *persistence.GetHistoryTasksRequest,
) (_ *persistence.InternalGetHistoryTasksResponse, retErr error) {
	defer s.observeOp("GetHistoryTasks")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("executionstore: uninitialised or nil GetHistoryTasks request")
	}
	catID := int32(req.TaskCategory.ID())
	pageSize := req.BatchSize
	if pageSize <= 0 {
		pageSize = 100
	}

	filter := bson.M{
		"shard_id":    req.ShardID,
		"category_id": catID,
	}

	// Range predicate over (fire_time, task_id) treated as a single
	// lexicographic key. A row is in [minKey, maxKey) iff:
	//   NOT less_than(minKey)  AND  less_than(maxKey)
	// which is equivalent to:
	//   greater_or_equal(minKey) AND less_than(maxKey).
	clauses := []bson.M{
		geKey(req.InclusiveMinTaskKey.FireTime.UTC(), req.InclusiveMinTaskKey.TaskID),
		ltKey(req.ExclusiveMaxTaskKey.FireTime.UTC(), req.ExclusiveMaxTaskKey.TaskID),
	}

	if len(req.NextPageToken) > 0 {
		var tok getHistoryTasksPageToken
		if err := json.Unmarshal(req.NextPageToken, &tok); err != nil {
			return nil, internalmongo.Translate(err, "executionstore.GetHistoryTasks.token")
		}
		// Resume strictly after the last returned (fire_time, task_id).
		clauses = append(clauses, gtKey(time.Unix(0, tok.FireNS).UTC(), tok.TaskID))
	}
	filter["$and"] = clauses

	opts := options.Find().
		SetSort(bson.D{{Key: "fire_time", Value: 1}, {Key: "task_id", Value: 1}}).
		SetLimit(int64(pageSize) + 1)

	cursor, err := s.db.Collection(historyTasksColl).Find(ctx, filter, opts)
	if err != nil {
		return nil, internalmongo.Translate(err, "executionstore.GetHistoryTasks.find")
	}
	defer cursor.Close(ctx)

	var docs []historyTaskDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, internalmongo.Translate(err, "executionstore.GetHistoryTasks.decode")
	}

	resp := &persistence.InternalGetHistoryTasksResponse{}
	for i, d := range docs {
		if i == pageSize {
			last := docs[pageSize-1]
			tok, _ := json.Marshal(getHistoryTasksPageToken{
				FireNS: last.FireTime.UnixNano(),
				TaskID: last.TaskID,
			})
			resp.NextPageToken = tok
			break
		}
		resp.Tasks = append(resp.Tasks, persistence.InternalHistoryTask{
			Key:  tasks.NewKey(d.FireTime, d.TaskID),
			Blob: internalmongo.DataBlobFromBlob(d.Blob),
		})
	}
	return resp, nil
}

// CompleteHistoryTask deletes a single task. Non-existent tasks return
// nil.
func (s *Stub) CompleteHistoryTask(
	ctx context.Context,
	req *persistence.CompleteHistoryTaskRequest,
) (retErr error) {
	defer s.observeOp("CompleteHistoryTask")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("executionstore: uninitialised or nil CompleteHistoryTask request")
	}
	_, err := s.db.Collection(historyTasksColl).DeleteOne(ctx,
		bson.M{"_id": historyTaskID(req.ShardID, req.TaskCategory, req.TaskKey)})
	if err != nil {
		return internalmongo.Translate(err, "executionstore.CompleteHistoryTask")
	}
	return nil
}

// RangeCompleteHistoryTasks deletes every task in [min, max) for
// the given (shard, category).
func (s *Stub) RangeCompleteHistoryTasks(
	ctx context.Context,
	req *persistence.RangeCompleteHistoryTasksRequest,
) (retErr error) {
	defer s.observeOp("RangeCompleteHistoryTasks")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("executionstore: uninitialised or nil RangeCompleteHistoryTasks request")
	}
	filter := bson.M{
		"shard_id":    req.ShardID,
		"category_id": int32(req.TaskCategory.ID()),
		"$and": []bson.M{
			geKey(req.InclusiveMinTaskKey.FireTime.UTC(), req.InclusiveMinTaskKey.TaskID),
			ltKey(req.ExclusiveMaxTaskKey.FireTime.UTC(), req.ExclusiveMaxTaskKey.TaskID),
		},
	}
	_, err := s.db.Collection(historyTasksColl).DeleteMany(ctx, filter)
	if err != nil {
		return internalmongo.Translate(err, "executionstore.RangeCompleteHistoryTasks")
	}
	return nil
}

// ltKey / geKey / gtKey build the lexicographic comparison clauses for
// the (fire_time, task_id) compound key. Each returns a bson.M suitable
// for use as a single element of a $and array.

func ltKey(fire time.Time, taskID int64) bson.M {
	return bson.M{"$or": bson.A{
		bson.M{"fire_time": bson.M{"$lt": fire}},
		bson.M{"fire_time": fire, "task_id": bson.M{"$lt": taskID}},
	}}
}

func geKey(fire time.Time, taskID int64) bson.M {
	return bson.M{"$or": bson.A{
		bson.M{"fire_time": bson.M{"$gt": fire}},
		bson.M{"fire_time": fire, "task_id": bson.M{"$gte": taskID}},
	}}
}

func gtKey(fire time.Time, taskID int64) bson.M {
	return bson.M{"$or": bson.A{
		bson.M{"fire_time": bson.M{"$gt": fire}},
		bson.M{"fire_time": fire, "task_id": bson.M{"$gt": taskID}},
	}}
}

// EnsureTaskIndexes installs the compound index history tasks scan by.
func EnsureTaskIndexes(ctx context.Context, db *mongo.Database) error {
	_, err := db.Collection(historyTasksColl).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "shard_id", Value: 1},
			{Key: "category_id", Value: 1},
			{Key: "fire_time", Value: 1},
			{Key: "task_id", Value: 1},
		},
		Options: options.Index().SetName("history_tasks_shard_cat_fire_task"),
	})
	if err != nil {
		return internalmongo.Translate(err, "executionstore.EnsureTaskIndexes")
	}
	return nil
}
