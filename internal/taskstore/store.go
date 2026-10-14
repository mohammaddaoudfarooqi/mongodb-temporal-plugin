// Package taskstore is the MongoDB-backed implementation of
// persistence.TaskStore.
//
// Storage model:
//
//	collection: task_queues
//	  _id: "<namespace_id>:<task_queue>:<task_type>:<subqueue>"
//	  namespace_id, task_queue, task_type, subqueue
//	  range_id (int64)           -- CAS guard
//	  task_queue_info: {data, encoding}
//	  expiry_time                -- optional
//	  update_time
//
//	collection: tasks
//	  _id: "<namespace_id>:<task_queue>:<task_type>:<subqueue>:<task_id>"
//	  namespace_id, task_queue, task_type, subqueue, task_id
//	  task: {data, encoding}
//	  expiry_time: DateTime      -- indexed with TTL
//
//	collections: fair_task_queues, fair_tasks
//	  same shapes, for the fair backlog
//
//	collection: task_queue_user_data
//	  _id: "<namespace_id>:<task_queue>"
//	  namespace_id, task_queue
//	  version, user_data:{…}, build_ids:[…]
//
//	collection: build_id_to_task_queue
//	  _id: "<namespace_id>:<build_id>:<task_queue>"
//	  namespace_id, build_id, task_queue
//
// Every list query leads with namespace_id so cross-namespace
// operations never scatter-gather.
package taskstore

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

const (
	taskQueuesColl     = "task_queues"
	tasksColl          = "tasks"
	fairTaskQueuesColl = "fair_task_queues"
	fairTasksColl      = "fair_tasks"
	userDataColl       = "task_queue_user_data"
	buildIDIndexColl   = "build_id_to_task_queue"
)

type taskQueueDoc struct {
	ID            string              `bson:"_id"`
	NamespaceID   string              `bson:"namespace_id"`
	TaskQueue     string              `bson:"task_queue"`
	TaskType      int32               `bson:"task_type"`
	Subqueue      int32               `bson:"subqueue"`
	RangeID       int64               `bson:"range_id"`
	TaskQueueInfo *internalmongo.Blob `bson:"task_queue_info"`
	ExpiryTime    time.Time           `bson:"expiry_time,omitempty"`
	UpdateTime    time.Time           `bson:"update_time"`
}

type taskDoc struct {
	ID          string              `bson:"_id"`
	NamespaceID string              `bson:"namespace_id"`
	TaskQueue   string              `bson:"task_queue"`
	TaskType    int32               `bson:"task_type"`
	Subqueue    int32               `bson:"subqueue"`
	Pass        int64               `bson:"pass"`
	TaskID      int64               `bson:"task_id"`
	Task        *internalmongo.Blob `bson:"task"`
	ExpiryTime  time.Time           `bson:"expiry_time,omitempty"`
}

type userDataDoc struct {
	ID          string              `bson:"_id"`
	NamespaceID string              `bson:"namespace_id"`
	TaskQueue   string              `bson:"task_queue"`
	Version     int64               `bson:"version"`
	UserData    *internalmongo.Blob `bson:"user_data"`
	BuildIDs    []string            `bson:"build_ids,omitempty"`
}

func taskQueueDocID(nsID, tq string, ttype enumspb.TaskQueueType, subq int32) string {
	return fmt.Sprintf("%s:%s:%d:%d", nsID, tq, int32(ttype), subq)
}
func taskDocID(nsID, tq string, ttype enumspb.TaskQueueType, subq int32, taskID int64) string {
	return fmt.Sprintf("%s:%s:%d:%d:%020d", nsID, tq, int32(ttype), subq, taskID)
}
func userDataDocID(nsID, tq string) string {
	return fmt.Sprintf("%s:%s", nsID, tq)
}

// CreateTaskQueue inserts a new task queue.
func (s *Stub) CreateTaskQueue(
	ctx context.Context,
	req *persistence.InternalCreateTaskQueueRequest,
) (retErr error) {
	defer s.observeOp("CreateTaskQueue")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("taskstore: uninitialised or nil request")
	}
	doc := taskQueueDoc{
		ID:            taskQueueDocID(req.NamespaceID, req.TaskQueue, req.TaskType, 0),
		NamespaceID:   req.NamespaceID,
		TaskQueue:     req.TaskQueue,
		TaskType:      int32(req.TaskType),
		RangeID:       req.RangeID,
		TaskQueueInfo: internalmongo.BlobFromDataBlob(req.TaskQueueInfo),
		UpdateTime:    time.Now().UTC(),
	}
	if req.ExpiryTime != nil {
		doc.ExpiryTime = req.ExpiryTime.AsTime()
	}
	_, err := s.db.Collection(s.queuesColl).InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return &persistence.ConditionFailedError{
				Msg: fmt.Sprintf("task queue %s already exists", doc.ID),
			}
		}
		return internalmongo.Translate(err, "taskstore.CreateTaskQueue")
	}
	return nil
}

// GetTaskQueue returns a task queue by key, NotFound otherwise.
func (s *Stub) GetTaskQueue(
	ctx context.Context,
	req *persistence.InternalGetTaskQueueRequest,
) (_ *persistence.InternalGetTaskQueueResponse, retErr error) {
	defer s.observeOp("GetTaskQueue")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("taskstore: uninitialised or nil request")
	}
	var doc taskQueueDoc
	err := s.db.Collection(s.queuesColl).FindOne(ctx,
		bson.M{"_id": taskQueueDocID(req.NamespaceID, req.TaskQueue, req.TaskType, 0)}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, serviceerror.NewNotFound(fmt.Sprintf(
				"task queue ns=%s tq=%s type=%v not found",
				req.NamespaceID, req.TaskQueue, req.TaskType))
		}
		return nil, internalmongo.Translate(err, "taskstore.GetTaskQueue")
	}
	return &persistence.InternalGetTaskQueueResponse{
		RangeID:       doc.RangeID,
		TaskQueueInfo: internalmongo.DataBlobFromBlob(doc.TaskQueueInfo),
	}, nil
}

// UpdateTaskQueue replaces a task queue under a RangeID CAS.
func (s *Stub) UpdateTaskQueue(
	ctx context.Context,
	req *persistence.InternalUpdateTaskQueueRequest,
) (_ *persistence.UpdateTaskQueueResponse, retErr error) {
	defer s.observeOp("UpdateTaskQueue")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("taskstore: uninitialised or nil request")
	}
	id := taskQueueDocID(req.NamespaceID, req.TaskQueue, req.TaskType, 0)
	filter := bson.M{"_id": id, "range_id": req.PrevRangeID}
	update := bson.M{"$set": bson.M{
		"range_id":        req.RangeID,
		"task_queue_info": internalmongo.BlobFromDataBlob(req.TaskQueueInfo),
		"update_time":     time.Now().UTC(),
	}}
	res, err := s.db.Collection(s.queuesColl).UpdateOne(ctx, filter, update)
	if err != nil {
		return nil, internalmongo.Translate(err, "taskstore.UpdateTaskQueue")
	}
	if res.MatchedCount == 0 {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("task queue %s stale PrevRangeID %d", id, req.PrevRangeID),
		}
	}
	return &persistence.UpdateTaskQueueResponse{}, nil
}

// ListTaskQueue returns a paginated list of task queues (admin op).
func (s *Stub) ListTaskQueue(
	ctx context.Context,
	req *persistence.ListTaskQueueRequest,
) (_ *persistence.InternalListTaskQueueResponse, retErr error) {
	defer s.observeOp("ListTaskQueue")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("taskstore: uninitialised or nil request")
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	filter := bson.M{}
	if len(req.PageToken) > 0 {
		filter["_id"] = bson.M{"$gt": string(req.PageToken)}
	}
	cursor, err := s.db.Collection(s.queuesColl).Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(pageSize)+1))
	if err != nil {
		return nil, internalmongo.Translate(err, "taskstore.ListTaskQueue")
	}
	defer cursor.Close(ctx)
	var docs []taskQueueDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, internalmongo.Translate(err, "taskstore.ListTaskQueue.decode")
	}
	resp := &persistence.InternalListTaskQueueResponse{}
	for i, d := range docs {
		if i == pageSize {
			resp.NextPageToken = []byte(docs[pageSize-1].ID)
			break
		}
		resp.Items = append(resp.Items, &persistence.InternalListTaskQueueItem{
			TaskQueue: internalmongo.DataBlobFromBlob(d.TaskQueueInfo),
			RangeID:   d.RangeID,
		})
	}
	return resp, nil
}

// DeleteTaskQueue removes the queue and every task in it atomically.
func (s *Stub) DeleteTaskQueue(
	ctx context.Context,
	req *persistence.DeleteTaskQueueRequest,
) (retErr error) {
	defer s.observeOp("DeleteTaskQueue")(&retErr)
	if s.db == nil || req == nil || req.TaskQueue == nil {
		return errors.New("taskstore: uninitialised or nil DeleteTaskQueue request")
	}
	sess, err := s.db.Client().StartSession()
	if err != nil {
		return internalmongo.Translate(err, "taskstore.DeleteTaskQueue.session")
	}
	defer sess.EndSession(ctx)
	tq := req.TaskQueue

	_, err = internalmongo.WithTxn[struct{}](ctx, internalmongo.NewSessionRunner(sess),
		func(sctx context.Context) (struct{}, error) {
			filter := bson.M{
				"_id":      taskQueueDocID(tq.NamespaceID, tq.TaskQueueName, tq.TaskQueueType, 0),
				"range_id": req.RangeID,
			}
			res, err := s.db.Collection(s.queuesColl).DeleteOne(sctx, filter)
			if err != nil {
				return struct{}{}, internalmongo.Wrap(err, "taskstore.DeleteTaskQueue.queue")
			}
			if res.DeletedCount == 0 {
				return struct{}{}, &persistence.ConditionFailedError{
					Msg: fmt.Sprintf("task queue %s not found or stale RangeID", tq.TaskQueueName),
				}
			}
			if _, err := s.db.Collection(s.tasksColl).DeleteMany(sctx, bson.M{
				"namespace_id": tq.NamespaceID,
				"task_queue":   tq.TaskQueueName,
				"task_type":    int32(tq.TaskQueueType),
			}); err != nil {
				return struct{}{}, internalmongo.Wrap(err, "taskstore.DeleteTaskQueue.tasks")
			}
			return struct{}{}, nil
		})
	return err
}

// CreateTasks inserts a batch of tasks.
func (s *Stub) CreateTasks(
	ctx context.Context,
	req *persistence.InternalCreateTasksRequest,
) (_ *persistence.CreateTasksResponse, retErr error) {
	defer s.observeOp("CreateTasks")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("taskstore: uninitialised or nil request")
	}
	if len(req.Tasks) == 0 {
		return &persistence.CreateTasksResponse{}, nil
	}
	docs := make([]any, 0, len(req.Tasks))
	for _, t := range req.Tasks {
		d := taskDoc{
			ID:          taskDocID(req.NamespaceID, req.TaskQueue, req.TaskType, int32(t.Subqueue), t.TaskId),
			NamespaceID: req.NamespaceID,
			TaskQueue:   req.TaskQueue,
			TaskType:    int32(req.TaskType),
			Subqueue:    int32(t.Subqueue),
			Pass:        t.TaskPass,
			TaskID:      t.TaskId,
			Task:        internalmongo.BlobFromDataBlob(t.Task),
		}
		if t.ExpiryTime != nil {
			d.ExpiryTime = t.ExpiryTime.AsTime()
		}
		docs = append(docs, d)
	}
	sess, err := s.db.Client().StartSession()
	if err != nil {
		return nil, internalmongo.Translate(err, "taskstore.CreateTasks.session")
	}
	defer sess.EndSession(ctx)

	// The queue owner is fenced on RangeID in the same transaction as the
	// inserts, and the queue info is refreshed atomically with them, so a
	// matching node that lost its lease cannot add tasks.
	// Duplicate task IDs abort the transaction and surface as an error
	//.
	_, err = internalmongo.WithTxn[struct{}](ctx, internalmongo.NewSessionRunner(sess),
		func(sctx context.Context) (struct{}, error) {
			res, err := s.db.Collection(s.queuesColl).UpdateOne(sctx,
				bson.M{
					"_id":      taskQueueDocID(req.NamespaceID, req.TaskQueue, req.TaskType, 0),
					"range_id": req.RangeID,
				},
				bson.M{"$set": bson.M{
					"task_queue_info": internalmongo.BlobFromDataBlob(req.TaskQueueInfo),
					"update_time":     time.Now().UTC(),
				}})
			if err != nil {
				return struct{}{}, internalmongo.Wrap(err, "taskstore.CreateTasks.fence")
			}
			if res.MatchedCount == 0 {
				return struct{}{}, &persistence.ConditionFailedError{
					Msg: fmt.Sprintf("task queue %s: stale RangeID %d", req.TaskQueue, req.RangeID),
				}
			}
			if _, err := s.db.Collection(s.tasksColl).InsertMany(sctx, docs,
				options.InsertMany().SetOrdered(false)); err != nil {
				return struct{}{}, internalmongo.Wrap(err, "taskstore.CreateTasks.insert")
			}
			return struct{}{}, nil
		})
	if err != nil {
		return nil, err
	}
	return &persistence.CreateTasksResponse{}, nil
}

// GetTasks returns tasks in a range, ordered by TaskID.
func (s *Stub) GetTasks(
	ctx context.Context,
	req *persistence.GetTasksRequest,
) (_ *persistence.InternalGetTasksResponse, retErr error) {
	defer s.observeOp("GetTasks")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("taskstore: uninitialised or nil request")
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	// Tasks are ordered by (pass, task_id). Classic queues always use pass
	// 0, so this reduces to task_id order; fair queues read from
	// (InclusiveMinPass, InclusiveMinTaskID) onward.
	minPass, minID := req.InclusiveMinPass, req.InclusiveMinTaskID
	if len(req.NextPageToken) > 0 {
		lastPass, lastID, err := decodeTaskPageToken(req.NextPageToken)
		if err != nil {
			return nil, err
		}
		minPass, minID = lastPass, lastID+1
	}
	filter := bson.M{
		"namespace_id": req.NamespaceID,
		"task_queue":   req.TaskQueue,
		"task_type":    int32(req.TaskType),
		"subqueue":     int32(req.Subqueue),
		"task_id":      bson.M{"$lt": req.ExclusiveMaxTaskID},
		"$or":          passIDAtLeast(minPass, minID),
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "pass", Value: 1}, {Key: "task_id", Value: 1}}).
		SetLimit(int64(pageSize) + 1)
	cursor, err := s.db.Collection(s.tasksColl).Find(ctx, filter, opts)
	if err != nil {
		return nil, internalmongo.Translate(err, "taskstore.GetTasks")
	}
	defer cursor.Close(ctx)
	var docs []taskDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, internalmongo.Translate(err, "taskstore.GetTasks.decode")
	}
	resp := &persistence.InternalGetTasksResponse{}
	for i, d := range docs {
		if i == pageSize {
			last := docs[pageSize-1]
			resp.NextPageToken = []byte(fmt.Sprintf("%d:%d", last.Pass, last.TaskID))
			break
		}
		resp.Tasks = append(resp.Tasks, internalmongo.DataBlobFromBlob(d.Task))
	}
	return resp, nil
}

// CompleteTasksLessThan deletes tasks below (ExclusiveMaxPass,
// ExclusiveMaxTaskID) up to Limit rows.
func (s *Stub) CompleteTasksLessThan(
	ctx context.Context,
	req *persistence.CompleteTasksLessThanRequest,
) (_ int, retErr error) {
	defer s.observeOp("CompleteTasksLessThan")(&retErr)
	if s.db == nil || req == nil {
		return 0, errors.New("taskstore: uninitialised or nil request")
	}
	// MongoDB does not cap delete-many cleanly; for the limit path we
	// find the TaskID boundary first then delete by range.
	coll := s.db.Collection(s.tasksColl)
	filter := bson.M{
		"namespace_id": req.NamespaceID,
		"task_queue":   req.TaskQueueName,
		"task_type":    int32(req.TaskType),
		"subqueue":     int32(req.Subqueue),
		// (pass, task_id) < (ExclusiveMaxPass, ExclusiveMaxTaskID); with
		// pass 0 this is task_id < ExclusiveMaxTaskID.
		"$or": passIDBelow(req.ExclusiveMaxPass, req.ExclusiveMaxTaskID),
	}
	// If a Limit is supplied, scan up to Limit IDs first to bound the delete.
	effective := filter
	if req.Limit > 0 {
		cursor, err := coll.Find(ctx, filter,
			options.Find().SetSort(bson.D{{Key: "pass", Value: 1}, {Key: "task_id", Value: 1}}).SetLimit(int64(req.Limit)))
		if err != nil {
			return 0, internalmongo.Translate(err, "taskstore.CompleteTasksLessThan.find")
		}
		var docs []taskDoc
		if err := cursor.All(ctx, &docs); err != nil {
			cursor.Close(ctx)
			return 0, internalmongo.Translate(err, "taskstore.CompleteTasksLessThan.decode")
		}
		cursor.Close(ctx)
		if len(docs) == 0 {
			return 0, nil
		}
		ids := make([]string, 0, len(docs))
		for _, d := range docs {
			ids = append(ids, d.ID)
		}
		effective = bson.M{"_id": bson.M{"$in": ids}}
	}
	// Temporal 1.32+ may fence the delete on the queue owner's RangeID
	//: the queue document is written in the same
	// transaction, so a stale owner deletes nothing.
	if rangeID := conditionRangeID(req); rangeID != 0 {
		sess, err := s.db.Client().StartSession()
		if err != nil {
			return 0, internalmongo.Translate(err, "taskstore.CompleteTasksLessThan.session")
		}
		defer sess.EndSession(ctx)
		return internalmongo.WithTxn[int](ctx, internalmongo.NewSessionRunner(sess),
			func(sctx context.Context) (int, error) {
				fenced, err := s.db.Collection(s.queuesColl).UpdateOne(sctx,
					bson.M{
						"_id":      taskQueueDocID(req.NamespaceID, req.TaskQueueName, req.TaskType, 0),
						"range_id": rangeID,
					},
					bson.M{"$set": bson.M{"update_time": time.Now().UTC()}})
				if err != nil {
					return 0, internalmongo.Wrap(err, "taskstore.CompleteTasksLessThan.fence")
				}
				if fenced.MatchedCount == 0 {
					return 0, &persistence.ConditionFailedError{
						Msg: fmt.Sprintf("task queue %s: stale RangeID %d", req.TaskQueueName, rangeID),
					}
				}
				res, err := coll.DeleteMany(sctx, effective)
				if err != nil {
					return 0, internalmongo.Wrap(err, "taskstore.CompleteTasksLessThan.delete")
				}
				return int(res.DeletedCount), nil
			})
	}
	res, err := coll.DeleteMany(ctx, effective)
	if err != nil {
		return 0, internalmongo.Translate(err, "taskstore.CompleteTasksLessThan.delete")
	}
	return int(res.DeletedCount), nil
}

// conditionRangeID returns CompleteTasksLessThanRequest.ConditionRangeID,
// a field Temporal 1.32 added. Reading it by name keeps the plugin
// building against 1.31, where it does not exist (0 = no condition).
func conditionRangeID(req *persistence.CompleteTasksLessThanRequest) int64 {
	f := reflect.ValueOf(req).Elem().FieldByName("ConditionRangeID")
	if !f.IsValid() || !f.CanInt() {
		return 0
	}
	return f.Int()
}

// GetTaskQueueUserData returns the per-queue user-data blob.
func (s *Stub) GetTaskQueueUserData(
	ctx context.Context,
	req *persistence.GetTaskQueueUserDataRequest,
) (_ *persistence.InternalGetTaskQueueUserDataResponse, retErr error) {
	defer s.observeOp("GetTaskQueueUserData")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("taskstore: uninitialised or nil request")
	}
	var doc userDataDoc
	err := s.db.Collection(userDataColl).FindOne(ctx,
		bson.M{"_id": userDataDocID(req.NamespaceID, req.TaskQueue)}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, serviceerror.NewNotFound(fmt.Sprintf(
				"task queue user data ns=%s tq=%s not found", req.NamespaceID, req.TaskQueue))
		}
		return nil, internalmongo.Translate(err, "taskstore.GetTaskQueueUserData")
	}
	return &persistence.InternalGetTaskQueueUserDataResponse{
		Version:  doc.Version,
		UserData: internalmongo.DataBlobFromBlob(doc.UserData),
	}, nil
}

// UpdateTaskQueueUserData writes a batch of user-data updates inside
// one transaction with build-id index maintenance.
func (s *Stub) UpdateTaskQueueUserData(
	ctx context.Context,
	req *persistence.InternalUpdateTaskQueueUserDataRequest,
) (retErr error) {
	defer s.observeOp("UpdateTaskQueueUserData")(&retErr)
	if s.db == nil || req == nil || len(req.Updates) == 0 {
		return nil
	}
	sess, err := s.db.Client().StartSession()
	if err != nil {
		return internalmongo.Translate(err, "taskstore.UpdateTaskQueueUserData.session")
	}
	defer sess.EndSession(ctx)

	// Flags describe this call only: reset them so values from an earlier
	// attempt never leak through a rollback.
	for _, upd := range req.Updates {
		if upd.Applied != nil {
			*upd.Applied = false
		}
		if upd.Conflicting != nil {
			*upd.Conflicting = false
		}
	}

	_, err = internalmongo.WithTxn[struct{}](ctx, internalmongo.NewSessionRunner(sess),
		func(sctx context.Context) (struct{}, error) {
			// Version CAS for every entry first; the batch is all-or-nothing.
			var conflicts []string
			for tq, upd := range req.Updates {
				if upd.Conflicting != nil {
					*upd.Conflicting = false
				}
				var cur userDataDoc
				err := s.db.Collection(userDataColl).FindOne(sctx, bson.M{"_id": userDataDocID(req.NamespaceID, tq)}).Decode(&cur)
				switch {
				case errors.Is(err, mongo.ErrNoDocuments):
					cur.Version = 0
				case err != nil:
					return struct{}{}, internalmongo.Wrap(err, "taskstore.UpdateTaskQueueUserData.read")
				}
				if cur.Version != upd.Version {
					conflicts = append(conflicts, tq)
					if upd.Conflicting != nil {
						*upd.Conflicting = true
					}
				}
			}
			if len(conflicts) > 0 {
				sort.Strings(conflicts)
				return struct{}{}, &persistence.ConditionFailedError{
					Msg: fmt.Sprintf("task queue user data version conflict: %s", strings.Join(conflicts, ", ")),
				}
			}
			for tq, upd := range req.Updates {
				docID := userDataDocID(req.NamespaceID, tq)
				// Expected version upd.Version is stored as upd.Version+1. A
				// concurrent writer of the same document makes this write
				// conflict, so the driver retries and the check above re-runs.
				doc := userDataDoc{
					ID:          docID,
					NamespaceID: req.NamespaceID,
					TaskQueue:   tq,
					Version:     upd.Version + 1,
					UserData:    internalmongo.BlobFromDataBlob(upd.UserData),
				}
				if _, err := s.db.Collection(userDataColl).ReplaceOne(sctx,
					bson.M{"_id": docID}, doc, options.Replace().SetUpsert(true)); err != nil {
					return struct{}{}, internalmongo.Wrap(err, "taskstore.UpdateTaskQueueUserData.user")
				}
				// Build-id index maintenance.
				for _, bid := range upd.BuildIdsAdded {
					bidID := fmt.Sprintf("%s:%s:%s", req.NamespaceID, bid, tq)
					_, err := s.db.Collection(buildIDIndexColl).ReplaceOne(sctx,
						bson.M{"_id": bidID},
						bson.M{
							"_id":          bidID,
							"namespace_id": req.NamespaceID,
							"build_id":     bid,
							"task_queue":   tq,
						},
						options.Replace().SetUpsert(true))
					if err != nil {
						return struct{}{}, internalmongo.Wrap(err, "taskstore.UpdateTaskQueueUserData.bidadd")
					}
				}
				for _, bid := range upd.BuildIdsRemoved {
					bidID := fmt.Sprintf("%s:%s:%s", req.NamespaceID, bid, tq)
					_, err := s.db.Collection(buildIDIndexColl).DeleteOne(sctx, bson.M{"_id": bidID})
					if err != nil {
						return struct{}{}, internalmongo.Wrap(err, "taskstore.UpdateTaskQueueUserData.bidrm")
					}
				}
			}
			return struct{}{}, nil
		})
	if err != nil {
		return err
	}
	// Applied is reported only once the transaction has committed.
	for _, upd := range req.Updates {
		if upd.Applied != nil {
			*upd.Applied = true
		}
	}
	return nil
}

// ListTaskQueueUserDataEntries returns a paginated list of user-data
// entries for a namespace.
func (s *Stub) ListTaskQueueUserDataEntries(
	ctx context.Context,
	req *persistence.ListTaskQueueUserDataEntriesRequest,
) (_ *persistence.InternalListTaskQueueUserDataEntriesResponse, retErr error) {
	defer s.observeOp("ListTaskQueueUserDataEntries")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("taskstore: uninitialised or nil request")
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	filter := bson.M{"namespace_id": req.NamespaceID}
	if len(req.NextPageToken) > 0 {
		filter["_id"] = bson.M{"$gt": string(req.NextPageToken)}
	}
	cursor, err := s.db.Collection(userDataColl).Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(pageSize)+1))
	if err != nil {
		return nil, internalmongo.Translate(err, "taskstore.ListTaskQueueUserDataEntries")
	}
	defer cursor.Close(ctx)
	var docs []userDataDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, internalmongo.Translate(err, "taskstore.ListTaskQueueUserDataEntries.decode")
	}
	resp := &persistence.InternalListTaskQueueUserDataEntriesResponse{}
	for i, d := range docs {
		if i == pageSize {
			resp.NextPageToken = []byte(docs[pageSize-1].ID)
			break
		}
		resp.Entries = append(resp.Entries, persistence.InternalTaskQueueUserDataEntry{
			TaskQueue: d.TaskQueue,
			Version:   d.Version,
			Data:      internalmongo.DataBlobFromBlob(d.UserData),
		})
	}
	return resp, nil
}

// GetTaskQueuesByBuildId returns every task queue that has the given
// build_id registered.
func (s *Stub) GetTaskQueuesByBuildId(
	ctx context.Context,
	req *persistence.GetTaskQueuesByBuildIdRequest,
) (_ []string, retErr error) {
	defer s.observeOp("GetTaskQueuesByBuildId")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("taskstore: uninitialised or nil request")
	}
	cursor, err := s.db.Collection(buildIDIndexColl).Find(ctx, bson.M{
		"namespace_id": req.NamespaceID,
		"build_id":     req.BuildID,
	})
	if err != nil {
		return nil, internalmongo.Translate(err, "taskstore.GetTaskQueuesByBuildId")
	}
	defer cursor.Close(ctx)
	var docs []struct {
		TaskQueue string `bson:"task_queue"`
	}
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, internalmongo.Translate(err, "taskstore.GetTaskQueuesByBuildId.decode")
	}
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		out = append(out, d.TaskQueue)
	}
	return out, nil
}

// CountTaskQueuesByBuildId returns the count analogue.
func (s *Stub) CountTaskQueuesByBuildId(
	ctx context.Context,
	req *persistence.CountTaskQueuesByBuildIdRequest,
) (_ int, retErr error) {
	defer s.observeOp("CountTaskQueuesByBuildId")(&retErr)
	if s.db == nil || req == nil {
		return 0, errors.New("taskstore: uninitialised or nil request")
	}
	n, err := s.db.Collection(buildIDIndexColl).CountDocuments(ctx, bson.M{
		"namespace_id": req.NamespaceID,
		"build_id":     req.BuildID,
	})
	if err != nil {
		return 0, internalmongo.Translate(err, "taskstore.CountTaskQueuesByBuildId")
	}
	return int(n), nil
}

// EnsureIndexes installs the indexes task-store queries rely on.
// Leading namespace_id on every compound, TTL on tasks.expiry_time,
// and uniqueness on the task queue key.
func EnsureIndexes(ctx context.Context, db *mongo.Database) error {
	// task_queues: _id already uniquely composites the key.
	// The classic and fair task collections share one index layout;
	// index names take the collection name as their prefix.
	for _, coll := range []string{tasksColl, fairTasksColl} {
		if err := ensureTaskIndexes(ctx, db, coll); err != nil {
			return err
		}
	}
	// build_id_to_task_queue: lookup by (namespace_id, build_id).
	_, err := db.Collection(buildIDIndexColl).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "namespace_id", Value: 1},
			{Key: "build_id", Value: 1},
		},
		Options: options.Index().SetName("build_id_ns_bid"),
	})
	if err != nil {
		return internalmongo.Translate(err, "taskstore.EnsureIndexes.buildid")
	}
	return nil
}

// ensureTaskIndexes installs the range-scan and TTL indexes on one task
// collection.
func ensureTaskIndexes(ctx context.Context, db *mongo.Database, coll string) error {
	// Range scans: (namespace_id, task_queue, task_type, subqueue, task_id).
	_, err := db.Collection(coll).Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "namespace_id", Value: 1},
				{Key: "task_queue", Value: 1},
				{Key: "task_type", Value: 1},
				{Key: "subqueue", Value: 1},
				{Key: "task_id", Value: 1},
			},
			Options: options.Index().SetName(coll + "_ns_tq_type_sub_id"),
		},
		{
			// Fair queues read in (pass, task_id) order.
			Keys: bson.D{
				{Key: "namespace_id", Value: 1},
				{Key: "task_queue", Value: 1},
				{Key: "task_type", Value: 1},
				{Key: "subqueue", Value: 1},
				{Key: "pass", Value: 1},
				{Key: "task_id", Value: 1},
			},
			Options: options.Index().SetName(coll + "_ns_tq_type_sub_pass_id"),
		},
		{
			Keys: bson.D{{Key: "expiry_time", Value: 1}},
			Options: options.Index().
				SetExpireAfterSeconds(0).
				SetPartialFilterExpression(bson.M{"expiry_time": bson.M{"$gt": time.Unix(0, 0)}}).
				SetName(coll + "_expiry_ttl"),
		},
	})
	if err != nil {
		return internalmongo.Translate(err, "taskstore.EnsureIndexes."+coll)
	}
	return nil
}

// passEq matches a pass value. Tasks written before fairness support
// carry no pass field; they are pass 0.
func passEq(pass int64) any {
	if pass == 0 {
		return bson.M{"$in": bson.A{int64(0), nil}}
	}
	return pass
}

// passIDAtLeast expresses (pass, task_id) >= (pass, id).
func passIDAtLeast(pass, id int64) bson.A {
	return bson.A{
		bson.M{"pass": bson.M{"$gt": pass}},
		bson.M{"pass": passEq(pass), "task_id": bson.M{"$gte": id}},
	}
}

// passIDBelow expresses (pass, task_id) < (pass, id).
func passIDBelow(pass, id int64) bson.A {
	return bson.A{
		bson.M{"pass": bson.M{"$lt": pass}},
		bson.M{"pass": passEq(pass), "task_id": bson.M{"$lt": id}},
	}
}

// decodeTaskPageToken parses "<pass>:<task_id>" (or a bare task_id from
// earlier releases).
func decodeTaskPageToken(raw []byte) (pass, id int64, err error) {
	if _, err := fmt.Sscanf(string(raw), "%d:%d", &pass, &id); err == nil {
		return pass, id, nil
	}
	if _, err := fmt.Sscanf(string(raw), "%d", &id); err == nil {
		return 0, id, nil
	}
	return 0, 0, serviceerror.NewInvalidArgument(fmt.Sprintf("taskstore.GetTasks: invalid page token %q", raw))
}
