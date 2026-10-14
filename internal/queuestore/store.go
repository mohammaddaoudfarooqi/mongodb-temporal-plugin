// Package queuestore is the MongoDB-backed implementation of the two
// persistence queue interfaces:
//   - Queue (legacy, single queue per QueueType).
//   - QueueV2 (named queues + per-queue metadata).
//
// Storage model:
//
//	collection: legacy_queues        -- per-QueueType singleton
//	  _id:        "queue:<type>"
//	  ack_level:  *internalmongo.Blob (serialized InternalQueueMetadata)
//	  next_id:    <int64>           -- monotonic message ID allocator
//
//	collection: legacy_queue_messages
//	  _id:        "queue:<type>:<id>"
//	  queue_type, message_id, data
//
//	collection: legacy_dlq_metadata   -- per-QueueType DLQ singleton
//	collection: legacy_dlq_messages
//
//	collection: v2_queues             -- named-queue metadata
//	  _id: "<type>:<name>"
//	  type, name, next_id, last_message_id, message_count, created_at
//
//	collection: v2_messages
//	  _id: "<type>:<name>:<id>"
//	  type, name, id, data
//
// Monotonic message-ID allocation uses the metadata doc's
// next_id field via $inc + findOneAndUpdate; each enqueue picks up
// the pre-bump value and writes it onto its message doc. The driver's
// WithTransaction retry handles TransientTransactionError; the
// plugin itself does NOT add a retry loop.
package queuestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

const (
	legacyMetaColl    = "legacy_queues"
	legacyMsgColl     = "legacy_queue_messages"
	legacyDLQMetaColl = "legacy_dlq_metadata"
	legacyDLQMsgColl  = "legacy_dlq_messages"

	v2QueuesColl   = "v2_queues"
	v2MessagesColl = "v2_messages"
)

// --- legacy Queue implementation ---------------------------------------

type legacyMetaDoc struct {
	ID       string              `bson:"_id"`
	AckLevel *internalmongo.Blob `bson:"ack_level,omitempty"`
	NextID   int64               `bson:"next_id"`
	Version  int64               `bson:"version"`
}

type legacyMsgDoc struct {
	ID        string              `bson:"_id"`
	QueueType int32               `bson:"queue_type"`
	MessageID int64               `bson:"message_id"`
	Data      *internalmongo.Blob `bson:"data"`
	Encoding  string              `bson:"encoding"`
}

func legacyQueueID(t persistence.QueueType) string {
	return fmt.Sprintf("queue:%d", int(t))
}
func legacyMsgID(t persistence.QueueType, id int64) string {
	return fmt.Sprintf("queue:%d:%020d", int(t), id)
}

// Init is a no-op under this schema: metadata is created on first
// enqueue via upsert.
func (s *Stub) Init(_ context.Context, _ *commonpb.DataBlob) error {
	return nil
}

// EnqueueMessage writes one message with a monotonic ID.
func (s *Stub) EnqueueMessage(ctx context.Context, b *commonpb.DataBlob) (retErr error) {
	defer s.observeOp("EnqueueMessage")(&retErr)
	if s.db == nil {
		return errors.New("queuestore: uninitialised")
	}
	sess, err := s.db.Client().StartSession()
	if err != nil {
		return internalmongo.Translate(err, "queuestore.EnqueueMessage.session")
	}
	defer sess.EndSession(ctx)
	_, err = internalmongo.WithTxn[struct{}](ctx, internalmongo.NewSessionRunner(sess),
		func(sctx context.Context) (struct{}, error) {
			id, err := s.bumpLegacyID(sctx)
			if err != nil {
				return struct{}{}, err
			}
			doc := legacyMsgDoc{
				ID:        legacyMsgID(s.queueType, id),
				QueueType: int32(s.queueType),
				MessageID: id,
				Data:      internalmongo.BlobFromDataBlob(b),
			}
			if _, err := s.db.Collection(legacyMsgColl).InsertOne(sctx, doc); err != nil {
				return struct{}{}, internalmongo.Wrap(err, "queuestore.EnqueueMessage.insert")
			}
			return struct{}{}, nil
		})
	return err
}

func (s *Stub) bumpLegacyID(ctx context.Context) (int64, error) {
	var meta legacyMetaDoc
	err := s.db.Collection(legacyMetaColl).FindOneAndUpdate(ctx,
		bson.M{"_id": legacyQueueID(s.queueType)},
		bson.M{"$inc": bson.M{"next_id": int64(1)}},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&meta)
	if err != nil {
		return 0, internalmongo.Wrap(err, "queuestore.bumpLegacyID")
	}
	return meta.NextID, nil
}

// ReadMessages returns up to maxCount messages strictly after
// lastMessageID.
func (s *Stub) ReadMessages(ctx context.Context, lastMessageID int64, maxCount int) (_ []*persistence.QueueMessage, retErr error) {
	defer s.observeOp("ReadMessages")(&retErr)
	if s.db == nil {
		return nil, errors.New("queuestore: uninitialised")
	}
	cursor, err := s.db.Collection(legacyMsgColl).Find(ctx,
		bson.M{"queue_type": int32(s.queueType), "message_id": bson.M{"$gt": lastMessageID}},
		options.Find().SetSort(bson.D{{Key: "message_id", Value: 1}}).SetLimit(int64(maxCount)))
	if err != nil {
		return nil, internalmongo.Translate(err, "queuestore.ReadMessages")
	}
	defer cursor.Close(ctx)
	var docs []legacyMsgDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, internalmongo.Translate(err, "queuestore.ReadMessages.decode")
	}
	out := make([]*persistence.QueueMessage, 0, len(docs))
	for _, d := range docs {
		msg := &persistence.QueueMessage{
			QueueType: s.queueType,
			ID:        d.MessageID,
		}
		if d.Data != nil {
			msg.Data = d.Data.Data
			msg.Encoding = d.Data.Encoding
		}
		out = append(out, msg)
	}
	return out, nil
}

// DeleteMessagesBefore removes every message with ID < messageID; the
// bound is exclusive per the upstream contract.
func (s *Stub) DeleteMessagesBefore(ctx context.Context, messageID int64) (retErr error) {
	defer s.observeOp("DeleteMessagesBefore")(&retErr)
	if s.db == nil {
		return errors.New("queuestore: uninitialised")
	}
	_, err := s.db.Collection(legacyMsgColl).DeleteMany(ctx, bson.M{
		"queue_type": int32(s.queueType),
		"message_id": bson.M{"$lt": messageID},
	})
	if err != nil {
		return internalmongo.Translate(err, "queuestore.DeleteMessagesBefore")
	}
	return nil
}

// UpdateAckLevel writes the caller's ack-level blob into the metadata
// singleton under a CAS on the version it read.
func (s *Stub) UpdateAckLevel(ctx context.Context, md *persistence.InternalQueueMetadata) (retErr error) {
	defer s.observeOp("UpdateAckLevel")(&retErr)
	if s.db == nil || md == nil {
		return errors.New("queuestore: uninitialised or nil metadata")
	}
	return s.casAckLevel(ctx, legacyMetaColl, md, "queuestore.UpdateAckLevel")
}

// casAckLevel stores an ack-level blob if the stored version still equals
// md.Version, then bumps the version. A metadata document without a
// version field counts as version 0.
func (s *Stub) casAckLevel(ctx context.Context, coll string, md *persistence.InternalQueueMetadata, op string) error {
	filter := bson.M{"_id": legacyQueueID(s.queueType), "version": md.Version}
	if md.Version == 0 {
		filter["version"] = bson.M{"$in": bson.A{int64(0), nil}}
	}
	res, err := s.db.Collection(coll).UpdateOne(ctx, filter,
		bson.M{
			"$set": bson.M{"ack_level": internalmongo.BlobFromDataBlob(md.Blob)},
			"$inc": bson.M{"version": int64(1)},
		},
		options.UpdateOne().SetUpsert(md.Version == 0))
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return internalmongo.Translate(err, op)
	}
	if err != nil || (res.MatchedCount == 0 && res.UpsertedCount == 0) {
		return &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("%s: queue metadata version mismatch (expected %d)", op, md.Version),
		}
	}
	return nil
}

// GetAckLevels returns the stored ack-level blob or an empty metadata
// if the singleton has not been written yet.
func (s *Stub) GetAckLevels(ctx context.Context) (_ *persistence.InternalQueueMetadata, retErr error) {
	defer s.observeOp("GetAckLevels")(&retErr)
	if s.db == nil {
		return nil, errors.New("queuestore: uninitialised")
	}
	var doc legacyMetaDoc
	err := s.db.Collection(legacyMetaColl).FindOne(ctx,
		bson.M{"_id": legacyQueueID(s.queueType)}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return &persistence.InternalQueueMetadata{}, nil
		}
		return nil, internalmongo.Translate(err, "queuestore.GetAckLevels")
	}
	return &persistence.InternalQueueMetadata{
		Blob:    internalmongo.DataBlobFromBlob(doc.AckLevel),
		Version: doc.Version,
	}, nil
}

// DLQ variants mirror the main methods against the DLQ collections.
// They share the same ID allocator pattern; compact implementations
// keep the file size in check.

func (s *Stub) EnqueueMessageToDLQ(ctx context.Context, b *commonpb.DataBlob) (_ int64, retErr error) {
	defer s.observeOp("EnqueueMessageToDLQ")(&retErr)
	if s.db == nil {
		return 0, errors.New("queuestore: uninitialised")
	}
	sess, err := s.db.Client().StartSession()
	if err != nil {
		return 0, internalmongo.Translate(err, "queuestore.EnqueueMessageToDLQ.session")
	}
	defer sess.EndSession(ctx)
	var id int64
	_, err = internalmongo.WithTxn[struct{}](ctx, internalmongo.NewSessionRunner(sess),
		func(sctx context.Context) (struct{}, error) {
			var meta legacyMetaDoc
			if err := s.db.Collection(legacyDLQMetaColl).FindOneAndUpdate(sctx,
				bson.M{"_id": legacyQueueID(s.queueType)},
				bson.M{"$inc": bson.M{"next_id": int64(1)}},
				options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&meta); err != nil {
				return struct{}{}, internalmongo.Wrap(err, "queuestore.EnqueueMessageToDLQ.bump")
			}
			id = meta.NextID
			doc := legacyMsgDoc{
				ID:        fmt.Sprintf("dlq:%d:%020d", int(s.queueType), id),
				QueueType: int32(s.queueType),
				MessageID: id,
				Data:      internalmongo.BlobFromDataBlob(b),
			}
			if _, err := s.db.Collection(legacyDLQMsgColl).InsertOne(sctx, doc); err != nil {
				return struct{}{}, internalmongo.Wrap(err, "queuestore.EnqueueMessageToDLQ.insert")
			}
			return struct{}{}, nil
		})
	return id, err
}

func (s *Stub) ReadMessagesFromDLQ(ctx context.Context, firstMessageID, lastMessageID int64, pageSize int, _ []byte) (_ []*persistence.QueueMessage, _ []byte, retErr error) {
	defer s.observeOp("ReadMessagesFromDLQ")(&retErr)
	if s.db == nil {
		return nil, nil, errors.New("queuestore: uninitialised")
	}
	cursor, err := s.db.Collection(legacyDLQMsgColl).Find(ctx,
		bson.M{
			"queue_type": int32(s.queueType),
			"message_id": bson.M{"$gt": firstMessageID, "$lte": lastMessageID},
		},
		options.Find().SetSort(bson.D{{Key: "message_id", Value: 1}}).SetLimit(int64(pageSize)))
	if err != nil {
		return nil, nil, internalmongo.Translate(err, "queuestore.ReadMessagesFromDLQ")
	}
	defer cursor.Close(ctx)
	var docs []legacyMsgDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, nil, internalmongo.Translate(err, "queuestore.ReadMessagesFromDLQ.decode")
	}
	out := make([]*persistence.QueueMessage, 0, len(docs))
	for _, d := range docs {
		msg := &persistence.QueueMessage{QueueType: s.queueType, ID: d.MessageID}
		if d.Data != nil {
			msg.Data = d.Data.Data
			msg.Encoding = d.Data.Encoding
		}
		out = append(out, msg)
	}
	return out, nil, nil
}

func (s *Stub) DeleteMessageFromDLQ(ctx context.Context, messageID int64) (retErr error) {
	defer s.observeOp("DeleteMessageFromDLQ")(&retErr)
	if s.db == nil {
		return errors.New("queuestore: uninitialised")
	}
	_, err := s.db.Collection(legacyDLQMsgColl).DeleteOne(ctx,
		bson.M{"queue_type": int32(s.queueType), "message_id": messageID})
	if err != nil {
		return internalmongo.Translate(err, "queuestore.DeleteMessageFromDLQ")
	}
	return nil
}

func (s *Stub) RangeDeleteMessagesFromDLQ(ctx context.Context, firstMessageID, lastMessageID int64) (retErr error) {
	defer s.observeOp("RangeDeleteMessagesFromDLQ")(&retErr)
	if s.db == nil {
		return errors.New("queuestore: uninitialised")
	}
	_, err := s.db.Collection(legacyDLQMsgColl).DeleteMany(ctx, bson.M{
		"queue_type": int32(s.queueType),
		"message_id": bson.M{"$gte": firstMessageID, "$lte": lastMessageID},
	})
	if err != nil {
		return internalmongo.Translate(err, "queuestore.RangeDeleteMessagesFromDLQ")
	}
	return nil
}

func (s *Stub) UpdateDLQAckLevel(ctx context.Context, md *persistence.InternalQueueMetadata) (retErr error) {
	defer s.observeOp("UpdateDLQAckLevel")(&retErr)
	if s.db == nil || md == nil {
		return errors.New("queuestore: uninitialised or nil metadata")
	}
	return s.casAckLevel(ctx, legacyDLQMetaColl, md, "queuestore.UpdateDLQAckLevel")
}

func (s *Stub) GetDLQAckLevels(ctx context.Context) (_ *persistence.InternalQueueMetadata, retErr error) {
	defer s.observeOp("GetDLQAckLevels")(&retErr)
	if s.db == nil {
		return nil, errors.New("queuestore: uninitialised")
	}
	var doc legacyMetaDoc
	err := s.db.Collection(legacyDLQMetaColl).FindOne(ctx,
		bson.M{"_id": legacyQueueID(s.queueType)}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return &persistence.InternalQueueMetadata{}, nil
		}
		return nil, internalmongo.Translate(err, "queuestore.GetDLQAckLevels")
	}
	return &persistence.InternalQueueMetadata{
		Blob:    internalmongo.DataBlobFromBlob(doc.AckLevel),
		Version: doc.Version,
	}, nil
}

// --- QueueV2 implementation -------------------------------------------

type v2QueueDoc struct {
	ID            string    `bson:"_id"`
	Type          int32     `bson:"type"`
	Name          string    `bson:"name"`
	NextID        int64     `bson:"next_id"`
	LastMessageID int64     `bson:"last_message_id"`
	MessageCount  int64     `bson:"message_count"`
	CreatedAt     time.Time `bson:"created_at"`
}

type v2MsgDoc struct {
	ID   string              `bson:"_id"`
	Type int32               `bson:"type"`
	Name string              `bson:"name"`
	MID  int64               `bson:"mid"`
	Data *internalmongo.Blob `bson:"data"`
}

func v2QueueID(qt persistence.QueueV2Type, name string) string {
	return fmt.Sprintf("%d:%s", int(qt), name)
}
func v2MsgID(qt persistence.QueueV2Type, name string, id int64) string {
	return fmt.Sprintf("%d:%s:%020d", int(qt), name, id)
}

// CreateQueue inserts the queue metadata; duplicate key surfaces as
// ErrQueueAlreadyExists so the upstream manager can react cleanly.
func (v *V2Stub) CreateQueue(
	ctx context.Context,
	req *persistence.InternalCreateQueueRequest,
) (_ *persistence.InternalCreateQueueResponse, retErr error) {
	defer v.observeOpV2("CreateQueue")(&retErr)
	if v.db == nil || req == nil {
		return nil, errors.New("queuestore: uninitialised or nil request")
	}
	doc := v2QueueDoc{
		ID:            v2QueueID(req.QueueType, req.QueueName),
		Type:          int32(req.QueueType),
		Name:          req.QueueName,
		NextID:        persistence.FirstQueueMessageID,
		LastMessageID: persistence.FirstQueueMessageID - 1, // empty queue
		CreatedAt:     time.Now().UTC(),
	}
	if _, err := v.db.Collection(v2QueuesColl).InsertOne(ctx, doc); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil, fmt.Errorf("%w: type = %v and name = %v",
				persistence.ErrQueueAlreadyExists, req.QueueType, req.QueueName)
		}
		return nil, internalmongo.Translate(err, "queuestore.v2.CreateQueue")
	}
	return &persistence.InternalCreateQueueResponse{}, nil
}

// EnqueueMessage allocates a monotonic ID inside a transaction and
// persists the message. The driver's WithTransaction handles transient retry; no plugin-level retry loop.
func (v *V2Stub) EnqueueMessage(
	ctx context.Context,
	req *persistence.InternalEnqueueMessageRequest,
) (_ *persistence.InternalEnqueueMessageResponse, retErr error) {
	defer v.observeOpV2("EnqueueMessage")(&retErr)
	if v.db == nil || req == nil {
		return nil, errors.New("queuestore: uninitialised or nil request")
	}
	sess, err := v.db.Client().StartSession()
	if err != nil {
		return nil, internalmongo.Translate(err, "queuestore.v2.EnqueueMessage.session")
	}
	defer sess.EndSession(ctx)

	var id int64
	_, err = internalmongo.WithTxn[struct{}](ctx, internalmongo.NewSessionRunner(sess),
		func(sctx context.Context) (struct{}, error) {
			// Allocate the next ID, record it as the last message and
			// count the message in one atomic update. IDs are never
			// reused, even after every message has been deleted.
			var q v2QueueDoc
			err := v.db.Collection(v2QueuesColl).FindOneAndUpdate(sctx,
				bson.M{"_id": v2QueueID(req.QueueType, req.QueueName)},
				mongo.Pipeline{{{Key: "$set", Value: bson.M{
					"last_message_id": "$next_id",
					"next_id":         bson.M{"$add": bson.A{"$next_id", int64(1)}},
					"message_count":   bson.M{"$add": bson.A{bson.M{"$ifNull": bson.A{"$message_count", int64(0)}}, int64(1)}},
				}}}},
				options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&q)
			if err != nil {
				if errors.Is(err, mongo.ErrNoDocuments) {
					return struct{}{}, persistence.NewQueueNotFoundError(req.QueueType, req.QueueName)
				}
				return struct{}{}, internalmongo.Wrap(err, "queuestore.v2.EnqueueMessage.bump")
			}
			id = q.LastMessageID

			msg := v2MsgDoc{
				ID:   v2MsgID(req.QueueType, req.QueueName, id),
				Type: int32(req.QueueType),
				Name: req.QueueName,
				MID:  id,
				Data: internalmongo.BlobFromDataBlob(req.Blob),
			}
			if _, err := v.db.Collection(v2MessagesColl).InsertOne(sctx, msg); err != nil {
				return struct{}{}, internalmongo.Wrap(err, "queuestore.v2.EnqueueMessage.insert")
			}
			return struct{}{}, nil
		})
	if err != nil {
		return nil, err
	}
	return &persistence.InternalEnqueueMessageResponse{
		Metadata: persistence.MessageMetadata{ID: id},
	}, nil
}

// ReadMessages returns messages ordered by mid; pagination via string
// cursor.
func (v *V2Stub) ReadMessages(
	ctx context.Context,
	req *persistence.InternalReadMessagesRequest,
) (_ *persistence.InternalReadMessagesResponse, retErr error) {
	defer v.observeOpV2("ReadMessages")(&retErr)
	if v.db == nil || req == nil {
		return nil, errors.New("queuestore: uninitialised or nil request")
	}
	if req.PageSize <= 0 {
		return nil, persistence.ErrNonPositiveReadQueueMessagesPageSize
	}
	filter := bson.M{
		"type": int32(req.QueueType),
		"name": req.QueueName,
	}
	if len(req.NextPageToken) > 0 {
		lastID, err := strconv.ParseInt(string(req.NextPageToken), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", persistence.ErrInvalidReadQueueMessagesNextPageToken, err)
		}
		filter["mid"] = bson.M{"$gt": lastID}
	}
	cursor, err := v.db.Collection(v2MessagesColl).Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "mid", Value: 1}}).SetLimit(int64(req.PageSize)))
	if err != nil {
		return nil, internalmongo.Translate(err, "queuestore.v2.ReadMessages")
	}
	defer cursor.Close(ctx)
	var docs []v2MsgDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, internalmongo.Translate(err, "queuestore.v2.ReadMessages.decode")
	}
	resp := &persistence.InternalReadMessagesResponse{}
	for _, d := range docs {
		blob := internalmongo.DataBlobFromBlob(d.Data)
		if blob == nil || blob.EncodingType == enumspb.ENCODING_TYPE_UNSPECIFIED {
			encoding := ""
			if d.Data != nil {
				encoding = d.Data.Encoding
			}
			return nil, serialization.NewUnknownEncodingTypeError(encoding,
				enumspb.ENCODING_TYPE_PROTO3, enumspb.ENCODING_TYPE_JSON)
		}
		resp.Messages = append(resp.Messages, persistence.QueueV2Message{
			MetaData: persistence.MessageMetadata{ID: d.MID},
			Data:     blob,
		})
	}
	// A full page may have more behind it; the caller stops on an empty
	// page with no token (upstream QueueV2 contract).
	if len(docs) == req.PageSize {
		resp.NextPageToken = []byte(strconv.FormatInt(docs[len(docs)-1].MID, 10))
	}
	return resp, nil
}

// RangeDeleteMessages removes messages up to and including
// InclusiveMaxMessageMetadata.ID.
func (v *V2Stub) RangeDeleteMessages(
	ctx context.Context,
	req *persistence.InternalRangeDeleteMessagesRequest,
) (_ *persistence.InternalRangeDeleteMessagesResponse, retErr error) {
	defer v.observeOpV2("RangeDeleteMessages")(&retErr)
	if v.db == nil || req == nil {
		return nil, errors.New("queuestore: uninitialised or nil request")
	}
	maxID := req.InclusiveMaxMessageMetadata.ID
	if maxID < persistence.FirstQueueMessageID {
		return nil, fmt.Errorf("%w: max message ID %d is below the first message ID %d",
			persistence.ErrInvalidQueueRangeDeleteMaxMessageID, maxID, persistence.FirstQueueMessageID)
	}
	sess, err := v.db.Client().StartSession()
	if err != nil {
		return nil, internalmongo.Translate(err, "queuestore.v2.RangeDeleteMessages.session")
	}
	defer sess.EndSession(ctx)

	// Delete and decrement the queue's message count atomically; the
	// last message ID is left alone so IDs are never reused.
	deleted, err := internalmongo.WithTxn[int64](ctx, internalmongo.NewSessionRunner(sess),
		func(sctx context.Context) (int64, error) {
			queueID := v2QueueID(req.QueueType, req.QueueName)
			if err := v.db.Collection(v2QueuesColl).FindOne(sctx, bson.M{"_id": queueID}).Err(); err != nil {
				if errors.Is(err, mongo.ErrNoDocuments) {
					return 0, persistence.NewQueueNotFoundError(req.QueueType, req.QueueName)
				}
				return 0, internalmongo.Wrap(err, "queuestore.v2.RangeDeleteMessages.queue")
			}
			res, err := v.db.Collection(v2MessagesColl).DeleteMany(sctx, bson.M{
				"type": int32(req.QueueType),
				"name": req.QueueName,
				"mid":  bson.M{"$lte": maxID},
			})
			if err != nil {
				return 0, internalmongo.Wrap(err, "queuestore.v2.RangeDeleteMessages.delete")
			}
			if res.DeletedCount > 0 {
				if _, err := v.db.Collection(v2QueuesColl).UpdateOne(sctx, bson.M{"_id": queueID},
					bson.M{"$inc": bson.M{"message_count": -res.DeletedCount}}); err != nil {
					return 0, internalmongo.Wrap(err, "queuestore.v2.RangeDeleteMessages.count")
				}
			}
			return res.DeletedCount, nil
		})
	if err != nil {
		return nil, err
	}
	return &persistence.InternalRangeDeleteMessagesResponse{MessagesDeleted: deleted}, nil
}

// ListQueues returns paginated queue metadata for a given QueueType.
func (v *V2Stub) ListQueues(
	ctx context.Context,
	req *persistence.InternalListQueuesRequest,
) (_ *persistence.InternalListQueuesResponse, retErr error) {
	defer v.observeOpV2("ListQueues")(&retErr)
	if v.db == nil || req == nil {
		return nil, errors.New("queuestore: uninitialised or nil request")
	}
	if req.PageSize <= 0 {
		return nil, persistence.ErrNonPositiveListQueuesPageSize
	}
	filter := bson.M{"type": int32(req.QueueType)}
	if len(req.NextPageToken) > 0 {
		after, err := decodeListQueuesToken(req.NextPageToken)
		if err != nil {
			return nil, err
		}
		filter["name"] = bson.M{"$gt": after}
	}
	cursor, err := v.db.Collection(v2QueuesColl).Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "name", Value: 1}}).SetLimit(int64(req.PageSize)))
	if err != nil {
		return nil, internalmongo.Translate(err, "queuestore.v2.ListQueues")
	}
	defer cursor.Close(ctx)
	var docs []v2QueueDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, internalmongo.Translate(err, "queuestore.v2.ListQueues.decode")
	}
	resp := &persistence.InternalListQueuesResponse{}
	for _, d := range docs {
		resp.Queues = append(resp.Queues, persistence.QueueInfo{
			QueueName:     d.Name,
			MessageCount:  d.MessageCount,
			LastMessageID: d.LastMessageID,
		})
	}
	if len(docs) == req.PageSize {
		resp.NextPageToken = encodeListQueuesToken(docs[len(docs)-1].Name)
	}
	return resp, nil
}

// listQueuesToken is the ListQueues resume cursor: the last queue name
// returned. JSON keeps arbitrary names unambiguous and lets corrupt
// tokens be rejected.
type listQueuesToken struct {
	After string `json:"after"`
}

func encodeListQueuesToken(after string) []byte {
	raw, _ := json.Marshal(listQueuesToken{After: after})
	return raw
}

func decodeListQueuesToken(raw []byte) (string, error) {
	var tok listQueuesToken
	if err := json.Unmarshal(raw, &tok); err != nil {
		return "", fmt.Errorf("%w: %v", persistence.ErrInvalidListQueuesNextPageToken, err)
	}
	return tok.After, nil
}

// EnsureIndexes installs the indexes each collection relies on.
func EnsureIndexes(ctx context.Context, db *mongo.Database) error {
	if _, err := db.Collection(legacyMsgColl).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "queue_type", Value: 1}, {Key: "message_id", Value: 1}},
		Options: options.Index().SetName("legacy_queue_type_mid"),
	}); err != nil {
		return internalmongo.Translate(err, "queuestore.EnsureIndexes.legacy")
	}
	if _, err := db.Collection(legacyDLQMsgColl).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "queue_type", Value: 1}, {Key: "message_id", Value: 1}},
		Options: options.Index().SetName("legacy_dlq_type_mid"),
	}); err != nil {
		return internalmongo.Translate(err, "queuestore.EnsureIndexes.legacy_dlq")
	}
	if _, err := db.Collection(v2MessagesColl).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "type", Value: 1},
			{Key: "name", Value: 1},
			{Key: "mid", Value: 1},
		},
		Options: options.Index().SetName("v2_messages_type_name_mid"),
	}); err != nil {
		return internalmongo.Translate(err, "queuestore.EnsureIndexes.v2_messages")
	}
	return nil
}
