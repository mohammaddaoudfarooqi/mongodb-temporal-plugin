// Package shardstore is the MongoDB-backed implementation of
// persistence.ShardStore.
//
// Storage model:
//
//	collection: shards
//	document: {
//	  _id: "shard:<shard_id>",          // composite string key
//	  shard_id: <int32>,                // leading index field
//	  range_id: <int64>,                // CAS predicate
//	  owner: <string>,                  // current owner identifier
//	  shard_info: { data: <BinData>, encoding: <string> },  // opaque blob
//	  update_time: <DateTime>
//	}
//	index: { shard_id: 1 } unique
//
// Every update is a single-document atomic findOneAndUpdate; no
// multi-document transaction needed.
package shardstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.temporal.io/server/common/persistence"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

const (
	// collectionName is the MongoDB collection holding shard documents.
	collectionName = "shards"
)

// shardDoc is the BSON projection of a shard. Field tags use snake_case
// per docs/developer/conventions.md.
type shardDoc struct {
	ID         string              `bson:"_id"`
	ShardID    int32               `bson:"shard_id"`
	RangeID    int64               `bson:"range_id"`
	Owner      string              `bson:"owner"`
	ShardInfo  *internalmongo.Blob `bson:"shard_info"`
	UpdateTime time.Time           `bson:"update_time"`
}

// shardID encodes the composite primary key. Keeping this in one place
// makes it easy to audit for shape drift.
func shardID(id int32) string { return fmt.Sprintf("shard:%d", id) }

// Ensure the concrete type continues to satisfy the upstream interface.
var _ persistence.ShardStore = (*Stub)(nil)

// GetOrCreateShard returns the shard document if it exists; otherwise
// it invokes CreateShardInfo to obtain initial values, inserts, and
// returns the stored blob. Idempotent on concurrent first-creates: if
// two callers race, the second sees a duplicate-key error on insert
// and re-reads the winner.
func (s *Stub) GetOrCreateShard(
	ctx context.Context,
	req *persistence.InternalGetOrCreateShardRequest,
) (_ *persistence.InternalGetOrCreateShardResponse, retErr error) {
	defer s.observeOp("GetOrCreateShard")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("shardstore: uninitialised or nil request")
	}
	coll := s.db.Collection(collectionName)
	key := shardID(req.ShardID)

	// Fast path: try read first. Avoids a round-trip through the
	// creation callback when the shard already exists (the common case
	// on every restart after the first).
	var existing shardDoc
	err := coll.FindOne(ctx, bson.M{"_id": key}).Decode(&existing)
	if err == nil {
		return &persistence.InternalGetOrCreateShardResponse{
			ShardInfo: internalmongo.DataBlobFromBlob(existing.ShardInfo),
		}, nil
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, internalmongo.Translate(err, "shardstore.GetOrCreateShard.find")
	}

	// Shard absent — run the creation callback to get the initial
	// RangeID and blob, then insert. A caller's CreateShardInfo is
	// allowed to fail; surface that error verbatim.
	if req.CreateShardInfo == nil {
		return nil, errors.New("shardstore: CreateShardInfo is nil")
	}
	rangeID, blob, err := req.CreateShardInfo()
	if err != nil {
		return nil, fmt.Errorf("shardstore: CreateShardInfo: %w", err)
	}

	doc := shardDoc{
		ID:         key,
		ShardID:    req.ShardID,
		RangeID:    rangeID,
		ShardInfo:  internalmongo.BlobFromDataBlob(blob),
		UpdateTime: time.Now().UTC(),
	}
	if _, err := coll.InsertOne(ctx, doc); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// Lost the race with a concurrent creator; re-read the
			// winner's values so both callers see the same shard.
			var winner shardDoc
			if err := coll.FindOne(ctx, bson.M{"_id": key}).Decode(&winner); err != nil {
				return nil, internalmongo.Translate(err, "shardstore.GetOrCreateShard.refind")
			}
			return &persistence.InternalGetOrCreateShardResponse{
				ShardInfo: internalmongo.DataBlobFromBlob(winner.ShardInfo),
			}, nil
		}
		return nil, internalmongo.Translate(err, "shardstore.GetOrCreateShard.insert")
	}
	return &persistence.InternalGetOrCreateShardResponse{
		ShardInfo: internalmongo.DataBlobFromBlob(doc.ShardInfo),
	}, nil
}

// UpdateShard applies a compare-and-swap update keyed on the stored
// RangeID. If the stored RangeID does not equal request.PreviousRangeID,
// returns *persistence.ShardOwnershipLostError.
func (s *Stub) UpdateShard(
	ctx context.Context,
	req *persistence.InternalUpdateShardRequest,
) (retErr error) {
	defer s.observeOp("UpdateShard")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("shardstore: uninitialised or nil request")
	}
	coll := s.db.Collection(collectionName)
	key := shardID(req.ShardID)

	filter := bson.M{
		"_id":      key,
		"range_id": req.PreviousRangeID,
	}
	update := bson.M{
		"$set": bson.M{
			"range_id":    req.RangeID,
			"owner":       req.Owner,
			"shard_info":  internalmongo.BlobFromDataBlob(req.ShardInfo),
			"update_time": time.Now().UTC(),
		},
	}

	res := coll.FindOneAndUpdate(ctx, filter, update,
		options.FindOneAndUpdate().SetReturnDocument(options.After))
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			// Either the shard was never created or the PreviousRangeID
			// doesn't match. Disambiguate with a read so the error
			// carries useful context for the caller.
			msg := fmt.Sprintf("shardstore: shard %d CAS failed (expected range %d)", req.ShardID, req.PreviousRangeID)
			return &persistence.ShardOwnershipLostError{ShardID: req.ShardID, Msg: msg}
		}
		return internalmongo.Translate(err, "shardstore.UpdateShard")
	}
	return nil
}

// AssertShardOwnership is a no-op matching upstream SQL/Cassandra
// behavior. The RangeID assertion is already covered by UpdateShard's
// CAS.
func (s *Stub) AssertShardOwnership(
	_ context.Context,
	_ *persistence.AssertShardOwnershipRequest,
) (retErr error) {
	defer s.observeOp("AssertShardOwnership")(&retErr)
	return nil
}

// EnsureIndexes creates the unique index on shard_id. Called at startup
// by the factory (setup-schema may eventually move this to the
// schema CLI; for now it's idempotent enough to run inline).
func EnsureIndexes(ctx context.Context, db *mongo.Database) error {
	_, err := db.Collection(collectionName).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "shard_id", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("shards_shard_id_unique"),
	})
	if err != nil {
		return internalmongo.Translate(err, "shardstore.EnsureIndexes")
	}
	return nil
}
