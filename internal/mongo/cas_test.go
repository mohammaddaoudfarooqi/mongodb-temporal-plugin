package mongo_test

import (
	"context"
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

// shardDoc mirrors the eventual shard schema shape closely enough to
// exercise the CAS helper's return decoding. The real schema lives in
// internal/shardstore; this is a test-local double.
type shardDoc struct {
	ID      string `bson:"_id"`
	RangeID int64  `bson:"range_id"`
	Owner   string `bson:"owner"`
}

// findOneAndUpdateRunner is the interface we assume the helper takes.
// Tests set .doc / .err; the helper invokes .Decode(dst).
type findOneAndUpdateRunner struct {
	doc any
	err error
}

func (f *findOneAndUpdateRunner) Run(
	_ context.Context,
	_ any, // filter
	_ any, // update
	_ ...options.Lister[options.FindOneAndUpdateOptions],
) internalmongo.Decoder {
	return &decoderStub{doc: f.doc, err: f.err}
}

type decoderStub struct {
	doc any
	err error
}

func (d *decoderStub) Decode(dst any) error {
	if d.err != nil {
		return d.err
	}
	// Marshal then unmarshal to mimic driver behavior and catch type
	// mismatches early.
	raw, err := bson.Marshal(d.doc)
	if err != nil {
		return err
	}
	return bson.Unmarshal(raw, dst)
}

// TestUpdateIfCond_PredicateHolds_ReturnsPostImage exercises the happy
// path: the fake returns the updated document, the helper decodes it,
// and the caller sees it with no error.
func TestUpdateIfCond_PredicateHolds_ReturnsPostImage(t *testing.T) {
	runner := &findOneAndUpdateRunner{
		doc: bson.M{"_id": "shard:1", "range_id": int64(43), "owner": "worker-b"},
	}

	got, err := internalmongo.UpdateIfCond[shardDoc](
		context.Background(), runner,
		bson.M{"_id": "shard:1", "range_id": int64(42)},
		bson.M{"$set": bson.M{"range_id": int64(43), "owner": "worker-b"}},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.RangeID != 43 {
		t.Errorf("expected RangeID=43 post-image; got %d", got.RangeID)
	}
	if got.Owner != "worker-b" {
		t.Errorf("expected Owner=worker-b; got %q", got.Owner)
	}
}

// TestUpdateIfCond_PredicateMisses_ReturnsErrConditionFailed: when the
// driver reports no-documents-matched, the helper surfaces the sentinel
// error ErrConditionFailed so callers can pattern-match on it (the shard
// store wraps it into ShardOwnershipLostError).
func TestUpdateIfCond_PredicateMisses_ReturnsErrConditionFailed(t *testing.T) {
	runner := &findOneAndUpdateRunner{
		err: mongo.ErrNoDocuments,
	}
	_, err := internalmongo.UpdateIfCond[shardDoc](
		context.Background(), runner,
		bson.M{"_id": "shard:1", "range_id": int64(42)},
		bson.M{"$set": bson.M{"range_id": int64(43)}},
	)
	if !errors.Is(err, internalmongo.ErrConditionFailed) {
		t.Fatalf("expected ErrConditionFailed; got %T: %v", err, err)
	}
}

// TestUpdateIfCond_OtherErrorPropagates: an unrelated driver error
// surfaces unchanged so the caller's Translate() can classify it.
func TestUpdateIfCond_OtherErrorPropagates(t *testing.T) {
	want := errors.New("network flap")
	runner := &findOneAndUpdateRunner{err: want}
	_, err := internalmongo.UpdateIfCond[shardDoc](
		context.Background(), runner,
		bson.M{"_id": "shard:1"}, bson.M{"$set": bson.M{"x": 1}},
	)
	if !errors.Is(err, want) {
		t.Fatalf("expected underlying error to propagate; got %v", err)
	}
}
