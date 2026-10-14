package mongo

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ErrConditionFailed signals that a compare-and-swap predicate did not
// match any document. Stores wrap this into domain-specific errors
// (e.g. ShardOwnershipLostError). Callers should use
// errors.Is, not equality, in case future refinements add context.
var ErrConditionFailed = errors.New("mongo: CAS condition not satisfied")

// Decoder is the subset of mongo.SingleResult the helper needs. Keeping
// it narrow lets unit tests supply a fake without mocking the whole
// driver surface.
type Decoder interface {
	Decode(dst any) error
}

// FindOneAndUpdater is the CAS escape hatch. Real code wires in a
// thin adapter around mongo.Collection.FindOneAndUpdate; tests inject a
// fake that returns a Decoder with a canned document. The options
// variadic matches the driver's signature so callers can pass the usual
// `After` / `Upsert` flags.
type FindOneAndUpdater interface {
	Run(ctx context.Context, filter, update any, opts ...options.Lister[options.FindOneAndUpdateOptions]) Decoder
}

// UpdateIfCond performs a compare-and-swap using `findOneAndUpdate`: if
// any document matches `filter`, it receives `update` atomically and the
// post-image is decoded into T. If nothing matched, the helper returns
// ErrConditionFailed. This is the single-document CAS primitive behind
// the shard RangeID predicate.
//
// The generic parameter `T` is the document type. Pointers are not
// recommended — UpdateIfCond does its own zero-value allocation.
//
// Every caller should wrap the error via errors/Translate() if it flows
// out to the manager layer; ErrConditionFailed is a plugin-internal
// sentinel and should not escape untransformed.
func UpdateIfCond[T any](
	ctx context.Context,
	runner FindOneAndUpdater,
	filter, update any,
	opts ...options.Lister[options.FindOneAndUpdateOptions],
) (T, error) {
	// ReturnDocument.After is the canonical way to get the post-image
	// from findOneAndUpdate. Callers can override via opts; their
	// override wins because we append first.
	merged := append(
		[]options.Lister[options.FindOneAndUpdateOptions]{
			options.FindOneAndUpdate().SetReturnDocument(options.After),
		},
		opts...,
	)

	var out T
	dec := runner.Run(ctx, filter, update, merged...)
	if err := dec.Decode(&out); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return out, ErrConditionFailed
		}
		return out, err
	}
	return out, nil
}

// NewCollectionUpdater adapts a live *mongo.Collection into the
// FindOneAndUpdater interface. The adapter is the "real" code path used
// outside tests; it is thin enough to not warrant its own tests — the
// integration tests for the shard store will exercise it end-to-end.
func NewCollectionUpdater(c *mongo.Collection) FindOneAndUpdater {
	return collectionUpdater{c: c}
}

type collectionUpdater struct {
	c *mongo.Collection
}

func (u collectionUpdater) Run(
	ctx context.Context,
	filter, update any,
	opts ...options.Lister[options.FindOneAndUpdateOptions],
) Decoder {
	return u.c.FindOneAndUpdate(ctx, filter, update, opts...)
}
