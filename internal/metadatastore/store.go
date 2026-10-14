// Package metadatastore is the MongoDB-backed implementation of
// persistence.MetadataStore.
//
// Storage model:
//
//	collection: namespaces
//	  _id: <namespace_id>                 // composite-free; name is the natural key
//	  name: <string>                      // UNIQUE
//	  namespace: { data, encoding }       // opaque NamespaceDetail blob
//	  is_global: <bool>
//	  notification_version: <int64>       // version snapshot at the last write
//	  update_time: <DateTime>
//
//	collection: namespace_metadata
//	  _id: "singleton"
//	  notification_version: <int64>
//
// Every mutation advances the singleton's notification_version inside a
// multi-document transaction so the written namespace always carries
// the correct version.
package metadatastore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

const (
	namespacesColl = "namespaces"
	metadataColl   = "namespace_metadata"
	metadataDocID  = "singleton"
)

type nsDoc struct {
	ID                  string              `bson:"_id"`
	Name                string              `bson:"name"`
	Namespace           *internalmongo.Blob `bson:"namespace"`
	IsGlobal            bool                `bson:"is_global"`
	NotificationVersion int64               `bson:"notification_version"`
	UpdateTime          time.Time           `bson:"update_time"`
}

type metadataDoc struct {
	ID                  string `bson:"_id"`
	NotificationVersion int64  `bson:"notification_version"`
}

// CreateNamespace inserts a namespace record and advances the singleton
// notification version in one transaction. Duplicate name or ID
// surfaces as *persistence.ConditionFailedError via the driver's
// duplicate-key error path.
func (s *Stub) CreateNamespace(
	ctx context.Context,
	req *persistence.InternalCreateNamespaceRequest,
) (_ *persistence.CreateNamespaceResponse, retErr error) {
	defer s.observeOp("CreateNamespace")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("metadatastore: uninitialised or nil request")
	}

	// Use a session + WithTransaction so the singleton bump and the
	// namespace insert commit atomically. Driver retry handles
	// transient failovers.
	sess, err := s.db.Client().StartSession()
	if err != nil {
		return nil, internalmongo.Translate(err, "metadatastore.CreateNamespace.session")
	}
	defer sess.EndSession(ctx)

	result, err := internalmongo.WithTxn[string](ctx, internalmongo.NewSessionRunner(sess),
		func(sctx context.Context) (string, error) {
			newVersion, err := s.incrementMetadata(sctx)
			if err != nil {
				return "", err
			}
			doc := nsDoc{
				ID:                  req.ID,
				Name:                req.Name,
				Namespace:           internalmongo.BlobFromDataBlob(req.Namespace),
				IsGlobal:            req.IsGlobal,
				NotificationVersion: newVersion,
				UpdateTime:          time.Now().UTC(),
			}
			if _, err := s.db.Collection(namespacesColl).InsertOne(sctx, doc); err != nil {
				if mongo.IsDuplicateKeyError(err) {
					// Upstream `InitializeSystemNamespaces` and the
					// frontend CreateNamespace RPC both branch on this
					// specific error type to treat "namespace exists"
					// as a benign idempotent outcome. Returning
					// *persistence.ConditionFailedError — as this
					// code did before — made the server crash-loop on
					// every boot after the first successful init,
					// because upstream's type assertion missed.
					return "", serviceerror.NewNamespaceAlreadyExists(
						fmt.Sprintf("namespace %q (id=%s) already exists", req.Name, req.ID),
					)
				}
				return "", internalmongo.Wrap(err, "metadatastore.CreateNamespace.insert")
			}
			return req.ID, nil
		})
	if err != nil {
		return nil, err
	}
	return &persistence.CreateNamespaceResponse{ID: result}, nil
}

// GetNamespace returns the namespace document by ID or name. Only one
// of the two should be set; the upstream manager enforces that. If both
// are empty, an error is returned.
func (s *Stub) GetNamespace(
	ctx context.Context,
	req *persistence.GetNamespaceRequest,
) (_ *persistence.InternalGetNamespaceResponse, retErr error) {
	defer s.observeOp("GetNamespace")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("metadatastore: uninitialised or nil request")
	}
	var filter bson.M
	switch {
	case req.ID != "":
		filter = bson.M{"_id": req.ID}
	case req.Name != "":
		filter = bson.M{"name": req.Name}
	default:
		return nil, serviceerror.NewInvalidArgument("metadatastore: GetNamespace requires ID or Name")
	}

	var doc nsDoc
	if err := s.db.Collection(namespacesColl).FindOne(ctx, filter).Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, serviceerror.NewNamespaceNotFound(idOrName(req))
		}
		return nil, internalmongo.Translate(err, "metadatastore.GetNamespace")
	}
	return &persistence.InternalGetNamespaceResponse{
		Namespace:           internalmongo.DataBlobFromBlob(doc.Namespace),
		IsGlobal:            doc.IsGlobal,
		NotificationVersion: doc.NotificationVersion,
	}, nil
}

// UpdateNamespace updates a namespace and bumps the singleton version
// in one transaction, using the caller-supplied NotificationVersion as
// the CAS guard on the singleton.
func (s *Stub) UpdateNamespace(
	ctx context.Context,
	req *persistence.InternalUpdateNamespaceRequest,
) (retErr error) {
	defer s.observeOp("UpdateNamespace")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("metadatastore: uninitialised or nil request")
	}
	sess, err := s.db.Client().StartSession()
	if err != nil {
		return internalmongo.Translate(err, "metadatastore.UpdateNamespace.session")
	}
	defer sess.EndSession(ctx)

	_, err = internalmongo.WithTxn[struct{}](ctx, internalmongo.NewSessionRunner(sess),
		func(sctx context.Context) (struct{}, error) {
			// The caller's NotificationVersion is the metadata version it
			// read; only one writer per version may win.
			newVersion, err := s.casIncrementMetadata(sctx, req.NotificationVersion)
			if err != nil {
				return struct{}{}, err
			}
			filter := bson.M{"_id": req.Id}
			update := bson.M{"$set": bson.M{
				"name":                 req.Name,
				"namespace":            internalmongo.BlobFromDataBlob(req.Namespace),
				"is_global":            req.IsGlobal,
				"notification_version": newVersion,
				"update_time":          time.Now().UTC(),
			}}
			res, err := s.db.Collection(namespacesColl).UpdateOne(sctx, filter, update)
			if err != nil {
				if mongo.IsDuplicateKeyError(err) {
					// A rename collided with an existing namespace.
					// *NamespaceAlreadyExists is the type upstream
					// clients check for; ConditionFailedError would be
					// swallowed by retry loops as a transient issue
					// when it's actually a permanent conflict.
					return struct{}{}, serviceerror.NewNamespaceAlreadyExists(
						fmt.Sprintf("namespace name %q conflict", req.Name),
					)
				}
				return struct{}{}, internalmongo.Wrap(err, "metadatastore.UpdateNamespace")
			}
			if res.MatchedCount == 0 {
				return struct{}{}, serviceerror.NewNamespaceNotFound(req.Id)
			}
			return struct{}{}, nil
		})
	return err
}

// RenameNamespace renames a namespace. The name uniqueness
// index enforces atomicity naturally — a racing create/rename on the
// new name returns DuplicateKey which we translate to ConditionFailed.
func (s *Stub) RenameNamespace(
	ctx context.Context,
	req *persistence.InternalRenameNamespaceRequest,
) (retErr error) {
	defer s.observeOp("RenameNamespace")(&retErr)
	if req == nil || req.InternalUpdateNamespaceRequest == nil {
		return errors.New("metadatastore: nil rename request")
	}
	// Delegate to UpdateNamespace — same shape, same guarantees; the
	// unique index on `name` handles the rename-conflict branch.
	return s.UpdateNamespace(ctx, req.InternalUpdateNamespaceRequest)
}

// DeleteNamespace removes by ID. Idempotent.
func (s *Stub) DeleteNamespace(
	ctx context.Context,
	req *persistence.DeleteNamespaceRequest,
) (retErr error) {
	defer s.observeOp("DeleteNamespace")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("metadatastore: uninitialised or nil request")
	}
	_, err := s.db.Collection(namespacesColl).DeleteOne(ctx, bson.M{"_id": req.ID})
	if err != nil {
		return internalmongo.Translate(err, "metadatastore.DeleteNamespace")
	}
	return nil
}

// DeleteNamespaceByName removes by name. Idempotent.
func (s *Stub) DeleteNamespaceByName(
	ctx context.Context,
	req *persistence.DeleteNamespaceByNameRequest,
) (retErr error) {
	defer s.observeOp("DeleteNamespaceByName")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("metadatastore: uninitialised or nil request")
	}
	_, err := s.db.Collection(namespacesColl).DeleteOne(ctx, bson.M{"name": req.Name})
	if err != nil {
		return internalmongo.Translate(err, "metadatastore.DeleteNamespaceByName")
	}
	return nil
}

// ListNamespaces returns a paginated list ordered by ID ascending.
// Pagination uses the last-returned ID as the resume cursor, matching
// the deterministic-order requirement.
func (s *Stub) ListNamespaces(
	ctx context.Context,
	req *persistence.InternalListNamespacesRequest,
) (_ *persistence.InternalListNamespacesResponse, retErr error) {
	defer s.observeOp("ListNamespaces")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("metadatastore: uninitialised or nil request")
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}

	filter := bson.M{}
	if len(req.NextPageToken) > 0 {
		filter["_id"] = bson.M{"$gt": string(req.NextPageToken)}
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetLimit(int64(pageSize) + 1) // +1 so we can tell if there's another page

	cursor, err := s.db.Collection(namespacesColl).Find(ctx, filter, opts)
	if err != nil {
		return nil, internalmongo.Translate(err, "metadatastore.ListNamespaces.find")
	}
	defer cursor.Close(ctx)

	resp := &persistence.InternalListNamespacesResponse{}
	var docs []nsDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, internalmongo.Translate(err, "metadatastore.ListNamespaces.decode")
	}
	for i, d := range docs {
		if i == pageSize {
			// Record the resume token and drop the over-fetched row.
			resp.NextPageToken = []byte(docs[pageSize-1].ID)
			break
		}
		resp.Namespaces = append(resp.Namespaces, &persistence.InternalGetNamespaceResponse{
			Namespace:           internalmongo.DataBlobFromBlob(d.Namespace),
			IsGlobal:            d.IsGlobal,
			NotificationVersion: d.NotificationVersion,
		})
	}
	return resp, nil
}

// GetMetadata returns the singleton notification version. Lazy-created
// on first read so early-lifecycle queries do not fail.
func (s *Stub) GetMetadata(ctx context.Context) (_ *persistence.GetMetadataResponse, retErr error) {
	defer s.observeOp("GetMetadata")(&retErr)
	if s.db == nil {
		return nil, errors.New("metadatastore: uninitialised")
	}
	var doc metadataDoc
	err := s.db.Collection(metadataColl).FindOne(ctx, bson.M{"_id": metadataDocID}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return &persistence.GetMetadataResponse{NotificationVersion: 0}, nil
		}
		return nil, internalmongo.Translate(err, "metadatastore.GetMetadata")
	}
	return &persistence.GetMetadataResponse{NotificationVersion: doc.NotificationVersion}, nil
}

// incrementMetadata atomically bumps the singleton version and returns
// the new value. Uses upsert so the first mutation lazily creates the
// singleton. Called only from within a transaction.
func (s *Stub) incrementMetadata(ctx context.Context) (int64, error) {
	filter := bson.M{"_id": metadataDocID}
	update := bson.M{"$inc": bson.M{"notification_version": int64(1)}}
	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)
	var doc metadataDoc
	if err := s.db.Collection(metadataColl).FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc); err != nil {
		return 0, internalmongo.Wrap(err, "metadatastore.incrementMetadata")
	}
	return doc.NotificationVersion, nil
}

// casIncrementMetadata bumps the singleton only if it still holds
// expected, returning ConditionFailedError otherwise. Called only from
// within a transaction.
func (s *Stub) casIncrementMetadata(ctx context.Context, expected int64) (int64, error) {
	filter := bson.M{"_id": metadataDocID, "notification_version": expected}
	if expected == 0 {
		// A fresh cluster has no singleton yet.
		filter["notification_version"] = bson.M{"$in": bson.A{int64(0), nil}}
	}
	var doc metadataDoc
	err := s.db.Collection(metadataColl).FindOneAndUpdate(ctx, filter,
		bson.M{"$inc": bson.M{"notification_version": int64(1)}},
		options.FindOneAndUpdate().SetReturnDocument(options.After).SetUpsert(expected == 0)).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) || mongo.IsDuplicateKeyError(err) {
		return 0, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("metadata notification version mismatch (expected %d)", expected),
		}
	}
	if err != nil {
		return 0, internalmongo.Wrap(err, "metadatastore.casIncrementMetadata")
	}
	return doc.NotificationVersion, nil
}

// EnsureIndexes creates the unique index on name. Like
// shardstore.EnsureIndexes, it is safe to call repeatedly; the schema
// CLI will own this long-term.
func EnsureIndexes(ctx context.Context, db *mongo.Database) error {
	_, err := db.Collection(namespacesColl).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "name", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("namespaces_name_unique"),
	})
	if err != nil {
		return internalmongo.Translate(err, "metadatastore.EnsureIndexes")
	}
	return nil
}

func idOrName(req *persistence.GetNamespaceRequest) string {
	if req.ID != "" {
		return req.ID
	}
	return req.Name
}
