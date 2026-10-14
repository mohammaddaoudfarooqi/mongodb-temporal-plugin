// Package nexusstore is the MongoDB-backed implementation of
// persistence.NexusEndpointStore.
//
// Storage model:
//
//	collection: nexus_endpoints
//	  _id:     <endpoint_id>
//	  version: <int64>                  -- CAS guard for record
//	  data:    {data, encoding}         -- opaque endpoint blob
//	  update_time: <DateTime>
//
//	collection: nexus_endpoints_table
//	  _id: "singleton"
//	  version: <int64>                  -- table-level CAS guard
//
// Every mutation commits alongside a table-version bump in one
// multi-document transaction so LastKnownTableVersion provides
// cluster-wide ordering.
package nexusstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

const (
	endpointsColl      = "nexus_endpoints"
	endpointsTableColl = "nexus_endpoints_table"
	tableDocID         = "singleton"
)

type endpointDoc struct {
	ID         string              `bson:"_id"`
	Version    int64               `bson:"version"`
	Data       *internalmongo.Blob `bson:"data"`
	UpdateTime time.Time           `bson:"update_time"`
}

type tableDoc struct {
	ID      string `bson:"_id"`
	Version int64  `bson:"version"`
}

// CreateOrUpdateNexusEndpoint inserts a new endpoint or updates an
// existing one under a double-CAS (endpoint.Version + table.Version)
// within one transaction.
func (s *Stub) CreateOrUpdateNexusEndpoint(
	ctx context.Context,
	req *persistence.InternalCreateOrUpdateNexusEndpointRequest,
) (retErr error) {
	defer s.observeOp("CreateOrUpdateNexusEndpoint")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("nexusstore: uninitialised or nil request")
	}
	sess, err := s.db.Client().StartSession()
	if err != nil {
		return internalmongo.Translate(err, "nexusstore.CreateOrUpdateNexusEndpoint.session")
	}
	defer sess.EndSession(ctx)

	_, err = internalmongo.WithTxn[struct{}](ctx, internalmongo.NewSessionRunner(sess),
		func(sctx context.Context) (struct{}, error) {
			// Table-version CAS.
			newTableVersion, err := s.bumpTableVersion(sctx, req.LastKnownTableVersion)
			if err != nil {
				return struct{}{}, err
			}
			_ = newTableVersion

			ep := req.Endpoint
			newVersion := ep.Version + 1
			now := time.Now().UTC()

			if ep.Version == 0 {
				// Fresh create — insert-if-not-exists.
				doc := endpointDoc{
					ID:         ep.ID,
					Version:    newVersion,
					Data:       internalmongo.BlobFromDataBlob(ep.Data),
					UpdateTime: now,
				}
				_, err := s.db.Collection(endpointsColl).InsertOne(sctx, doc)
				if err != nil {
					if mongo.IsDuplicateKeyError(err) {
						return struct{}{}, &persistence.ConditionFailedError{
							Msg: fmt.Sprintf("nexus endpoint version mismatch: %s already exists", ep.ID),
						}
					}
					return struct{}{}, internalmongo.Wrap(err, "nexusstore.CreateOrUpdateNexusEndpoint.insert")
				}
				return struct{}{}, nil
			}

			// Update path — CAS on stored version == ep.Version.
			filter := bson.M{"_id": ep.ID, "version": ep.Version}
			update := bson.M{"$set": bson.M{
				"version":     newVersion,
				"data":        internalmongo.BlobFromDataBlob(ep.Data),
				"update_time": now,
			}}
			res, err := s.db.Collection(endpointsColl).UpdateOne(sctx, filter, update)
			if err != nil {
				return struct{}{}, internalmongo.Wrap(err, "nexusstore.CreateOrUpdateNexusEndpoint.update")
			}
			if res.MatchedCount == 0 {
				return struct{}{}, &persistence.ConditionFailedError{
					Msg: fmt.Sprintf("nexus endpoint version mismatch: %s expected version %d", ep.ID, ep.Version),
				}
			}
			return struct{}{}, nil
		})
	return err
}

// GetNexusEndpoint returns one endpoint, NotFound otherwise.
func (s *Stub) GetNexusEndpoint(
	ctx context.Context,
	req *persistence.GetNexusEndpointRequest,
) (_ *persistence.InternalNexusEndpoint, retErr error) {
	defer s.observeOp("GetNexusEndpoint")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("nexusstore: uninitialised or nil request")
	}
	var doc endpointDoc
	err := s.db.Collection(endpointsColl).FindOne(ctx, bson.M{"_id": req.ID}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, serviceerror.NewNotFound(fmt.Sprintf("nexus endpoint %s not found", req.ID))
		}
		return nil, internalmongo.Translate(err, "nexusstore.GetNexusEndpoint")
	}
	return &persistence.InternalNexusEndpoint{
		ID:      doc.ID,
		Version: doc.Version,
		Data:    internalmongo.DataBlobFromBlob(doc.Data),
	}, nil
}

// DeleteNexusEndpoint removes an endpoint and bumps the table version
// in one transaction.
func (s *Stub) DeleteNexusEndpoint(
	ctx context.Context,
	req *persistence.DeleteNexusEndpointRequest,
) (retErr error) {
	defer s.observeOp("DeleteNexusEndpoint")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("nexusstore: uninitialised or nil request")
	}
	sess, err := s.db.Client().StartSession()
	if err != nil {
		return internalmongo.Translate(err, "nexusstore.DeleteNexusEndpoint.session")
	}
	defer sess.EndSession(ctx)

	_, err = internalmongo.WithTxn[struct{}](ctx, internalmongo.NewSessionRunner(sess),
		func(sctx context.Context) (struct{}, error) {
			if _, err := s.bumpTableVersion(sctx, req.LastKnownTableVersion); err != nil {
				return struct{}{}, err
			}
			res, err := s.db.Collection(endpointsColl).DeleteOne(sctx, bson.M{"_id": req.ID})
			if err != nil {
				return struct{}{}, internalmongo.Wrap(err, "nexusstore.DeleteNexusEndpoint.delete")
			}
			if res.DeletedCount == 0 {
				// Returning an error rolls back the table-version bump.
				return struct{}{}, serviceerror.NewNotFound(fmt.Sprintf("nexus endpoint not found: %s", req.ID))
			}
			return struct{}{}, nil
		})
	return err
}

// ListNexusEndpoints returns a paginated list with the current table
// version. Ordered by _id ascending for determinism.
func (s *Stub) ListNexusEndpoints(
	ctx context.Context,
	req *persistence.ListNexusEndpointsRequest,
) (_ *persistence.InternalListNexusEndpointsResponse, retErr error) {
	defer s.observeOp("ListNexusEndpoints")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("nexusstore: uninitialised or nil request")
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	filter := bson.M{}
	if len(req.NextPageToken) > 0 {
		filter["_id"] = bson.M{"$gt": string(req.NextPageToken)}
	}
	sess, err := s.db.Client().StartSession()
	if err != nil {
		return nil, internalmongo.Translate(err, "nexusstore.ListNexusEndpoints.session")
	}
	defer sess.EndSession(ctx)

	// Read the table version and the page in one snapshot so the version
	// returned always describes the endpoints returned.
	type page struct {
		version int64
		docs    []endpointDoc
	}
	pg, err := internalmongo.WithTxn[page](ctx, internalmongo.NewSessionRunner(sess),
		func(sctx context.Context) (page, error) {
			version, err := s.currentTableVersion(sctx)
			if err != nil {
				return page{}, err
			}
			if req.LastKnownTableVersion != 0 && req.LastKnownTableVersion != version {
				return page{}, &persistence.ConditionFailedError{
					Msg: fmt.Sprintf("nexus endpoints table version mismatch: last known %d, current %d",
						req.LastKnownTableVersion, version),
				}
			}
			cursor, err := s.db.Collection(endpointsColl).Find(sctx, filter,
				options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(pageSize)+1))
			if err != nil {
				return page{}, internalmongo.Wrap(err, "nexusstore.ListNexusEndpoints.find")
			}
			defer cursor.Close(sctx)
			var docs []endpointDoc
			if err := cursor.All(sctx, &docs); err != nil {
				return page{}, internalmongo.Wrap(err, "nexusstore.ListNexusEndpoints.decode")
			}
			return page{version: version, docs: docs}, nil
		})
	if err != nil {
		return nil, err
	}
	docs := pg.docs

	resp := &persistence.InternalListNexusEndpointsResponse{}
	resp.TableVersion = pg.version
	for i, d := range docs {
		if i == pageSize {
			resp.NextPageToken = []byte(docs[pageSize-1].ID)
			break
		}
		resp.Endpoints = append(resp.Endpoints, persistence.InternalNexusEndpoint{
			ID:      d.ID,
			Version: d.Version,
			Data:    internalmongo.DataBlobFromBlob(d.Data),
		})
	}
	return resp, nil
}

// bumpTableVersion enforces table-level CAS and increments the
// singleton. Upsert handles the first-time case; stale
// LastKnownTableVersion returns ConditionFailedError.
func (s *Stub) bumpTableVersion(ctx context.Context, lastKnown int64) (int64, error) {
	// LastKnownTableVersion 0 means "no table yet": it only succeeds while
	// the singleton is absent or still at 0.
	if lastKnown == 0 {
		current, err := s.currentTableVersion(ctx)
		if err != nil {
			return 0, err
		}
		if current != 0 {
			return 0, tableVersionMismatch(lastKnown)
		}
		_, err = s.db.Collection(endpointsTableColl).UpdateOne(ctx,
			bson.M{"_id": tableDocID},
			bson.M{"$inc": bson.M{"version": int64(1)}},
			options.UpdateOne().SetUpsert(true))
		if err != nil {
			return 0, internalmongo.Wrap(err, "nexusstore.bumpTableVersion.init")
		}
		return 1, nil
	}
	filter := bson.M{"_id": tableDocID, "version": lastKnown}
	update := bson.M{"$inc": bson.M{"version": int64(1)}}
	var updated tableDoc
	err := s.db.Collection(endpointsTableColl).FindOneAndUpdate(ctx, filter, update,
		options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&updated)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return 0, tableVersionMismatch(lastKnown)
		}
		return 0, internalmongo.Wrap(err, "nexusstore.bumpTableVersion")
	}
	return updated.Version, nil
}

// currentTableVersion reads the singleton without advancing it. Returns
// 0 when the singleton is absent (no endpoints ever created).
func (s *Stub) currentTableVersion(ctx context.Context) (int64, error) {
	var doc tableDoc
	err := s.db.Collection(endpointsTableColl).FindOne(ctx, bson.M{"_id": tableDocID}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, nil
	}
	if err != nil {
		return 0, internalmongo.Wrap(err, "nexusstore.currentTableVersion")
	}
	return doc.Version, nil
}

func tableVersionMismatch(lastKnown int64) error {
	return &persistence.ConditionFailedError{
		Msg: fmt.Sprintf("nexus endpoints table version mismatch: last known %d", lastKnown),
	}
}

// Keep commonpb import acknowledged for future extensions (endpoint
// metadata sub-fields). Removing this line on a future refactor is
// safe if commonpb is not otherwise referenced.
var _ = commonpb.DataBlob{}
