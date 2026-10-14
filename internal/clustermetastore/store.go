// Package clustermetastore is the MongoDB-backed implementation of
// persistence.ClusterMetadataStore.
//
// Storage model:
//
//	collection: cluster_metadata
//	  _id: <cluster_name>
//	  cluster_metadata: { data, encoding }   // MutableClusterMetadata blob
//	  version: <int64>                       // CAS guard
//	  update_time: <DateTime>
//
//	collection: cluster_membership
//	  _id: "<role>:<host_id_hex>"            // natural key
//	  role: <int32>                          // ServiceType
//	  host_id: <BinData>
//	  rpc_address: <string>
//	  rpc_port: <int32>
//	  session_start: <DateTime>
//	  last_heartbeat: <DateTime>
//	  record_expiry: <DateTime>              // indexed with TTL
//
// record_expiry must be a TTL index. MongoDB's TTL
// sweeper runs about every 60s, so tests use relaxed timing when they
// verify expiry.
package clustermetastore

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

const (
	metadataColl   = "cluster_metadata"
	membershipColl = "cluster_membership"
)

type metadataRec struct {
	ID              string              `bson:"_id"`
	ClusterMetadata *internalmongo.Blob `bson:"cluster_metadata"`
	Version         int64               `bson:"version"`
	UpdateTime      time.Time           `bson:"update_time"`
}

type membershipRec struct {
	ID            string    `bson:"_id"`
	Role          int32     `bson:"role"`
	HostID        []byte    `bson:"host_id"`
	RPCAddress    string    `bson:"rpc_address"`
	RPCPort       uint16    `bson:"rpc_port"`
	SessionStart  time.Time `bson:"session_start"`
	LastHeartbeat time.Time `bson:"last_heartbeat"`
	RecordExpiry  time.Time `bson:"record_expiry"`
}

// ListClusterMetadata returns paginated cluster metadata, ordered by name.
func (s *Stub) ListClusterMetadata(
	ctx context.Context,
	req *persistence.InternalListClusterMetadataRequest,
) (_ *persistence.InternalListClusterMetadataResponse, retErr error) {
	defer s.observeOp("ListClusterMetadata")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("clustermetastore: uninitialised")
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	filter := bson.M{}
	if len(req.NextPageToken) > 0 {
		filter["_id"] = bson.M{"$gt": string(req.NextPageToken)}
	}
	cursor, err := s.db.Collection(metadataColl).Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(pageSize)+1))
	if err != nil {
		return nil, internalmongo.Translate(err, "clustermetastore.ListClusterMetadata")
	}
	defer cursor.Close(ctx)

	var docs []metadataRec
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, internalmongo.Translate(err, "clustermetastore.ListClusterMetadata.decode")
	}
	resp := &persistence.InternalListClusterMetadataResponse{}
	for i, d := range docs {
		if i == pageSize {
			resp.NextPageToken = []byte(docs[pageSize-1].ID)
			break
		}
		resp.ClusterMetadata = append(resp.ClusterMetadata, &persistence.InternalGetClusterMetadataResponse{
			ClusterMetadata: internalmongo.DataBlobFromBlob(d.ClusterMetadata),
			Version:         d.Version,
		})
	}
	return resp, nil
}

// GetClusterMetadata returns the record for ClusterName or NotFound.
func (s *Stub) GetClusterMetadata(
	ctx context.Context,
	req *persistence.InternalGetClusterMetadataRequest,
) (_ *persistence.InternalGetClusterMetadataResponse, retErr error) {
	defer s.observeOp("GetClusterMetadata")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("clustermetastore: uninitialised")
	}
	var doc metadataRec
	err := s.db.Collection(metadataColl).FindOne(ctx, bson.M{"_id": req.ClusterName}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, serviceerror.NewNotFound(
				fmt.Sprintf("cluster metadata %q not found", req.ClusterName))
		}
		return nil, internalmongo.Translate(err, "clustermetastore.GetClusterMetadata")
	}
	return &persistence.InternalGetClusterMetadataResponse{
		ClusterMetadata: internalmongo.DataBlobFromBlob(doc.ClusterMetadata),
		Version:         doc.Version,
	}, nil
}

// SaveClusterMetadata upserts with CAS on Version. First create uses
// Version==0 and the document is inserted. Subsequent updates must
// carry the current stored Version; mismatches return ok=false so the
// caller retries with a fresh Get.
func (s *Stub) SaveClusterMetadata(
	ctx context.Context,
	req *persistence.InternalSaveClusterMetadataRequest,
) (_ bool, retErr error) {
	defer s.observeOp("SaveClusterMetadata")(&retErr)
	if s.db == nil || req == nil {
		return false, errors.New("clustermetastore: uninitialised")
	}
	coll := s.db.Collection(metadataColl)
	newVersion := req.Version + 1

	if req.Version == 0 {
		// First-time save: insert-if-not-exists.
		doc := metadataRec{
			ID:              req.ClusterName,
			ClusterMetadata: internalmongo.BlobFromDataBlob(req.ClusterMetadata),
			Version:         newVersion,
			UpdateTime:      time.Now().UTC(),
		}
		_, err := coll.InsertOne(ctx, doc)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				// Someone else created it first; caller should re-read and retry.
				return false, nil
			}
			return false, internalmongo.Translate(err, "clustermetastore.SaveClusterMetadata.insert")
		}
		return true, nil
	}

	// CAS update path.
	filter := bson.M{"_id": req.ClusterName, "version": req.Version}
	update := bson.M{"$set": bson.M{
		"cluster_metadata": internalmongo.BlobFromDataBlob(req.ClusterMetadata),
		"version":          newVersion,
		"update_time":      time.Now().UTC(),
	}}
	res, err := coll.UpdateOne(ctx, filter, update)
	if err != nil {
		return false, internalmongo.Translate(err, "clustermetastore.SaveClusterMetadata.update")
	}
	return res.MatchedCount > 0, nil
}

// DeleteClusterMetadata is idempotent.
func (s *Stub) DeleteClusterMetadata(
	ctx context.Context,
	req *persistence.InternalDeleteClusterMetadataRequest,
) (retErr error) {
	defer s.observeOp("DeleteClusterMetadata")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("clustermetastore: uninitialised")
	}
	_, err := s.db.Collection(metadataColl).DeleteOne(ctx, bson.M{"_id": req.ClusterName})
	if err != nil {
		return internalmongo.Translate(err, "clustermetastore.DeleteClusterMetadata")
	}
	return nil
}

// GetClusterMembers applies RoleEquals, HostIDEquals, RPCAddressEquals,
// LastHeartbeatWithin, and SessionStartedAfter filters. Only members
// whose record_expiry is in the future are returned (the TTL sweeper
// eventually removes them, but we filter defensively in case the
// sweeper lags).
func (s *Stub) GetClusterMembers(
	ctx context.Context,
	req *persistence.GetClusterMembersRequest,
) (_ *persistence.GetClusterMembersResponse, retErr error) {
	defer s.observeOp("GetClusterMembers")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("clustermetastore: uninitialised")
	}
	filter := bson.M{
		"record_expiry": bson.M{"$gt": time.Now().UTC()},
	}
	if req.RoleEquals != persistence.All {
		filter["role"] = int32(req.RoleEquals)
	}
	if len(req.HostIDEquals) > 0 {
		filter["host_id"] = req.HostIDEquals
	}
	if req.RPCAddressEquals != nil {
		filter["rpc_address"] = req.RPCAddressEquals.String()
	}
	if req.LastHeartbeatWithin > 0 {
		filter["last_heartbeat"] = bson.M{"$gt": time.Now().Add(-req.LastHeartbeatWithin).UTC()}
	}
	if !req.SessionStartedAfter.IsZero() {
		filter["session_start"] = bson.M{"$gt": req.SessionStartedAfter.UTC()}
	}

	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(pageSize) + 1)
	if len(req.NextPageToken) > 0 {
		filter["_id"] = bson.M{"$gt": string(req.NextPageToken)}
	}
	cursor, err := s.db.Collection(membershipColl).Find(ctx, filter, opts)
	if err != nil {
		return nil, internalmongo.Translate(err, "clustermetastore.GetClusterMembers")
	}
	defer cursor.Close(ctx)

	var docs []membershipRec
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, internalmongo.Translate(err, "clustermetastore.GetClusterMembers.decode")
	}
	resp := &persistence.GetClusterMembersResponse{}
	for i, d := range docs {
		if i == pageSize {
			resp.NextPageToken = []byte(docs[pageSize-1].ID)
			break
		}
		resp.ActiveMembers = append(resp.ActiveMembers, &persistence.ClusterMember{
			Role:          persistence.ServiceType(d.Role),
			HostID:        d.HostID,
			RPCAddress:    net.ParseIP(d.RPCAddress),
			RPCPort:       d.RPCPort,
			SessionStart:  d.SessionStart,
			LastHeartbeat: d.LastHeartbeat,
			RecordExpiry:  d.RecordExpiry,
		})
	}
	return resp, nil
}

// UpsertClusterMembership adds or refreshes a membership record.
func (s *Stub) UpsertClusterMembership(
	ctx context.Context,
	req *persistence.UpsertClusterMembershipRequest,
) (retErr error) {
	defer s.observeOp("UpsertClusterMembership")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("clustermetastore: uninitialised")
	}
	now := time.Now().UTC()
	id := membershipID(req.Role, req.HostID)
	update := bson.M{"$set": bson.M{
		"_id":            id,
		"role":           int32(req.Role),
		"host_id":        req.HostID,
		"rpc_address":    req.RPCAddress.String(),
		"rpc_port":       req.RPCPort,
		"session_start":  req.SessionStart.UTC(),
		"last_heartbeat": now,
		"record_expiry":  now.Add(req.RecordExpiry),
	}}
	_, err := s.db.Collection(membershipColl).UpdateOne(ctx,
		bson.M{"_id": id}, update, options.UpdateOne().SetUpsert(true))
	if err != nil {
		return internalmongo.Translate(err, "clustermetastore.UpsertClusterMembership")
	}
	return nil
}

// PruneClusterMembership deletes expired records (belt-and-suspenders
// alongside the TTL index).
func (s *Stub) PruneClusterMembership(
	ctx context.Context,
	req *persistence.PruneClusterMembershipRequest,
) (retErr error) {
	defer s.observeOp("PruneClusterMembership")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("clustermetastore: uninitialised")
	}
	filter := bson.M{"record_expiry": bson.M{"$lt": time.Now().UTC()}}
	_, err := s.db.Collection(membershipColl).DeleteMany(ctx, filter)
	if err != nil {
		return internalmongo.Translate(err, "clustermetastore.PruneClusterMembership")
	}
	return nil
}

// EnsureIndexes creates the TTL index on cluster_membership.record_expiry
// and a role index for filtered reads.
func EnsureIndexes(ctx context.Context, db *mongo.Database) error {
	membership := db.Collection(membershipColl)
	// TTL: expireAfterSeconds=0 → delete when record_expiry is in the past.
	_, err := membership.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "record_expiry", Value: 1}},
			Options: options.Index().
				SetExpireAfterSeconds(0).
				SetName("membership_record_expiry_ttl"),
		},
		{
			Keys:    bson.D{{Key: "role", Value: 1}, {Key: "last_heartbeat", Value: -1}},
			Options: options.Index().SetName("membership_role_heartbeat"),
		},
	})
	if err != nil {
		return internalmongo.Translate(err, "clustermetastore.EnsureIndexes")
	}
	return nil
}

func membershipID(role persistence.ServiceType, hostID []byte) string {
	return fmt.Sprintf("%d:%s", int(role), hex.EncodeToString(hostID))
}
