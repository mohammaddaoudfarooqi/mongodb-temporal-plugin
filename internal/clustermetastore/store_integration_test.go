//go:build integration

// Integration tests for the MongoDB-backed ClusterMetadataStore
// . Exercised against a real replica set because the
// TTL-index behaviour depends on real server semantics.
package clustermetastore_test

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/clustermetastore"
	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil"
)

func startDB(t *testing.T) *mongo.Database {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, err := mongodb.Run(ctx, testutil.MongoImage(), mongodb.WithReplicaSet("rs0"))
	if err != nil {
		t.Fatalf("start mongo: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	raw, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	u, _ := url.Parse(raw)
	q := u.Query()
	q.Del("replicaSet")
	u.RawQuery = q.Encode()
	uri := strings.TrimRight(u.String(), "?") + "?directConnection=true"
	provider, err := internalmongo.NewClientProvider(ctx, internalmongo.ClientConfig{
		URI: uri, Database: "temporal_cluster_test", AppName: "test",
	})
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close(context.Background()) })
	db := provider.Database()
	if err := testutil.WaitForPrimary(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := clustermetastore.EnsureIndexes(ctx, db); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
	return db
}

func blobBytes(s string) *commonpb.DataBlob {
	return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte(s)}
}

// happy path: first save succeeds and creates the record.
func TestSaveClusterMetadata_FirstTime_CreatesWithVersion(t *testing.T) {
	db := startDB(t)
	s := clustermetastore.New(db)
	ctx := context.Background()

	ok, err := s.SaveClusterMetadata(ctx, &persistence.InternalSaveClusterMetadataRequest{
		ClusterName:     "cluster-a",
		ClusterMetadata: blobBytes("v1"),
		Version:         0, // fresh
	})
	if err != nil {
		t.Fatalf("SaveClusterMetadata: %v", err)
	}
	if !ok {
		t.Error("expected ok=true for first create")
	}

	// Read-back.
	resp, err := s.GetClusterMetadata(ctx, &persistence.InternalGetClusterMetadataRequest{ClusterName: "cluster-a"})
	if err != nil {
		t.Fatalf("GetClusterMetadata: %v", err)
	}
	if string(resp.ClusterMetadata.Data) != "v1" {
		t.Errorf("blob mismatch: %q", string(resp.ClusterMetadata.Data))
	}
	if resp.Version <= 0 {
		t.Errorf("version should be > 0 after save; got %d", resp.Version)
	}
}

// TestSaveClusterMetadata_WrongVersion_Fails CAS path.
func TestSaveClusterMetadata_WrongVersion_Fails(t *testing.T) {
	db := startDB(t)
	s := clustermetastore.New(db)
	ctx := context.Background()

	_, err := s.SaveClusterMetadata(ctx, &persistence.InternalSaveClusterMetadataRequest{
		ClusterName: "cluster-b", ClusterMetadata: blobBytes("v1"), Version: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Now attempt an update with a stale version.
	ok, err := s.SaveClusterMetadata(ctx, &persistence.InternalSaveClusterMetadataRequest{
		ClusterName: "cluster-b", ClusterMetadata: blobBytes("v2"), Version: 999,
	})
	// Either returns ok=false OR a condition error — both are accepted signals.
	if err == nil && ok {
		t.Error("stale version update should fail; got ok=true, err=nil")
	}
}

func TestGetClusterMetadata_NotFound(t *testing.T) {
	db := startDB(t)
	s := clustermetastore.New(db)
	_, err := s.GetClusterMetadata(context.Background(), &persistence.InternalGetClusterMetadataRequest{ClusterName: "nope"})
	if err == nil {
		t.Fatal("expected NotFound; got nil")
	}
	var nf *serviceerror.NotFound
	if !errors.As(err, &nf) {
		t.Fatalf("expected NotFound; got %T: %v", err, err)
	}
}

// TestDeleteClusterMetadata_Idempotent: a missing cluster deletes cleanly.
func TestDeleteClusterMetadata_Idempotent(t *testing.T) {
	db := startDB(t)
	s := clustermetastore.New(db)
	if err := s.DeleteClusterMetadata(context.Background(), &persistence.InternalDeleteClusterMetadataRequest{
		ClusterName: "nope",
	}); err != nil {
		t.Errorf("expected nil; got %v", err)
	}
}

// TestListClusterMetadata_Paginated exercises the paginated list path.
func TestListClusterMetadata_Paginated(t *testing.T) {
	db := startDB(t)
	s := clustermetastore.New(db)
	ctx := context.Background()

	for _, name := range []string{"cluster-x", "cluster-y", "cluster-z"} {
		_, err := s.SaveClusterMetadata(ctx, &persistence.InternalSaveClusterMetadataRequest{
			ClusterName: name, ClusterMetadata: blobBytes(name), Version: 0,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	resp, err := s.ListClusterMetadata(ctx, &persistence.InternalListClusterMetadataRequest{PageSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.ClusterMetadata) != 3 {
		t.Errorf("want 3; got %d", len(resp.ClusterMetadata))
	}
}

// TestUpsertClusterMembership_AndGet/115: upsert a
// membership record and read it back with filters.
func TestUpsertClusterMembership_AndGet(t *testing.T) {
	db := startDB(t)
	s := clustermetastore.New(db)
	ctx := context.Background()

	hostID := []byte("host-1")
	req := &persistence.UpsertClusterMembershipRequest{
		Role:         persistence.History,
		HostID:       hostID,
		RPCAddress:   net.ParseIP("127.0.0.1"),
		RPCPort:      7234,
		SessionStart: time.Now().UTC(),
		RecordExpiry: 5 * time.Minute,
	}
	if err := s.UpsertClusterMembership(ctx, req); err != nil {
		t.Fatalf("UpsertClusterMembership: %v", err)
	}

	resp, err := s.GetClusterMembers(ctx, &persistence.GetClusterMembersRequest{
		RoleEquals: persistence.History,
	})
	if err != nil {
		t.Fatalf("GetClusterMembers: %v", err)
	}
	if len(resp.ActiveMembers) != 1 {
		t.Fatalf("want 1 member; got %d", len(resp.ActiveMembers))
	}
	if string(resp.ActiveMembers[0].HostID) != "host-1" {
		t.Errorf("HostID mismatch: %q", string(resp.ActiveMembers[0].HostID))
	}
}

func TestGetClusterMembers_RoleFilter(t *testing.T) {
	db := startDB(t)
	s := clustermetastore.New(db)
	ctx := context.Background()

	entries := []struct {
		hostID []byte
		role   persistence.ServiceType
	}{
		{[]byte("h-h1"), persistence.History},
		{[]byte("h-h2"), persistence.History},
		{[]byte("h-m1"), persistence.Matching},
	}
	for _, e := range entries {
		err := s.UpsertClusterMembership(ctx, &persistence.UpsertClusterMembershipRequest{
			Role: e.role, HostID: e.hostID,
			RPCAddress: net.ParseIP("127.0.0.1"), RPCPort: 1,
			SessionStart: time.Now().UTC(), RecordExpiry: time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	resp, err := s.GetClusterMembers(ctx, &persistence.GetClusterMembersRequest{
		RoleEquals: persistence.History,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.ActiveMembers) != 2 {
		t.Errorf("want 2 History members; got %d", len(resp.ActiveMembers))
	}
}

// TestMembership_TTLIndexHasExpireAfterZero covers backlog B-11's
// membership TTL: the record_expiry index must carry
// expireAfterSeconds=0 so MongoDB's background sweeper reaps rows
// as soon as their record_expiry is in the past. Index metadata is
// stable under the sweeper and deterministic to assert; running the
// full ~60s sweep cycle is too slow for every CI run.
func TestMembership_TTLIndexHasExpireAfterZero(t *testing.T) {
	db := startDB(t)
	ctx := context.Background()

	cursor, err := db.Collection("cluster_membership").Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	defer cursor.Close(ctx)

	var indexes []bson.M
	if err := cursor.All(ctx, &indexes); err != nil {
		t.Fatal(err)
	}
	var ttl *bson.M
	for i := range indexes {
		if idx := indexes[i]; idx["name"] == "membership_record_expiry_ttl" {
			ttl = &idx
			break
		}
	}
	if ttl == nil {
		t.Fatalf("expected membership_record_expiry_ttl index; got: %+v", indexes)
	}
	// expireAfterSeconds=0 is the canonical "when record_expiry < now()"
	// semantics Temporal relies on.
	if v := (*ttl)["expireAfterSeconds"]; v != int32(0) {
		t.Errorf("expected expireAfterSeconds=0; got %v (%T)", v, v)
	}
}
