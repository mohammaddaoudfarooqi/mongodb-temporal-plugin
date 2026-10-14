//go:build integration

// Integration tests for the real ShardStore against a live replica set.
// These are the shard-store behavior tests; unit tests with a mocked
// driver would not catch BSON encoding drift or single-doc CAS semantics.
package shardstore_test

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/mongo"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/shardstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil"
)

func startReplicaSet(t *testing.T) *mongo.Database {
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
		URI: uri, Database: "temporal_shard_test", AppName: "test",
	})
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close(context.Background()) })
	return provider.Database()
}

func blobBytes(s string) *commonpb.DataBlob {
	return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte(s)}
}

// TestGetOrCreateShard_FirstCall_Creates first branch:
// a shard that does not exist gets inserted using CreateShardInfo's
// return value.
func TestGetOrCreateShard_FirstCall_Creates(t *testing.T) {
	db := startReplicaSet(t)
	s := shardstore.New(db, "cluster0")

	req := &persistence.InternalGetOrCreateShardRequest{
		ShardID: 1,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return 42, blobBytes("initial-shard-1"), nil
		},
	}
	resp, err := s.GetOrCreateShard(context.Background(), req)
	if err != nil {
		t.Fatalf("GetOrCreateShard: %v", err)
	}
	if resp == nil || resp.ShardInfo == nil {
		t.Fatal("expected non-nil response with ShardInfo")
	}
	if string(resp.ShardInfo.Data) != "initial-shard-1" {
		t.Errorf("blob bytes not preserved: %q", string(resp.ShardInfo.Data))
	}
}

// second branch: a shard that already exists is returned verbatim; the
// CreateShardInfo callback is NOT invoked.
func TestGetOrCreateShard_SecondCall_ReturnsExisting(t *testing.T) {
	db := startReplicaSet(t)
	s := shardstore.New(db, "cluster0")

	_, err := s.GetOrCreateShard(context.Background(), &persistence.InternalGetOrCreateShardRequest{
		ShardID: 2,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return 100, blobBytes("first-call"), nil
		},
	})
	if err != nil {
		t.Fatalf("first GetOrCreateShard: %v", err)
	}

	var callbackInvoked bool
	resp, err := s.GetOrCreateShard(context.Background(), &persistence.InternalGetOrCreateShardRequest{
		ShardID: 2,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			callbackInvoked = true
			return 999, blobBytes("should-not-apply"), nil
		},
	})
	if err != nil {
		t.Fatalf("second GetOrCreateShard: %v", err)
	}
	if callbackInvoked {
		t.Error("CreateShardInfo invoked on an existing shard")
	}
	if string(resp.ShardInfo.Data) != "first-call" {
		t.Errorf("existing shard was overwritten: %q", string(resp.ShardInfo.Data))
	}
}

func TestUpdateShard_CorrectPreviousRangeID_Succeeds(t *testing.T) {
	db := startReplicaSet(t)
	s := shardstore.New(db, "cluster0")

	_, err := s.GetOrCreateShard(context.Background(), &persistence.InternalGetOrCreateShardRequest{
		ShardID: 3,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return 5, blobBytes("v1"), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.UpdateShard(context.Background(), &persistence.InternalUpdateShardRequest{
		ShardID:         3,
		RangeID:         6,
		PreviousRangeID: 5,
		Owner:           "worker-b",
		ShardInfo:       blobBytes("v2"),
	})
	if err != nil {
		t.Fatalf("UpdateShard: %v", err)
	}

	// Read back via GetOrCreateShard; the new payload and range should be persisted.
	resp, err := s.GetOrCreateShard(context.Background(), &persistence.InternalGetOrCreateShardRequest{
		ShardID: 3,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return 0, blobBytes("ignored"), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.ShardInfo.Data) != "v2" {
		t.Errorf("UpdateShard did not persist new blob; got %q", string(resp.ShardInfo.Data))
	}
}

// CAS mismatch produces the domain-specific error type.
func TestUpdateShard_WrongPreviousRangeID_ReturnsShardOwnershipLost(t *testing.T) {
	db := startReplicaSet(t)
	s := shardstore.New(db, "cluster0")

	_, err := s.GetOrCreateShard(context.Background(), &persistence.InternalGetOrCreateShardRequest{
		ShardID: 4,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return 10, blobBytes("v1"), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.UpdateShard(context.Background(), &persistence.InternalUpdateShardRequest{
		ShardID:         4,
		RangeID:         12,
		PreviousRangeID: 11, // wrong: stored is 10
		Owner:           "worker-c",
		ShardInfo:       blobBytes("v2-bogus"),
	})
	if err == nil {
		t.Fatal("expected ShardOwnershipLostError on wrong PreviousRangeID")
	}
	var shardLost *persistence.ShardOwnershipLostError
	if !errors.As(err, &shardLost) {
		t.Fatalf("expected *ShardOwnershipLostError; got %T: %v", err, err)
	}
}

// TestAssertShardOwnership_ReturnsNil: the method is
// deliberately a no-op for parity with upstream SQL/Cassandra.
func TestAssertShardOwnership_ReturnsNil(t *testing.T) {
	db := startReplicaSet(t)
	s := shardstore.New(db, "cluster0")
	if err := s.AssertShardOwnership(context.Background(), &persistence.AssertShardOwnershipRequest{
		ShardID: 5, RangeID: 100,
	}); err != nil {
		t.Errorf("expected nil; got %v", err)
	}
}

// TestShardStore_GetClusterName covers a small but upstream-required
// detail: GetClusterName returns the cluster name the factory passed.
func TestShardStore_GetClusterName(t *testing.T) {
	db := startReplicaSet(t)
	s := shardstore.New(db, "cluster-xyz")
	if got := s.GetClusterName(); got != "cluster-xyz" {
		t.Errorf("GetClusterName = %q; want cluster-xyz", got)
	}
}
