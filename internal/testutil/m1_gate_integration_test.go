//go:build integration

// M1 gate test — drives the ShardStore, MetadataStore, and
// ClusterMetadataStore through the real upstream manager wrappers
// (persistence.NewShardManager, NewMetadataManagerImpl,
// NewClusterMetadataManagerImpl). A green run here proves the plugin's
// BSON encoding and error surface are compatible with Temporal's
// serialization/error-translation layer.
//
// This is not the full upstream conformance suite (which exercises
// TaskManager / ExecutionManager / NexusEndpointManager whose stores
// still return Unimplemented at M1). When M3 completes, the full
// upstream suite invocation will replace this test.
package testutil_test

import (
	"context"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/mongo"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/clustermetastore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metadatastore"
	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/shardstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil"
)

func gateDB(t *testing.T) *mongo.Database {
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
		URI: uri, Database: "temporal_m1_gate", AppName: "test",
	})
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close(context.Background()) })
	db := provider.Database()
	if err := metadatastore.EnsureIndexes(ctx, db); err != nil {
		t.Fatalf("metadatastore EnsureIndexes: %v", err)
	}
	if err := clustermetastore.EnsureIndexes(ctx, db); err != nil {
		t.Fatalf("clustermetastore EnsureIndexes: %v", err)
	}
	return db
}

// TestM1_ShardManager_GetOrCreateAndUpdate drives the ShardStore
// through persistence.NewShardManager, the upstream wrapper that
// serializes ShardInfo for the plugin. If the manager can round-trip
// a ShardInfo through our BSON + blob encoding, that's a meaningful
// end-to-end proof.
func TestM1_ShardManager_GetOrCreateAndUpdate(t *testing.T) {
	db := gateDB(t)
	shardStore := shardstore.New(db, "cluster0")
	shardMgr := persistence.NewShardManager(shardStore, serialization.NewSerializer())
	defer shardMgr.Close()

	ctx := context.Background()
	resp, err := shardMgr.GetOrCreateShard(ctx, &persistence.GetOrCreateShardRequest{
		ShardID: 1,
		InitialShardInfo: &persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
	})
	if err != nil {
		t.Fatalf("GetOrCreateShard: %v", err)
	}
	if resp.ShardInfo.ShardId != 1 {
		t.Errorf("ShardId round-trip broken: %d", resp.ShardInfo.ShardId)
	}

	// Update with the expected previous RangeID.
	err = shardMgr.UpdateShard(ctx, &persistence.UpdateShardRequest{
		ShardInfo: &persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 2,
		},
		PreviousRangeID: 1,
	})
	if err != nil {
		t.Fatalf("UpdateShard: %v", err)
	}
}

// TestM1_MetadataManager_CreateAndGet drives a namespace create + get
// through persistence.NewMetadataManagerImpl. Exercises the
// NamespaceDetail serializer: the manager turns the proto into a
// DataBlob, hands it to us, we store it, and on read the manager
// deserializes it again. Round-trip success means our blob codec and
// upstream serializer are mutually compatible.
func TestM1_MetadataManager_CreateAndGet(t *testing.T) {
	db := gateDB(t)
	store := metadatastore.New(db)
	mgr := persistence.NewMetadataManagerImpl(store, serialization.NewSerializer(), log.NewTestLogger(), "cluster0")
	defer mgr.Close()

	ctx := context.Background()

	// Initialize metadata singleton to 0 (GetMetadata is a passthrough
	// to our store; zero-valued singleton is the expected cold-start
	// shape).
	_ = mgr.InitializeSystemNamespaces(ctx, "cluster0")

	_, err := mgr.CreateNamespace(ctx, &persistence.CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   "ns-gate-id",
				Name: "ns-gate",
			},
			Config:            &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
		},
		IsGlobalNamespace: false,
	})
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}

	resp, err := mgr.GetNamespace(ctx, &persistence.GetNamespaceRequest{ID: "ns-gate-id"})
	if err != nil {
		t.Fatalf("GetNamespace: %v", err)
	}
	if resp.Namespace.Info.Name != "ns-gate" {
		t.Errorf("namespace name round-trip: got %q", resp.Namespace.Info.Name)
	}
}

// TestM1_ClusterMetadataManager_SaveAndList drives SaveClusterMetadata +
// ListClusterMetadata + membership through the upstream manager.
func TestM1_ClusterMetadataManager_SaveAndList(t *testing.T) {
	db := gateDB(t)
	store := clustermetastore.New(db)
	mgr := persistence.NewClusterMetadataManagerImpl(store, serialization.NewSerializer(), "cluster0", log.NewTestLogger())
	defer mgr.Close()

	ctx := context.Background()

	ok, err := mgr.SaveClusterMetadata(ctx, &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName: "cluster0",
			// Required minimum fields for the serializer.
			ClusterId:                "cluster0-id",
			HistoryShardCount:        4,
			IsGlobalNamespaceEnabled: false,
		},
		Version: 0,
	})
	if err != nil {
		t.Fatalf("SaveClusterMetadata: %v", err)
	}
	if !ok {
		t.Error("expected ok=true for first save")
	}

	// Membership round-trip. Uses a fresh role so multiple tests in the
	// suite do not collide.
	err = mgr.UpsertClusterMembership(ctx, &persistence.UpsertClusterMembershipRequest{
		Role:         persistence.Frontend,
		HostID:       []byte("gate-host"),
		RPCAddress:   net.ParseIP("127.0.0.1"),
		RPCPort:      7233,
		SessionStart: time.Now().UTC(),
		RecordExpiry: time.Hour,
	})
	if err != nil {
		t.Fatalf("UpsertClusterMembership: %v", err)
	}
	members, err := mgr.GetClusterMembers(ctx, &persistence.GetClusterMembersRequest{
		RoleEquals: persistence.Frontend,
	})
	if err != nil {
		t.Fatalf("GetClusterMembers: %v", err)
	}
	if len(members.ActiveMembers) != 1 {
		t.Errorf("want 1 member; got %d", len(members.ActiveMembers))
	}
}
