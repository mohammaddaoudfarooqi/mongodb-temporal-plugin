package conformance

import (
	"errors"
	"net"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/clustermetastore"
)

// runClusterMetadataStore exercises cluster-metadata
// CAS semantics, membership upsert + role-filter, pruning, list.
func runClusterMetadataStore(t *testing.T, f Fixture) {
	t.Run("SaveClusterMetadata_FirstTime_Creates", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", clustermetastore.EnsureIndexes(tc.Ctx, tc.DB))
		s := clustermetastore.New(tc.DB)

		name := uniqStr("cluster")
		ok, err := s.SaveClusterMetadata(tc.Ctx, &persistence.InternalSaveClusterMetadataRequest{
			ClusterName: name, ClusterMetadata: dataBlob("v1"), Version: 0,
		})
		fatalOnErr(t, "save first", err)
		if !ok {
			t.Fatal("expected ok=true on first save")
		}

		got, err := s.GetClusterMetadata(tc.Ctx, &persistence.InternalGetClusterMetadataRequest{ClusterName: name})
		fatalOnErr(t, "get", err)
		if string(got.ClusterMetadata.Data) != "v1" {
			t.Errorf("blob drift: %q", got.ClusterMetadata.Data)
		}
		if got.Version <= 0 {
			t.Errorf("version should advance past 0; got %d", got.Version)
		}
	})

	t.Run("SaveClusterMetadata_StaleVersion_Fails", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", clustermetastore.EnsureIndexes(tc.Ctx, tc.DB))
		s := clustermetastore.New(tc.DB)

		name := uniqStr("cluster")
		_, err := s.SaveClusterMetadata(tc.Ctx, &persistence.InternalSaveClusterMetadataRequest{
			ClusterName: name, ClusterMetadata: dataBlob("v1"), Version: 0,
		})
		fatalOnErr(t, "seed", err)

		// Attempt a stale-version update; expect ok=false or an error.
		ok, err := s.SaveClusterMetadata(tc.Ctx, &persistence.InternalSaveClusterMetadataRequest{
			ClusterName: name, ClusterMetadata: dataBlob("v2"), Version: 999,
		})
		if err == nil && ok {
			t.Error("stale-version save should fail; got ok=true, err=nil")
		}
	})

	t.Run("GetClusterMetadata_NotFound_ReturnsNotFound", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", clustermetastore.EnsureIndexes(tc.Ctx, tc.DB))
		s := clustermetastore.New(tc.DB)

		_, err := s.GetClusterMetadata(tc.Ctx, &persistence.InternalGetClusterMetadataRequest{ClusterName: uniqStr("nope")})
		if err == nil {
			t.Fatal("expected NotFound; got nil")
		}
		var nf *serviceerror.NotFound
		if !errors.As(err, &nf) {
			t.Errorf("expected *NotFound; got %T: %v", err, err)
		}
	})

	// Cluster-metadata suite previously covered Save / Get /
	// stale-CAS / NotFound only. Delete + List fill the gap.

	t.Run("DeleteClusterMetadata_Idempotent", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", clustermetastore.EnsureIndexes(tc.Ctx, tc.DB))
		s := clustermetastore.New(tc.DB)

		name := uniqStr("cluster")
		_, err := s.SaveClusterMetadata(tc.Ctx, &persistence.InternalSaveClusterMetadataRequest{
			ClusterName: name, ClusterMetadata: dataBlob("v1"), Version: 0,
		})
		fatalOnErr(t, "seed", err)

		fatalOnErr(t, "first delete", s.DeleteClusterMetadata(tc.Ctx,
			&persistence.InternalDeleteClusterMetadataRequest{ClusterName: name}))
		// Repeat delete must be a no-op.
		fatalOnErr(t, "idempotent delete", s.DeleteClusterMetadata(tc.Ctx,
			&persistence.InternalDeleteClusterMetadataRequest{ClusterName: name}))
	})

	t.Run("ListClusterMetadata_ReturnsSeededRows", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", clustermetastore.EnsureIndexes(tc.Ctx, tc.DB))
		s := clustermetastore.New(tc.DB)

		seeded := []string{uniqStr("cl-list"), uniqStr("cl-list"), uniqStr("cl-list")}
		for _, name := range seeded {
			_, err := s.SaveClusterMetadata(tc.Ctx, &persistence.InternalSaveClusterMetadataRequest{
				ClusterName: name, ClusterMetadata: dataBlob("v1"), Version: 0,
			})
			fatalOnErr(t, "seed", err)
		}

		resp, err := s.ListClusterMetadata(tc.Ctx, &persistence.InternalListClusterMetadataRequest{PageSize: 100})
		fatalOnErr(t, "list", err)
		if len(resp.ClusterMetadata) < len(seeded) {
			t.Errorf("expected >=%d rows; got %d", len(seeded), len(resp.ClusterMetadata))
		}
	})

	t.Run("Membership_UpsertAndRoleFilter", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", clustermetastore.EnsureIndexes(tc.Ctx, tc.DB))
		s := clustermetastore.New(tc.DB)

		for _, e := range []struct {
			role   persistence.ServiceType
			hostID []byte
		}{
			{persistence.History, []byte(uniqStr("h-h"))},
			{persistence.History, []byte(uniqStr("h-h"))},
			{persistence.Matching, []byte(uniqStr("h-m"))},
		} {
			err := s.UpsertClusterMembership(tc.Ctx, &persistence.UpsertClusterMembershipRequest{
				Role:         e.role,
				HostID:       e.hostID,
				RPCAddress:   net.ParseIP("127.0.0.1"),
				RPCPort:      1,
				SessionStart: time.Now().UTC(),
				RecordExpiry: time.Hour,
			})
			fatalOnErr(t, "upsert", err)
		}

		resp, err := s.GetClusterMembers(tc.Ctx, &persistence.GetClusterMembersRequest{
			RoleEquals: persistence.History,
		})
		fatalOnErr(t, "get role-filtered", err)
		if len(resp.ActiveMembers) < 2 {
			t.Errorf("expected >=2 History members; got %d", len(resp.ActiveMembers))
		}
	})
}
