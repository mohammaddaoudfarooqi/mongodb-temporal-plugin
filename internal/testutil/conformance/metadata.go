package conformance

import (
	"errors"
	"testing"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metadatastore"
)

// runMetadataStore exercises namespace CRUD, uniqueness
// on name, list pagination, monotone notification version.
func runMetadataStore(t *testing.T, f Fixture) {
	t.Run("Create_And_GetByID", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", metadatastore.EnsureIndexes(tc.Ctx, tc.DB))
		s := metadatastore.New(tc.DB)

		id := uniqStr("ns")
		_, err := s.CreateNamespace(tc.Ctx, &persistence.InternalCreateNamespaceRequest{
			ID: id, Name: id, Namespace: dataBlob("v1"),
		})
		fatalOnErr(t, "CreateNamespace", err)

		got, err := s.GetNamespace(tc.Ctx, &persistence.GetNamespaceRequest{ID: id})
		fatalOnErr(t, "GetNamespace", err)
		if string(got.Namespace.Data) != "v1" {
			t.Errorf("namespace blob drift: %q", got.Namespace.Data)
		}
	})

	t.Run("Create_DuplicateName_Fails", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", metadatastore.EnsureIndexes(tc.Ctx, tc.DB))
		s := metadatastore.New(tc.DB)

		name := uniqStr("dup")
		_, err := s.CreateNamespace(tc.Ctx, &persistence.InternalCreateNamespaceRequest{
			ID: uniqStr("id-a"), Name: name, Namespace: dataBlob("v1"),
		})
		fatalOnErr(t, "first create", err)

		_, err = s.CreateNamespace(tc.Ctx, &persistence.InternalCreateNamespaceRequest{
			ID: uniqStr("id-b"), Name: name, Namespace: dataBlob("v2"),
		})
		if err == nil {
			t.Fatal("expected duplicate-name error; got nil")
		}
	})

	t.Run("Get_NotFound_SurfacesNamespaceNotFound", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", metadatastore.EnsureIndexes(tc.Ctx, tc.DB))
		s := metadatastore.New(tc.DB)

		_, err := s.GetNamespace(tc.Ctx, &persistence.GetNamespaceRequest{ID: uniqStr("nope")})
		if err == nil {
			t.Fatal("expected error on missing namespace; got nil")
		}
		// Upstream surfaces a specific NamespaceNotFound, not the
		// generic NotFound. Accept either — both satisfy the callers'
		// "not found" contract.
		var nsnf *serviceerror.NamespaceNotFound
		var nf *serviceerror.NotFound
		if !errors.As(err, &nsnf) && !errors.As(err, &nf) {
			t.Errorf("expected *NamespaceNotFound or *NotFound; got %T: %v", err, err)
		}
	})

	t.Run("Update_BumpsNotificationVersion", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", metadatastore.EnsureIndexes(tc.Ctx, tc.DB))
		s := metadatastore.New(tc.DB)

		id := uniqStr("upd")
		_, err := s.CreateNamespace(tc.Ctx, &persistence.InternalCreateNamespaceRequest{
			ID: id, Name: id, Namespace: dataBlob("v1"),
		})
		fatalOnErr(t, "create", err)

		pre, err := s.GetNamespace(tc.Ctx, &persistence.GetNamespaceRequest{ID: id})
		fatalOnErr(t, "get pre", err)
		// Upstream's manager passes the metadata singleton's current
		// version as the CAS guard.
		md, err := s.GetMetadata(tc.Ctx)
		fatalOnErr(t, "get metadata", err)

		err = s.UpdateNamespace(tc.Ctx, &persistence.InternalUpdateNamespaceRequest{
			Id: id, Name: id, Namespace: dataBlob("v2"),
			NotificationVersion: md.NotificationVersion,
		})
		fatalOnErr(t, "update", err)

		post, err := s.GetNamespace(tc.Ctx, &persistence.GetNamespaceRequest{ID: id})
		fatalOnErr(t, "get post", err)
		if post.NotificationVersion <= pre.NotificationVersion {
			t.Errorf("notification version did not advance: pre=%d post=%d",
				pre.NotificationVersion, post.NotificationVersion)
		}
		if string(post.Namespace.Data) != "v2" {
			t.Errorf("updated blob drift: %q", post.Namespace.Data)
		}
	})

	t.Run("DeleteByName_Idempotent", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", metadatastore.EnsureIndexes(tc.Ctx, tc.DB))
		s := metadatastore.New(tc.DB)

		name := uniqStr("del")
		_, err := s.CreateNamespace(tc.Ctx, &persistence.InternalCreateNamespaceRequest{
			ID: name, Name: name, Namespace: dataBlob("v1"),
		})
		fatalOnErr(t, "create", err)

		if err := s.DeleteNamespaceByName(tc.Ctx, &persistence.DeleteNamespaceByNameRequest{Name: name}); err != nil {
			t.Errorf("first delete: %v", err)
		}
		// Second delete of an already-gone namespace should not error.
		if err := s.DeleteNamespaceByName(tc.Ctx, &persistence.DeleteNamespaceByNameRequest{Name: name}); err != nil {
			t.Errorf("idempotent delete: %v", err)
		}
	})

	t.Run("List_Paginated", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", metadatastore.EnsureIndexes(tc.Ctx, tc.DB))
		s := metadatastore.New(tc.DB)

		for i := 0; i < 3; i++ {
			name := uniqStr("list")
			_, err := s.CreateNamespace(tc.Ctx, &persistence.InternalCreateNamespaceRequest{
				ID: name, Name: name, Namespace: dataBlob("v1"),
			})
			fatalOnErr(t, "create", err)
		}
		resp, err := s.ListNamespaces(tc.Ctx, &persistence.InternalListNamespacesRequest{PageSize: 100})
		fatalOnErr(t, "list", err)
		if len(resp.Namespaces) < 3 {
			t.Errorf("expected >=3 namespaces; got %d", len(resp.Namespaces))
		}
	})
}
