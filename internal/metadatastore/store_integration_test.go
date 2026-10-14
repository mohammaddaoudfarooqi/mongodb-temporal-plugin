//go:build integration

// Integration tests for the MongoDB-backed MetadataStore. Exercises each
// namespace CRUD branch against a real replica set.
package metadatastore_test

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
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metadatastore"
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
		URI: uri, Database: "temporal_metadata_test", AppName: "test",
	})
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close(context.Background()) })
	db := provider.Database()
	if err := testutil.WaitForPrimary(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := metadatastore.EnsureIndexes(ctx, db); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
	return db
}

func blobBytes(s string) *commonpb.DataBlob {
	return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte(s)}
}

// TestCreateNamespace_FreshAndReadBack
// happy path: Create succeeds and GetNamespace returns the document
// with the correct NotificationVersion (starts at 1 after the first
// create).
func TestCreateNamespace_FreshAndReadBack(t *testing.T) {
	db := startDB(t)
	s := metadatastore.New(db)
	ctx := context.Background()

	_, err := s.CreateNamespace(ctx, &persistence.InternalCreateNamespaceRequest{
		ID:        "ns-1-id",
		Name:      "ns-1",
		Namespace: blobBytes("ns-1-blob"),
		IsGlobal:  false,
	})
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}

	resp, err := s.GetNamespace(ctx, &persistence.GetNamespaceRequest{ID: "ns-1-id"})
	if err != nil {
		t.Fatalf("GetNamespace: %v", err)
	}
	if resp.Namespace == nil || string(resp.Namespace.Data) != "ns-1-blob" {
		t.Errorf("GetNamespace: blob mismatch; got %+v", resp.Namespace)
	}
	if resp.NotificationVersion <= 0 {
		t.Errorf("NotificationVersion should be > 0 after first create; got %d", resp.NotificationVersion)
	}
}

// TestCreateNamespace_DuplicateName: uniqueness on name.
func TestCreateNamespace_DuplicateName(t *testing.T) {
	db := startDB(t)
	s := metadatastore.New(db)
	ctx := context.Background()

	_, err := s.CreateNamespace(ctx, &persistence.InternalCreateNamespaceRequest{
		ID: "a", Name: "dup", Namespace: blobBytes("a"),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.CreateNamespace(ctx, &persistence.InternalCreateNamespaceRequest{
		ID: "b", Name: "dup", Namespace: blobBytes("b"),
	})
	if err == nil {
		t.Fatal("expected conflict error on duplicate name")
	}
	var cfe *persistence.ConditionFailedError
	if !errors.As(err, &cfe) {
		// also acceptable: NamespaceAlreadyExists (upstream alias)
		var exists *serviceerror.NamespaceAlreadyExists
		if !errors.As(err, &exists) {
			t.Fatalf("expected ConditionFailedError; got %T: %v", err, err)
		}
	}
}

func TestGetNamespace_NotFound(t *testing.T) {
	db := startDB(t)
	s := metadatastore.New(db)
	_, err := s.GetNamespace(context.Background(), &persistence.GetNamespaceRequest{ID: "missing"})
	if err == nil {
		t.Fatal("expected NamespaceNotFound; got nil")
	}
	var nnf *serviceerror.NamespaceNotFound
	if !errors.As(err, &nnf) {
		t.Fatalf("expected NamespaceNotFound; got %T: %v", err, err)
	}
}

// TestRenameNamespace_Atomic: rename updates both
// name→ID index and namespace record atomically.
func TestRenameNamespace_Atomic(t *testing.T) {
	db := startDB(t)
	s := metadatastore.New(db)
	ctx := context.Background()

	_, err := s.CreateNamespace(ctx, &persistence.InternalCreateNamespaceRequest{
		ID: "r-id", Name: "old-name", Namespace: blobBytes("v1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	gresp, err := s.GetNamespace(ctx, &persistence.GetNamespaceRequest{ID: "r-id"})
	if err != nil {
		t.Fatal(err)
	}

	err = s.RenameNamespace(ctx, &persistence.InternalRenameNamespaceRequest{
		PreviousName: "old-name",
		InternalUpdateNamespaceRequest: &persistence.InternalUpdateNamespaceRequest{
			Id:                  "r-id",
			Name:                "new-name",
			Namespace:           blobBytes("v2"),
			NotificationVersion: gresp.NotificationVersion,
		},
	})
	if err != nil {
		t.Fatalf("RenameNamespace: %v", err)
	}

	// Lookup by new name works.
	g2, err := s.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: "new-name"})
	if err != nil {
		t.Fatalf("GetNamespace by new name: %v", err)
	}
	if string(g2.Namespace.Data) != "v2" {
		t.Errorf("blob not updated: %q", string(g2.Namespace.Data))
	}

	// Lookup by old name fails.
	_, err = s.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: "old-name"})
	if err == nil {
		t.Error("old name still resolves after rename")
	}
}

// TestGetMetadata_NotificationVersion: the singleton
// tracks notification version, starting from 1 after the first create
// and incrementing on each namespace mutation.
func TestGetMetadata_NotificationVersion(t *testing.T) {
	db := startDB(t)
	s := metadatastore.New(db)
	ctx := context.Background()

	md0, err := s.GetMetadata(ctx)
	if err != nil {
		t.Fatalf("GetMetadata: %v", err)
	}

	_, err = s.CreateNamespace(ctx, &persistence.InternalCreateNamespaceRequest{
		ID: "nv", Name: "nv", Namespace: blobBytes("x"),
	})
	if err != nil {
		t.Fatal(err)
	}

	md1, err := s.GetMetadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if md1.NotificationVersion <= md0.NotificationVersion {
		t.Errorf("NotificationVersion did not advance: %d → %d",
			md0.NotificationVersion, md1.NotificationVersion)
	}
}

// TestDeleteNamespace_Idempotent: deleting a
// non-existent namespace returns nil.
func TestDeleteNamespace_Idempotent(t *testing.T) {
	db := startDB(t)
	s := metadatastore.New(db)
	ctx := context.Background()

	if err := s.DeleteNamespace(ctx, &persistence.DeleteNamespaceRequest{ID: "nope"}); err != nil {
		t.Errorf("expected nil; got %v", err)
	}
	if err := s.DeleteNamespaceByName(ctx, &persistence.DeleteNamespaceByNameRequest{Name: "nope"}); err != nil {
		t.Errorf("expected nil; got %v", err)
	}
}

// TestListNamespaces_DeterministicOrder: pagination
// ordered by ID.
func TestListNamespaces_DeterministicOrder(t *testing.T) {
	db := startDB(t)
	s := metadatastore.New(db)
	ctx := context.Background()

	for _, name := range []string{"c", "a", "b"} {
		_, err := s.CreateNamespace(ctx, &persistence.InternalCreateNamespaceRequest{
			ID: name + "-id", Name: name, Namespace: blobBytes(name),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	resp, err := s.ListNamespaces(ctx, &persistence.InternalListNamespacesRequest{PageSize: 100})
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	if len(resp.Namespaces) != 3 {
		t.Fatalf("want 3 namespaces; got %d", len(resp.Namespaces))
	}
	// List order is by ID ascending, so "a-id", "b-id", "c-id".
	wantOrder := []string{"a", "b", "c"}
	for i, ns := range resp.Namespaces {
		if string(ns.Namespace.Data) != wantOrder[i] {
			t.Errorf("pos %d: got %q, want %q", i, string(ns.Namespace.Data), wantOrder[i])
		}
	}
}
