//go:build integration

package metadatastore_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metadatastore"
)

func metadataVersion(t *testing.T, s *metadatastore.Stub) int64 {
	t.Helper()
	md, err := s.GetMetadata(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return md.NotificationVersion
}

func TestUpdateNamespace_StaleNotificationVersion_ConditionFailed(t *testing.T) {
	db := startDB(t)
	s := metadatastore.New(db)
	ctx := context.Background()
	if _, err := s.CreateNamespace(ctx, &persistence.InternalCreateNamespaceRequest{ID: "id-1", Name: "ns-1", Namespace: blobBytes("v1")}); err != nil {
		t.Fatal(err)
	}
	stale := metadataVersion(t, s) - 1
	err := s.UpdateNamespace(ctx, &persistence.InternalUpdateNamespaceRequest{
		Id: "id-1", Name: "ns-1", Namespace: blobBytes("v2"), NotificationVersion: stale,
	})
	var cf *persistence.ConditionFailedError
	if !errors.As(err, &cf) {
		t.Fatalf("want ConditionFailedError; got %T %v", err, err)
	}
	got, err := s.GetNamespace(ctx, &persistence.GetNamespaceRequest{ID: "id-1"})
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Namespace.Data) != "v1" {
		t.Fatal("stale update was written")
	}
}

// TestUpdateNamespace_Concurrent_ExactlyOneWins: the
// upstream metadata suite races updates that all carry the same
// notification version and expects exactly one to succeed.
func TestUpdateNamespace_Concurrent_ExactlyOneWins(t *testing.T) {
	db := startDB(t)
	s := metadatastore.New(db)
	ctx := context.Background()
	if _, err := s.CreateNamespace(ctx, &persistence.InternalCreateNamespaceRequest{ID: "id-c", Name: "ns-c", Namespace: blobBytes("v0")}); err != nil {
		t.Fatal(err)
	}
	version := metadataVersion(t, s)
	var wins int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := s.UpdateNamespace(ctx, &persistence.InternalUpdateNamespaceRequest{
				Id: "id-c", Name: "ns-c", Namespace: blobBytes(fmt.Sprintf("v%d", i+1)), NotificationVersion: version,
			})
			if err == nil {
				atomic.AddInt32(&wins, 1)
			}
		}(i)
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("%d concurrent updates with the same notification version succeeded; want 1", wins)
	}
}
