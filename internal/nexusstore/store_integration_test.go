//go:build integration

// Integration tests for NexusEndpointStore.
package nexusstore_test

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

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/nexusstore"
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
		URI: uri, Database: "temporal_nexus_test", AppName: "test",
	})
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close(context.Background()) })
	db := provider.Database()
	if err := testutil.WaitForPrimary(ctx, db); err != nil {
		t.Fatal(err)
	}
	return db
}

func blob(s string) *commonpb.DataBlob {
	return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte(s)}
}

// TestCreateOrUpdate_FreshEndpoint covers the fresh-endpoint path.
func TestCreateOrUpdate_FreshEndpoint(t *testing.T) {
	db := startDB(t)
	s := nexusstore.New(db)
	ctx := context.Background()

	err := s.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: 0,
		Endpoint: persistence.InternalNexusEndpoint{
			ID:      "ep-1",
			Version: 0,
			Data:    blob("endpoint-data-v1"),
		},
	})
	if err != nil {
		t.Fatalf("CreateOrUpdate fresh: %v", err)
	}

	got, err := s.GetNexusEndpoint(ctx, &persistence.GetNexusEndpointRequest{ID: "ep-1"})
	if err != nil {
		t.Fatalf("GetNexusEndpoint: %v", err)
	}
	if got.ID != "ep-1" {
		t.Errorf("id mismatch: %q", got.ID)
	}
	if string(got.Data.Data) != "endpoint-data-v1" {
		t.Errorf("data bytes mismatch: %q", string(got.Data.Data))
	}
}

// TestCreateOrUpdate_VersionCAS: update with
// wrong endpoint version fails.
func TestCreateOrUpdate_VersionCAS(t *testing.T) {
	db := startDB(t)
	s := nexusstore.New(db)
	ctx := context.Background()

	_ = s.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: 0,
		Endpoint: persistence.InternalNexusEndpoint{
			ID: "ep", Version: 0, Data: blob("v1"),
		},
	})
	// Stored endpoint version is now 1. An update claiming Version=99 fails.
	err := s.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: 1,
		Endpoint: persistence.InternalNexusEndpoint{
			ID: "ep", Version: 99, Data: blob("bogus"),
		},
	})
	if err == nil {
		t.Fatal("expected ConditionFailedError on stale endpoint version")
	}
	var cfe *persistence.ConditionFailedError
	if !errors.As(err, &cfe) {
		t.Fatalf("expected ConditionFailedError; got %T: %v", err, err)
	}
}

func TestDeleteNexusEndpoint_IncrementsTableVersion(t *testing.T) {
	db := startDB(t)
	s := nexusstore.New(db)
	ctx := context.Background()

	_ = s.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: 0,
		Endpoint:              persistence.InternalNexusEndpoint{ID: "ep-del", Version: 0, Data: blob("d")},
	})
	before, err := s.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{PageSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	err = s.DeleteNexusEndpoint(ctx, &persistence.DeleteNexusEndpointRequest{
		ID:                    "ep-del",
		LastKnownTableVersion: before.TableVersion,
	})
	if err != nil {
		t.Fatalf("DeleteNexusEndpoint: %v", err)
	}
	after, err := s.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{PageSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	if after.TableVersion <= before.TableVersion {
		t.Errorf("table version did not advance on delete: %d → %d", before.TableVersion, after.TableVersion)
	}
	if len(after.Endpoints) != 0 {
		t.Errorf("endpoint not deleted; remaining: %d", len(after.Endpoints))
	}
}

func TestGetNexusEndpoint_NotFound(t *testing.T) {
	db := startDB(t)
	s := nexusstore.New(db)
	_, err := s.GetNexusEndpoint(context.Background(), &persistence.GetNexusEndpointRequest{ID: "missing"})
	var nf *serviceerror.NotFound
	if !errors.As(err, &nf) {
		t.Fatalf("expected NotFound; got %T: %v", err, err)
	}
}

func TestListNexusEndpoints_Paginated(t *testing.T) {
	db := startDB(t)
	s := nexusstore.New(db)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		id := string(rune('a' + i))
		tableVersion := int64(i)
		err := s.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: tableVersion,
			Endpoint: persistence.InternalNexusEndpoint{
				ID: id, Version: 0, Data: blob(id),
			},
		})
		if err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	resp, err := s.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{PageSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Endpoints) != 3 {
		t.Errorf("page 1 size: got %d want 3", len(resp.Endpoints))
	}
	if len(resp.NextPageToken) == 0 {
		t.Fatal("expected NextPageToken")
	}
	p2, err := s.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{
		PageSize: 3, NextPageToken: resp.NextPageToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.Endpoints) != 2 {
		t.Errorf("page 2 size: got %d want 2", len(p2.Endpoints))
	}
}
