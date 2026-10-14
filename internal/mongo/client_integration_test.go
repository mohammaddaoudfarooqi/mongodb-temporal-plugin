//go:build integration

// Integration tests for the MongoDB client provider. They spin up a
// single-node replica set via testcontainers-go and exercise the real
// driver path. Runs only under `make integration`.
package mongo_test

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil"
)

// stripReplicaSetQuery removes the ?replicaSet=... param from a Mongo URI.
// The testcontainers module appends it automatically, but we want the
// driver to connect directly to the mapped host:port rather than trying
// to reach the member by its container-internal IP.
func stripReplicaSetQuery(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	q := u.Query()
	q.Del("replicaSet")
	u.RawQuery = q.Encode()
	// Strip trailing / for a cleaner base.
	return strings.TrimRight(u.String(), "?")
}

// startReplicaSet brings up a single-node replica set (image selected
// by MONGO_VERSION, default mongo:6) and returns its connection URI
// plus a cleanup.
func startReplicaSet(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := mongodb.Run(ctx, testutil.MongoImage(),
		mongodb.WithReplicaSet("rs0"),
	)
	if err != nil {
		t.Fatalf("start mongo replica set: %v", err)
	}
	t.Cleanup(func() {
		_ = testcontainers.TerminateContainer(container)
	})

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	// The module's URI appends `?replicaSet=rs0`; combined with the
	// testcontainers-configured member host (the container's internal
	// IP) the driver times out trying to dial the member directly.
	// directConnection=true tells the driver to skip SDAM and treat
	// the configured host as the primary, which is what our host-side
	// tests need. Strip the replicaSet param first.
	return stripReplicaSetQuery(uri) + "?directConnection=true"
}

// TestClientProvider_ConnectToReplicaSet_Succeeds happy
// path warmup. A replica set connection succeeds and the
// topology validator passes.
func TestClientProvider_ConnectToReplicaSet_Succeeds(t *testing.T) {
	uri := startReplicaSet(t)
	cfg := internalmongo.ClientConfig{
		URI:      uri,
		Database: "temporal",
		AppName:  "temporal-mongodb-plugin/test",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p, err := internalmongo.NewClientProvider(ctx, cfg)
	if err != nil {
		t.Fatalf("NewClientProvider: %v", err)
	}
	defer func() {
		if err := p.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	if p.Client() == nil {
		t.Fatal("Client() returned nil on successful provider")
	}
	if p.Database().Name() != "temporal" {
		t.Errorf("Database() name mismatch: %q", p.Database().Name())
	}
}

// TestClientProvider_Close_CompletesInBudget: Close
// returns within 10 seconds under normal conditions.
func TestClientProvider_Close_CompletesInBudget(t *testing.T) {
	uri := startReplicaSet(t)
	cfg := internalmongo.ClientConfig{
		URI:      uri,
		Database: "temporal",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p, err := internalmongo.NewClientProvider(ctx, cfg)
	if err != nil {
		t.Fatalf("NewClientProvider: %v", err)
	}
	start := time.Now()
	if err := p.Close(context.Background()); err != nil {
		t.Errorf("Close: %v", err)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("Close exceeded 10s budget: %v", took)
	}
}

// TestClientProvider_SharedClientAcrossStores: the
// provider's Client() returns the same handle regardless of caller.
// (The full guarantee — every store sharing one driver pool — is
// validated end-to-end by the factory wiring tests.)
func TestClientProvider_SharedClientAcrossStores(t *testing.T) {
	uri := startReplicaSet(t)
	cfg := internalmongo.ClientConfig{
		URI:      uri,
		Database: "temporal",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p, err := internalmongo.NewClientProvider(ctx, cfg)
	if err != nil {
		t.Fatalf("NewClientProvider: %v", err)
	}
	t.Cleanup(func() { _ = p.Close(context.Background()) })

	c1 := p.Client()
	c2 := p.Client()
	if c1 != c2 {
		t.Error("Client() returned different pointers on successive calls")
	}
}
