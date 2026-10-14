// Package conformance exposes a reusable test suite that any
// MongoDB-backed fixture can drive through RunAll(t, fixture). The
// suite exercises every persistence store's public surface, the
// architectural invariants (shard-id leading predicate, embedded-array
// CAS, blob opacity), the error-classification matrix, and the
// visibility surface in both aggregation and atlas-search modes.
//
// Fixtures come from drivers:
//   - `driver_mongo_test.go` builds a testcontainer replica set,
//     iterated over MONGO_VERSION={6.0,7.0,8.0};
//   - `driver_atlas_test.go` connects to a live Atlas cluster via
//     ATLAS_URI / ATLAS_DATABASE when set.
//
// The design goal: drop a new backend in by implementing Fixture
// and the whole suite runs against it. Every store-level scenario
// is phrased as t.Run so a single regression surfaces as a
// named failure rather than a truncated output blob.
package conformance

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
)

// Capabilities advertises what the backend supports. Drivers flip
// these flags; suite scenarios skip when the fixture can't host them.
// This lets one suite cover container-RS, Atlas, and (eventually)
// sharded-cluster backends without branching per-driver.
type Capabilities struct {
	// AtlasSearch: the backend is an Atlas cluster with $search
	// available; enables the atlas-search-mode visibility suite.
	AtlasSearch bool

	// Transactions: the backend supports multi-document transactions.
	// Required for the execution-mutation subset (create/update/
	// conflict-resolve). Replica sets and mongos-fronted shards have
	// this; standalone mongod does not.
	Transactions bool

	// Sharded: the backend is a sharded cluster (mongos). Currently
	// reserved for future use — the static shard-key invariants
	// (docs/operator/sharding.md) are enforced in unit tests.
	Sharded bool

	// TTLSweeps controls whether we include scenarios that wait out
	// MongoDB's ~60s TTL sweep cycle. Defaults false because the
	// sweep is too slow for every CI run; scheduled canaries flip it.
	TTLSweeps bool
}

// Fixture is the backend abstraction. A driver instantiates one
// fixture per top-level test; the suite calls Capabilities() once
// and DatabaseForSubtest(t) on every t.Run to get an isolated
// database handle (so concurrent scenarios do not collide).
type Fixture interface {
	// Capabilities reports the backend's feature flags.
	Capabilities() Capabilities

	// DatabaseForSubtest returns a per-test mongo.Database. The
	// suite expects this to be fresh (no pre-seeded collections)
	// or at least isolated by name so parallel t.Run calls don't
	// stomp on each other. Driver is responsible for cleanup via
	// t.Cleanup.
	DatabaseForSubtest(t *testing.T) *mongo.Database

	// AtlasSearchIndexName returns the name of the Atlas Search
	// index the visibility store should bind to. Ignored when
	// Capabilities.AtlasSearch is false.
	AtlasSearchIndexName() string

	// EnsureAtlasSearchIndex creates the Atlas Search index in the
	// given database's visibility_executions collection and blocks
	// until it reports READY. Ignored when AtlasSearch is false.
	EnsureAtlasSearchIndex(t *testing.T, db *mongo.Database)
}

// TestContext carries the wiring every suite entry point needs.
// Built by RunAll and passed to each per-store entry point.
type TestContext struct {
	Ctx          context.Context
	DB           *mongo.Database
	Logger       log.Logger
	Metrics      metrics.Handler
	Serializer   serialization.Serializer
	Capabilities Capabilities
	Fixture      Fixture
}

// newTestContext assembles a TestContext for a subtest. The logger
// and metrics handler are Temporal's test defaults so drivers don't
// need to supply them.
func newTestContext(t *testing.T, f Fixture) TestContext {
	t.Helper()
	return TestContext{
		Ctx:          context.Background(),
		DB:           f.DatabaseForSubtest(t),
		Logger:       log.NewTestLogger(),
		Metrics:      metrics.NoopMetricsHandler,
		Serializer:   serialization.NewSerializer(),
		Capabilities: f.Capabilities(),
		Fixture:      f,
	}
}
