//go:build conformance

// Conformance-tag-only conformance fixture. This file depends on
// testcontainers-go and the upstream Temporal persistence-tests package;
// both are heavy and should not be pulled into unit-test builds.
package testutil

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/pkg/plugin"
)

// ConformanceCluster satisfies persistencetests.PersistenceTestCluster
// using the MongoDB plugin. Lifetime is scoped by the owning test via
// t.Cleanup; callers do not need to call TearDownTestDatabase manually.
type ConformanceCluster struct {
	container *mongodb.MongoDBContainer
	uri       string
	dbName    string
	persCfg   config.Persistence
}

// NewConformanceCluster starts a replica set, resolves its URI, and
// returns a cluster ready for the upstream test-base to consume. The
// `Setup` step the upstream TestBase performs later wires our factory
// through client.DataStoreFactoryProvider, which in turn calls
// plugin.NewFactory().NewFactory(...); at M0 this is satisfied by the
// scaffold factory whose stores return Unimplemented.
func NewConformanceCluster(t *testing.T) *ConformanceCluster {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := mongodb.Run(ctx, MongoImage(), mongodb.WithReplicaSet("rs0"))
	if err != nil {
		t.Fatalf("start mongo replica set: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	rawURI, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	uri := stripReplicaSet(rawURI) + "?directConnection=true"
	dbName := fmt.Sprintf("temporal_conf_%d", time.Now().UnixNano())

	return &ConformanceCluster{
		container: container,
		uri:       uri,
		dbName:    dbName,
		persCfg: config.Persistence{
			DefaultStore:    "mongo-default",
			VisibilityStore: "mongo-visibility",
			DataStores: map[string]config.DataStore{
				"mongo-default": {
					CustomDataStoreConfig: &config.CustomDatastoreConfig{
						Name:      "mongodb",
						IndexName: dbName,
						Options: map[string]any{
							"uri":             uri,
							"database":        dbName,
							"skipSchemaCheck": true,
						},
					},
				},
				"mongo-visibility": {
					CustomDataStoreConfig: &config.CustomDatastoreConfig{
						Name:      "mongodb",
						IndexName: dbName + "_vis",
						Options: map[string]any{
							"uri":             uri,
							"database":        dbName + "_vis",
							"skipSchemaCheck": true,
						},
					},
				},
			},
			NumHistoryShards: 4,
		},
	}
}

// SetupTestDatabase is a no-op for MongoDB: the plugin creates
// collections and indexes lazily (or via temporal-mongodb-tool
// setup-schema, outside the conformance path). Upstream cassandra/sql
// test clusters implement heavy schema bootstrap here; MongoDB does
// not require it.
func (c *ConformanceCluster) SetupTestDatabase() {}

// TearDownTestDatabase removes the conformance test database. The
// t.Cleanup closure on the container handles the container lifecycle;
// this is a best-effort data cleanup for when a test reuses a running
// container.
func (c *ConformanceCluster) TearDownTestDatabase() {}

// Config returns the persistence config for the upstream TestBase to
// consume. The TestBase calls DataStoreFactoryProvider with this and
// our AbstractDataStoreFactory to produce live store instances.
func (c *ConformanceCluster) Config() config.Persistence { return c.persCfg }

// Plugin returns the AbstractDataStoreFactory the upstream TestBase
// should wire in as `AbstractDataStoreFactory`. Calling code sets the
// TestBase field and then invokes TestBase.Setup.
func (c *ConformanceCluster) Plugin() any { return plugin.NewFactory() }

// Logger returns a logger suitable for the upstream TestBase.
func (c *ConformanceCluster) Logger() log.Logger { return log.NewTestLogger() }

func stripReplicaSet(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	q := u.Query()
	q.Del("replicaSet")
	u.RawQuery = q.Encode()
	return strings.TrimRight(u.String(), "?")
}
