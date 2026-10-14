//go:build integration

// Live-Atlas driver for the conformance suite. Runs only when
// ATLAS_URI + ATLAS_DATABASE are set; otherwise skips so PRs that
// land without Atlas secrets do not fail locally.
//
// The Atlas driver advertises Capabilities.AtlasSearch = true so the
// visibility atlas-search suite runs. Every other suite uses the same
// code paths as the Mongo driver.
package conformance_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil/conformance"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility"
)

// atlasFixture wraps a live-Atlas connection and hands the suite
// per-subtest isolated databases under the user-supplied prefix.
type atlasFixture struct {
	client       *mongo.Client
	dbPrefix     string
	seq          atomic.Int64
	indexName    string
	indexDefJSON map[string]any
}

func startAtlasFixture(t *testing.T) *atlasFixture {
	t.Helper()
	uri := os.Getenv("ATLAS_URI")
	if uri == "" {
		t.Skip("ATLAS_URI not set — skipping Atlas conformance run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	provider, err := internalmongo.NewClientProvider(ctx, internalmongo.ClientConfig{
		URI:                      uri,
		Database:                 "conformance_base",
		AppName:                  "temporal-mongodb-plugin/conformance-atlas",
		WriteConcern:             "majority",
		ReadConcern:              "majority",
		ReadPreference:           "primary",
		RetryWrites:              true,
		RetryReads:               true,
		ServerSelectionTimeoutMs: 15000,
	})
	if err != nil {
		t.Fatalf("NewClientProvider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close(context.Background()) })

	// Load the committed Atlas Search index definition once; the
	// EnsureAtlasSearchIndex call uses it for every visibility subtest.
	_, thisFile, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "schema", "atlas_search", "visibility.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read Atlas Search manifest: %v", err)
	}
	var m struct {
		Name       string         `json:"name"`
		Definition map[string]any `json:"definition"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("parse Atlas Search manifest: %v", err)
	}

	// Prefix ensures this run's databases are isolated even when a
	// previous test left stragglers on the same cluster.
	prefix := fmt.Sprintf("conf_atlas_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		// Best-effort cleanup: drop every database the fixture created.
		ctx := context.Background()
		names, err := provider.Database().Client().ListDatabaseNames(ctx, bson.M{"name": bson.M{"$regex": "^" + prefix}})
		if err != nil {
			return
		}
		for _, n := range names {
			_ = provider.Database().Client().Database(n).Drop(ctx)
		}
	})

	return &atlasFixture{
		client:       provider.Database().Client(),
		dbPrefix:     prefix,
		indexName:    m.Name,
		indexDefJSON: m.Definition,
	}
}

func (f *atlasFixture) Capabilities() conformance.Capabilities {
	return conformance.Capabilities{
		Transactions: true,
		AtlasSearch:  true,
		Sharded:      false,
	}
}

func (f *atlasFixture) DatabaseForSubtest(t *testing.T) *mongo.Database {
	t.Helper()
	n := f.seq.Add(1)
	dbName := fmt.Sprintf("%s_%s_%d", f.dbPrefix, sanitizeName(t.Name()), n)
	if len(dbName) > 63 { // Atlas db name limit
		dbName = dbName[:63]
	}
	db := f.client.Database(dbName)
	t.Cleanup(func() {
		_ = db.Drop(context.Background())
	})
	return db
}

func (f *atlasFixture) AtlasSearchIndexName() string { return f.indexName }

// EnsureAtlasSearchIndex creates the committed index definition on the
// db's visibility_executions collection and polls until READY.
// Idempotent: if the index already exists (e.g. a prior subtest on the
// same db), it issues updateSearchIndex to refresh the definition.
func (f *atlasFixture) EnsureAtlasSearchIndex(t *testing.T, db *mongo.Database) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Create the collection so $listSearchIndexes has a target.
	_ = db.CreateCollection(ctx, "visibility_executions")

	status, err := visibility.QueryIndexStatus(ctx, db, "visibility_executions", f.indexName)
	if err != nil {
		t.Fatalf("QueryIndexStatus: %v", err)
	}
	if status == visibility.IndexStatusUnknown {
		cmd := bson.D{
			{Key: "createSearchIndexes", Value: "visibility_executions"},
			{Key: "indexes", Value: []bson.M{{
				"name":       f.indexName,
				"definition": f.indexDefJSON,
			}}},
		}
		if err := db.RunCommand(ctx, cmd).Err(); err != nil {
			t.Fatalf("createSearchIndexes: %v", err)
		}
	} else {
		cmd := bson.D{
			{Key: "updateSearchIndex", Value: "visibility_executions"},
			{Key: "name", Value: f.indexName},
			{Key: "definition", Value: f.indexDefJSON},
		}
		if err := db.RunCommand(ctx, cmd).Err(); err != nil {
			t.Logf("updateSearchIndex (continuing): %v", err)
		}
	}

	// Exponential backoff through the shared WaitFor helper
	// so flakiness under cluster load is bounded and the polling
	// cadence is the same here as in the visibility conformance path.
	if err := conformance.WaitFor(ctx, conformance.WaitForOptions{Total: 8 * time.Minute},
		func(ctx context.Context) (bool, error) {
			st, err := visibility.QueryIndexStatus(ctx, db, "visibility_executions", f.indexName)
			return st == visibility.IndexStatusReady, err
		}); err != nil {
		t.Fatalf("index %s never reached READY: %v", f.indexName, err)
	}
}

// TestConformance_Atlas runs the full suite against a live Atlas
// cluster, including the atlas-search visibility mode. Skipped when
// ATLAS_URI is not set.
func TestConformance_Atlas(t *testing.T) {
	if os.Getenv("ATLAS_URI") == "" {
		t.Skip("ATLAS_URI not set — Atlas conformance skipped")
	}
	f := startAtlasFixture(t)
	conformance.RunAll(t, f)
}

// Compile-time guard: the fixture is referenced in driver tests so the
// unused-import warning stays bounded to the live-Atlas build.
var _ = strings.ToLower
