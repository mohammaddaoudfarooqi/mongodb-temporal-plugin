//go:build integration

package schema_test

import (
	"context"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/schema"
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
		t.Fatal(err)
	}
	u, _ := url.Parse(raw)
	q := u.Query()
	q.Del("replicaSet")
	u.RawQuery = q.Encode()
	uri := strings.TrimRight(u.String(), "?") + "?directConnection=true"
	p, err := internalmongo.NewClientProvider(ctx, internalmongo.ClientConfig{
		URI: uri, Database: "temporal_schema_test", AppName: "schema-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close(context.Background()) })
	db := p.Database()
	if err := testutil.WaitForPrimary(ctx, db); err != nil {
		t.Fatal(err)
	}
	return db
}

func loadV1(t *testing.T) (*schema.Collections, *schema.Indexes) {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(thisFile), "..", "..", "schema", "versioned", "v1.0")
	c, err := schema.LoadCollections(dir + "/collections.json")
	if err != nil {
		t.Fatal(err)
	}
	i, err := schema.LoadIndexes(dir + "/indexes.json")
	if err != nil {
		t.Fatal(err)
	}
	return c, i
}

// TestApplySchema_FreshCluster: on a
// fresh cluster, every collection + index is created; a second
// identical apply is a no-op.
func TestApplySchema_FreshCluster(t *testing.T) {
	db := startDB(t)
	collections, indexes := loadV1(t)
	ctx := context.Background()

	created1, err := schema.ApplyCollections(ctx, db, collections)
	if err != nil {
		t.Fatal(err)
	}
	if len(created1) != len(collections.Collections) {
		t.Errorf("first run: created %d want %d", len(created1), len(collections.Collections))
	}

	res1, err := schema.ApplyIndexes(ctx, db, indexes)
	if err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if len(res1.IndexesMismatched) != 0 {
		t.Errorf("first apply: %d mismatched (should be 0)", len(res1.IndexesMismatched))
	}

	// Second apply — every index already exists, none created.
	created2, err := schema.ApplyCollections(ctx, db, collections)
	if err != nil {
		t.Fatal(err)
	}
	if len(created2) != 0 {
		t.Errorf("second run: created %d want 0", len(created2))
	}
	res2, err := schema.ApplyIndexes(ctx, db, indexes)
	if err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if len(res2.IndexesCreated) != 0 {
		t.Errorf("second run: %d created (expected 0)", len(res2.IndexesCreated))
	}
}

// TestApplySchema_MismatchedIndex: a pre-existing
// index sharing the manifest's name but with a different definition
// is flagged as Mismatched and surfaces an error pointing the operator
// at update-schema.
func TestApplySchema_MismatchedIndex(t *testing.T) {
	db := startDB(t)
	collections, indexes := loadV1(t)
	ctx := context.Background()

	if _, err := schema.ApplyCollections(ctx, db, collections); err != nil {
		t.Fatal(err)
	}
	// Pre-create a "shards_shard_id_unique" index with a different
	// key shape (range_id instead of shard_id) under the manifest's
	// target name.
	im := mongo.IndexModel{
		Keys:    bson.D{{Key: "range_id", Value: 1}},
		Options: options.Index().SetName("shards_shard_id_unique"),
	}
	if _, err := db.Collection("shards").Indexes().CreateOne(ctx, im); err != nil {
		t.Fatalf("seed wrong index: %v", err)
	}

	res, err := schema.ApplyIndexes(ctx, db, indexes)
	if err == nil {
		t.Fatal("expected mismatch error; got nil")
	}
	if res == nil || len(res.IndexesMismatched) == 0 {
		t.Errorf("expected a mismatched index; got %+v", res)
	}
	if !strings.Contains(err.Error(), "update-schema") {
		t.Errorf("error should point at update-schema: %q", err.Error())
	}
}

// TestWriteInitialMetadata_Idempotent:
// two concurrent setup-schema runs converge on the same singleton
// without creating duplicates.
func TestWriteInitialMetadata_Idempotent(t *testing.T) {
	db := startDB(t)
	ctx := context.Background()

	if err := schema.WriteInitialMetadata(ctx, db, "1.0.0", 4); err != nil {
		t.Fatal(err)
	}
	// Second call with different values — upsert-if-absent should NOT
	// overwrite.
	if err := schema.WriteInitialMetadata(ctx, db, "2.0.0", 99); err != nil {
		t.Fatal(err)
	}
	meta, err := schema.ReadMetadata(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if meta == nil {
		t.Fatal("metadata missing after upserts")
	}
	if meta.SchemaVersion != "1.0.0" {
		t.Errorf("SchemaVersion drift: got %q want 1.0.0", meta.SchemaVersion)
	}
	if meta.NumHistoryShards != 4 {
		t.Errorf("NumHistoryShards drift: got %d want 4", meta.NumHistoryShards)
	}
	if meta.State != string(schema.StateReady) {
		t.Errorf("initial state should be ready; got %q", meta.State)
	}
}

// TestSetState_TransitionsAndReadsBack happy path.
func TestSetState_TransitionsAndReadsBack(t *testing.T) {
	db := startDB(t)
	ctx := context.Background()

	if err := schema.WriteInitialMetadata(ctx, db, "1.0.0", 4); err != nil {
		t.Fatal(err)
	}
	if err := schema.SetState(ctx, db, "upgrading-from-1.0.0-to-1.1.0", "1.0.0"); err != nil {
		t.Fatal(err)
	}
	meta, _ := schema.ReadMetadata(ctx, db)
	if meta.State != "upgrading-from-1.0.0-to-1.1.0" {
		t.Errorf("state: got %q", meta.State)
	}

	if err := schema.SetState(ctx, db, string(schema.StateReady), "1.1.0"); err != nil {
		t.Fatal(err)
	}
	meta, _ = schema.ReadMetadata(ctx, db)
	if meta.State != "ready" || meta.SchemaVersion != "1.1.0" {
		t.Errorf("post-transition: state=%q ver=%q", meta.State, meta.SchemaVersion)
	}
}
