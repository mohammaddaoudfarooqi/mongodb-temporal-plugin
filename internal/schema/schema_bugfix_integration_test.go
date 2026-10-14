//go:build integration

// Schema tooling regression tests.
package schema_test

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/schema"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil/conformance"
)

func versionedRoot() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..")
}

func loadVersion(t *testing.T, version string) (*schema.Collections, *schema.Indexes) {
	t.Helper()
	dir, err := schema.VersionedDir(versionedRoot(), version)
	if err != nil {
		t.Fatal(err)
	}
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

func loadLatest(t *testing.T) (string, *schema.Collections, *schema.Indexes) {
	t.Helper()
	v, err := schema.LatestVersionedDir(versionedRoot())
	if err != nil {
		t.Fatal(err)
	}
	c, i := loadVersion(t, v)
	return v, c, i
}

func indexSpecs(t *testing.T, coll *mongo.Collection) map[string]bson.Raw {
	t.Helper()
	cur, err := coll.Indexes().List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var specs []bson.Raw
	if err := cur.All(context.Background(), &specs); err != nil {
		t.Fatal(err)
	}
	out := map[string]bson.Raw{}
	for _, s := range specs {
		out[s.Lookup("name").StringValue()] = s
	}
	return out
}

// TestManifest_TasksTTL_PartialFilterIsDate/080: the
// TTL index only expires documents its partial filter matches, and
// expiry_time is a BSON date.
func TestManifest_TasksTTL_PartialFilterIsDate(t *testing.T) {
	db := startDB(t)
	_, colls, idx := loadLatest(t)
	if _, err := schema.ApplyCollections(context.Background(), db, colls); err != nil {
		t.Fatal(err)
	}
	if _, err := schema.ApplyIndexes(context.Background(), db, idx); err != nil {
		t.Fatal(err)
	}
	spec, ok := indexSpecs(t, db.Collection("tasks"))["tasks_expiry_ttl"]
	if !ok {
		t.Fatal("tasks_expiry_ttl missing")
	}
	bound, err := spec.LookupErr("partialFilterExpression", "expiry_time", "$gt")
	if err != nil {
		t.Fatalf("partial filter: %v", err)
	}
	if bound.Type != bson.TypeDateTime {
		t.Fatalf("TTL partial filter compares expiry_time with %s; want a date", bound.Type)
	}
}

// TestApplyIndexes_PartialFilterDiff_Mismatch: a
// partial filter that differs from the manifest is reported, not
// silently treated as matching.
func TestApplyIndexes_PartialFilterDiff_Mismatch(t *testing.T) {
	db := startDB(t)
	_, v10 := loadVersion(t, "v1.0")
	if _, err := schema.ApplyIndexes(context.Background(), db, v10); err != nil {
		t.Fatal(err)
	}
	_, _, latest := loadLatest(t)
	res, err := schema.ApplyIndexes(context.Background(), db, latest)
	if err == nil || res == nil || !contains(res.IndexesMismatched, "tasks.tasks_expiry_ttl") {
		t.Fatalf("partial-filter change not detected: err=%v res=%+v", err, res)
	}
}

func TestUpgrade_ReconcilesChangedIndexAndAddsNew(t *testing.T) {
	db := startDB(t)
	ctx := context.Background()
	c10, i10 := loadVersion(t, "v1.0")
	if _, err := schema.ApplyCollections(ctx, db, c10); err != nil {
		t.Fatal(err)
	}
	if _, err := schema.ApplyIndexes(ctx, db, i10); err != nil {
		t.Fatal(err)
	}
	if err := schema.WriteInitialMetadata(ctx, db, c10.SchemaVersion, 4); err != nil {
		t.Fatal(err)
	}
	_, colls, idx := loadLatest(t)
	if _, err := schema.Upgrade(ctx, db, colls, idx); err != nil {
		t.Fatalf("Upgrade: %v", err)
	}
	meta, err := schema.ReadMetadata(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if meta.SchemaVersion != idx.SchemaVersion || meta.State != string(schema.StateReady) {
		t.Fatalf("metadata after upgrade: %+v", meta)
	}
	bound, err := indexSpecs(t, db.Collection("tasks"))["tasks_expiry_ttl"].LookupErr("partialFilterExpression", "expiry_time", "$gt")
	if err != nil || bound.Type != bson.TypeDateTime {
		t.Fatalf("TTL index not reconciled: %v %v", bound.Type, err)
	}
	if res, err := schema.ApplyIndexes(ctx, db, idx); err != nil || len(res.IndexesMismatched) > 0 {
		t.Fatalf("upgraded cluster does not match manifest: %v %+v", err, res)
	}
}

func TestUpgrade_Downgrade_Rejected(t *testing.T) {
	db := startDB(t)
	ctx := context.Background()
	_, colls, idx := loadLatest(t)
	if _, err := schema.ApplyCollections(ctx, db, colls); err != nil {
		t.Fatal(err)
	}
	if _, err := schema.ApplyIndexes(ctx, db, idx); err != nil {
		t.Fatal(err)
	}
	if err := schema.WriteInitialMetadata(ctx, db, idx.SchemaVersion, 4); err != nil {
		t.Fatal(err)
	}
	c10, i10 := loadVersion(t, "v1.0")
	if _, err := schema.Upgrade(ctx, db, c10, i10); err == nil || !strings.Contains(err.Error(), "downgrade") {
		t.Fatalf("downgrade not rejected: %v", err)
	}
	meta, _ := schema.ReadMetadata(ctx, db)
	if meta.SchemaVersion != idx.SchemaVersion || meta.State != string(schema.StateReady) {
		t.Fatalf("downgrade attempt changed metadata: %+v", meta)
	}
}

// TestManifest_CoversStoreIndexes: every index a store
// relies on (Ensure*Indexes) is declared by the latest manifest, so a
// cluster provisioned only by the schema tool serves the same queries.
func TestManifest_CoversStoreIndexes(t *testing.T) {
	ctx := context.Background()
	stores := startDB(t)
	if err := conformance.EnsureAllIndexes(ctx, stores); err != nil {
		t.Fatal(err)
	}
	tool := stores.Client().Database("manifest_only")
	_, colls, idx := loadLatest(t)
	if _, err := schema.ApplyCollections(ctx, tool, colls); err != nil {
		t.Fatal(err)
	}
	if _, err := schema.ApplyIndexes(ctx, tool, idx); err != nil {
		t.Fatal(err)
	}
	names, err := stores.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		t.Fatal(err)
	}
	var missing []string
	for _, coll := range names {
		declared := map[string]bool{}
		for _, spec := range indexSpecs(t, tool.Collection(coll)) {
			declared[spec.Lookup("key").String()] = true
		}
		for name, spec := range indexSpecs(t, stores.Collection(coll)) {
			if name == "_id_" {
				continue
			}
			if !declared[spec.Lookup("key").String()] {
				missing = append(missing, fmt.Sprintf("%s.%s %s", coll, name, spec.Lookup("key")))
			}
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("store indexes absent from the latest manifest:\n%s", strings.Join(missing, "\n"))
	}
}
