package schema_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/schema"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..")
}

// TestLoadCollections_V1 parses schema/versioned/v1.0/collections.json
// and validates the committed catalogue.
func TestLoadCollections_V1(t *testing.T) {
	dir, err := schema.VersionedDir(repoRoot(t), "v1.0")
	if err != nil {
		t.Fatal(err)
	}
	c, err := schema.LoadCollections(filepath.Join(dir, "collections.json"))
	if err != nil {
		t.Fatalf("LoadCollections: %v", err)
	}
	if c.SchemaVersion != "1.0.0" {
		t.Errorf("schema_version: got %q want 1.0.0", c.SchemaVersion)
	}

	// Every store package the plugin has must have a collection
	// declared here. Catches drift between code and schema.
	required := []string{
		"shards",
		"namespaces", "namespace_metadata",
		"cluster_metadata", "cluster_membership",
		"executions", "current_executions",
		"history_nodes", "history_trees",
		"history_tasks", "replication_dlq",
		"task_queues", "tasks", "task_queue_user_data", "build_id_to_task_queue",
		"nexus_endpoints", "nexus_endpoints_table",
		"legacy_queues", "legacy_queue_messages",
		"legacy_dlq_metadata", "legacy_dlq_messages",
		"v2_queues", "v2_messages",
		"visibility_executions", "visibility_search_attributes",
	}
	have := map[string]bool{}
	for _, e := range c.Collections {
		have[e.Name] = true
	}
	for _, name := range required {
		if !have[name] {
			t.Errorf("collections.json missing required collection %q", name)
		}
	}
}

// TestLoadIndexes_V1 parses + validates indexes.json.
func TestLoadIndexes_V1(t *testing.T) {
	dir, err := schema.VersionedDir(repoRoot(t), "v1.0")
	if err != nil {
		t.Fatal(err)
	}
	idx, err := schema.LoadIndexes(filepath.Join(dir, "indexes.json"))
	if err != nil {
		t.Fatalf("LoadIndexes: %v", err)
	}
	if idx.SchemaVersion != "1.0.0" {
		t.Errorf("schema_version: got %q", idx.SchemaVersion)
	}

	// Every code-declared index name must appear in the manifest.
	// Extracted from EnsureIndexes calls across the store packages.
	required := []string{
		"shards_shard_id_unique",
		"namespaces_name_unique",
		"membership_record_expiry_ttl",
		"membership_role_heartbeat",
		"executions_shard_ns_wf_run",
		"history_nodes_tree_branch_node",
		"history_tasks_shard_cat_fire_task",
		"replication_dlq_shard_src_task",
		"tasks_ns_tq_type_sub_id",
		"tasks_expiry_ttl",
		"build_id_ns_bid",
		"legacy_queue_type_mid",
		"legacy_dlq_type_mid",
		"v2_messages_type_name_mid",
		"visibility_ns_id",
		"visibility_ns_starttime",
		"visibility_ns_closetime",
		"visibility_ns_status_start",
	}
	have := map[string]bool{}
	for _, e := range idx.Indexes {
		have[e.Name] = true
	}
	for _, name := range required {
		if !have[name] {
			t.Errorf("indexes.json missing required index %q", name)
		}
	}
}

// TestValidateIndex_RejectsBadKeys guards against definition regressions.
func TestValidateIndex_RejectsBadKeys(t *testing.T) {
	bad := schema.Index{
		Collection: "c", Name: "n",
		Keys: []schema.IndexKey{{Field: "x", Order: 2}},
	}
	if err := bad.Validate(); err == nil {
		t.Error("expected validation error for order=2")
	}
	bad = schema.Index{Collection: "c", Name: "n"}
	if err := bad.Validate(); err == nil {
		t.Error("expected validation error for empty keys")
	}
}

// TestListVersionedDirs_AtLeastOne asserts the versioned directory
// catalogue is non-empty (every release must ship at least v1.0).
func TestListVersionedDirs_AtLeastOne(t *testing.T) {
	dirs, err := schema.ListVersionedDirs(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) == 0 {
		t.Fatal("no versioned schema directories found")
	}
	found := false
	for _, d := range dirs {
		if d == "v1.0" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected v1.0 to exist; got %v", dirs)
	}
}

// TestListVersionedDirs_SemverOrder: v1.10 sorts after
// v1.2, and LatestVersionedDir picks the highest version.
func TestListVersionedDirs_SemverOrder(t *testing.T) {
	root := t.TempDir()
	for _, v := range []string{"v1.10", "v1.2", "v1.0", "notaversion"} {
		if err := os.MkdirAll(filepath.Join(root, "schema", "versioned", v), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	dirs, err := schema.ListVersionedDirs(root)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(dirs) != "[v1.0 v1.2 v1.10]" {
		t.Fatalf("ListVersionedDirs = %v", dirs)
	}
	latest, err := schema.LatestVersionedDir(root)
	if err != nil || latest != "v1.10" {
		t.Fatalf("LatestVersionedDir = %q, %v", latest, err)
	}
}

func TestCompareVersions(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"1.0.0", "1.1.0", -1}, {"1.10.0", "1.2.0", 1}, {"1.1.0", "1.1.0", 0}, {"v1.1", "1.1.0", 0},
	}
	for _, c := range cases {
		got, err := schema.CompareVersions(c.a, c.b)
		if err != nil || got != c.want {
			t.Errorf("CompareVersions(%q, %q) = %d, %v; want %d", c.a, c.b, got, err, c.want)
		}
	}
	if _, err := schema.CompareVersions("one", "1.0.0"); err == nil {
		t.Error("non-numeric version accepted")
	}
}

func latestManifests(t *testing.T) (*schema.Collections, *schema.Indexes) {
	t.Helper()
	latest, err := schema.LatestVersionedDir(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	dir, err := schema.VersionedDir(repoRoot(t), latest)
	if err != nil {
		t.Fatal(err)
	}
	c, err := schema.LoadCollections(filepath.Join(dir, "collections.json"))
	if err != nil {
		t.Fatal(err)
	}
	idx, err := schema.LoadIndexes(filepath.Join(dir, "indexes.json"))
	if err != nil {
		t.Fatal(err)
	}
	return c, idx
}

func names(c *schema.Collections) map[string]bool {
	m := map[string]bool{}
	for _, coll := range c.Collections {
		m[coll.Name] = true
	}
	return m
}

// TestCollections_ForStores_SplitsCoreAndVisibility: a database that
// holds one store gets only that store's collections, so the
// two-database layout has no empty copies; "all" (one database) gets
// every collection.
func TestCollections_ForStores_SplitsCoreAndVisibility(t *testing.T) {
	c, _ := latestManifests(t)
	vis := names(c.ForStores(schema.StoresVisibility))
	if len(vis) != 2 || !vis["visibility_executions"] || !vis["visibility_search_attributes"] {
		t.Errorf("visibility collections = %v", vis)
	}
	core := names(c.ForStores(schema.StoresCore))
	if core["visibility_executions"] || core["visibility_search_attributes"] || !core["executions"] || !core["shards"] {
		t.Errorf("core collections = %v", core)
	}
	if len(core)+len(vis) != len(c.Collections) {
		t.Errorf("core %d + visibility %d != %d collections", len(core), len(vis), len(c.Collections))
	}
	if all := c.ForStores(schema.StoresAll); len(all.Collections) != len(c.Collections) {
		t.Errorf("all = %d collections, want %d", len(all.Collections), len(c.Collections))
	}
}

// TestIndexes_ForCollections_KeepsOnlyThoseCollections: indexes follow
// their collection into the store's database.
func TestIndexes_ForCollections_KeepsOnlyThoseCollections(t *testing.T) {
	c, idx := latestManifests(t)
	vis := c.ForStores(schema.StoresVisibility)
	got := idx.ForCollections(vis)
	if len(got.Indexes) == 0 {
		t.Fatal("no visibility indexes")
	}
	want := names(vis)
	for _, i := range got.Indexes {
		if !want[i.Collection] {
			t.Errorf("index %s on %s kept for the visibility database", i.Name, i.Collection)
		}
	}
	core := idx.ForCollections(c.ForStores(schema.StoresCore))
	if len(core.Indexes)+len(got.Indexes) != len(idx.Indexes) {
		t.Errorf("core %d + visibility %d != %d indexes", len(core.Indexes), len(got.Indexes), len(idx.Indexes))
	}
}

func TestParseStores(t *testing.T) {
	for in, want := range map[string]schema.Stores{"": schema.StoresAll, "all": schema.StoresAll, "core": schema.StoresCore, "visibility": schema.StoresVisibility} {
		if got, err := schema.ParseStores(in); err != nil || got != want {
			t.Errorf("ParseStores(%q) = %v, %v", in, got, err)
		}
	}
	if _, err := schema.ParseStores("both"); err == nil {
		t.Error("ParseStores accepted an unknown value")
	}
}

// TestManifests_EveryCollectionHasAKnownStore covers every version, so
// --stores works with --schema-version too.
func TestManifests_EveryCollectionHasAKnownStore(t *testing.T) {
	dirs, err := schema.ListVersionedDirs(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range dirs {
		c, err := schema.LoadCollections(filepath.Join(repoRoot(t), "schema", "versioned", d, "collections.json"))
		if err != nil {
			t.Fatal(err)
		}
		for _, coll := range c.Collections {
			want := "core"
			if coll.Name == "visibility_executions" || coll.Name == "visibility_search_attributes" {
				want = "visibility"
			}
			if coll.Store != want {
				t.Errorf("%s: %s has store %q, want %q", d, coll.Name, coll.Store, want)
			}
		}
	}
}

// TestLoadCollections_UnknownStore_Fails: a collection without a known
// store would silently drop out of a filtered setup.
func TestLoadCollections_UnknownStore_Fails(t *testing.T) {
	for _, body := range []string{
		`{"schema_version":"9.9.9","collections":[{"name":"x"}]}`,
		`{"schema_version":"9.9.9","collections":[{"name":"x","store":"search"}]}`,
	} {
		p := filepath.Join(t.TempDir(), "collections.json")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := schema.LoadCollections(p); err == nil {
			t.Errorf("LoadCollections accepted %s", body)
		}
	}
}

// TestOtherStoreData: a single-store setup refuses a database whose
// other store's collections hold documents (a combined database), but
// not the empty copies an older two-database setup left behind.
func TestOtherStoreData(t *testing.T) {
	c, _ := latestManifests(t)
	combined := map[string]int64{"executions": 10, "visibility_executions": 7}
	if got := schema.OtherStoreData(c, schema.StoresVisibility, combined); len(got) != 1 || got[0] != "executions" {
		t.Errorf("visibility on a combined database: %v", got)
	}
	if got := schema.OtherStoreData(c, schema.StoresCore, combined); len(got) != 1 || got[0] != "visibility_executions" {
		t.Errorf("core on a combined database: %v", got)
	}
	legacy := map[string]int64{"executions": 0, "shards": 0, "visibility_executions": 7}
	if got := schema.OtherStoreData(c, schema.StoresVisibility, legacy); len(got) != 0 {
		t.Errorf("visibility on an old visibility database: %v", got)
	}
	if got := schema.OtherStoreData(c, schema.StoresAll, combined); len(got) != 0 {
		t.Errorf("all: %v", got)
	}
}
