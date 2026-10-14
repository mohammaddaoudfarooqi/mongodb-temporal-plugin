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
