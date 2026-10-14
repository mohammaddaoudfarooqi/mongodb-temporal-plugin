package schema_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/schema"
)

// The sharded-cluster invariants we enforce here are the static parts of
// the shard-id leading-predicate rule and the P1 unique-index rule. Running a full mongos+shard
// topology is an integration cost we defer to scheduled CI; this
// test locks the schema declarations so a well-formed schema never gets
// merged with an index that would block sharding.

type indexDecl struct {
	Collection string `json:"collection"`
	Name       string `json:"name"`
	Keys       []struct {
		Field string `json:"field"`
		Order int    `json:"order"`
	} `json:"keys"`
	Unique bool `json:"unique"`
	TTL    any  `json:"ttl"`
}

type indexesFile struct {
	Indexes []indexDecl `json:"indexes"`
}

// shardKeys captures the recommended shard key per collection (from
// docs/operator/sharding.md). The leading-predicate check uses the
// FIRST field in this slice as the "shard-id prefix" that compound
// indexes serving list/range queries must start with.
//
// Collections not listed here are not sharded-by-default (small/
// singleton or unsharded by policy per ADR-008); their indexes are
// exempt from the leading-predicate check but still pass the P1
// unique-index rule (because they are not sharded).
var shardKeys = map[string]string{
	"executions":            "shard_id",
	"history_nodes":         "tree_id",
	"history_tasks":         "shard_id",
	"replication_dlq":       "shard_id",
	"tasks":                 "namespace_id",
	"visibility_executions": "namespace_id",
	"v2_messages":           "type",
}

// TestShardingInvariant_LeadingPredicate: every compound index on a
// routinely-sharded collection starts with the collection's shard-key
// prefix. This avoids scatter-gather on every list/range query.
func TestShardingInvariant_LeadingPredicate(t *testing.T) {
	file := loadIndexes(t)
	for _, idx := range file.Indexes {
		leading, sharded := shardKeys[idx.Collection]
		if !sharded {
			continue
		}
		if len(idx.Keys) == 0 {
			continue
		}
		// TTL-only indexes are excluded — they operate on the expiry
		// field, not on a query-path predicate; shard key leading is
		// not a correctness requirement for them.
		if idx.TTL != nil {
			continue
		}
		if idx.Keys[0].Field != leading {
			t.Errorf(
				"index %s on %s: leading key %q does not match shard-key prefix %q",
				idx.Name, idx.Collection, idx.Keys[0].Field, leading,
			)
		}
	}
}

// TestShardingInvariant_UniqueIndexIncludesShardKey: MongoDB sharded
// clusters require unique indexes to include the shard key as a prefix
// (P1). For each unique index on a sharded collection, confirm the
// shard-key field appears among its keys.
func TestShardingInvariant_UniqueIndexIncludesShardKey(t *testing.T) {
	file := loadIndexes(t)
	for _, idx := range file.Indexes {
		if !idx.Unique {
			continue
		}
		leading, sharded := shardKeys[idx.Collection]
		if !sharded {
			// Unsharded unique indexes (e.g., shards.shard_id,
			// namespaces.name) are fine — the P1 rule only applies
			// if someone enables sharding on the collection. Those
			// are called out in docs/operator/sharding.md.
			continue
		}
		found := false
		for _, k := range idx.Keys {
			if k.Field == leading {
				found = true
				break
			}
		}
		if !found {
			t.Errorf(
				"unique index %s on %s does not include shard-key %q (P1 blocks sharding-enable)",
				idx.Name, idx.Collection, leading,
			)
		}
	}
}

func loadIndexes(t *testing.T) indexesFile {
	t.Helper()
	schemaDir := findSchemaDir(t)
	latest, err := schema.LatestVersionIn(filepath.Join(schemaDir, "versioned"))
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(schemaDir, "versioned", latest, "indexes.json")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	var f indexesFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parse %s: %v", p, err)
	}
	return f
}

// findSchemaDir walks up from CWD looking for the top-level "schema"
// directory (the one with versioned/ + atlas_search/). Works regardless
// of where `go test` runs from inside the module.
func findSchemaDir(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		candidate := filepath.Join(dir, "schema", "versioned")
		if st, err := os.Stat(candidate); err == nil && st.IsDir() {
			return filepath.Join(dir, "schema")
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("schema/versioned not found from %s", dir)
		}
		dir = parent
	}
}
