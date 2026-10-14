// Tests that every Atlas Search manifest under schema/atlas_search/
// parses, names a target collection, and declares the system search
// attributes the plugin depends on.
package schema_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/searchattribute/sadefs"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility/converter"
)

type atlasIndexManifest struct {
	Name       string                 `json:"name"`
	Collection string                 `json:"collection"`
	Definition map[string]interface{} `json:"definition"`
}

func schemaDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "schema", "atlas_search")
}

// TestAtlasSearchManifests_Parse ensures every *.json file in
// schema/atlas_search/ is valid JSON with the required top-level
// fields.
func TestAtlasSearchManifests_Parse(t *testing.T) {
	dir := schemaDir(t)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read schema/atlas_search: %v", err)
	}
	count := 0
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		count++
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		var m atlasIndexManifest
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Errorf("%s: parse error: %v", e.Name(), err)
			continue
		}
		if m.Name == "" {
			t.Errorf("%s: missing `name`", e.Name())
		}
		if m.Collection == "" {
			t.Errorf("%s: missing `collection`", e.Name())
		}
		if m.Definition == nil {
			t.Errorf("%s: missing `definition`", e.Name())
		}
	}
	if count == 0 {
		t.Error("no Atlas Search manifests found; at least visibility.json is required")
	}
}

// TestVisibilityManifest_MapsSystemSAs ensures the visibility.json
// manifest declares every system search attribute the plugin queries.
// Catches drift between the visibility doc schema and the Search index.
func TestVisibilityManifest_MapsSystemSAs(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(schemaDir(t), "visibility.json"))
	if err != nil {
		t.Fatalf("read visibility.json: %v", err)
	}
	var m atlasIndexManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	// Drill into definition.mappings.fields.
	mappings, ok := m.Definition["mappings"].(map[string]interface{})
	if !ok {
		t.Fatal("definition.mappings missing or malformed")
	}
	fields, ok := mappings["fields"].(map[string]interface{})
	if !ok {
		t.Fatal("definition.mappings.fields missing")
	}
	required := []string{
		"namespace_id", "run_id", "workflow_id", "workflow_type_name",
		"task_queue", "status",
		"start_time", "execution_time", "close_time",
		"search_attributes",
	}
	for _, f := range required {
		if _, ok := fields[f]; !ok {
			t.Errorf("visibility Atlas Search index missing required field %q", f)
		}
	}
	// search_attributes must be dynamic so custom SAs are indexed
	// without an explicit rebuild; a typeSet makes dynamic strings
	// usable by both equals (token) and wildcard (keyword string).
	sa, ok := fields["search_attributes"].(map[string]interface{})
	if !ok {
		t.Fatal("search_attributes not a document mapping")
	}
	switch dyn := sa["dynamic"].(type) {
	case bool:
		if !dyn {
			t.Error("search_attributes must be dynamic for custom SAs")
		}
	case map[string]interface{}:
		if dyn["typeSet"] == nil {
			t.Error("search_attributes dynamic mapping must name a typeSet")
		}
	default:
		t.Error("search_attributes must be dynamic for custom SAs")
	}
}

// fieldTypes returns the Atlas Search types declared for a mapping entry
// (a single {type: …} object or an array of them).
func fieldTypes(entry any) map[string]map[string]any {
	out := map[string]map[string]any{}
	add := func(m map[string]any) {
		if typ, ok := m["type"].(string); ok {
			out[typ] = m
		}
	}
	switch v := entry.(type) {
	case map[string]any:
		add(v)
	case []any:
		for _, e := range v {
			if m, ok := e.(map[string]any); ok {
				add(m)
			}
		}
	}
	return out
}

// TestAtlasSearchMapping_CoversConverterFields: every
// top-level system attribute the converter queries is mapped with the
// Atlas types its operators need — keyword fields need token (equals,
// IN) and a lucene.keyword string (STARTS_WITH via wildcard), datetimes
// need date, numbers need number — and dynamic custom attributes get the
// same through a typeSet.
func TestAtlasSearchMapping_CoversConverterFields(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(schemaDir(t), "visibility.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m atlasIndexManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	mappings := m.Definition["mappings"].(map[string]any)
	fields := mappings["fields"].(map[string]any)
	requireTypes := func(path string, want ...string) {
		t.Helper()
		got := fieldTypes(fields[path])
		for _, w := range want {
			if _, ok := got[w]; !ok {
				t.Errorf("%s: missing Atlas type %q (have %v)", path, w, got)
			}
		}
		if s, ok := got["string"]; ok && contains(want, "string") && s["analyzer"] != "lucene.keyword" {
			t.Errorf("%s: string mapping must use lucene.keyword for prefix wildcards", path)
		}
	}
	for name, typ := range sadefs.System() {
		path := converter.FieldPath(name)
		// ExecutionStatus is stored as the numeric enum (checked below).
		if strings.HasPrefix(path, "search_attributes.") || path == "status" {
			continue
		}
		switch typ {
		case enumspb.INDEXED_VALUE_TYPE_KEYWORD:
			requireTypes(path, "token", "string")
		case enumspb.INDEXED_VALUE_TYPE_DATETIME:
			requireTypes(path, "date")
		case enumspb.INDEXED_VALUE_TYPE_INT, enumspb.INDEXED_VALUE_TYPE_DOUBLE:
			requireTypes(path, "number")
		}
	}
	requireTypes("status", "number")
	requireTypes("archetype_id", "number")

	sa := fields["search_attributes"].(map[string]any)
	dyn, _ := sa["dynamic"].(map[string]any)
	setName, _ := dyn["typeSet"].(string)
	var set map[string]map[string]any
	for _, ts := range m.Definition["typeSets"].([]any) {
		if tsm := ts.(map[string]any); tsm["name"] == setName {
			set = fieldTypes(tsm["types"])
		}
	}
	for _, w := range []string{"token", "string", "number", "date", "boolean"} {
		if _, ok := set[w]; !ok {
			t.Errorf("search_attributes typeSet %q lacks %q (have %v)", setName, w, set)
		}
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
