// Package schema parses the declarative schema manifests under
// schema/versioned/v<major>.<minor>/ and schema/atlas_search/. The
// temporal-mongodb-tool CLI consumes these to apply collections + B-tree
// indexes and Atlas Search indexes idempotently.
package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Collections is the shape of schema/versioned/v*/collections.json.
type Collections struct {
	SchemaVersion string       `json:"schema_version"`
	Collections   []Collection `json:"collections"`
}

// Collection names one MongoDB collection.
type Collection struct {
	Name string `json:"name"`
}

// Indexes is the shape of schema/versioned/v*/indexes.json.
type Indexes struct {
	SchemaVersion string  `json:"schema_version"`
	Indexes       []Index `json:"indexes"`
}

// Index is the plugin-native declarative shape for a B-tree / TTL /
// partial index. The CLI translates it into a mongo.IndexModel when
// applying.
type Index struct {
	Collection string      `json:"collection"`
	Name       string      `json:"name"`
	Keys       []IndexKey  `json:"keys"`
	Unique     bool        `json:"unique,omitempty"`
	Sparse     bool        `json:"sparse,omitempty"`
	TTL        *TTLOptions `json:"ttl,omitempty"`
	// PartialFilter is MongoDB Extended JSON, so values such as dates can
	// be expressed ({"$date": "…"}) — plain JSON cannot.
	PartialFilter json.RawMessage `json:"partial_filter,omitempty"`
}

// IndexKey is one entry in the sort-order-preserving keys list.
type IndexKey struct {
	Field string `json:"field"`
	// Order is either +1 (ascending) or -1 (descending).
	Order int `json:"order"`
}

// TTLOptions configures a TTL index.
type TTLOptions struct {
	ExpireAfterSeconds int32 `json:"expire_after_seconds"`
}

// LoadCollections parses collections.json at the given path.
func LoadCollections(path string) (*Collections, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("schema: read %s: %w", path, err)
	}
	var c Collections
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("schema: parse %s: %w", path, err)
	}
	if c.SchemaVersion == "" {
		return nil, fmt.Errorf("schema: %s missing schema_version", path)
	}
	return &c, nil
}

// LoadIndexes parses indexes.json at the given path and validates each
// entry.
func LoadIndexes(path string) (*Indexes, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("schema: read %s: %w", path, err)
	}
	var idx Indexes
	if err := json.Unmarshal(raw, &idx); err != nil {
		return nil, fmt.Errorf("schema: parse %s: %w", path, err)
	}
	if idx.SchemaVersion == "" {
		return nil, fmt.Errorf("schema: %s missing schema_version", path)
	}
	for i, entry := range idx.Indexes {
		if err := entry.Validate(); err != nil {
			return nil, fmt.Errorf("schema: indexes[%d] (%s): %w", i, entry.Name, err)
		}
	}
	return &idx, nil
}

// Validate checks an Index for obviously-bad definitions so setup fails
// fast. Field-order only accepts ±1; empty names break createIndexes.
func (i Index) Validate() error {
	if i.Collection == "" {
		return errors.New("missing collection")
	}
	if i.Name == "" {
		return errors.New("missing name")
	}
	if len(i.Keys) == 0 {
		return errors.New("missing keys")
	}
	for j, k := range i.Keys {
		if k.Field == "" {
			return fmt.Errorf("keys[%d] missing field", j)
		}
		if k.Order != 1 && k.Order != -1 {
			return fmt.Errorf("keys[%d] order must be +1 or -1; got %d", j, k.Order)
		}
	}
	if _, err := i.PartialFilterDoc(); err != nil {
		return err
	}
	return nil
}

// PartialFilterDoc decodes the Extended JSON partial filter, preserving
// key order. Returns nil when the index has no partial filter.
func (i Index) PartialFilterDoc() (bson.D, error) {
	if len(i.PartialFilter) == 0 {
		return nil, nil
	}
	var d bson.D
	if err := bson.UnmarshalExtJSON(i.PartialFilter, false, &d); err != nil {
		return nil, fmt.Errorf("partial_filter: %w", err)
	}
	return d, nil
}

// VersionedDir locates schema/versioned/v<N>.<M>/ relative to a
// project root. Returns ErrNotFound if the directory does not exist.
func VersionedDir(repoRoot, version string) (string, error) {
	if !strings.HasPrefix(version, "v") {
		version = "v" + version
	}
	dir := filepath.Join(repoRoot, "schema", "versioned", version)
	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("schema: versioned dir %s not found", dir)
		}
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("schema: %s is not a directory", dir)
	}
	return dir, nil
}

// ListVersionedDirs returns every v<N>.<M>/ under schema/versioned/
// sorted lexicographically. The CLI's update-schema iterates these in
// order.
func ListVersionedDirs(repoRoot string) ([]string, error) {
	return ListVersionsIn(filepath.Join(repoRoot, "schema", "versioned"))
}

// ListVersionsIn lists the v<major>.<minor> directories directly under a
// schema/versioned directory, in ascending version order.
func ListVersionsIn(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("schema: read %s: %w", root, err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "v") {
			continue
		}
		if _, err := parseVersion(e.Name()); err != nil {
			continue
		}
		out = append(out, e.Name())
	}
	// Numeric order: v1.10 follows v1.2.
	sort.Slice(out, func(i, j int) bool {
		c, _ := CompareVersions(out[i], out[j])
		return c < 0
	})
	return out, nil
}

// LatestVersionedDir returns the highest schema version directory.
func LatestVersionedDir(repoRoot string) (string, error) {
	return LatestVersionIn(filepath.Join(repoRoot, "schema", "versioned"))
}

// LatestVersionIn returns the highest version directory under root.
func LatestVersionIn(root string) (string, error) {
	dirs, err := ListVersionsIn(root)
	if err != nil {
		return "", err
	}
	if len(dirs) == 0 {
		return "", fmt.Errorf("schema: no versioned schema directories under %s", root)
	}
	return dirs[len(dirs)-1], nil
}

// CompareVersions compares dotted numeric versions ("1.1.0", "v1.1");
// missing components count as zero.
func CompareVersions(a, b string) (int, error) {
	va, err := parseVersion(a)
	if err != nil {
		return 0, err
	}
	vb, err := parseVersion(b)
	if err != nil {
		return 0, err
	}
	for i := 0; i < len(va) || i < len(vb); i++ {
		var x, y int
		if i < len(va) {
			x = va[i]
		}
		if i < len(vb) {
			y = vb[i]
		}
		switch {
		case x < y:
			return -1, nil
		case x > y:
			return 1, nil
		}
	}
	return 0, nil
}

func parseVersion(v string) ([]int, error) {
	parts := strings.Split(strings.TrimPrefix(v, "v"), ".")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("schema: invalid version %q", v)
		}
		out = append(out, n)
	}
	return out, nil
}
