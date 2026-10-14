//go:build integration

// M6 gate — runs the temporal-mongodb-tool CLI end-to-end against
// a live Mongo replica set (testcontainer) and a live Atlas cluster
// (when ATLAS_URI + ATLAS_DATABASE are set).
//
// Exercises:
//   - `setup-schema` on a fresh DB: collections + indexes + metadata
//   - `setup-schema` second run: idempotent
//   - `verify` exits 0 when state=ready
//   - `update-schema`: same version → "nothing to do"
//   - `setup-atlas-search`: creates + polls to READY
//   - `drop-atlas-search`: removes the index
package testutil_test

import (
	"bytes"
	"context"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/schema"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil"
)

// repoRoot walks up from this test file to the module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..")
}

// buildCLI builds temporal-mongodb-tool into a temp binary and returns
// its path. Cached across sub-tests via the test binary's lifetime.
func buildCLI(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "temporal-mongodb-tool")
	cmd := exec.Command("go", "build", "-o", out,
		"./cmd/temporal-mongodb-tool")
	cmd.Dir = repoRoot(t)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("go build: %v\n%s", err, stderr.String())
	}
	return out
}

// startRSForCLI spins up a Mongo replica set (image selected by
// MONGO_VERSION, default mongo:6) and returns a directConnection URI
// suitable for the CLI.
func startRSForCLI(t *testing.T) string {
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
	return strings.TrimRight(u.String(), "?") + "?directConnection=true"
}

// runCLI runs the built binary with args and returns (stdout, err).
func runCLI(t *testing.T, bin string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Dir = repoRoot(t)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	combined := stdout.String()
	if stderr.Len() > 0 {
		combined += "\n[stderr]\n" + stderr.String()
	}
	return combined, err
}

// TestM6_CLISelfHostedCycle drives setup → verify → update against a
// fresh Mongo replica set. No Atlas required.
func TestM6_CLISelfHostedCycle(t *testing.T) {
	bin := buildCLI(t)
	uri := startRSForCLI(t)
	dbName := "temporal_m6"
	latest := latestSchemaVersion(t)

	// setup-schema fresh — expect all collections + indexes created.
	out, err := runCLI(t, bin, "setup-schema",
		"--uri", uri, "--database", dbName,
		"--num-history-shards", "4")
	if err != nil {
		t.Fatalf("setup-schema: %v\n%s", err, out)
	}
	if !strings.Contains(out, "_schema_version: initialised at "+latest) {
		t.Errorf("setup-schema output missing init line:\n%s", out)
	}

	// setup-schema re-run — idempotent, no creations.
	out, err = runCLI(t, bin, "setup-schema",
		"--uri", uri, "--database", dbName,
		"--num-history-shards", "4")
	if err != nil {
		t.Fatalf("setup-schema re-run: %v\n%s", err, out)
	}
	if !strings.Contains(out, "already present") {
		t.Errorf("re-run should report already-present:\n%s", out)
	}

	// verify — expect ready.
	out, err = runCLI(t, bin, "verify",
		"--uri", uri, "--database", dbName)
	if err != nil {
		t.Fatalf("verify: %v\n%s", err, out)
	}
	if !strings.Contains(out, `"state": "ready"`) {
		t.Errorf("verify output missing ready state:\n%s", out)
	}
	if !strings.Contains(out, `"schema_version": "`+latest+`"`) {
		t.Errorf("verify output missing schema_version:\n%s", out)
	}

	// update-schema at same version — "nothing to do".
	out, err = runCLI(t, bin, "update-schema",
		"--uri", uri, "--database", dbName)
	if err != nil {
		t.Fatalf("update-schema: %v\n%s", err, out)
	}
	if !strings.Contains(out, "nothing to do") {
		t.Errorf("update-schema expected no-op output; got:\n%s", out)
	}
}

// TestM6_CLIUpgradeFromV10 end to end: a cluster set
// up at v1.0 is upgraded by a plain `update-schema` to the latest
// version, including the changed TTL index, and verifies ready.
func TestM6_CLIUpgradeFromV10(t *testing.T) {
	bin := buildCLI(t)
	uri := startRSForCLI(t)
	dbName := "temporal_m6_upgrade"
	latest := latestSchemaVersion(t)

	out, err := runCLI(t, bin, "setup-schema", "--uri", uri, "--database", dbName,
		"--num-history-shards", "4", "--schema-version", "v1.0")
	if err != nil || !strings.Contains(out, "initialised at 1.0.0") {
		t.Fatalf("setup-schema v1.0: %v\n%s", err, out)
	}
	out, err = runCLI(t, bin, "update-schema", "--uri", uri, "--database", dbName)
	if err != nil {
		t.Fatalf("update-schema: %v\n%s", err, out)
	}
	if !strings.Contains(out, "1.0.0 → "+latest+" (ready)") || !strings.Contains(out, "1 replaced") {
		t.Fatalf("update-schema did not upgrade and replace the TTL index:\n%s", out)
	}
	out, err = runCLI(t, bin, "verify", "--uri", uri, "--database", dbName)
	if err != nil || !strings.Contains(out, `"schema_version": "`+latest+`"`) || !strings.Contains(out, `"state": "ready"`) {
		t.Fatalf("verify after upgrade: %v\n%s", err, out)
	}
	out, err = runCLI(t, bin, "update-schema", "--uri", uri, "--database", dbName, "--schema-version", "v1.0")
	if err == nil || !strings.Contains(out, "downgrade") {
		t.Fatalf("downgrade not refused: %v\n%s", err, out)
	}
}

// latestSchemaVersion reads schema_version from the newest manifest.
func latestSchemaVersion(t *testing.T) string {
	t.Helper()
	dir, err := schema.LatestVersionedDir(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	idx, err := schema.LoadIndexes(filepath.Join(repoRoot(t), "schema", "versioned", dir, "indexes.json"))
	if err != nil {
		t.Fatal(err)
	}
	return idx.SchemaVersion
}

// TestM6_CLIVerify_AbsentCluster covers the failure path: verify on a
// fresh database without setup-schema should exit 1.
func TestM6_CLIVerify_AbsentCluster(t *testing.T) {
	bin := buildCLI(t)
	uri := startRSForCLI(t)

	out, err := runCLI(t, bin, "verify",
		"--uri", uri, "--database", "empty_db")
	if err == nil {
		t.Fatal("verify against empty cluster should exit non-zero")
	}
	if !strings.Contains(out, "absent") {
		t.Errorf("verify output should mention absent singleton:\n%s", out)
	}
}

// TestM6_CLIAtlasSearch_Live runs setup-atlas-search + drop-atlas-search
// against a live Atlas cluster. Skips when ATLAS_URI/DATABASE unset.
func TestM6_CLIAtlasSearch_Live(t *testing.T) {
	uri := os.Getenv("ATLAS_URI")
	db := os.Getenv("ATLAS_DATABASE")
	if uri == "" || db == "" {
		t.Skip("ATLAS_URI / ATLAS_DATABASE not set — skipping CLI Atlas gate")
	}
	bin := buildCLI(t)

	// setup-atlas-search: expect eventual READY.
	indexName := "visibility_executions_search_m6"
	out, err := runCLI(t, bin, "setup-atlas-search",
		"--uri", uri, "--database", db,
		"--atlas-index-name", indexName,
		"--timeout", "900")
	if err != nil {
		t.Fatalf("setup-atlas-search: %v\n%s", err, out)
	}
	if !strings.Contains(out, "is READY") {
		t.Errorf("setup-atlas-search output missing READY:\n%s", out)
	}

	// drop-atlas-search: remove the index we just created.
	out, err = runCLI(t, bin, "drop-atlas-search",
		"--uri", uri, "--database", db,
		"--atlas-index-name", indexName)
	if err != nil {
		t.Fatalf("drop-atlas-search: %v\n%s", err, out)
	}
	if !strings.Contains(out, "dropped") && !strings.Contains(out, "already absent") {
		t.Errorf("drop-atlas-search output unexpected:\n%s", out)
	}
}
