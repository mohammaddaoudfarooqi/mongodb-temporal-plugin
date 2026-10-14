//go:build integration

// M5 gate — validates the Atlas Search visibility path against a live
// Atlas M10+ cluster. Runs only when ATLAS_URI + ATLAS_DATABASE are
// set; otherwise skips with a clear message. CI wires these from
// GitHub Actions secrets under the scheduled atlas-e2e job.
//
// What the gate covers:
//
//	ProbeAtlasSearchAvailable succeeds against real Atlas.
//	Index lifecycle polling reaches READY within the timeout.
//	$search queries return indexed documents.
//	$searchMeta count returns a non-negative total.
//	SearchAfter-based pagination resumes deterministically.
//
// The test creates its own collection (visibility_executions inside
// ATLAS_DATABASE), the index manifest in schema/atlas_search/
// visibility.json, and cleans up on exit.
package testutil_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil/conformance"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility"
)

func atlasConfig(t *testing.T) (string, string) {
	t.Helper()
	uri := os.Getenv("ATLAS_URI")
	db := os.Getenv("ATLAS_DATABASE")
	if uri == "" || db == "" {
		t.Skip("ATLAS_URI / ATLAS_DATABASE not set — skipping live Atlas gate")
	}
	return uri, db
}

// loadSearchIndexDefinition reads schema/atlas_search/visibility.json and
// returns the definition body the createSearchIndex command expects.
func loadSearchIndexDefinition(t *testing.T) (string, map[string]any) {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(thisFile), "..", "..", "schema", "atlas_search", "visibility.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m struct {
		Name       string         `json:"name"`
		Definition map[string]any `json:"definition"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	return m.Name, m.Definition
}

// ensureSearchIndex applies the visibility.json manifest to the target
// collection. If the index doesn't exist we create it; if it exists
// we issue updateSearchIndex so the test always runs against the
// committed definition (previous runs with a stale schema would
// otherwise pin the gate to the old shape).
func ensureSearchIndex(t *testing.T, db *mongo.Database, coll, indexName string, definition map[string]any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	status, err := visibility.QueryIndexStatus(ctx, db, coll, indexName)
	if err != nil {
		t.Fatalf("QueryIndexStatus: %v", err)
	}
	switch status {
	case visibility.IndexStatusUnknown:
		cmd := bson.D{
			{Key: "createSearchIndexes", Value: coll},
			{Key: "indexes", Value: []bson.M{{
				"name":       indexName,
				"definition": definition,
			}}},
		}
		if err := db.RunCommand(ctx, cmd).Err(); err != nil {
			t.Fatalf("createSearchIndexes: %v", err)
		}
	default:
		// Existing index — refresh the definition so the test runs
		// against the current schema. updateSearchIndex is idempotent:
		// an unchanged definition is a no-op but still returns success.
		cmd := bson.D{
			{Key: "updateSearchIndex", Value: coll},
			{Key: "name", Value: indexName},
			{Key: "definition", Value: definition},
		}
		if err := db.RunCommand(ctx, cmd).Err(); err != nil {
			t.Logf("updateSearchIndex (continuing): %v", err)
		}
	}

	// Poll for READY. After update, Atlas transitions to PENDING/BUILDING
	// briefly before returning to READY.
	deadline := time.Now().Add(8 * time.Minute)
	lastLog := time.Time{}
	// Bounded exponential backoff via the conformance package's
	// shared WaitFor helper. Same cadence as driver_atlas, so both
	// gates respond to a slow cluster identically.
	err = conformance.WaitFor(ctx,
		conformance.WaitForOptions{
			Total: time.Until(deadline),
			OnWait: func(_ int, _ time.Duration) {
				if time.Since(lastLog) > 10*time.Second {
					st, _ := visibility.QueryIndexStatus(ctx, db, coll, indexName)
					t.Logf("index %s status=%s — waiting…", indexName, st)
					lastLog = time.Now()
				}
			},
		},
		func(ctx context.Context) (bool, error) {
			st, err := visibility.QueryIndexStatus(ctx, db, coll, indexName)
			return st == visibility.IndexStatusReady, err
		})
	if err != nil {
		t.Fatalf("index %s did not reach READY: %v", indexName, err)
	}
}

// TestM5_AtlasLifecycle_Live exercises the full Atlas Search path.
func TestM5_AtlasLifecycle_Live(t *testing.T) {
	uri, dbName := atlasConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// Build a ClientProvider against the live cluster.
	provider, err := internalmongo.NewClientProvider(ctx, internalmongo.ClientConfig{
		URI:                      uri,
		Database:                 dbName,
		AppName:                  "temporal-mongodb-plugin/m5-gate",
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

	db := provider.Database()

	// Probe Atlas Search availability.
	if err := visibility.ProbeAtlasSearchAvailable(ctx, db); err != nil {
		t.Fatalf("Atlas Search probe failed (confirm cluster tier M10+): %v", err)
	}

	// Ensure collection + indexes.
	if err := visibility.EnsureIndexes(ctx, db); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
	indexName, definition := loadSearchIndexDefinition(t)
	ensureSearchIndex(t, db, "visibility_executions", indexName, definition)

	// Tracker that polls live Atlas.
	tr := visibility.NewIndexTracker(db, indexName, 5*time.Second)
	tr.Start(ctx)
	t.Cleanup(tr.Stop)

	// Build a visibility Store in Atlas Search mode with the tracker wired.
	s := visibility.New(db, visibility.ModeAtlasSearch, indexName,
		visibility.WithIndexTracker(tr),
	)
	t.Cleanup(s.Close)

	// Tracker should observe READY quickly after
	// ensureSearchIndex returned. WaitFor bounds the wait at 2 min
	// with exponential backoff.
	if err := conformance.WaitFor(ctx,
		conformance.WaitForOptions{Total: 2 * time.Minute},
		func(context.Context) (bool, error) { return tr.IsReady(), nil },
	); err != nil {
		t.Fatalf("tracker did not observe READY; last status=%s: %v", tr.LastStatus(), err)
	}

	// Record 5 executions.
	nsID := fmt.Sprintf("atlas-gate-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = db.Collection("visibility_executions").DeleteMany(
			context.Background(), bson.M{"namespace_id": nsID})
	})
	for i := 0; i < 5; i++ {
		err := s.RecordWorkflowExecutionStarted(ctx, &store.InternalRecordWorkflowExecutionStartedRequest{
			InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
				NamespaceID:      nsID,
				WorkflowID:       fmt.Sprintf("wf-%d", i),
				RunID:            fmt.Sprintf("run-%d", i),
				WorkflowTypeName: "Gate",
				StartTime:        time.Now().UTC(),
				Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				TaskQueue:        "gate-tq",
			},
		})
		if err != nil {
			t.Fatalf("Record %d: %v", i, err)
		}
	}

	// Poll CountWorkflowExecutions until the cluster has
	// finished indexing our 5 seed docs. 90 s bound + exponential
	// backoff so a slow Atlas region surfaces as a timeout, not a
	// silently-wrong count.
	if err := conformance.WaitFor(ctx,
		conformance.WaitForOptions{Total: 90 * time.Second},
		func(ctx context.Context) (bool, error) {
			r, err := s.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{
				NamespaceID: namespace.ID(nsID),
			})
			if err != nil {
				return false, err
			}
			return r.Count >= 5, nil
		},
	); err != nil {
		t.Fatalf("Atlas did not finish indexing 5 seeds: %v", err)
	}

	// List via $search. Empty query → namespace-only filter.
	p1, err := s.ListWorkflowExecutions(ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: namespace.ID(nsID),
		PageSize:    3,
	})
	if err != nil {
		t.Fatalf("List page 1: %v", err)
	}
	if len(p1.Executions) != 3 {
		t.Errorf("page 1: got %d, want 3", len(p1.Executions))
	}
	if len(p1.NextPageToken) == 0 {
		t.Error("expected NextPageToken")
	}

	// Page 2 — searchAfter resume.
	p2, err := s.ListWorkflowExecutions(ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   namespace.ID(nsID),
		PageSize:      3,
		NextPageToken: p1.NextPageToken,
	})
	if err != nil {
		t.Fatalf("List page 2: %v", err)
	}
	if len(p2.Executions) != 2 {
		t.Errorf("page 2: got %d, want 2", len(p2.Executions))
	}

	// Count via $searchMeta.
	cresp, err := s.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespace.ID(nsID),
	})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cresp.Count != 5 {
		t.Errorf("count: got %d, want 5", cresp.Count)
	}
}

// TestM5_AtlasProbe_RejectsPlainMongo is a negative test that runs
// against the standard mongo:6 replica set and confirms
// ProbeAtlasSearchAvailable surfaces the "switch to aggregation"
// guidance. No Atlas required; keeps the signal alive even when
// ATLAS_URI is unset.
func TestM5_AtlasProbe_RejectsPlainMongo(t *testing.T) {
	// Reuse the M1 gate's helper that spins up a mongo:6 replica set.
	db := gateDB(t)
	err := visibility.ProbeAtlasSearchAvailable(context.Background(), db)
	if err == nil {
		t.Fatal("expected probe to reject plain MongoDB; got nil")
	}
	// Guidance text is part of the error message.
	msg := err.Error()
	if !contains(msg, "Atlas Search unavailable") && !contains(msg, "aggregation") {
		t.Errorf("error should guide operator to aggregation mode: %q", msg)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
