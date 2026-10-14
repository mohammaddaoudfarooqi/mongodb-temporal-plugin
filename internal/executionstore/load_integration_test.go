//go:build integration

// Load-test harness. Drives
// UpdateWorkflowExecution against a live testcontainers replica set
// at a configurable concurrency and measures p50/p95/p99 latency
// plus sustained STPS (steps per second). Writes a JSON report so
// CI can track the throughput/latency target delta over releases.
//
// Scope note: the target is aspirational (500 STPS, p99 <= 50ms on
// Atlas M30+ co-located). A single-container replica set on CI
// hardware is not M30+, so the numbers this test reports are
// plugin-overhead-biased, not Atlas-sized. The harness exists so
// that:
//  1. A regression in plugin overhead (e.g. an accidental txn on
//     the hot path) shows up as a p99 spike.
//  2. The throughput/latency target can be re-measured on Atlas by any
//     contributor by pointing the same harness at a real cluster
//     (override the URI via PLUGIN_LOADTEST_MONGO_URI).
//
// Outputs:
//   - stdout: one line per percentile + total STPS.
//   - file:  `loadtest-report.json` in $PWD when PLUGIN_LOADTEST_REPORT
//     is non-empty. CI artifact upload reads this.
//
// Run:
//
//	go test -tags=integration -run=TestLoad_UpdateWorkflow -timeout=10m \
//	  ./internal/executionstore/ -v \
//	  -args -loadtest.duration=30s -loadtest.concurrency=32
//
// Against an existing cluster (skips testcontainers):
//
//	PLUGIN_LOADTEST_MONGO_URI="mongodb+srv://..." \
//	  go test -tags=integration -run=TestLoad_UpdateWorkflow ...
package executionstore_test

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
)

var (
	loadDuration    = flag.Duration("loadtest.duration", 10*time.Second, "load-test duration (default 10s)")
	loadConcurrency = flag.Int("loadtest.concurrency", 16, "load-test worker goroutines (default 16)")
	loadWarmup      = flag.Duration("loadtest.warmup", 2*time.Second, "load-test warm-up before sampling")
	loadShards      = flag.Int("loadtest.shards", 1, "load-test history shards the workers are spread over (1 = worst-case single-shard contention)")
)

// TestLoad_UpdateWorkflow drives concurrent UpdateWorkflowExecution
// against a fresh workflow and reports p50/p95/p99 latency + STPS.
// Skipped unless -loadtest.duration > 0 (the default IS >0 — pass
// -loadtest.duration=0 to skip on slow runners).
//
// The test succeeds unconditionally (never fails on latency); its
// role is to emit a measurement, not to gate on the target. A future
// iteration can layer a gate on top once baseline numbers are
// established per-hardware.
func TestLoad_UpdateWorkflow(t *testing.T) {
	if *loadDuration <= 0 {
		t.Skip("loadtest.duration <= 0: skipping load test")
	}

	db := startDB(t)
	const rangeID int64 = 1
	shards := max(1, *loadShards)
	for sh := 1; sh <= shards; sh++ {
		seedShard(t, db, int32(sh), rangeID)
	}
	shardOf := func(worker int) int32 { return int32(worker%shards + 1) }
	store := executionstore.New(db, "cluster0")
	ctx := context.Background()

	// Create N workflows so the concurrent updaters don't contend on a
	// single document — one workflow per worker so every CAS hits a
	// fresh document and we measure pure update-path latency, not
	// lock contention.
	snaps := make([]persistence.InternalWorkflowSnapshot, *loadConcurrency)
	for i := 0; i < *loadConcurrency; i++ {
		snaps[i] = makeSnapshot("ns", fmt.Sprintf("wf-%d", i), fmt.Sprintf("run-%d", i))
		if _, err := store.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
			ShardID: shardOf(i), RangeID: rangeID,
			Mode:                persistence.CreateWorkflowModeBrandNew,
			NewWorkflowSnapshot: snaps[i],
		}); err != nil {
			t.Fatalf("seed workflow %d: %v", i, err)
		}
	}

	// Per-workflow CAS state survives across runUpdates calls: warm-up
	// advances the stored versions, so the sampling window must resume
	// from where warm-up stopped rather than from the Create-time values.
	cas := make([]casCursor, len(snaps))
	for i := range cas {
		cas[i] = casCursor{nextEventID: 2, dbVersion: 1}
	}

	// Warm-up: run at the target concurrency for a short period so
	// connection pools, prepared statements, and JIT paths settle
	// before the sampling window starts.
	if *loadWarmup > 0 {
		runUpdates(ctx, store, snaps, cas, shardOf, rangeID, *loadWarmup, nil)
	}

	// Sampling window.
	var latencies []time.Duration
	var mu sync.Mutex
	var ops atomic.Int64
	var errs atomic.Int64
	var firstErr atomic.Value
	record := func(d time.Duration, err error) {
		ops.Add(1)
		if err != nil {
			errs.Add(1)
			firstErr.CompareAndSwap(nil, err.Error())
			return
		}
		mu.Lock()
		latencies = append(latencies, d)
		mu.Unlock()
	}

	start := time.Now()
	runUpdates(ctx, store, snaps, cas, shardOf, rangeID, *loadDuration, record)
	elapsed := time.Since(start)

	totalOps := ops.Load()
	totalErrs := errs.Load()
	stps := float64(totalOps-totalErrs) / elapsed.Seconds()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	p := func(q float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		idx := int(float64(len(latencies)-1) * q)
		return latencies[idx]
	}
	report := struct {
		Concurrency int           `json:"concurrency"`
		Duration    string        `json:"duration"`
		TotalOps    int64         `json:"total_ops"`
		Errors      int64         `json:"errors"`
		STPS        float64       `json:"stps"`
		P50         string        `json:"p50"`
		P95         string        `json:"p95"`
		P99         string        `json:"p99"`
		Max         time.Duration `json:"max_ns"`
	}{
		Concurrency: *loadConcurrency,
		Duration:    elapsed.String(),
		TotalOps:    totalOps,
		Errors:      totalErrs,
		STPS:        stps,
		P50:         p(0.50).String(),
		P95:         p(0.95).String(),
		P99:         p(0.99).String(),
		Max:         p(1.0),
	}
	t.Logf("load report: shards=%d concurrency=%d duration=%s ops=%d errors=%d stps=%.1f p50=%s p95=%s p99=%s",
		shards, report.Concurrency, report.Duration, report.TotalOps, report.Errors, report.STPS,
		report.P50, report.P95, report.P99)
	// A harness that only measures failures is worse than none: each
	// worker owns its workflow, so every update must succeed.
	if totalErrs > 0 || totalOps == 0 {
		t.Errorf("load test invalid: %d of %d updates failed (first error: %v)", totalErrs, totalOps, firstErr.Load())
	}

	if os.Getenv("PLUGIN_LOADTEST_REPORT") != "" {
		data, _ := json.MarshalIndent(report, "", "  ")
		path := "loadtest-report.json"
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Logf("WARN: write %s: %v", path, err)
		} else {
			t.Logf("wrote %s", path)
		}
	}
}

// casCursor is one workflow's last committed next_event_id and
// db_record_version.
type casCursor struct {
	nextEventID int64
	dbVersion   int64
}

// runUpdates drives *loadConcurrency goroutines for dur, each looping
// UpdateWorkflowExecution on its own pre-seeded workflow. record may
// be nil (warm-up path).
func runUpdates(
	ctx context.Context,
	store *executionstore.Stub,
	snaps []persistence.InternalWorkflowSnapshot,
	cas []casCursor,
	shardOf func(int) int32, rangeID int64,
	dur time.Duration,
	record func(time.Duration, error),
) {
	stopAt := time.Now().Add(dur)
	var wg sync.WaitGroup
	for i := 0; i < len(snaps); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			snap := snaps[idx]
			// CAS semantics (see mutable_state_impl):
			//   Condition       = current stored next_event_id
			//   DBRecordVersion = new version (pre-update is N-1)
			c := &cas[idx]
			nextEventID, dbVersion := c.nextEventID, c.dbVersion
			defer func() { c.nextEventID, c.dbVersion = nextEventID, dbVersion }()
			for time.Now().Before(stopAt) {
				mut := snapshotToMutation(snap)
				mut.Condition = nextEventID
				mut.DBRecordVersion = dbVersion + 1
				mut.NextEventID = nextEventID + 1
				t0 := time.Now()
				err := store.UpdateWorkflowExecution(ctx,
					&persistence.InternalUpdateWorkflowExecutionRequest{
						ShardID: shardOf(idx), RangeID: rangeID,
						Mode:                   persistence.UpdateWorkflowModeUpdateCurrent,
						UpdateWorkflowMutation: mut,
					})
				if record != nil {
					record(time.Since(t0), err)
				}
				if err != nil {
					// CAS misses shouldn't happen (single writer per wf),
					// but if something goes wrong, back off briefly so
					// we don't spin hot.
					time.Sleep(1 * time.Millisecond)
					continue
				}
				nextEventID++
				dbVersion++
			}
		}(i)
	}
	wg.Wait()
}
