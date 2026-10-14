package conformance

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
)

// uniqueCounter seeds unique names within a process so parallel
// subtests don't collide on composite keys. Atomic; safe across
// goroutines.
var uniqueCounter atomic.Int64

// uniqStr returns a per-process-unique label with the given prefix.
// Stable enough to appear in fixture IDs without colliding across
// t.Run calls that share a backend.
func uniqStr(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, uniqueCounter.Add(1))
}

// dataBlob wraps raw bytes in the protobuf DataBlob shape the upstream
// interface expects.
func dataBlob(label string) *commonpb.DataBlob {
	return &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte(label),
	}
}

// stateBlob is the canonical running-execution state blob for a
// workflow being created in a suite scenario. Returned separately
// because some callers mutate it.
func stateBlob(runID string) *persistencespb.WorkflowExecutionState {
	return &persistencespb.WorkflowExecutionState{
		CreateRequestId: "req-" + runID,
		RunId:           runID,
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
}

// makeSnapshot builds an InternalWorkflowSnapshot with minimal but
// plausibly-valid fields: the mutable-state encoding, a stub execution
// info blob, a running state, NextEventID=2, DBRecordVersion=1.
func makeSnapshot(ns, wf, run string) persistence.InternalWorkflowSnapshot {
	return persistence.InternalWorkflowSnapshot{
		NamespaceID:        ns,
		WorkflowID:         wf,
		RunID:              run,
		ExecutionInfoBlob:  dataBlob(fmt.Sprintf("info:%s:%s:%s", ns, wf, run)),
		ExecutionStateBlob: dataBlob(fmt.Sprintf("state:%s", run)),
		ExecutionState:     stateBlob(run),
		NextEventID:        2,
		DBRecordVersion:    1,
		StartVersion:       1,
		LastWriteVersion:   1,
	}
}

// fatalOnErr is the suite's terse assert-nil helper. Cuts repetitive
// `if err != nil { t.Fatal(err) }` noise.
func fatalOnErr(t *testing.T, op string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
}

// WaitForOptions controls WaitFor's polling cadence and deadline.
// Defaults are chosen to be friendly to both local tests and Atlas
// (first tick fires quickly; interval grows with backoff up to a cap).
type WaitForOptions struct {
	// Total is the maximum wall-clock time to spend polling before
	// giving up. Zero picks a sensible default (2 minutes).
	Total time.Duration
	// Initial is the first sleep between retries. Zero picks 200ms.
	Initial time.Duration
	// Max caps the per-iteration sleep. Zero picks 5s.
	Max time.Duration
	// OnWait, if non-nil, is called once per poll with the current
	// backoff so callers can log progress without spamming.
	OnWait func(attempt int, slept time.Duration)
}

// WaitFor polls cond() until it returns (true, nil) or the deadline
// expires. Backoff starts at Initial and doubles up to Max. Returns
// nil on success, the last error seen from cond, or a timeout
// error if cond never returned true.
//
// Replaces the hand-rolled time.Sleep loops in Atlas-facing
// tests (M5 gate, conformance visibility). Centralising the pattern
// gives every caller uniform flakiness resistance — the polling
// shape is documented, bounded, and testable.
func WaitFor(ctx context.Context, opt WaitForOptions, cond func(ctx context.Context) (bool, error)) error {
	if opt.Total == 0 {
		opt.Total = 2 * time.Minute
	}
	if opt.Initial == 0 {
		opt.Initial = 200 * time.Millisecond
	}
	if opt.Max == 0 {
		opt.Max = 5 * time.Second
	}
	deadline := time.Now().Add(opt.Total)
	wait := opt.Initial
	attempt := 0
	var lastErr error
	for {
		ok, err := cond(ctx)
		if ok {
			return nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			if lastErr != nil {
				return fmt.Errorf("WaitFor timed out after %s: %w", opt.Total, lastErr)
			}
			return fmt.Errorf("WaitFor timed out after %s (cond never returned true)", opt.Total)
		}
		attempt++
		if opt.OnWait != nil {
			opt.OnWait(attempt, wait)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		wait *= 2
		if wait > opt.Max {
			wait = opt.Max
		}
	}
}
