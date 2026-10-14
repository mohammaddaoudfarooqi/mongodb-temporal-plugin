package visibility

import (
	"testing"
	"time"
)

// TestIndexTracker_DefaultNotReady bootstrap state:
// a tracker that has never observed READY must report not-ready so
// the store's read path surfaces Unavailable. No goroutine is started
// here; we only validate the initial atomic state.
func TestIndexTracker_DefaultNotReady(t *testing.T) {
	tr := NewIndexTracker(nil, "idx", 5*time.Second)
	if tr.IsReady() {
		t.Error("fresh tracker should report not-ready")
	}
	if tr.LastStatus() != IndexStatusUnknown {
		t.Errorf("initial status should be unknown; got %q", tr.LastStatus())
	}
}

// TestIndexTracker_PollBounds ensures the poll interval clamps to the
// range [1s, 30s] so Atlas API load stays bounded.
func TestIndexTracker_PollBounds(t *testing.T) {
	tr := NewIndexTracker(nil, "idx", 100*time.Millisecond)
	if tr.poll < time.Second {
		t.Errorf("poll should clamp to >=1s; got %v", tr.poll)
	}
	tr2 := NewIndexTracker(nil, "idx", 5*time.Minute)
	if tr2.poll > 30*time.Second {
		t.Errorf("poll should clamp to <=30s; got %v", tr2.poll)
	}
}

// TestIndexTracker_StopIdempotent ensures Stop() can be called before
// Start() or more than once without panic.
func TestIndexTracker_StopIdempotent(t *testing.T) {
	tr := NewIndexTracker(nil, "idx", 5*time.Second)
	tr.Stop() // before Start
	tr.Stop() // still safe
}

// TestIndexTracker_StatusTransitions covers backlog B-11: Atlas can
// transition the index READY → FAILED (e.g., schema mismatch) and
// back to READY after a rebuild. The tracker must flip ready ↔ !ready
// in lockstep so visibility reads surface Unavailable on FAILED and
// resume on the next READY.
func TestIndexTracker_StatusTransitions(t *testing.T) {
	tr := NewIndexTracker(nil, "idx", 5*time.Second)

	// Start: unknown, not ready.
	if tr.IsReady() || tr.LastStatus() != IndexStatusUnknown {
		t.Fatalf("fresh tracker drifted: ready=%v status=%q", tr.IsReady(), tr.LastStatus())
	}

	tr.setStatusForTest(IndexStatusBuilding)
	if tr.IsReady() {
		t.Error("BUILDING must not be READY")
	}
	if tr.LastStatus() != IndexStatusBuilding {
		t.Errorf("status drift: %q", tr.LastStatus())
	}

	tr.setStatusForTest(IndexStatusReady)
	if !tr.IsReady() {
		t.Error("READY must flip IsReady to true")
	}

	// Fail: tracker must drop IsReady on FAILED so visibility reads
	// surface Unavailable instead of returning empty pages.
	tr.setStatusForTest(IndexStatusFailed)
	if tr.IsReady() {
		t.Error("FAILED must flip IsReady back to false")
	}
	if tr.LastStatus() != IndexStatusFailed {
		t.Errorf("expected FAILED; got %q", tr.LastStatus())
	}

	// Recover: next READY observation restores service without
	// needing a plugin restart.
	tr.setStatusForTest(IndexStatusReady)
	if !tr.IsReady() {
		t.Error("recovery to READY should re-enable IsReady")
	}
}
