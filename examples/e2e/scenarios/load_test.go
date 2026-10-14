package scenarios

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
)

func TestClassify_KnownErrors_StableClasses(t *testing.T) {
	cases := map[string]error{
		"deadline_exceeded":           fmt.Errorf("wrapped: %w", context.DeadlineExceeded),
		"canceled":                    context.Canceled,
		"application:PaymentDeclined": temporal.NewNonRetryableApplicationError("no", "PaymentDeclined", nil),
		"serviceerror.Unavailable":    serviceerror.NewUnavailable("down"),
		"other":                       errors.New("boom"),
	}
	for want, err := range cases {
		if got := Classify(err); got != want {
			t.Errorf("Classify(%v) = %q, want %q", err, got, want)
		}
	}
}

func TestDrive_CountMode_RunsNJobsAndCountsFailuresByClass(t *testing.T) {
	var inflight, peak atomic.Int64
	r := Drive(context.Background(), 4, 20, 0, func(ctx context.Context, i int) error {
		if n := inflight.Add(1); n > peak.Load() {
			peak.Store(n)
		}
		defer inflight.Add(-1)
		time.Sleep(2 * time.Millisecond)
		if i%5 == 0 {
			return serviceerror.NewUnavailable("x")
		}
		return nil
	})
	if r.Workflows != 20 || r.Failed != 4 || r.Completed != 16 {
		t.Fatalf("workflows=%d failed=%d completed=%d, want 20/4/16", r.Workflows, r.Failed, r.Completed)
	}
	if r.ErrorsByClass["serviceerror.Unavailable"] != 4 {
		t.Errorf("errors by class = %v", r.ErrorsByClass)
	}
	if peak.Load() > 4 {
		t.Errorf("peak in flight %d exceeds concurrency 4", peak.Load())
	}
	if r.ErrorRate != 0.2 {
		t.Errorf("error rate %v, want 0.2", r.ErrorRate)
	}
}

func TestDrive_DurationMode_KeepsStartingUntilDeadline(t *testing.T) {
	t0 := time.Now()
	r := Drive(context.Background(), 3, 0, 150*time.Millisecond, func(ctx context.Context, i int) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})
	if el := time.Since(t0); el < 150*time.Millisecond || el > time.Second {
		t.Fatalf("duration run took %s", el)
	}
	if r.Workflows < 20 || r.Completed != int64(r.Workflows) {
		t.Fatalf("workflows=%d completed=%d; want >= 20, all completed", r.Workflows, r.Completed)
	}
}

func TestLadder_StopsAtFirstBreach_ReportsHighestPassingStep(t *testing.T) {
	p99 := map[int]time.Duration{10: time.Second, 20: 2 * time.Second, 40: 9 * time.Second, 80: time.Second}
	var ran []int
	lr := Ladder([]int{40, 10, 80, 20}, Thresholds{MaxP99: 5 * time.Second, MaxErrorRate: 0.01},
		func(c int) LoadReport {
			ran = append(ran, c)
			return LoadReport{Concurrency: c, P99: p99[c]}
		})
	if fmt.Sprint(ran) != "[10 20 40]" {
		t.Fatalf("steps run %v, want ascending and stop after breach [10 20 40]", ran)
	}
	if lr.MaxSustainable != 20 || lr.StoppedAt != 40 {
		t.Fatalf("max sustainable %d stopped at %d, want 20 and 40", lr.MaxSustainable, lr.StoppedAt)
	}
}

// Error-rate breach also stops the ladder.
func TestLadder_ErrorRateBreach_Stops(t *testing.T) {
	lr := Ladder([]int{1, 2}, Thresholds{MaxP99: time.Hour, MaxErrorRate: 0.01},
		func(c int) LoadReport { return LoadReport{Concurrency: c, ErrorRate: 0.5} })
	if lr.MaxSustainable != 0 || lr.StoppedAt != 1 || len(lr.Steps) != 1 {
		t.Fatalf("got %+v", lr)
	}
}
