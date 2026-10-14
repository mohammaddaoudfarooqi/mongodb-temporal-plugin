package scenarios

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/temporal"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/e2e/report"
)

// LoadReport summarises a load run.
type LoadReport = report.Load

// LadderReport summarises a concurrency ladder.
type LadderReport = report.Ladder

// Thresholds bound a sustainable ladder step.
type Thresholds struct {
	MaxP99       time.Duration
	MaxErrorRate float64
}

// Drive runs job with at most concurrency calls in flight. With n > 0
// it makes n calls; otherwise it keeps starting calls until d elapses.
// Latency percentiles cover successful calls only.
func Drive(ctx context.Context, concurrency, n int, d time.Duration, job func(ctx context.Context, i int) error) LoadReport {
	sem := make(chan struct{}, concurrency)
	var mu sync.Mutex
	var lat []time.Duration
	classes := map[string]int64{}
	var failed atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	started := 0
	for ; n > 0 && started < n || n <= 0 && time.Since(start) < d; started++ {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(i int) {
			defer func() { <-sem; wg.Done() }()
			t0 := time.Now()
			if err := job(ctx, i); err != nil {
				failed.Add(1)
				mu.Lock()
				classes[Classify(err)]++
				mu.Unlock()
				return
			}
			mu.Lock()
			lat = append(lat, time.Since(t0))
			mu.Unlock()
		}(started)
	}
	wg.Wait()
	elapsed := time.Since(start)
	sortDurations(lat)
	r := LoadReport{
		Workflows: started, Concurrency: concurrency, Completed: int64(len(lat)), Failed: failed.Load(),
		Elapsed: elapsed, PerSecond: float64(len(lat)) / elapsed.Seconds(),
		P50: percentile(lat, .5), P95: percentile(lat, .95), P99: percentile(lat, .99), Max: percentile(lat, 1),
	}
	if started > 0 {
		r.ErrorRate = float64(r.Failed) / float64(started)
	}
	if len(classes) > 0 {
		r.ErrorsByClass = classes
	}
	return r
}

// Classify names an error's class for the load report: context
// deadline/cancel, the Temporal application error type, the server's
// serviceerror type, or "other".
func Classify(err error) string {
	var app *temporal.ApplicationError
	var timeout *temporal.TimeoutError
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.As(err, &app):
		return "application:" + app.Type()
	case errors.As(err, &timeout):
		return "timeout:" + timeout.TimeoutType().String()
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		if t := fmt.Sprintf("%T", e); len(t) > len("*serviceerror.") && t[:len("*serviceerror.")] == "*serviceerror." {
			return t[1:]
		}
	}
	return "other"
}

// Ladder runs step for each concurrency in ascending order and stops
// after the first step that breaches th.
func Ladder(steps []int, th Thresholds, step func(concurrency int) LoadReport) LadderReport {
	sorted := append([]int(nil), steps...)
	sort.Ints(sorted)
	lr := LadderReport{MaxP99: th.MaxP99, MaxErrorRate: th.MaxErrorRate}
	for _, c := range sorted {
		r := step(c)
		lr.Steps = append(lr.Steps, r)
		if r.P99 > th.MaxP99 || r.ErrorRate > th.MaxErrorRate {
			lr.StoppedAt = c
			break
		}
		lr.MaxSustainable = c
	}
	return lr
}
