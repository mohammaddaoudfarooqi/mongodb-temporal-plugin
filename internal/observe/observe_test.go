package observe_test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/server/common/log"
	tmetrics "go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"

	pluginmetrics "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metrics"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/observe"
)

// countingHandler implements tmetrics.Handler with an atomic counter
// per metric-name. Enough to prove observe.Op fires the expected
// triplet (counter + timer + error-counter) on a simulated method.
type countingHandler struct {
	mu     sync.Mutex
	counts map[string]*atomic.Int64
	tags   []tmetrics.Tag
}

func newCountingHandler() *countingHandler {
	return &countingHandler{counts: map[string]*atomic.Int64{}}
}
func (h *countingHandler) bump(name string) {
	h.mu.Lock()
	c, ok := h.counts[name]
	if !ok {
		c = &atomic.Int64{}
		h.counts[name] = c
	}
	h.mu.Unlock()
	c.Add(1)
}
func (h *countingHandler) get(name string) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c, ok := h.counts[name]; ok {
		return c.Load()
	}
	return 0
}
func (h *countingHandler) WithTags(tags ...tmetrics.Tag) tmetrics.Handler {
	return &countingHandler{counts: h.counts, tags: append(append([]tmetrics.Tag{}, h.tags...), tags...)}
}
func (h *countingHandler) Counter(name string) tmetrics.CounterIface {
	return tmetrics.CounterFunc(func(int64, ...tmetrics.Tag) { h.bump(name) })
}
func (h *countingHandler) Gauge(name string) tmetrics.GaugeIface {
	return tmetrics.GaugeFunc(func(float64, ...tmetrics.Tag) { h.bump(name) })
}
func (h *countingHandler) Timer(name string) tmetrics.TimerIface {
	return tmetrics.TimerFunc(func(time.Duration, ...tmetrics.Tag) { h.bump(name) })
}
func (h *countingHandler) Histogram(name string, _ tmetrics.MetricUnit) tmetrics.HistogramIface {
	return tmetrics.HistogramFunc(func(int64, ...tmetrics.Tag) { h.bump(name) })
}
func (h *countingHandler) StartBatch(_ string) tmetrics.BatchHandler { return nil }
func (h *countingHandler) Stop(_ log.Logger)                         {}

var _ tmetrics.Handler = (*countingHandler)(nil)

// TestOp_FiresFullTripletOnSuccess covers the happy path:
// incremented counter + observed latency, no error counter.
func TestOp_FiresFullTripletOnSuccess(t *testing.T) {
	h := newCountingHandler()
	em := pluginmetrics.New(h).ForStore("fake-store")

	func() (retErr error) {
		defer observe.Op(em, "DoThing")(&retErr)
		return nil
	}()

	if n := h.get(pluginmetrics.NameOpCount); n != 1 {
		t.Errorf("op counter: got %d want 1", n)
	}
	if n := h.get(pluginmetrics.NameOpLatency); n != 1 {
		t.Errorf("latency: got %d want 1", n)
	}
	if n := h.get(pluginmetrics.NameOpErrors); n != 0 {
		t.Errorf("error counter should not fire on success: got %d", n)
	}
}

// TestOp_FiresErrorCounterOnFailure covers the failure path.
func TestOp_FiresErrorCounterOnFailure(t *testing.T) {
	h := newCountingHandler()
	em := pluginmetrics.New(h).ForStore("fake-store")

	func() (retErr error) {
		defer observe.Op(em, "DoThing")(&retErr)
		return errors.New("synthetic failure")
	}()

	if n := h.get(pluginmetrics.NameOpErrors); n != 1 {
		t.Errorf("error counter: got %d want 1", n)
	}
}

// TestOp_NilEmitter_NoOp proves the helper does not panic with a nil
// emitter (plain New callers take this path).
func TestOp_NilEmitter_NoOp(t *testing.T) {
	err := func() (retErr error) {
		defer observe.Op(nil, "DoThing")(&retErr)
		return errors.New("nil-safe")
	}()
	if err == nil || err.Error() != "nil-safe" {
		t.Errorf("nil-emitter wrapper must not swallow the error: got %v", err)
	}
}

// TestOp_TransactionSizeLimit_CountsSizeLimitExceeded: a write refused
// for size is visible as its own metric, not only as an error.
func TestOp_TransactionSizeLimit_CountsSizeLimitExceeded(t *testing.T) {
	h := newCountingHandler()
	em := pluginmetrics.New(h).ForStore("executionstore")
	func() (retErr error) {
		defer observe.Op(em, "AppendHistoryNodes")(&retErr)
		return fmt.Errorf("wrapped: %w", &persistence.TransactionSizeLimitError{Msg: "too big"})
	}()
	if n := h.get(pluginmetrics.NameSizeLimitExceeded); n != 1 {
		t.Errorf("size-limit counter: got %d want 1", n)
	}
	func() (retErr error) {
		defer observe.Op(em, "AppendHistoryNodes")(&retErr)
		return errors.New("other failure")
	}()
	if n := h.get(pluginmetrics.NameSizeLimitExceeded); n != 1 {
		t.Errorf("an ordinary error bumped the size-limit counter: %d", n)
	}
}
