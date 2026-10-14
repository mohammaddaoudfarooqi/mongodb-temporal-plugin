package executionstore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/server/common/log"
	tmetrics "go.temporal.io/server/common/metrics"

	pluginmetrics "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metrics"
)

// countingHandler is a minimal tmetrics.Handler that counts every
// Counter/Timer/Histogram emission by metric name. Enough to prove
// the metrics wire-up works end-to-end without a live Mongo.
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
	return &countingHandler{
		counts: h.counts,
		tags:   append([]tmetrics.Tag{}, append(h.tags, tags...)...),
	}
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

// TestObserveOp_EmitsMetricsEndToEnd: when a store is
// constructed with NewWithMetrics, every call to an observed public
// method bumps persistence_mongo_op_count + persistence_mongo_op_latency.
// A failure path additionally bumps persistence_mongo_errors.
//
// We drive the "nil request" early-return branch — enough to prove
// the observeOp deferred closure fires without needing a Mongo
// connection. Real traffic is covered by the integration matrix.
func TestObserveOp_EmitsMetricsEndToEnd(t *testing.T) {
	h := newCountingHandler()
	em := pluginmetrics.New(h)
	// Construct with nil db so the method body returns immediately
	// via the nil-check, but the observe helper still runs.
	s := NewWithMetrics(nil, "cluster0", em)

	// Create: nil req → immediate error, but metrics fired.
	_, err := s.CreateWorkflowExecution(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error on nil request")
	}

	if got := h.get(pluginmetrics.NameOpCount); got < 1 {
		t.Errorf("op count not incremented: got %d", got)
	}
	if got := h.get(pluginmetrics.NameOpLatency); got < 1 {
		t.Errorf("latency not observed: got %d", got)
	}
	if got := h.get(pluginmetrics.NameOpErrors); got < 1 {
		t.Errorf("error counter not incremented on nil-request path: got %d", got)
	}

	// Sanity: a nil-emitter store should not panic when instrumented
	// methods run. Proves observeOp's nil-safe path works.
	sNil := New(nil, "cluster0")
	if _, err := sNil.CreateWorkflowExecution(context.Background(), nil); err == nil {
		t.Fatal("expected error")
	}
}

// Ensure the countingHandler satisfies tmetrics.Handler. If the
// interface ever gains a method, this guard catches it.
var _ tmetrics.Handler = (*countingHandler)(nil)
