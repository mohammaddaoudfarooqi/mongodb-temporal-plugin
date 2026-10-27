package metrics_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/common/log"
	tmetrics "go.temporal.io/server/common/metrics"

	pmetrics "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metrics"
)

// fakeHandler implements tmetrics.Handler and records every Record
// call as a distinct series keyed on (name, sorted tags).
type fakeHandler struct {
	mu     sync.Mutex
	series map[string]struct{}
	tags   []tmetrics.Tag
}

func newFakeHandler() *fakeHandler {
	return &fakeHandler{series: map[string]struct{}{}}
}

func (f *fakeHandler) track(name string, extra []tmetrics.Tag) {
	f.mu.Lock()
	defer f.mu.Unlock()
	all := append([]tmetrics.Tag{}, f.tags...)
	all = append(all, extra...)
	key := name
	for _, t := range all {
		key += "|" + t.Key + "=" + t.Value
	}
	f.series[key] = struct{}{}
}

func (f *fakeHandler) WithTags(tags ...tmetrics.Tag) tmetrics.Handler {
	clone := &fakeHandler{series: f.series, tags: append([]tmetrics.Tag{}, f.tags...)}
	clone.tags = append(clone.tags, tags...)
	return clone
}

func (f *fakeHandler) Counter(name string) tmetrics.CounterIface {
	return tmetrics.CounterFunc(func(v int64, extra ...tmetrics.Tag) { f.track(name, extra) })
}
func (f *fakeHandler) Gauge(name string) tmetrics.GaugeIface {
	return tmetrics.GaugeFunc(func(v float64, extra ...tmetrics.Tag) { f.track(name, extra) })
}
func (f *fakeHandler) Timer(name string) tmetrics.TimerIface {
	return tmetrics.TimerFunc(func(v time.Duration, extra ...tmetrics.Tag) { f.track(name, extra) })
}
func (f *fakeHandler) Histogram(name string, _ tmetrics.MetricUnit) tmetrics.HistogramIface {
	return tmetrics.HistogramFunc(func(v int64, extra ...tmetrics.Tag) { f.track(name, extra) })
}
func (f *fakeHandler) StartBatch(_ string) tmetrics.BatchHandler { return nil }
func (f *fakeHandler) Stop(_ log.Logger)                         {}

// TestEmitter_NilHandler_Nop guards the zero-value / nil-handler
// contract so store code can unconditionally call Emit methods.
func TestEmitter_NilHandler_Nop(t *testing.T) {
	e := pmetrics.New(nil)
	// None of these should panic.
	e.IncOp("op")
	e.ObserveLatency("op", 0)
	e.IncError("op", "cat")
	e.TxnStarted("op")
	e.TxnFailed("op")
	e.TxnLatency("op", time.Millisecond)
	e.SizeLimitExceeded("op")
	e.HealthStatus(true)
	e.AtlasSearchIndexReady(false)
}

// TestEmitter_ForStore_TagsStore covers the store pre-tagging path.
func TestEmitter_ForStore_TagsStore(t *testing.T) {
	h := newFakeHandler()
	e := pmetrics.New(h).ForStore("shardstore")
	e.IncOp("GetOrCreateShard")

	// Must have emitted exactly one series.
	if len(h.series) != 1 {
		t.Fatalf("want 1 series; got %d", len(h.series))
	}
	for k := range h.series {
		if k != "persistence_mongo_op_total|store=shardstore|operation=GetOrCreateShard" {
			t.Errorf("unexpected series: %q", k)
		}
	}
}

// TestEmitter_CardinalityBudget_Bounded + P5: even
// under 1000 distinct namespaces, the series count is bounded to
// (stores × operations), not O(N). The test emits per-namespace but
// asserts that namespace_id is NOT a tag.
func TestEmitter_CardinalityBudget_Bounded(t *testing.T) {
	h := newFakeHandler()
	e := pmetrics.New(h)

	stores := []string{"shardstore", "executionstore", "taskstore"}
	ops := []string{"Get", "Update", "Delete"}

	// Emit as if 1000 distinct namespaces hit each (store, op) combo.
	for i := 0; i < 1000; i++ {
		for _, s := range stores {
			for _, op := range ops {
				e.ForStore(s).IncOp(op)
			}
		}
	}

	want := len(stores) * len(ops) // 9
	if len(h.series) != want {
		t.Errorf("cardinality: got %d series, want %d — likely a per-namespace tag leaked in",
			len(h.series), want)
	}

	// Spot-check: series should never contain namespace_id.
	for k := range h.series {
		if containsFold(k, "namespace_id") {
			t.Errorf("cardinality violation: namespace_id in series %q", k)
		}
	}
}

// TestEmitter_IncError_CategoryTag verifies error counts carry the
// category bucket (used to label the errors-total counter for
// downstream dashboards).
func TestEmitter_IncError_CategoryTag(t *testing.T) {
	h := newFakeHandler()
	e := pmetrics.New(h).ForStore("shardstore")
	e.IncError("UpdateShard", "unavailable")
	e.IncError("UpdateShard", "internal")
	if len(h.series) != 2 {
		t.Fatalf("expected 2 distinct series (one per category); got %d", len(h.series))
	}
}

// containsFold is a tiny case-insensitive substring check.
func containsFold(s, sub string) bool {
	return len(s) >= len(sub) && stringContainsLower(toLower(s), toLower(sub))
}

func toLower(s string) string {
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		out[i] = c
	}
	return string(out)
}

func stringContainsLower(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

var _ = fmt.Sprintf

// TestEmitter_TxnMetrics_Recorded: the transaction metrics the runbook
// tells operators to watch are actually emitted.
func TestEmitter_TxnMetrics_Recorded(t *testing.T) {
	h := newFakeHandler()
	e := pmetrics.New(h)
	e.TxnStarted("op")
	e.TxnRetried("op")
	e.TxnFailed("op")
	e.TxnLatency("op", 5*time.Millisecond)
	for _, name := range []string{pmetrics.NameTxnStarted, pmetrics.NameTxnRetried, pmetrics.NameTxnFailed, pmetrics.NameTxnLatency} {
		if _, ok := h.series[name+"|"+pmetrics.TagOperation+"=op"]; !ok {
			t.Errorf("%s not recorded (series: %v)", name, h.series)
		}
	}
}
