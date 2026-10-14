// Package metrics wraps Temporal's metrics.Handler so the plugin can
// emit its persistence_mongo_* series with a uniform set of base tags.
//
// Design and P5 (cardinality budget):
//   - Every metric carries only {operation, store} base tags.
//   - namespace_id, shard_id, workflow_id, run_id, run_id, etc. are
//     NEVER base tags — they go into logs and traces, not metrics,
//     because they would explode the time-series cardinality under
//     any realistic workload (thousands of namespaces × dozens of
//     ops × multiple stores).
//
// Cardinality budget test: for N distinct
// namespaces, the plugin produces (operations × stores × N) time
// series where N is a small constant (~1-2 per op from e.g.
// retry-outcome), not O(N). Enforced by a unit test that emits
// metrics for 1000 namespaces and asserts the series count is
// bounded.
package metrics

import (
	"time"

	tmetrics "go.temporal.io/server/common/metrics"
)

// Metric names (all prefixed persistence_mongo_*).
const (
	// Transactions lifecycle.
	NameTxnStarted = "persistence_mongo_transactions_started_total"
	NameTxnRetried = "persistence_mongo_transactions_retried_total"
	NameTxnFailed  = "persistence_mongo_transactions_failed_total"
	NameTxnLatency = "persistence_mongo_transaction_latency_ms"

	// Per-operation counters + latencies.
	NameOpCount   = "persistence_mongo_op_total"
	NameOpLatency = "persistence_mongo_op_latency_ms"
	NameOpErrors  = "persistence_mongo_errors_total"

	// Resource gauges.
	NamePoolSize    = "persistence_mongo_pool_size"
	NameSessionPool = "persistence_mongo_session_pool_size"

	// Size-limit hit.
	NameSizeLimitExceeded = "persistence_mongo_size_limit_exceeded_total"

	// Health probe.
	NameHealth = "persistence_mongo_health_status"

	// Atlas Search index readiness.
	NameAtlasSearchReady = "persistence_mongo_atlas_search_index_ready"
)

// Tag keys. Keep this list small by design to bound metric cardinality.
const (
	TagStore     = "store"
	TagOperation = "operation"
	// category classifies an error into a stable bucket from
	// internal/mongo.ErrCategory; keeps cardinality ≤ 6.
	TagErrorCategory = "category"
)

// Emitter is the plugin's convenience wrapper over a Temporal
// metrics.Handler. A zero-value Emitter is a no-op, so stores that
// haven't received a Handler yet can still call Emit methods without
// nil-checks at every call site.
type Emitter struct {
	h tmetrics.Handler
}

// New constructs an Emitter. A nil handler is accepted and becomes
// a no-op via the Noop handler.
func New(h tmetrics.Handler) *Emitter {
	if h == nil {
		h = tmetrics.NoopMetricsHandler
	}
	return &Emitter{h: h}
}

// ForStore returns a new Emitter pre-tagged with store=<name>. Stores
// construct one at init and reuse it across calls so the store tag
// does not get rebuilt per operation.
func (e *Emitter) ForStore(store string) *Emitter {
	return &Emitter{h: e.h.WithTags(tmetrics.Tag{Key: TagStore, Value: store})}
}

// IncOp increments the per-operation counter. Base tags: {store,
// operation}. Emit from every store method at entry.
func (e *Emitter) IncOp(op string) {
	e.h.Counter(NameOpCount).Record(1, tmetrics.Tag{Key: TagOperation, Value: op})
}

// ObserveLatency records an operation latency in milliseconds. Call
// with defer func() { emitter.ObserveLatency(op, time.Since(start)) }().
func (e *Emitter) ObserveLatency(op string, d time.Duration) {
	e.h.Histogram(NameOpLatency, tmetrics.Milliseconds).
		Record(d.Milliseconds(), tmetrics.Tag{Key: TagOperation, Value: op})
}

// IncError increments the errors counter. category is an
// internal/mongo.ErrCategory string ("unavailable", "internal", …)
// so the dashboard can pivot on error-kind without per-call-site
// mapping.
func (e *Emitter) IncError(op, category string) {
	e.h.Counter(NameOpErrors).Record(1,
		tmetrics.Tag{Key: TagOperation, Value: op},
		tmetrics.Tag{Key: TagErrorCategory, Value: category},
	)
}

// TxnStarted / TxnRetried / TxnFailed are emitted by the transaction
// wrapper. The driver's WithTransaction
// runs retries internally; the wrapper counts each retry so the
// operator can see driver-level churn.
func (e *Emitter) TxnStarted(op string) {
	e.h.Counter(NameTxnStarted).Record(1, tmetrics.Tag{Key: TagOperation, Value: op})
}

func (e *Emitter) TxnRetried(op string) {
	e.h.Counter(NameTxnRetried).Record(1, tmetrics.Tag{Key: TagOperation, Value: op})
}

func (e *Emitter) TxnFailed(op string) {
	e.h.Counter(NameTxnFailed).Record(1, tmetrics.Tag{Key: TagOperation, Value: op})
}

// SizeLimitExceeded bumps when an execution-doc write is rejected at
// the 12 MB payload budget.
func (e *Emitter) SizeLimitExceeded(op string) {
	e.h.Counter(NameSizeLimitExceeded).Record(1, tmetrics.Tag{Key: TagOperation, Value: op})
}

// HealthStatus sets the health gauge to 1 (healthy) or 0 (unhealthy).
func (e *Emitter) HealthStatus(healthy bool) {
	v := float64(0)
	if healthy {
		v = 1
	}
	e.h.Gauge(NameHealth).Record(v)
}

// AtlasSearchIndexReady toggles the gauge to 1 when the index is
// READY and 0 otherwise. Emitted from IndexTracker.pollLoop.
func (e *Emitter) AtlasSearchIndexReady(ready bool) {
	v := float64(0)
	if ready {
		v = 1
	}
	e.h.Gauge(NameAtlasSearchReady).Record(v)
}

// Handler returns the underlying Temporal handler so store code can
// pass it to upstream helpers that require it directly.
func (e *Emitter) Handler() tmetrics.Handler {
	return e.h
}
