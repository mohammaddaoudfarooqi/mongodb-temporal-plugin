// Package observe is a shared operation-observer helper used by every
// store in internal/*/. It wraps the per-store metrics.Emitter so the
// call-site pattern is one line (`defer observe.Op(e, "X")(err)`) and
// the emission surface is uniform across stores.
//
// Why a shared package:
//  1. Single source of truth for the persistence_mongo_op_* triplet
//     (counter + latency + error classification). A convention that
//     lives in one file is easier to audit than eight copies.
//  2. Lets `ci/scripts/lint-observe.sh` grep every store for the
//     call to catch a regression where a new method forgets to
//     observe itself.
//
// Nil-safe: the emitter argument may be nil when callers construct
// via the plain New(...) (no metrics). The returned closure then
// no-ops, so tests that never wire metrics pay zero overhead.
package observe

import (
	"errors"
	"time"

	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metrics"
	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

// Op records an operation's start, returning a closure that finalises
// it (latency + optional error category) on completion. Use with
// defer:
//
//	func (s *Stub) Foo(ctx context.Context, ...) (retErr error) {
//	    defer observe.Op(s.emitter, "Foo")(&retErr)
//	    ...
//	}
//
// The closure takes *error so the deferred call sees the final
// returned value even after defer-time adjustments.
func Op(e *metrics.Emitter, op string) func(*error) {
	if e == nil {
		return func(*error) {}
	}
	e.IncOp(op)
	start := time.Now()
	return func(errPtr *error) {
		e.ObserveLatency(op, time.Since(start))
		if errPtr != nil && *errPtr != nil {
			e.IncError(op, string(internalmongo.Categorize(*errPtr)))
			var tooBig *persistence.TransactionSizeLimitError
			if errors.As(*errPtr, &tooBig) {
				e.SizeLimitExceeded(op)
			}
		}
	}
}
