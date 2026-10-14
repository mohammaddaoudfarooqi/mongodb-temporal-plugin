package executionstore

import (
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/observe"
)

// observeOp delegates to the shared observe.Op helper. The returned
// closure accepts a *error so call-sites use the single-line defer
// pattern: `defer s.observeOp("X")(&retErr)`.
//
// Nil-safe — when the emitter isn't wired (plain New(...)), the
// returned closure is a no-op.
func (s *Stub) observeOp(op string) func(*error) {
	return observe.Op(s.emitter, op)
}
