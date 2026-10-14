package queuestore

import "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/observe"

// observeOp wraps observe.Op for the legacy Queue store (nil-safe).
func (s *Stub) observeOp(op string) func(*error) {
	return observe.Op(s.emitter, op)
}

// observeOpV2 wraps observe.Op for the QueueV2 store (nil-safe).
func (v *V2Stub) observeOpV2(op string) func(*error) {
	return observe.Op(v.emitter, op)
}
