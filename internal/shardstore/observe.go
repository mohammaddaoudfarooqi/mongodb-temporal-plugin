package shardstore

import "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/observe"

// observeOp wraps observe.Op with this store's emitter (nil-safe).
// Usage:
//
//	func (s *Stub) Foo(...) (retErr error) {
//	    defer s.observeOp("Foo")(&retErr)
//	    ...
//	}
func (s *Stub) observeOp(op string) func(*error) {
	return observe.Op(s.emitter, op)
}
