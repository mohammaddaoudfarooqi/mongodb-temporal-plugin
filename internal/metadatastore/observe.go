package metadatastore

import "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/observe"

// observeOp wraps observe.Op with this store's emitter (nil-safe).
func (s *Stub) observeOp(op string) func(*error) {
	return observe.Op(s.emitter, op)
}
