package nexusstore

import "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/observe"

func (s *Stub) observeOp(op string) func(*error) {
	return observe.Op(s.emitter, op)
}
