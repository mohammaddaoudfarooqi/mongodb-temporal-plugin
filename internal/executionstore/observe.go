package executionstore

import (
	"go.mongodb.org/mongo-driver/v2/mongo"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
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

// txnRunner is the session's transaction runner, reporting to the
// store's emitter (transactions started, retried and failed, and their
// latency) when one is wired.
func (s *Stub) txnRunner(sess *mongo.Session, op string) internalmongo.TxnRunner {
	r := internalmongo.NewSessionRunner(sess)
	if s.emitter == nil {
		return r
	}
	return internalmongo.ObserveTxns(r, s.emitter, op)
}
