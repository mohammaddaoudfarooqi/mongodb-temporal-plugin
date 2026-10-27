package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

// TxnRunner is the narrow interface WithTxn depends on. It matches the
// signature of *mongo.Session.WithTransaction exactly, so the real driver
// session satisfies it (via the adapter in sessionAdapter) and unit tests
// can substitute a fake. Keeping this surface minimal prevents the
// wrapper from sneaking business logic into the transaction-management
// path — the spec explicitly requires that retries stay
// inside the driver.
type TxnRunner interface {
	WithTransaction(
		ctx context.Context,
		fn func(ctx context.Context) (any, error),
		opts ...options.Lister[options.TransactionOptions],
	) (any, error)
}

// WithTxn runs `fn` inside a MongoDB transaction, delegating retry of
// transient errors (label TransientTransactionError) and
// commit-uncertain errors (UnknownTransactionCommitResult) to the
// driver's WithTransaction loop. THE PLUGIN SHALL NOT implement its own
// retry loop — doing so would risk double-apply
// of non-idempotent writes under primary failover.
//
// Error boundary: callbacks return driver
// errors untranslated (optionally annotated with Wrap) so the driver can
// read their labels. WithTxn translates driver errors exactly once after
// the driver loop finishes; persistence and service errors pass through
// unchanged.
//
// Typed return: the callback yields a T and WithTxn passes it back
// directly. A ctx cancel before the first attempt short-circuits the
// call so caller-side timeouts propagate promptly.
func WithTxn[T any](
	ctx context.Context,
	runner TxnRunner,
	fn func(ctx context.Context) (T, error),
	opts ...options.Lister[options.TransactionOptions],
) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	raw, err := runner.WithTransaction(ctx,
		func(c context.Context) (any, error) { return fn(c) },
		opts...,
	)
	if err != nil {
		err = TranslateDriverError(err)
		if raw == nil {
			return zero, err
		}
		// The driver may still hand back the callback's last value;
		// returning it alongside the error lets callers that care
		// recover partial results. Most stores discard `out` on error.
		typed, _ := raw.(T)
		return typed, err
	}
	typed, ok := raw.(T)
	if !ok {
		return zero, err
	}
	return typed, nil
}

// TxnObserver receives transaction lifecycle events; *metrics.Emitter
// implements it.
type TxnObserver interface {
	TxnStarted(op string)
	TxnRetried(op string)
	TxnFailed(op string)
	TxnLatency(op string, d time.Duration)
}

// ObserveTxns reports runner's transactions to obs under op. Every
// callback run after the first is a retry: the driver re-runs the
// callback on TransientTransactionError, which includes write conflicts.
// It only counts; the driver's loop still does the retrying. A nil obs
// returns runner unchanged; pass an untyped nil, not a nil *Emitter
// held in the interface, which is not nil.
func ObserveTxns(runner TxnRunner, obs TxnObserver, op string) TxnRunner {
	if obs == nil {
		return runner
	}
	return observedRunner{r: runner, obs: obs, op: op}
}

type observedRunner struct {
	r   TxnRunner
	obs TxnObserver
	op  string
}

func (o observedRunner) WithTransaction(
	ctx context.Context,
	fn func(ctx context.Context) (any, error),
	opts ...options.Lister[options.TransactionOptions],
) (any, error) {
	o.obs.TxnStarted(o.op)
	start, runs := time.Now(), 0
	out, err := o.r.WithTransaction(ctx, func(c context.Context) (any, error) {
		if runs++; runs > 1 {
			o.obs.TxnRetried(o.op)
		}
		return fn(c)
	}, opts...)
	o.obs.TxnLatency(o.op, time.Since(start))
	if err != nil {
		o.obs.TxnFailed(o.op)
	}
	return out, err
}

// NewSessionRunner adapts a live *mongo.Session into the TxnRunner
// interface so the real driver code path can plug in. This adapter is
// trivial; integration tests cover it end-to-end.
func NewSessionRunner(s *mongo.Session) TxnRunner {
	return sessionAdapter{s: s}
}

type sessionAdapter struct {
	s *mongo.Session
}

func (a sessionAdapter) WithTransaction(
	ctx context.Context,
	fn func(ctx context.Context) (any, error),
	opts ...options.Lister[options.TransactionOptions],
) (any, error) {
	if len(opts) == 0 {
		opts = []options.Lister[options.TransactionOptions]{txnOptionsLister()}
	}
	return a.s.WithTransaction(ctx, fn, opts...)
}

// Wrap adds operation context to a driver error without translating it.
// Use inside WithTxn callbacks: the driver's retry loop inspects error
// labels (TransientTransactionError) through errors.As, so the original
// driver error must stay in the chain until WithTxn returns.
func Wrap(err error, operation string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}

// TranslateDriverError converts a driver error (possibly annotated with
// Wrap) into a serviceerror. Anything else — persistence conditions,
// service errors, plain business errors — is returned as-is. WithTxn
// applies it on exit; helpers shared between transactional and plain
// call paths use Wrap and their non-transactional callers apply this.
func TranslateDriverError(err error) error {
	if !isDriverError(err) {
		return err
	}
	return Translate(err, "txn")
}

// isDriverError reports whether err originates in the MongoDB driver or
// the context the driver ran under.
func isDriverError(err error) bool {
	var se mongo.ServerError
	var le mongo.LabeledError
	var sse topology.ServerSelectionError
	var ce topology.ConnectionError
	switch {
	case errors.As(err, &se), errors.As(err, &le),
		errors.As(err, &sse), errors.As(err, &ce):
		return true
	case errors.Is(err, mongo.ErrNoDocuments),
		errors.Is(err, mongo.ErrClientDisconnected),
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		return true
	}
	return false
}
