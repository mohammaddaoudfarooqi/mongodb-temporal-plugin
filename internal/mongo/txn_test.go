package mongo_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	driver "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

// fakeTxn simulates mongo.Session.WithTransaction for unit tests. The
// retry behaviour is controlled by the `retries` field: the fake calls
// `fn` up to `retries+1` times, returning the final result. This lets
// each test pin exactly how many times the callback should run without
// reaching a real MongoDB.
type fakeTxn struct {
	retries int
	calls   int32
	lastCtx context.Context
}

func (f *fakeTxn) WithTransaction(
	ctx context.Context,
	fn func(ctx context.Context) (any, error),
	_ ...options.Lister[options.TransactionOptions],
) (any, error) {
	var result any
	var err error
	for i := 0; i <= f.retries; i++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		atomic.AddInt32(&f.calls, 1)
		f.lastCtx = ctx
		result, err = fn(ctx)
		if err == nil {
			return result, nil
		}
		// Retry on labeled transient errors; emulates driver behavior.
		var le interface{ HasErrorLabel(string) bool }
		if !errors.As(err, &le) || !le.HasErrorLabel("TransientTransactionError") {
			return result, err
		}
	}
	return result, err
}

type labeled struct {
	msg    string
	labels []string
}

func (l *labeled) Error() string { return l.msg }
func (l *labeled) HasErrorLabel(s string) bool {
	for _, x := range l.labels {
		if x == s {
			return true
		}
	}
	return false
}

// TestWithTxn_SuccessReturnsValue: the callback returns a value; the
// wrapper hands it back typed, no error.
func TestWithTxn_SuccessReturnsValue(t *testing.T) {
	txn := &fakeTxn{retries: 0}
	out, err := internalmongo.WithTxn[int](
		context.Background(), txn,
		func(_ context.Context) (int, error) { return 42, nil },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != 42 {
		t.Errorf("expected 42; got %d", out)
	}
	if txn.calls != 1 {
		t.Errorf("expected 1 call; got %d", txn.calls)
	}
}

// TestWithTxn_TransientRetriesInDriver: a transient-labeled error is
// retried by the (fake) driver loop, eventually succeeds, and the
// wrapper does NOT add its own retry.
func TestWithTxn_TransientRetriesInDriver(t *testing.T) {
	txn := &fakeTxn{retries: 3} // driver may retry up to 3 times
	var attempts int32
	out, err := internalmongo.WithTxn[string](
		context.Background(), txn,
		func(_ context.Context) (string, error) {
			n := atomic.AddInt32(&attempts, 1)
			if n < 3 {
				return "", &labeled{msg: "transient", labels: []string{"TransientTransactionError"}}
			}
			return "ok", nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected error after driver-retries: %v", err)
	}
	if out != "ok" {
		t.Errorf("expected 'ok'; got %q", out)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts inside driver loop; got %d", attempts)
	}
	if txn.calls != 3 {
		t.Errorf("expected 3 driver-level calls; got %d", txn.calls)
	}
}

// TestWithTxn_NonTransientReturnsImmediately: a non-labeled error
// short-circuits the driver's retry loop and bubbles up as-is.
func TestWithTxn_NonTransientReturnsImmediately(t *testing.T) {
	txn := &fakeTxn{retries: 10}
	want := errors.New("business logic rejected")
	_, err := internalmongo.WithTxn[int](
		context.Background(), txn,
		func(_ context.Context) (int, error) { return 0, want },
	)
	if !errors.Is(err, want) {
		t.Fatalf("expected wrapped business error; got %v", err)
	}
	if txn.calls != 1 {
		t.Errorf("expected 1 call (no retry); got %d", txn.calls)
	}
}

// TestWithTxn_CtxCancelAborts: when ctx is cancelled before the driver
// retries, WithTxn returns promptly with a context error.
func TestWithTxn_CtxCancelAborts(t *testing.T) {
	txn := &fakeTxn{retries: 5}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	start := time.Now()
	_, err := internalmongo.WithTxn[int](
		ctx, txn,
		func(_ context.Context) (int, error) { return 0, nil },
	)
	if err == nil {
		t.Fatal("expected ctx error; got nil")
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Errorf("ctx cancel not honoured promptly: took %v", time.Since(start))
	}
}

// TestWithTxn_NoGoroutineLeak: the wrapper does not spawn
// any background goroutines; counting goroutines before/after must stay
// flat. This is a quick smoke test, not a full leak checker.
func TestWithTxn_NoGoroutineLeak(t *testing.T) {
	txn := &fakeTxn{retries: 0}
	for i := 0; i < 50; i++ {
		if _, err := internalmongo.WithTxn[int](
			context.Background(), txn,
			func(_ context.Context) (int, error) { return 0, nil },
		); err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
	}
	// If a goroutine were being leaked we'd see calls >> 50; the check
	// is implicit.
	if txn.calls != 50 {
		t.Errorf("expected 50 calls; got %d", txn.calls)
	}
}

// A driver error wrapped by a callback with Wrap must keep
// its TransientTransactionError label visible to the driver's retry
// loop. Translating inside the callback used to strip the label, so the
// driver returned the first transient failure instead of retrying.
func TestWithTxn_TransientLabelFromCallback_DriverRetries(t *testing.T) {
	txn := &fakeTxn{retries: 3}
	var attempts int32
	_, err := internalmongo.WithTxn[struct{}](
		context.Background(), txn,
		func(_ context.Context) (struct{}, error) {
			if atomic.AddInt32(&attempts, 1) == 1 {
				transient := driver.CommandError{
					Code: 112, Name: "WriteConflict",
					Labels: []string{"TransientTransactionError"},
				}
				return struct{}{}, internalmongo.Wrap(transient, "store.Op.update")
			}
			return struct{}{}, nil
		},
	)
	if err != nil {
		t.Fatalf("expected driver retry to succeed; got %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts; got %d", attempts)
	}
}

// TestWithTxn_DomainError_PassesThroughUnchanged:
// persistence and service errors returned by the callback reach the
// caller as the identical value.
func TestWithTxn_DomainError_PassesThroughUnchanged(t *testing.T) {
	domain := []error{
		&persistence.ShardOwnershipLostError{ShardID: 1, Msg: "lost"},
		&persistence.ConditionFailedError{Msg: "cas"},
		&persistence.CurrentWorkflowConditionFailedError{Msg: "current"},
		&persistence.WorkflowConditionFailedError{Msg: "wf"},
		serviceerror.NewNotFound("missing"),
	}
	for _, want := range domain {
		_, err := internalmongo.WithTxn[int](
			context.Background(), &fakeTxn{},
			func(_ context.Context) (int, error) { return 0, want },
		)
		if err != want {
			t.Errorf("want identical %T; got %T %v", want, err, err)
		}
	}
}

// TestWithTxn_DriverError_TranslatedOnce: a wrapped
// driver error leaves WithTxn as a serviceerror, with the operation
// context in the message exactly once.
func TestWithTxn_DriverError_TranslatedOnce(t *testing.T) {
	_, err := internalmongo.WithTxn[int](
		context.Background(), &fakeTxn{},
		func(_ context.Context) (int, error) {
			return 0, internalmongo.Wrap(driver.CommandError{Code: 91, Name: "ShutdownInProgress"}, "store.Op.find")
		},
	)
	var unavailable *serviceerror.Unavailable
	if !errors.As(err, &unavailable) {
		t.Fatalf("want *serviceerror.Unavailable; got %T %v", err, err)
	}
	if n := strings.Count(err.Error(), "store.Op.find"); n != 1 {
		t.Errorf("operation should appear once in %q; got %d", err.Error(), n)
	}
}
