// Tests for the MongoDB → serviceerror translator. Table-driven so
// every classification and overload branch is exercised and the
// coverage is easy to audit in review.
package mongo_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
	"go.temporal.io/api/serviceerror"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

// wantKind names a serviceerror category. Using stringly-typed want
// values keeps the test table compact and makes the assertion messages
// easier to read.
type wantKind string

const (
	wantNil               wantKind = "nil"
	wantUnavailable       wantKind = "Unavailable"
	wantInternal          wantKind = "Internal"
	wantInvalidArgument   wantKind = "InvalidArgument"
	wantResourceExhausted wantKind = "ResourceExhausted"
	wantNotFound          wantKind = "NotFound"
)

func TestTranslate_Table(t *testing.T) {
	tests := []struct {
		name string
		err  error
		op   string
		want wantKind
	}{
		{"nil-passes-through", nil, "Get", wantNil},
		{"ErrNoDocuments-notfound", mongo.ErrNoDocuments, "GetShard", wantNotFound},
		{"context-canceled-unavailable", context.Canceled, "Get", wantUnavailable},
		{"context-deadline-unavailable", context.DeadlineExceeded, "Get", wantUnavailable},
		{
			"server-shutdown-unavailable",
			mongo.CommandError{Code: 91, Message: "ShutdownInProgress"},
			"Update",
			wantUnavailable,
		},
		{
			"not-primary-unavailable",
			mongo.CommandError{Code: 13435, Message: "NotPrimaryNoSecondaryOk"},
			"Read",
			wantUnavailable,
		},
		{
			"duplicate-key-invalid-argument",
			mongo.CommandError{Code: 11000, Message: "E11000 duplicate key"},
			"Create",
			wantInvalidArgument,
		},
		{
			"command-other-internal",
			mongo.CommandError{Code: 9999, Message: "unexpected"},
			"Update",
			wantInternal,
		},
		{
			"transient-label-unavailable",
			labeledErr{labels: []string{"TransientTransactionError"}, inner: errors.New("boom")},
			"Update",
			wantUnavailable,
		},
		{
			"unknown-commit-label-unavailable",
			labeledErr{labels: []string{"UnknownTransactionCommitResult"}, inner: errors.New("boom")},
			"Update",
			wantUnavailable,
		},
		{
			"bare-error-internal",
			errors.New("some driver glitch"),
			"Delete",
			wantInternal,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := internalmongo.Translate(tc.err, tc.op)
			assertKind(t, tc.want, got)
		})
	}
}

// TestTranslate_RateLimitResourceExhausted: a rate-limit
// signal from the driver maps to ResourceExhausted so upstream retry
// wrappers engage. We simulate the signal via the pool-error code 262.
func TestTranslate_RateLimitResourceExhausted(t *testing.T) {
	err := mongo.CommandError{Code: 262, Message: "rate limit"}
	got := internalmongo.Translate(err, "EnqueueMessage")
	assertKind(t, wantResourceExhausted, got)
}

// TestTranslate_PoolExhaustionResourceExhausted's
// connection-pool branch: a "pool cleared" or waiter-timeout error is
// ResourceExhausted from the caller's point of view.
func TestTranslate_PoolExhaustionResourceExhausted(t *testing.T) {
	err := poolTimeoutErr{}
	got := internalmongo.Translate(err, "Query")
	assertKind(t, wantResourceExhausted, got)
}

// TestTranslate_WrappedError exercises error unwrapping: if a caller
// wrapped the driver error with fmt.Errorf, the translator still
// classifies it correctly.
func TestTranslate_WrappedError(t *testing.T) {
	base := mongo.CommandError{Code: 91, Message: "ShutdownInProgress"}
	wrapped := errors.Join(errors.New("update workflow"), base)
	got := internalmongo.Translate(wrapped, "Update")
	assertKind(t, wantUnavailable, got)
}

// TestCategorize_NilReturnsUnknown locks the contract that the
// metrics label for a nil error is "unknown" — not silently empty —
// so downstream dashboards don't collapse nil-errors into any other bucket.
func TestCategorize_NilReturnsUnknown(t *testing.T) {
	if cat := internalmongo.Categorize(nil); cat != internalmongo.CatUnknown {
		t.Errorf("expected CatUnknown for nil; got %q", cat)
	}
}

// TestTranslate_AlternatePoolMessage covers the "connection pool …
// timeout" string variant that older drivers emit.
func TestTranslate_AlternatePoolMessage(t *testing.T) {
	err := errors.New("connection pool checkout timeout exceeded")
	got := internalmongo.Translate(err, "Query")
	assertKind(t, wantResourceExhausted, got)
}

// ---- test helpers ----

func assertKind(t *testing.T, want wantKind, got error) {
	t.Helper()
	if want == wantNil {
		if got != nil {
			t.Fatalf("expected nil error; got %T: %v", got, got)
		}
		return
	}
	if got == nil {
		t.Fatalf("expected %s; got nil", want)
	}
	switch want {
	case wantUnavailable:
		if _, ok := got.(*serviceerror.Unavailable); !ok {
			t.Fatalf("expected Unavailable; got %T: %v", got, got)
		}
	case wantInternal:
		if _, ok := got.(*serviceerror.Internal); !ok {
			t.Fatalf("expected Internal; got %T: %v", got, got)
		}
	case wantInvalidArgument:
		if _, ok := got.(*serviceerror.InvalidArgument); !ok {
			t.Fatalf("expected InvalidArgument; got %T: %v", got, got)
		}
	case wantResourceExhausted:
		if _, ok := got.(*serviceerror.ResourceExhausted); !ok {
			t.Fatalf("expected ResourceExhausted; got %T: %v", got, got)
		}
	case wantNotFound:
		if _, ok := got.(*serviceerror.NotFound); !ok {
			t.Fatalf("expected NotFound; got %T: %v", got, got)
		}
	}
}

// labeledErr simulates a driver error that satisfies mongo.LabeledError
// without pulling in a full CommandError. Lets us exercise the label
// branch in isolation.
type labeledErr struct {
	labels []string
	inner  error
}

func (e labeledErr) Error() string { return e.inner.Error() }
func (e labeledErr) HasErrorLabel(s string) bool {
	for _, l := range e.labels {
		if l == s {
			return true
		}
	}
	return false
}

// poolTimeoutErr simulates a WaitQueueTimeout error without importing
// the driver's internal topology package.
type poolTimeoutErr struct{}

func (poolTimeoutErr) Error() string { return "connection pool wait queue timeout" }

// TestCategorize_TransientDriverErrors_Unavailable:
// every transient failure mode seen during elections, network blips and
// write-concern timeouts must reach Temporal as Unavailable (retryable),
// never Internal.
func TestCategorize_TransientDriverErrors_Unavailable(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"network-label", mongo.CommandError{Message: "connection reset", Labels: []string{"NetworkError"}}},
		{"retryable-write-label", mongo.CommandError{Code: 0, Labels: []string{"RetryableWriteError"}}},
		{"server-selection", topology.ServerSelectionError{Desc: description.Topology{}, Wrapped: errors.New("no primary")}},
		{"max-time-ms-expired", mongo.CommandError{Code: 50, Name: "MaxTimeMSExpired"}},
		{"write-concern-shutdown", mongo.WriteException{WriteConcernError: &mongo.WriteConcernError{Code: 91, Name: "ShutdownInProgress"}}},
		{"write-concern-stepdown", mongo.WriteException{WriteConcernError: &mongo.WriteConcernError{Code: 189}}},
		{"write-concern-repl-change", mongo.WriteException{WriteConcernError: &mongo.WriteConcernError{Code: 11602}}},
		{"write-concern-timeout", mongo.WriteException{WriteConcernError: &mongo.WriteConcernError{Code: 64, Name: "WriteConcernFailed"}}},
		{"client-disconnected", mongo.ErrClientDisconnected},
		{"wrapped-network", fmt.Errorf("op: %w", mongo.CommandError{Labels: []string{"NetworkError"}})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := internalmongo.Categorize(tc.err); got != internalmongo.CatUnavailable {
				t.Fatalf("Categorize(%v) = %s; want unavailable", tc.err, got)
			}
			var u *serviceerror.Unavailable
			if !errors.As(internalmongo.Translate(tc.err, "op"), &u) {
				t.Fatalf("Translate should yield *serviceerror.Unavailable")
			}
		})
	}
}

// TestCategorize_WriteExceptionDuplicateKey_StillInvalidArgument pins
// duplicate-key classification alongside the new write-concern handling.
func TestCategorize_WriteExceptionDuplicateKey_StillInvalidArgument(t *testing.T) {
	err := mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000, Message: "E11000"}}}
	if got := internalmongo.Categorize(err); got != internalmongo.CatInvalidArgument {
		t.Fatalf("got %s; want invalid_argument", got)
	}
}
