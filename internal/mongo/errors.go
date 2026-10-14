package mongo

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
)

// ErrCategory names the operational classification the translator assigns
// to a Mongo error. Exported so the metrics layer can emit
// an errors-total counter labeled by category without re-classifying.
type ErrCategory string

const (
	CatUnavailable       ErrCategory = "unavailable"
	CatInternal          ErrCategory = "internal"
	CatInvalidArgument   ErrCategory = "invalid_argument"
	CatResourceExhausted ErrCategory = "resource_exhausted"
	CatNotFound          ErrCategory = "not_found"
	CatUnknown           ErrCategory = "unknown"
)

// Categorize returns the classification for err. Used by the metrics
// layer; the translator itself only needs it internally to build the
// right serviceerror.
func Categorize(err error) ErrCategory {
	if err == nil {
		return CatUnknown
	}
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		return CatNotFound
	case errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		return CatUnavailable
	case isPoolExhausted(err):
		return CatResourceExhausted
	case isRateLimit(err):
		return CatResourceExhausted
	case isRetryableServerError(err):
		return CatUnavailable
	case hasTransientLabel(err):
		return CatUnavailable
	case isTransientTransport(err):
		return CatUnavailable
	case isDuplicateKey(err):
		return CatInvalidArgument
	case isCommandError(err):
		// CommandError with an unrecognised code is a programming issue
		// from the plugin's point of view (unexpected from the server);
		// surface it as Internal so it shows up in alerts, not as a
		// retryable.
		return CatInternal
	default:
		return CatInternal
	}
}

// Translate converts a MongoDB driver error into a Temporal serviceerror
// value suitable for returning to the manager layer. operation is a
// short human name (e.g. "UpdateWorkflowExecution") included in the
// error message to aid log triage.
//
// The category label matches Categorize, no path panics, a ctx
// deadline is surfaced as Unavailable so Temporal's retry wrappers see
// it, and overload signals map to ResourceExhausted / Unavailable.
func Translate(err error, operation string) error {
	if err == nil {
		return nil
	}
	cat := Categorize(err)
	msg := fmt.Sprintf("mongo: %s: %s", operation, err.Error())

	switch cat {
	case CatNotFound:
		return serviceerror.NewNotFound(msg)
	case CatUnavailable:
		return serviceerror.NewUnavailable(msg)
	case CatInvalidArgument:
		return serviceerror.NewInvalidArgument(msg)
	case CatResourceExhausted:
		return serviceerror.NewResourceExhausted(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT,
			msg,
		)
	default:
		return serviceerror.NewInternal(msg)
	}
}

// --- internal classification predicates ---

func isCommandError(err error) bool {
	var ce mongo.CommandError
	return errors.As(err, &ce)
}

// isDuplicateKey checks the driver's IsDuplicateKeyError helper (handles
// both CommandError and WriteError / BulkWriteException variants).
func isDuplicateKey(err error) bool {
	return mongo.IsDuplicateKeyError(err)
}

// isRetryableServerError maps server-reported codes that indicate the
// primary is stepping down, the deployment is shutting down, or the
// replica set topology is in flux. All of these are transient from the
// caller's point of view — a retry against a fresh primary succeeds.
// Code list comes from the Temporal + MongoDB retryable-writes spec.
func isRetryableServerError(err error) bool {
	var ce mongo.CommandError
	if !errors.As(err, &ce) {
		return false
	}
	// Code list: curated from MongoDB server source
	// (src/mongo/s/is_mongos.cpp + retryable writes spec). Keep in
	// numeric-sorted order for easy review.
	retryable := []int32{
		6,     // HostUnreachable
		7,     // HostNotFound
		64,    // WriteConcernFailed (retryable subset)
		89,    // NetworkTimeout
		91,    // ShutdownInProgress
		189,   // PrimarySteppedDown
		262,   // ExceededTimeLimit  (handled as rate-limit too; see below)
		9001,  // SocketException
		10107, // NotWritablePrimary / NotMaster (legacy alias)
		11600, // InterruptedAtShutdown
		11602, // InterruptedDueToReplStateChange
		13435, // NotPrimaryNoSecondaryOk
		13436, // NotPrimaryOrSecondary
		14,    // UnknownError (catch-all some older servers use)
		134,   // ReadConcernMajorityNotAvailableYet
		133,   // FailedToSatisfyReadPreference
	}
	// Exclude 262 from this branch — it's classified as rate-limit above.
	for _, code := range retryable {
		if code == 262 {
			continue
		}
		if ce.HasErrorCode(int(code)) {
			return true
		}
	}
	// String match as a belt-and-suspenders for servers that return
	// code 0 with a descriptive codeName (e.g., older Mongos variants).
	msg := ce.Message
	return strings.Contains(msg, "ShutdownInProgress") ||
		strings.Contains(msg, "NotPrimaryNoSecondaryOk") ||
		strings.Contains(msg, "PrimarySteppedDown")
}

// isPoolExhausted checks for connection-pool wait-queue timeouts and
// related exhaustion signals. The driver's IsTimeout covers some paths;
// we add a message-match heuristic for custom stub errors in tests.
func isPoolExhausted(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, "wait queue timeout") ||
		strings.Contains(msg, "pool exhausted") ||
		strings.Contains(msg, "connection pool") && strings.Contains(msg, "timeout") {
		return true
	}
	return false
}

// isRateLimit handles the server-side rate-limit response. Mongos uses
// code 262 (ExceededTimeLimit) when slow-query governance kicks in.
func isRateLimit(err error) bool {
	var ce mongo.CommandError
	if !errors.As(err, &ce) {
		return false
	}
	return ce.HasErrorCode(262)
}

// hasTransientLabel covers the driver's labeled errors. The transaction
// wrapper relies on the driver's retry loop for TransientTransactionError
// and UnknownTransactionCommitResult, but when those labels
// escape the wrapper (e.g., the transaction exhausted its retry budget),
// the caller should see them as Unavailable so the upstream manager's
// own retry can engage.
func hasTransientLabel(err error) bool {
	var le mongo.LabeledError
	if errors.As(err, &le) {
		return le.HasErrorLabel("TransientTransactionError") ||
			le.HasErrorLabel("UnknownTransactionCommitResult")
	}
	// Accept duck-typed labeled errors (tests, stub transports).
	type labeled interface {
		HasErrorLabel(string) bool
	}
	var lbl labeled
	if errors.As(err, &lbl) {
		return lbl.HasErrorLabel("TransientTransactionError") ||
			lbl.HasErrorLabel("UnknownTransactionCommitResult")
	}
	return false
}

// retryableWriteConcernCodes are write-concern error codes that mean the
// write may not have replicated because the primary is going away or
// the majority acknowledgement timed out. A retry against the next
// primary resolves them.
var retryableWriteConcernCodes = map[int]bool{
	64:    true, // WriteConcernFailed (wtimeout)
	91:    true, // ShutdownInProgress
	189:   true, // PrimarySteppedDown
	11600: true, // InterruptedAtShutdown
	11602: true, // InterruptedDueToReplStateChange
}

// isTransientTransport covers failures that never reached a usable
// server or were cut short by one: network errors, server selection
// (no primary during an election), MaxTimeMS expiry, write-concern
// errors from a departing primary, the RetryableWriteError label, and a
// client that is already disconnected.
func isTransientTransport(err error) bool {
	if mongo.IsNetworkError(err) || errorHasLabel(err, "RetryableWriteError") {
		return true
	}
	if errors.Is(err, mongo.ErrClientDisconnected) {
		return true
	}
	var sse topology.ServerSelectionError
	if errors.As(err, &sse) {
		return true
	}
	var ce mongo.CommandError
	if errors.As(err, &ce) && ce.Code == 50 {
		return true
	}
	var we mongo.WriteException
	if errors.As(err, &we) && we.WriteConcernError != nil {
		return retryableWriteConcernCodes[we.WriteConcernError.Code]
	}
	return false
}

func errorHasLabel(err error, label string) bool {
	var le mongo.LabeledError
	return errors.As(err, &le) && le.HasErrorLabel(label)
}
