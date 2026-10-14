# ADR-013: Error classification into serviceerror tiers

- Status: Accepted
- Date: 2026-04-30
- Deciders: plugin architecture review

## Context

Temporal's upper layers treat errors differently based on their
`serviceerror` type:

- `Unavailable` → retry with backoff.
- `Internal` → fail the request; log.
- `InvalidArgument` → do not retry.
- `NotFound` → specific handling per caller.
- `ResourceExhausted` → retry with caution; back off the caller.

The MongoDB driver returns a mix of error types: `mongo.CommandError`
with server codes, `topology.ServerSelectionError`, Go's own
`context.DeadlineExceeded`, and more. The plugin must map these to
the right `serviceerror` tier for upstream to do the right thing.

## Decision

`internal/mongo/errors.go` holds the single classification table:
`Categorize(err)` assigns an error category and
`Translate(err, operation)` turns it into the matching `serviceerror`.
The mapping is:

| Driver / Mongo signal                              | Upstream tier        |
|----------------------------------------------------|----------------------|
| `context.DeadlineExceeded`, server selection, 189 (`PrimarySteppedDown`), 91 (`ShutdownInProgress`), `TransientTransactionError` label | `Unavailable`        |
| Mongo codes 50 (`MaxTimeMSExpired`), 251 (`NoSuchTransaction`), write concern errors post-commit | `Unavailable`        |
| 16500 (`CursorKilled`), pool wait-queue timeout    | `ResourceExhausted`  |
| 11000 duplicate key on unique index                | caller-specific (`NotFound` for shard CAS, `InvalidArgument` for dup metadata) |
| Validation errors (bad BSON, unknown option)       | `InvalidArgument`    |
| Everything else                                    | `Internal`           |

Every store funnels its errors through `Translate`; no ad-hoc
classification elsewhere. Inside a transaction callback, stores
wrap the driver error with `Wrap` instead, which keeps the
`TransientTransactionError` label visible so the driver can retry;
translation happens once the transaction returns.

## Alternatives considered

- **Pass `mongo.CommandError` through** — rejected: upper layers
  would have to know MongoDB specifics; leaks the storage choice.
- **Classify at each call site** — rejected: inevitable drift;
  one table beats many.

## Consequences

- `persistence_mongo_errors_total` tags errors with their category
  (`unavailable`, `internal`, `invalid_argument`, `resource_exhausted`,
  `not_found`, `unknown`) so operators can alert.
- New error paths must update the table or default to `Internal`
  (conservative). Unit tests exercise each row.
