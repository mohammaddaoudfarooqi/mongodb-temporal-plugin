# ADR-003: Multi-document transactions for mutable-state writes

- Status: Accepted
- Date: 2026-04-30
- Deciders: plugin architecture review

## Context

Three upstream methods must commit multiple documents atomically:

- `CreateWorkflowExecution` — shard range check + execution write +
  current-workflow pointer + (eventually) history events + tasks.
- `UpdateWorkflowExecution` — shard range check + execution CAS +
  task insert.
- `ConflictResolveWorkflowExecution` — same shape as Create.

If any document fails its precondition, none must commit.
Cassandra's lightweight transactions (LWT) give this via batch
conditional updates. SQL backends use a real transaction.

## Decision

Use MongoDB multi-document transactions for exactly these three
operations. Delegate retry to driver v2's
`(*Session).WithTransaction`, which handles `TransientTransactionError`
and `UnknownTransactionCommitResult` retries per the MongoDB spec.

No custom retry loop around `WithTransaction`.

## Alternatives considered

- **Single-document model** (pack everything into one doc) —
  rejected: history events + tasks live in their own collections by
  design (volume, TTL), can't be folded in.
- **Optimistic concurrency without transactions** — rejected:
  would need a manual undo protocol for partial failures; a lot of
  subtle code for a well-supported primitive.
- **Cassandra-style LWT emulation** — rejected: Mongo's
  transactions already serialize at the right level.

## Consequences

- All three methods REQUIRE a replica set or sharded cluster.
  Standalone mongod is rejected at startup.
- Cross-shard transactions (on sharded clusters) are slower.
  `docs/operator/sharding.md` documents the co-location guidance.
- Retries are invisible to callers; metrics count them via
  `persistence_mongo_transactions_retried_total`.

## References

- ADR-004 (single-document CAS).
- `docs/operator/sharding.md`.
