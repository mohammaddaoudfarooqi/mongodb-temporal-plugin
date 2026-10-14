# ADR-004: Single-document CAS where transactions are unnecessary

- Status: Accepted
- Date: 2026-04-30
- Deciders: plugin architecture review

## Context

Two CAS predicates show up repeatedly:

- Shard `range_id`: the current owner is allowed to update the
  shard only if `range_id` matches its cached value
  (`ShardOwnershipLostError` otherwise).
- Execution `db_record_version`: mutable state updates must rev the
  version atomically.

Both are single-document conditions. A multi-document transaction
would work but is overkill — it adds a two-phase commit round trip
and locks resources we do not need to hold.

## Decision

Use `findOneAndUpdate` with the precondition in the filter:

- `UpdateShard`: filter `{shard_id, range_id: prev}`, update
  `{$set: {range_id: new, ...}}`.
- `UpdateWorkflowExecution`: filter `{_id, db_record_version: prev}`,
  update `{$set: {db_record_version: new, ...}}`.

If `findOneAndUpdate` returns no document, the precondition failed
→ translate to `ShardOwnershipLostError` or
`WorkflowConditionFailedError`.

## Alternatives considered

- **Wrap every CAS in a transaction** — rejected: unnecessary
  overhead; `findOneAndUpdate` is atomic on a single document.
- **Read-then-conditional-write** — rejected: races between the
  read and the write, solved by putting the condition in the filter.

## Consequences

- CAS failures are cheap (one round trip, no commit) and
  observable via `persistence_mongo_errors_total{category="not_found"}`.
- Multi-document updates still go through ADR-003.
- Readers (`GetShard`, `GetWorkflowExecution`) bypass transactions
  entirely.

## References

- ADR-003 (multi-document transactions).
