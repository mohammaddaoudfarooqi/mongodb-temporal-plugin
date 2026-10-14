# ADR-005: Embedded arrays for per-execution sub-collections

- Status: Accepted
- Date: 2026-04-30
- Deciders: plugin architecture review

## Context

A Temporal workflow execution has several sub-entities that grow
and shrink during the workflow's lifetime:

- Activity infos
- Timer infos
- Child execution infos
- Request-cancel infos
- Signal infos
- CHASM node infos

Cassandra stores them as clustering rows under the execution
partition key. SQL backends use one table per sub-entity. Neither
option maps cleanly to MongoDB without either a multi-collection
transaction on every update or a lot of round trips.

## Decision

Embed each sub-entity as an array field on the `executions`
document:

```
{
  _id: "shard_id:namespace_id:workflow_id:run_id",
  execution_info: BinData,
  execution_state: BinData,
  activity_infos:      [{id, data, encoding}, ...],
  timer_infos:         [{id, data, encoding}, ...],
  child_execution_infos: [{...}, ...],
  ...
  db_record_version: Int64
}
```

Mutations use `$set`, `$push`, `$pull`, `$addFields` scoped to the
one document, updated via a single `findOneAndUpdate` with the
`db_record_version` CAS predicate from ADR-004.

## Alternatives considered

- **One collection per sub-entity** — rejected: every
  `UpdateWorkflowExecution` would need a multi-doc transaction
  touching 6+ collections. Real fan-out, real latency.
- **Sparse documents** (one doc per sub-entity) — rejected: same
  multi-collection transaction problem, plus forces ExistsFilter
  joins on every read.

## Consequences

- One document represents the full mutable state. Reads are one
  round trip.
- The 12 MB soft cap on execution documents (80% of BSON's 16 MB
  limit; writes beyond it fail with `TransactionSizeLimitError`)
  constrains how many sub-entities a single execution can
  accumulate.
- Arrays are append/remove ordered; no cross-entity secondary
  index is possible inside one execution.

## References

- ADR-003 (multi-document transactions), ADR-004 (single-document CAS).
