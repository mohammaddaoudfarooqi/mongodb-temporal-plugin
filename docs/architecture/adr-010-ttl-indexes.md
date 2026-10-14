# ADR-010: TTL indexes for task and cluster-membership expiry

- Status: Accepted
- Date: 2026-04-30
- Deciders: plugin architecture review

## Context

Two Temporal persistence concepts carry an expiry time that the
cluster wants cleaned up automatically:

- Task queue tasks that fired but were never acked (fire time + a
  grace window).
- Cluster membership rows whose owner stopped heartbeating.

Cassandra uses its native TTL. SQL backends sweep via scheduled
jobs. MongoDB provides a TTL index — documents with a `Date`
field older than `expireAfterSeconds` are reaped by a background
thread (sweep cadence ~60s).

## Decision

Use TTL indexes:

- `tasks.expiry_time` with `expireAfterSeconds: 0` plus a partial
  filter that excludes zero-valued expiry. The task-queue manager
  stamps `expiry_time` a few seconds ahead of the scheduled fire
  time so the TTL sweeper reaps the row.
- `cluster_membership.record_expiry` with a similar TTL. Workers
  heartbeat to refresh; if they stop, the row expires within ~60s
  and clean elections proceed.

`PruneClusterMembership` provides an explicit admin-path
for operators who want immediate cleanup.

## Alternatives considered

- **Sweeper goroutine inside the plugin** — rejected: violates
  ADR-012 (no goroutines outlive the factory). Operational cost
  for no benefit over the server-side TTL.
- **Caller-driven cleanup** — rejected: the task-queue manager
  already expects the store to reap expired rows.

## Consequences

- Mongo's TTL monitor adds a small background load (~one scan
  per minute per indexed collection). Fine at the volumes we
  target.
- TTL resolution is coarse (~60s). Callers must not assume
  sub-minute reaping.
- `history_nodes` and `visibility_executions` do NOT have TTL
  indexes. Retention there is caller-driven
  (`docs/operator/retention.md`).

## References

- ADR-012.
- `docs/operator/retention.md`.
