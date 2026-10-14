# Retention

How history and visibility data are retained under the
`mongodb-temporal-plugin`.

## History retention is caller-driven

The plugin stores history in two collections:

- `history_nodes` — event data keyed on (tree_id, branch_id, node_id).
- `history_trees` — branch metadata.

**Neither collection has a TTL index**. That is
intentional. History trees can fork — a branch's nodes may be
referenced by sibling branches created later. A cluster-wide TTL
would eventually delete events that a namespace with longer
retention still needs to read.

Retention is enforced by Temporal's **history-scavenger workflow**:

1. The scavenger runs per namespace on a schedule.
2. It walks closed executions and calls
   `ExecutionStore.DeleteHistoryBranch` for any branch whose
   workflow closed more than `namespace.retention_days` ago.
3. `DeleteHistoryBranch` atomically removes the branch document
   and its nodes.

If you disable the scavenger or lower its schedule frequency,
**history storage will grow without bound**.

## Visibility retention

Visibility records live in `visibility_executions` and have no TTL
either. Temporal's visibility manager deletes records via
`DeleteWorkflowExecution` when the history-scavenger closes the
corresponding history branch.

## Task retention

Task queue records have a TTL: `tasks.expiry_time` with
`expireAfterSeconds: 0` + a partial filter that excludes
zero-valued expiry. The task-queue manager writes an `expiry_time`
a few seconds ahead of the task's scheduled fire time, so MongoDB's
TTL sweeper (sweep cadence ~60s) reclaims fired-but-unacked tasks.

## Cluster membership

`cluster_membership` has a TTL on `record_expiry`. Temporal workers
heartbeat to refresh their records; if a worker dies, its row
expires within 60s and clean elections proceed. Manual cleanup is
also available via `PruneClusterMembership`.

## Sizing implications

Plan for:

- History: average `history_length × history_event_size` per
  workflow × retention_days × workflows_per_day.
- Visibility: roughly 1-4 KB per workflow × retention_days ×
  workflows_per_day.
- Task queues: only in-flight tasks, TTL-reaped — bounded by
  STPS × average ack latency.

See `docs/operator/sizing.md` for capacity-planning worksheets.

## Backups

Retention does NOT imply backups. The plugin does not create
snapshots; use Atlas's continuous cloud backup or MongoDB's
mongodump/mongorestore for your DR strategy.

Backups must cover at least:
- `namespaces` + `namespace_metadata` (source of truth for
  namespace config).
- `_schema_version` (plugin bookkeeping).
- `executions` + `current_executions` + `history_*` (workflow state).

Visibility records and task queues can be rebuilt from history +
namespace config, but restoring them directly accelerates recovery.
