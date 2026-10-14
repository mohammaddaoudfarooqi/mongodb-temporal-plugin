# Sharded MongoDB

The plugin supports sharded MongoDB clusters.
Below are the recommended shard keys per collection and the
constraints the plugin imposes.

## Shard key recommendations

| Collection                | Shard key                                          | Rationale                                                     |
|---------------------------|----------------------------------------------------|---------------------------------------------------------------|
| `shards`                  | `{ shard_id: 1 }`                                 | 1:1 with logical shards; perfectly distributed               |
| `executions`              | `{ shard_id: 1, namespace_id: 1, workflow_id: 1 }` | Leading shard_id matches compound index; avoids scatter-gather |
| `current_executions`      | `{ shard_id: 1, namespace_id: 1, workflow_id: 1 }` | Same as `executions`                                         |
| `history_nodes`           | `{ tree_id: 1, branch_id: 1 }`                    | Per-tree locality                                            |
| `history_trees`           | `{ tree_id: 1, branch_id: 1 }`                    | Same                                                          |
| `history_tasks`           | `{ shard_id: 1, category_id: 1 }`                 | Per-shard scan ordering                                      |
| `replication_dlq`         | `{ shard_id: 1, source_cluster_name: 1 }`         | Per-shard + per-source                                       |
| `task_queues`             | `{ namespace_id: 1, task_queue: 1 }`              | Balances namespace load                                      |
| `tasks`                   | `{ namespace_id: 1, task_queue: 1, task_type: 1 }`| Per-queue range-scan locality                                |
| `visibility_executions`   | `{ namespace_id: 1, _id: 1 }`                     | Leading namespace_id for list/count predicates               |
| `v2_messages`             | `{ type: 1, name: 1 }`                            | Per-queue order                                              |
| `cluster_metadata`, `namespace_metadata`, `_schema_version` | N/A (tiny, unsharded) | Singletons           |

## Unique-index constraints

MongoDB sharded clusters require unique indexes to include the shard
key as a prefix. The plugin's unique indexes accommodate this:

- `shards.shard_id` — uniqueness matches shard-key column.
- `namespaces.name` — namespaces is a small, unsharded collection.
  Sharding it is not recommended.
- `task_queues._id` — the composite string includes all shard-key
  components.

If you shard a collection whose unique index doesn't include the
shard key, MongoDB rejects the migration. Run `sh.status()` and
confirm before enabling sharding on the plugin's collections.

## Transactions across shards

Multi-document transactions on a sharded cluster work but are slower
than on a replica set. The plugin uses transactions in three places:

1. `CreateWorkflowExecution` — shard doc + execution doc + current
   pointer + history events (future) + tasks (future). All touch
   the same logical shard, so configure shard-key and collection
   placement so they co-locate.
2. `UpdateWorkflowExecution` — same.
3. `ConflictResolveWorkflowExecution` — same.

When all touched documents live on one MongoDB shard, the
transaction is single-shard and fast. Cross-shard commit is the
slow path and should be avoided.

## Chunk balancing

- Set the balancer to run during low-traffic windows (e.g., cron
  2am-6am) to avoid moving chunks mid-transaction.
- Pre-split chunks on very write-heavy collections
  (`history_nodes`, `visibility_executions`) before onboarding new
  namespaces, to avoid balancer churn.

## Zone sharding (optional)

For geo-distributed deployments, zone-shard `executions` and
`history_nodes` by a prefix of the `namespace_id` so
region-specific namespaces stay on region-local shards.

## Sharding is deferred for v1.0

The plugin is compatible with sharded clusters but v1.0 conformance
testing focused on replica sets. First production sharded
deployments should:

- Size for 20% lower effective STPS than a replica set of equivalent
  hardware.
- Keep a replica-set canary for comparison.
- Report explain() output via `persistence_mongo_op_latency_ms` so
  regressions surface.
