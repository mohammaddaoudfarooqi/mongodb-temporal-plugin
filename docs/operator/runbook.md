# Runbook

Playbook for common incidents. Each section lists symptoms, quick
diagnostics, and remediations.

## CAS storms on shard updates

**Symptoms**
- `ShardOwnershipLostError` metric rate spikes.
- Temporal history hosts logging "failed to acquire shard".

**Likely cause**
- Two history processes claiming the same shard (e.g., rolling
  restart with an overlapping leader-election window).

**Fix**
- Check Temporal's membership table:
  ```
  use temporal
  db.cluster_membership.find({role: 2}).pretty()  // History = role 2
  ```
- Expire stale membership: `temporal-mongodb-tool prune-membership`
  (not yet shipped; run `db.cluster_membership.deleteMany({record_expiry: {$lt: new Date()}})`).
- If it persists, confirm Temporal's `matchingRingHash` dynamic
  config is consistent across history hosts.

## Transaction abort storms on UpdateWorkflowExecution

**Symptoms**
- `persistence_mongo_transactions_retried_total` climbing on a
  single workflow.
- Client timeouts on `UpdateWorkflowExecution`.

**Likely cause**
- Hot workflow: a very short heartbeat loop hammering the same
  execution document. Each update is a single-document write to a
  large (>1 MB) document, so updates to one workflow serialise and
  queue up.

**Fix**
- Short term: increase the workflow's task-heartbeat interval.
- Medium term: verify the workflow isn't generating thousands of
  activities in a short window.

## Connection pool exhaustion

**Symptoms**
- `persistence_mongo_errors_total{category="resource_exhausted"}`
  ticking up.
- `WaitQueueTimeoutError` in logs.

**Likely cause**
- `maxPoolSize` too low for current STPS.
- A slow query holding a connection.

**Fix**
1. Check `persistence_mongo_op_latency_ms` p99 by operation. If one
   operation is >1s, that's your slow query.
2. Bump `maxPoolSize` in the plugin config.
3. Check Atlas's "Connection" metric (or `mongostat`) for the
   cluster's actual connection count; compare to the cluster's
   per-tier limit.

## Visibility Atlas Search index lag

**Symptoms**
- New workflows don't appear in `ListWorkflowExecutions` for up to
  a minute.
- `persistence_mongo_atlas_search_index_ready` gauge stays at 1
  (so the index is READY), but queries miss recent records.

**Likely cause**
- Atlas's Search indexer has a propagation delay (tens of seconds
  to ~1 min under load).

**Fix**
- This is expected Atlas behavior. Fall back to the aggregation
  mode for latency-critical queries; Search is for list/count.
- Check the Atlas Metrics dashboard for "Search replication lag".

## Oplog rollover (self-hosted)

**Symptoms**
- Secondaries fall behind; `replSetGetStatus` shows `lagMillis` > 60s.
- Eventual write errors from the plugin.

**Likely cause**
- Oplog sizing too small for the current write rate.

**Fix**
- Resize the oplog (`db.adminCommand({replSetResizeOplog: 1, size:
  <new-MB>})` on the primary).
- Long-term: monitor `db.oplog.rs.stats()` and size to retain at
  least 24h of writes.

## Health-check failures

**Symptoms**
- `persistence_mongo_health_status` gauge flapping between 0 and 1.
- Kubernetes readiness probe failing.

**Likely cause**
- Flaky network or Atlas maintenance.

**Fix**
1. Check `serverSelectionTimeoutMs` — if too short, transient
   re-election events fail probes. 15s is a sensible floor.
2. Check Atlas's alert feed for maintenance windows.

## Plugin refuses to start

### "_schema_version absent"

Cluster hasn't been initialised. Run:
```
temporal-mongodb-tool setup-schema --uri ... --database ... --num-history-shards <N>
```

### "state != ready" (upgrading-from-X-to-Y)

Previous `update-schema` run crashed mid-migration. Resume:
```
temporal-mongodb-tool update-schema --uri ... --database ...
```
If it reports `failed-from-X-to-Y`, inspect the cluster state
manually and consider restoring from backup before retrying.

### "numHistoryShards=A but _schema_version recorded B"

Someone changed `numHistoryShards` in the Temporal config. The
plugin refuses this because resharding requires a full data
migration, not a config change. Either revert the config to B or
stand up a fresh cluster with the new shard count + data migration.

### "Atlas Search unavailable" (when visibilityMode=atlas-search)

The cluster isn't an Atlas M10+ deployment. Either:
- Move to an M10+ Atlas cluster, or
- Change `visibilityMode` to `aggregation`.

## MongoDB exits at start-up on a new Linux kernel

**Symptom**: a self-hosted `mongod` (including the `mongo` Docker image)
exits with code 1 and logs `MongoDB cannot start: Linux kernel versions
6.19 and newer has a known incompatibility with this version of
MongoDB`. The plugin then fails to connect.

**Cause**: a MongoDB start-up guard, not the plugin. See
[Self-hosted MongoDB on Linux kernel 6.19 or newer](deployment.md#self-hosted-mongodb-on-linux-kernel-619-or-newer).

**Fix**: run MongoDB 9.0.4+ or 8.2 (or 8.0.35+ / 8.3.14+ once released)
on that host, or run on a kernel older than 6.19. Check with
`uname -r` and, on Ubuntu, `cat /proc/version_signature`.

## Escalation path

Production-affecting bugs in the plugin: open a GitHub issue with
a `priority/high` label on mohammaddaoudfarooqi/mongodb-temporal-plugin.
Attach:
- Temporal server version
- Plugin version
- MongoDB version + deployment shape (Atlas tier, self-hosted RS, sharded)
- Relevant logs (scrub credentials first)
- A minimal reproduction if available.
