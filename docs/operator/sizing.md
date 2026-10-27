# Sizing guidance

Measured on Atlas with the release-qualification harness
([`deploy/aws-qualify`](../../deploy/aws-qualify)), unless a section says
otherwise. Validate with your own workload: the numbers below come from
one test workflow and one deployment shape.

## What was measured

Each run used one AWS region (`us-east-1`), VPC peering to Atlas, two
Temporal server hosts (`c6i.2xlarge`, 512 history shards), one load
generator, and the qualification order workflow: about 16 execution
transactions per workflow (15 updates and 1 create), with a small
history.

| Atlas tier | MongoDB | Load worker pollers | Sustained throughput | Transactions/s | p50 / p99 workflow latency | Limited by |
|---|---|---|---|---|---|---|
| M30 | 8.0 | 2 + 2 (SDK default) | about 20 workflows/s | about 330 | not recorded | not determined (not re-run with more pollers) |
| M40 | 8.0 | 2 + 2 | about 31–33 workflows/s | about 510 | 1.0 s / 6.1 s | the load worker |
| M40 | 9.0 | 2 + 2 | about 34–35 workflows/s | about 560 | 0.9 s / 6.0 s | the load worker |
| **M40** | **9.0** | **16 + 16** | **about 74–76 workflows/s** | **about 1,100–1,200** | 0.7 s / 1.0 s at concurrency 50; 1.7 s / 6.5 s at 200 | **database CPU** |

"Pollers" are the load generator's workflow and activity task pollers.
With the Go SDK's defaults (two of each) the worker, not the database,
set the ceiling: on M40 the cluster had headroom (mongod CPU about 61%,
transactions flat at about 11 ms). With 16 of each, throughput rose
2.2× on the same cluster and the database became the limit: mongod CPU
about 75%, I/O wait about 0.1%, and transaction time up to about 42 ms
as transactions queued. Size your own workers' pollers for your
throughput; see Temporal's worker tuning guidance.

On MongoDB 9.0, where the plugin's transaction metrics were recorded,
no transaction was retried at any load, including the database-bound
one, so transactions did not conflict on the per-shard fence.

The longest runs:

| Tier | MongoDB | Run | Workflows |
|---|---|---|---|
| M30 | 8.0 | soak | 144,847 of 144,847 |
| M40 | 8.0 | 4-hour soak | 478,819 of 478,819 |
| M40 | 8.0 → 9.0 | 45 minutes through an in-place upgrade | 93,771 of 93,771 |
| M40 | 9.0 | 1-hour soak | 127,281 of 127,281 |

Every run also passed an Atlas primary failover and the loss and restart
of a Temporal server host with no failed workflows. On 9.0, Atlas first
refused the failover test because of the oplog (see [Oplog](#oplog));
after the oplog was enlarged online, with no failed workflows during the
change, the failover passed: 22,161 of 22,161. During the in-place upgrade from
MongoDB 8.0 to 9.0 under load, no workflow or transaction failed; the
driver retried at least 19 transactions during the primary election.

## Storage

Per workflow of the test workload (about 561,000 workflows measured):

| Collection | Share of data | Per workflow |
|---|---|---|
| `history_nodes` | about 65% | about 8 KB (about 12 documents) |
| `executions` | about 14% | about 1.8 KB |
| `visibility_executions` | about 10% | about 1.3 KB |
| the rest | about 11% | about 1.3 KB |

About 12.5 KB per workflow uncompressed, and about 6 KB on disk
including indexes. History dominates, and grows with the workflow's
events and payloads, so namespace retention is the main lever on
storage.

## Oplog

Temporal rewrites a workflow's state on every transition and inserts
and deletes tasks continuously, so the oplog grows much faster than the
data: about **76 KB of oplog per workflow** on the test workload, about
9 GB per hour at 33 workflows/s on M40. With the default oplog (about
1.9 GB on a 40 GB disk), the oplog window was about 12 minutes.

A short window means a secondary that is offline for longer than that
needs a full initial sync. Atlas also refuses a failover test
(`OPERATION_INVALID_MEMBER_OPLOG_CAPACITY`) while it reports less than
3 hours of oplog capacity; with the default oplog, Atlas reported 1.3
hours for the M40. Enlarging the oplog to 28 GB, an online change, fixed
it, and the failover test then passed. Size the oplog from your write
rate:

- On Atlas, set a **minimum oplog window**
  (`oplogMinRetentionHours`, under Additional Settings) together with
  storage auto-scaling, or set the oplog size explicitly.
- Oplog needed = oplog rate × window. The rate scales with throughput,
  at about 76 KB per workflow of the test workload: 9 GB/h at 33
  workflows/s, about 20 GB/h at 75. At 9 GB/h, a 6-hour window needs
  about 55 GB.
- Watch the oplog window and oplog rate in Atlas monitoring.

## Visibility considerations

Atlas Search indexes are memory-resident for query performance. Plan your
tier based on:

- Search index size (proportional to number of workflows and their search
  attribute cardinality).
- Query rate.

For self-hosted (Aggregation mode), standard MongoDB B-tree indexes serve
visibility. Equality and prefix queries are cheap; arbitrary `LIKE` is
not supported (anchored-prefix only).

## Temporal server memory

Most of a history host's memory is Temporal's own workflow cache, not
the plugin. By default the cache is bounded by entry count
(`history.hostLevelCacheMaxSize`, 128,000 entries), not bytes, and each
entry lives for `history.cacheTTL` (1 hour). Under steady load the heap
grows for about an hour, then levels off once entries start to expire.

On the release-qualification soak (Atlas M40, about 15 workflows/s per
history host, 256 shards per host), the in-use heap levelled off at
about 2.1 GB after an hour, and RSS at about 4.5 GB. Goroutines stayed
flat. Higher throughput puts more entries in the cache within the hour,
so the plateau rises with load, up to the entry cap.

- Give each history host about twice the cache's steady-state heap in
  RAM: Go's garbage collector lets the heap reach about twice its live
  size by default.
- To cap the cache by size instead, set `history.cacheSizeBasedLimit:
  true` in dynamic config. The cap is then
  `history.hostLevelCacheMaxSizeBytes` (default about 1 GB).
- Set `GOMEMLIMIT` (for example 80% of the container's memory limit) to
  keep RSS close to the live heap.

## Document size limits

MongoDB caps a document at 16 MB. Temporal's own limits keep every
document the plugin writes well below that:

| Collection | Bounded by (Temporal default) | Largest document |
|---|---|---|
| `executions` | `limit.mutableStateSize.error` (8 MB); at most 2,000 pending activities, child workflows, signals and cancellation requests each | about 8 MB |
| `history_nodes` (one batch of events) | `system.transactionSizeLimit` (4 MB); `limit.blobSize.error` (2 MB per payload) | about 4 MB |
| `visibility_executions` | `limit.memoSize.error` (2 MB), plus the search attribute size limits | about 2 MB |
| everything else | fixed-shape records | kilobytes |

When a workflow reaches one of these limits, Temporal terminates it with
the reason "transaction size exceeds limit": the write fails, and nothing
is partly written. The plugin adds a 12 MB check on execution documents
and on history batches, which returns the same error, so raising
Temporal's limits cannot make those documents fail with a MongoDB
`BSONObjectTooLarge` error that Temporal would keep retrying. Visibility
records have no such check; they stay within Temporal's memo and search
attribute limits.

**Do not raise `limit.mutableStateSize.error` or
`system.transactionSizeLimit` above 12 MB** on this backend: workflows
would then be terminated at 12 MB instead.

Workflows that come close to these limits in practice:

- **Wide fan-out from one workflow:** hundreds of pending activities or
  child workflows, each carrying a large heartbeat or a large result,
  all in one workflow's state.
- **Large payloads in one step:** a workflow task that schedules many
  activities with large inputs at once puts them all in one history
  batch.
- **Signal or update storms:** many large signals or updates that arrive
  while a workflow task is running are buffered in the workflow's state
  until it completes.
- **Large memos or search attributes.**

The usual remedies are Temporal's: keep large data outside the workflow
and pass references to it, split fan-out across child workflows, and use
continue-as-new before the history grows large.

## What hurts you if you don't plan for it

- **History collection growth.** History is about two-thirds of the
  data (see [Storage](#storage)). Set namespace retention, and use
  Atlas Online Archive if you need to keep closed workflows longer.
- **Hot-workflow contention.** A single workflow emitting events at
  extreme rates serializes on its executions document. No backend
  mitigates this at the plugin level; this is a workflow-design question.
- **Cross-region replica set.** Every state transition waits for majority
  ack. Cross-region majorities add tens of milliseconds per operation.
  Keep the primary and worker region co-located.
