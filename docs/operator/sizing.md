# Sizing guidance

Rough sizing reference. Validate with your own workload before committing.

## Units

Temporal uses **state transitions per second (STPS)** as the internal load
unit. One STPS = one workflow event persisted = roughly one multi-document
transaction in this plugin's execution store.

## Atlas tier recommendations

| Tier | Safe sustained STPS (unsharded) | Comfortable workload profile |
|------|--------------------------------|------------------------------|
| M30  | 300–800 | Dev, staging, small production |
| M40  | 1,000–2,500 | Small production with headroom |
| M60  | 3,000–6,000 | Mid production |
| M100 | 4,000–8,000 | Mid production, generous headroom |
| M200 | 15,000–25,000 | Large production |
| Sharded M200 × N | ~20,000 × N | Cassandra-tier workloads |

These numbers assume:

- Single-region deployment, co-located workers.
- `w: majority` writes (which Temporal requires).
- Majority-snapshot reads inside transactions.
- Provisioned IOPS, not burstable, above M40.

See `docs/operator/runbook.md` for the symptoms of approaching each ceiling.

## Self-hosted equivalents

Assume the same MongoDB version, WT cache sized at ~60% of RAM, local
NVMe, and 25+ GbE networking. Self-hosted throughput can beat Atlas by
20–40% at the same nominal hardware class because noisy-neighbor and
shared-backplane effects disappear.

## The first thing to saturate

On M30–M60, **IOPS** is the first binding resource — every transaction
writes to three collections plus the oplog. Provision IOPS ≥ 3× the
baseline if sustained writes exceed a few hundred STPS.

On M100 and above, **CPU** becomes the bottleneck before storage.

## Write amplification rule of thumb

Temporal adds roughly **30–50% write amplification** on top of the
business application's own write load at steady state, more at peak
because of history append bursts. When co-locating Temporal with an
application database on the same cluster, size the cluster for
`(business_peak_QPS × 1.5)`, not `business_peak_QPS`.

## Visibility considerations

Atlas Search indexes are memory-resident for query performance. Plan your
tier based on:

- Search index size (proportional to number of workflows and their search
  attribute cardinality).
- Query rate.

For self-hosted (Aggregation mode), standard MongoDB B-tree indexes serve
visibility. Equality and prefix queries are cheap; arbitrary `LIKE` is
not supported (anchored-prefix only).

## What hurts you if you don't plan for it

- **History collection growth.** Use Atlas Online Archive or a retention
  policy. Without it, expect index-cache pressure at ~100M+ historical
  workflows.
- **Hot-workflow contention.** A single workflow emitting events at
  extreme rates serializes on its executions document. No backend
  mitigates this at the plugin level; this is a workflow-design question.
- **Cross-region replica set.** Every state transition waits for majority
  ack. Cross-region majorities add tens of milliseconds per operation.
  Keep the primary and worker region co-located.

## Targets per plugin release

- `UpdateWorkflowExecution` p99 < 50 ms at 500 STPS sustained on an M30.

These numbers are aspirational load-test gates, not production guarantees
for arbitrary workloads.
