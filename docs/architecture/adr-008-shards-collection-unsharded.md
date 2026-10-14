# ADR-008: `shards` collection is not natively MongoDB-sharded

- Status: Accepted
- Date: 2026-04-30
- Deciders: plugin architecture review

## Context

Temporal logically shards work by `shard_id` (0..numHistoryShards-1).
Each shard is a single owner's contention domain.

In MongoDB, a collection can be sharded across physical nodes. For
the `shards` collection the question is: should we enable MongoDB
sharding on it, and if so, by what key?

The collection is tiny — `numHistoryShards` documents total
(typically 512 or 1024), each ~300 bytes. Total size: under 1 MB.

## Decision

Leave `shards` as an unsharded collection. Document `_id` is the
integer `shard_id`.

If an operator DOES choose to enable MongoDB sharding on the
cluster for large-scale deployments, the recommended shard key is
`{shard_id: 1}` — 1:1 with the logical shard, perfectly
distributed. But we do not ship this as the default.

## Alternatives considered

- **Shard by `{shard_id: 1}`** — valid but low-value at the
  typical size. Operators who want it can enable it; the unique
  index on `shard_id` already satisfies the sharded-cluster unique
  constraint.
- **Hash-shard** — rejected: sequentially numbered shard_ids
  already distribute well with range sharding.

## Consequences

- Simpler default deployment (no sharding of the control-plane
  collection).
- `docs/operator/sharding.md` calls out the recommended shard key
  for operators who enable sharding.
- Scatter-gather on `shards` is a non-issue because it is so small.
- Other collections (`executions`, `history_nodes`, `tasks`,
  `visibility_executions`) have shard-key guidance that leads with
  `shard_id` to avoid scatter-gather.

## References

- `docs/operator/sharding.md`.
