# ADR-009: Single history_nodes collection with compound index

- Status: Accepted
- Date: 2026-04-30
- Deciders: plugin architecture review

## Context

Workflow history events are read in monotonic `(branch_id,
node_id)` order. `ReadHistoryBranch` is the hot path — every
history replay and visibility decode goes through it.

The storage choices:

1. One document per history event, indexed on
   `(tree_id, branch_id, node_id)`.
2. One document per batch (node), containing the serialized event
   batch.
3. Per-tree collection.

## Decision

Option 2: one document per node. Each node holds a batch of
events. Compound index `{tree_id: 1, branch_id: 1, node_id: 1}`.

`ReadHistoryBranch` issues a range scan on this index and returns
the node documents in order. Because the projection only asks for
indexed fields + the data blob, MongoDB can answer the range
portion from the index and fetch the blob in order.

## Alternatives considered

- **Per-tree collection** — rejected: collection cardinality
  explodes with namespaces; ops cost dominates.
- **One document per event** — rejected: index size blows up,
  and round-trip count per history replay becomes a problem.
- **GridFS for blobs** — rejected: adds a second lookup per node
  for no observable benefit at Temporal's typical event-batch size.

## Consequences

- Index-forward read path; no document scans on the hot path.
- `DeleteHistoryBranch` walks the index and
  deletes the matching documents in a single bulk operation.
- No TTL index — see `docs/operator/retention.md` for why
  retention is caller-driven via the history-scavenger workflow.

## References

- `docs/operator/retention.md`.
