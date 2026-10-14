# Architecture Decision Records — Index

Every non-trivial architectural choice is captured as a numbered ADR in this
directory. ADRs are immutable once accepted; superseding decisions create a
new ADR and mark the old one `Superseded`.

| ID  | Status    | Title                                                 |
|-----|-----------|-------------------------------------------------------|
| [001](adr-001-out-of-tree-plugin.md) | Accepted | Out-of-tree plugin via AbstractDataStoreFactory |
| [002](adr-002-mongodb-driver-v2.md) | Accepted | MongoDB Go driver v2 only |
| [003](adr-003-multi-doc-txn-for-mutable-state.md) | Accepted | Multi-document transactions for mutable-state writes |
| [004](adr-004-single-doc-cas.md) | Accepted | Single-document CAS where transactions are unnecessary |
| [005](adr-005-embedded-arrays-for-subcollections.md) | Accepted | Embedded arrays for per-execution sub-collections |
| [006](adr-006-opaque-blobs.md) | Accepted | Opaque blobs via BSON Binary + encoding field |
| [007](adr-007-visibility-two-modes.md) | Accepted | Two visibility modes: Atlas Search + Aggregation |
| [008](adr-008-shards-collection-unsharded.md) | Accepted | Shards collection not natively MongoDB-sharded |
| [009](adr-009-history-nodes-single-collection.md) | Accepted | Single history_nodes collection with compound index |
| [010](adr-010-ttl-indexes.md) | Accepted | TTL indexes for task + membership expiry |
| [011](adr-011-separate-databases.md) | Accepted | Separate databases per Temporal datastore |
| [012](adr-012-no-goroutines-outlive-factory.md) | Accepted | No goroutines outlive the factory |
| [013](adr-013-error-classification.md) | Accepted | Error classification into serviceerror tiers |
| [014](adr-014-versioned-schema.md) | Accepted | Versioned schema with _schema_version document |

## ADR template

```markdown
# ADR-NNN: <Title>

- Status: Proposed | Accepted | Superseded by ADR-NNN
- Date: YYYY-MM-DD
- Deciders: <reviewers>

## Context
(What problem are we solving? What constraints apply?)

## Decision
(What did we decide?)

## Alternatives considered
(What did we rule out and why?)

## Consequences
(What does this decision commit us to, positive and negative?)

## References
- Related ADRs and docs.
```

Each listed ADR has its own file `adr-NNN-<slug>.md` in this directory.
The table above is the one-line summary; the ADR files expand on
context, alternatives, and consequences.
