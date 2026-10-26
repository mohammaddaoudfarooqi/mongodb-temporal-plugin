# ADR-011: Separate databases per Temporal datastore

- Status: Superseded by ADR-016
- Date: 2026-04-30
- Deciders: plugin architecture review

## Context

Temporal's `persistence` config allows splitting the core
(`defaultStore`) and visibility (`visibilityStore`) persistence
backends. SQL backends typically point them at the same database
(with separate tables). Cassandra uses two keyspaces.

MongoDB's unit of administrative isolation is the database:
collections, roles, and indexes live inside a database.

## Decision

When the operator configures two datastores, the plugin honors the
`database` option on each independently. The default is:

- `temporal` — core stores.
- `temporal_visibility` — visibility store.

A single shared `mongo.Client` backs both. No
connection pool fan-out on the client side.

## Alternatives considered

- **Single database, separate prefixes** — rejected: collision
  risk with operator-owned collections, and denies MongoDB's
  native database-scoped RBAC.
- **Shared collections with discriminator field** — rejected:
  harder to scale independently, harder to audit.

## Consequences

- Each database can be scaled, rotated, or restored independently.
  Atlas Search indexes live in the visibility database, keeping
  them out of the core-store's permission scope.
- `temporal_runtime` user's `readWrite` role applies to both
  databases (see `docs/operator/security.md`).
- Operators running a single combined datastore (same `database`
  value on both) still work — collections coexist.

## References

- `docs/operator/security.md`.
