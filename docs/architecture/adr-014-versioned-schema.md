# ADR-014: Versioned schema with a `_schema_version` document

- Status: Accepted
- Date: 2026-04-30
- Deciders: plugin architecture review

## Context

The plugin's BSON schema will change over its life — new
collections, new indexes, occasional field additions. Operators
need a way to:

- Initialize a fresh cluster correctly.
- Upgrade an existing cluster without downtime.
- Prevent a version-mismatched plugin from corrupting data.

Temporal's Cassandra and SQL backends use `schema_version` tables
with a similar contract. We follow the same idea.

## Decision

A singleton document `_schema_version` in the plugin's core
database tracks schema state:

```
{
  _id: "current",
  major: 1,
  minor: 3,
  num_history_shards: 512,
  state: "ready" | "upgrading-from-A-to-B" | "failed-from-A-to-B",
  updated_at: <Date>
}
```

- `temporal-mongodb-tool setup-schema` initializes it.
- `temporal-mongodb-tool update-schema` applies declarative
  migrations under `schema/versioned/v<N>.<M>/` idempotently.
- Plugin startup refuses to boot if:
  - `_schema_version` absent (cluster not initialized).
  - `state != "ready"` (migration half-applied).
  - `major` differs from the plugin's compiled-in major.
  - `num_history_shards` differs from the operator's
    `numHistoryShards` config.

A `minor` value lower than the plugin's logs a warning and
suggests `update-schema`. Higher minor refuses to boot (prevents
silent downgrade onto newer data).

No runtime auto-upgrade.

## Alternatives considered

- **Implicit schema inference** — rejected: undetectable silent
  drift.
- **Runtime auto-migration** — rejected: risk of races between
  multiple plugin instances migrating simultaneously; operators
  want explicit migration windows.

## Consequences

- `docs/developer/versioning.md` describes the policy that binds
  on-disk versions to plugin releases.
- First-time setup is a deliberate CLI step; Temporal does not
  boot against an un-initialized cluster.

## References

- `docs/developer/versioning.md`.
