# ADR-011: One database for both Temporal datastores

- Status: Accepted
- Date: 2026-10-27
- Deciders: plugin architecture review

## Context

Temporal configures two datastores: the core store (shards,
executions, history, task queues, metadata, queues) and the visibility
store (workflow search). Other backends commonly map them to separate
keyspaces or databases, and in a Cassandra or SQL deployment the
visibility store is often a separate system altogether, such as
Elasticsearch, kept in sync with the core store.

On MongoDB neither is needed:

- An Atlas Search index is built inside the cluster, from the
  collection it indexes. Workflow search needs no separate system and
  no sync pipeline.
- Both stores run on the same cluster and share its capacity, so
  separate databases would not scale them independently.
- Restoring the stores to different points in time would leave
  workflow search out of step with workflow state, so they are backed
  up and restored together.

The visibility store keeps its own collection, `visibility_executions`,
with a small record per workflow: IDs, type, status, times, search
attributes and memo. That record cannot live in the execution
documents:

- Workflow state is stored as opaque protobuf blobs (ADR-006), so its
  search attributes are not fields MongoDB can index.
- Temporal writes visibility through its own store interface,
  asynchronously, after the state commits. Writing those fields into
  the execution documents would contend with the execution
  transactions (ADR-003) on the most frequently written documents, and
  would add the visibility indexes and the Atlas Search index to the
  busiest collection.

## Decision

Both datastores use one database, `temporal`, by default. The
visibility store keeps its own collections in that database, with the
Atlas Search index on `visibility_executions`.

Separate databases are supported for teams that want database-level
permissions, for example read-only access to workflow search for a
reporting tool. `temporal-mongodb-tool setup-schema` and
`update-schema` take `--stores core|visibility|all` (default `all`),
and the collection manifests record each collection's store, so each
database gets only its own store's collections.

## Alternatives considered

- **Separate databases by default.** Rejected: they add a second set of
  grants and schema setup, and their one real benefit, database-scoped
  permissions, is needed by few deployments. It remains an option.
- **Visibility records in the `executions` collection.** Rejected:
  visibility writes would contend with execution transactions on the
  same documents, and the busiest collection would carry the
  visibility indexes.

## Consequences

- One database to provision, grant (`readWrite` for the runtime user,
  `dbAdmin` for the schema tool) and back up.
- Temporal's state, its workflow search and the application's own data,
  including full-text and vector search, can share one cluster.
- Each datastore opens its own connection pool, so `maxPoolSize` is
  sized for two pools per Temporal process.
- Verified on Atlas M40 (MongoDB 8.0): all end-to-end scenarios,
  including the Atlas Search visibility queries, pass with both stores
  in one database.
