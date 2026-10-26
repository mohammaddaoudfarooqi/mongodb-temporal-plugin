# ADR-016: One database for both Temporal datastores by default

- Status: Accepted
- Date: 2026-10-26
- Supersedes: ADR-011
- Deciders: plugin architecture review

## Context

Temporal configures two datastores: the core store (shards,
executions, history, task queues, metadata, queues) and the visibility
store (workflow search). ADR-011 mapped them to two MongoDB databases,
`temporal` and `temporal_visibility`.

In practice that bought little:

- **Search needs no separate system.** On Atlas, an Atlas Search index
  is built inside the cluster from the collection it indexes. Nothing
  has to be synced between a database and a search engine, whether the
  collection sits in its own database or not.
- **Database-scoped permissions were unused.** The runtime user needed
  `readWrite` on both databases.
- **Independent scaling and restore were overstated.** Both databases
  share the cluster's capacity, and restoring one to a different point
  in time than the other leaves workflow search out of step with
  workflow state.
- **It confused operators.** `setup-schema` applied the full manifest
  to both databases, so each held empty copies of the other's
  collections, and two databases read as duplicated data.

The visibility collection (`visibility_executions`) is a real second
copy of a small part of each workflow: IDs, type, status, times, search
attributes and memo. It exists regardless of the database layout:

- Workflow state is stored as opaque protobuf blobs (ADR-006), so its
  search attributes are not fields MongoDB can index.
- Temporal writes visibility through its own store interface,
  asynchronously, after the state commits. Writing those fields into
  the execution documents would contend with the execution transactions
  on the hottest documents (ADR-003), which already set the throughput
  ceiling, and would add the visibility indexes to the hottest
  collection.

So the separate visibility *collection* is deliberate; the separate
*database* is not needed.

## Decision

Both datastores use one database, `temporal`, by default. The
quickstarts, the release-qualification harness and the operator
documentation configure it that way.

Separate databases remain supported for teams that want
database-level permissions, for example read-only access to workflow
search for a reporting tool. `temporal-mongodb-tool setup-schema` and
`update-schema` take `--stores core|visibility|all` (default `all`),
so each database gets only its own store's collections. The
collection manifests record each collection's store.

## Alternatives considered

- **Keep two databases by default (ADR-011).** Rejected for the
  reasons above.
- **Visibility records in the `executions` collection, with a type
  field.** Rejected: visibility writes would contend with execution
  transactions on the same documents, and the hottest collection would
  carry the visibility indexes and the Atlas Search index. (ADR-011
  gave "harder to audit" as a reason; it was not one.)

## Consequences

- One database to provision, grant (`readWrite` and `dbAdmin` on
  `temporal`) and back up, and no empty duplicate collections.
- The pitch is one cluster: Temporal's state, its workflow search and
  the application's own data with full-text and vector search.
- Verified on Atlas M40 (MongoDB 8.0) on 2026-10-26: all 12 e2e
  scenarios, including the Atlas Search visibility queries, pass with
  both stores in one database.
- Deployments whose config names both databases keep working unchanged.
  Stacks started from the bundled configuration
  (`deploy/config/development.yaml`, the Compose stacks) before this
  change keep their visibility data in `temporal_visibility`; on the new
  configuration they would lose existing workflows from List and Count,
  so they keep the previous image or start afresh (see the deployment
  guide's upgrade note).
