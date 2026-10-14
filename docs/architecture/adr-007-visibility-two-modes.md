# ADR-007: Two visibility modes — Atlas Search and Aggregation

- Status: Accepted
- Date: 2026-04-30
- Deciders: plugin architecture review

## Context

Temporal's visibility store needs rich query capability:

- Equality, range, and existence predicates on structured fields.
- Full-text-style matching on `workflow_type`, `workflow_id`.
- Sort by start/close time with pagination.
- Counts with predicate (`CountWorkflowExecutions`).

Upstream SQL uses normalized indexes. Elasticsearch-backed
visibility uses ES's query DSL. MongoDB offers two relevant
paths:

1. **Aggregation pipeline** (`$match`, `$sort`, `$skip`, `$limit`)
   over a conventional collection with normal indexes.
2. **Atlas Search** (`$search`, `$searchMeta`, compound clauses)
   over a Lucene index managed by Atlas.

Atlas Search matches the Elasticsearch semantics users expect
from cloud Temporal. The aggregation path is the only option for
self-hosted.

## Decision

Ship both. Operator selects via
`customDatastore.options.visibilityMode`:

- `aggregation` (default) — works on any MongoDB replica set.
- `atlas-search` — requires Atlas M10+ and a configured Atlas
  Search index.

At startup, `atlas-search` mode probes
`$listSearchIndexes`. If the command is not supported, the plugin
**fails fast** rather than silently downgrading. If
the index exists but is not READY, writes still land; reads return
`serviceerror.Unavailable` until the index is ready.

## Alternatives considered

- **Atlas Search only** — rejected: excludes self-hosted
  operators.
- **Aggregation only** — rejected: no full-text, visibility
  scaling hits index-size walls earlier.
- **Silent downgrade** — rejected: operators who asked for Atlas
  Search and got aggregation would not know their queries are
  slower / less capable.

## Consequences

- Two code paths under `internal/visibility/`. The DSL converter
  (`query.StoreQueryConverter[bson.M]`) emits either `$match`
  clauses or `$search compound` clauses depending on mode.
- The DSL NEVER emits `$where`, `$function`, or `$accumulator`.
- Extra `persistence_mongo_atlas_search_index_ready` gauge for
  observability.

## References

- `docs/operator/config-reference.md` (visibility options).
