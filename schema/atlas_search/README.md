# Atlas Search index manifests

This directory holds the Atlas Search index definitions the plugin
needs in Atlas Search mode. `temporal-mongodb-tool
setup-atlas-search` applies every manifest here to the target
cluster.

## Format

Each `*.json` file is a top-level object:

| Field             | Purpose                                                |
|-------------------|--------------------------------------------------------|
| `name`            | Index name passed to `createSearchIndex`               |
| `collection`      | Target collection in the plugin's database             |
| `definition`      | The Atlas Search index definition (mappings, synonyms) |
| `synonymMappings` | Optional synonym-collection references                 |

## Field-type choices

- `token`: exact-match fields (workflow_id, run_id, namespace_id, task_queue,
  workflow_type_name, status). Temporal's DSL uses `= / IN / STARTS_WITH`
  on these; `token` supports keyword equality + prefix.
- `number`: int64 / int32 scalars used in range predicates.
- `date`: all timestamps. Supports BETWEEN.
- `document` with `dynamic: true`: the `search_attributes` sub-map —
  Atlas auto-indexes each field type on insert, so adding a custom SA
  does not require an index rebuild.

## Validation

The manifest is loaded at startup by
`temporal-mongodb-tool setup-atlas-search`, which calls
`db.command({ createSearchIndex: ... })`. If the definition collides
with an existing index, the tool updates it via `updateSearchIndex`.
