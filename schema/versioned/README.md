# Versioned schema manifests

Each `v<major>.<minor>/` directory holds the declarative schema for one
release of the plugin.

## Files

### `collections.json`

Lists every MongoDB collection the plugin touches. `setup-schema`
creates them if missing. The plugin can also create collections
lazily (MongoDB auto-creates on first insert), but an explicit list
lets operators provision storage policies up-front.

```json
{
  "schema_version": "1.0.0",
  "collections": [
    { "name": "shards" },
    ...
  ]
}
```

### `indexes.json`

Declares every B-tree / TTL / partial index. `setup-schema` applies
them idempotently: existing indexes with a matching definition are
no-ops; definition mismatches fail fast with a clear error pointing at
`update-schema`.

```json
{
  "schema_version": "1.0.0",
  "indexes": [
    {
      "collection": "shards",
      "name": "shards_shard_id_unique",
      "keys":   [ { "field": "shard_id", "order": 1 } ],
      "unique": true
    },
    {
      "collection": "cluster_membership",
      "name": "membership_record_expiry_ttl",
      "keys": [ { "field": "record_expiry", "order": 1 } ],
      "ttl":  { "expire_after_seconds": 0 }
    }
  ]
}
```

**Field semantics:**

| Field            | Required | Purpose                                                          |
|------------------|----------|------------------------------------------------------------------|
| `collection`     | yes      | Target collection in the plugin's database                       |
| `name`           | yes      | Index name passed to `createIndexes` (must match grep conventions) |
| `keys`           | yes      | Ordered list of `{field, order}` entries (order is 1 or -1)      |
| `unique`         | no       | `true` if the index enforces uniqueness                          |
| `ttl`            | no       | Sets `expireAfterSeconds`; implies a TTL index                   |
| `partial_filter` | no       | MongoDB partial-filter expression (JSON subset)                  |
| `sparse`         | no       | `true` for sparse indexes                                        |
| `name_only`      | no       | If true, only the name is compared on diff (for legacy migration)|

## Versioning

- Minor bumps (e.g. v1.0 → v1.1): **additive only**. New collections +
  new indexes. Existing indexes' definitions must not change. Safe to
  apply to a running cluster.
- Major bumps (e.g. v1 → v2): may require operator migration. The
  `update-schema` tool runs a resumable state machine.

## Atlas Search

Atlas Search indexes live in `schema/atlas_search/` — they are applied
by a separate `setup-atlas-search` subcommand because their lifecycle
(status polling, definition updates via a different API) differs from
B-tree indexes.
