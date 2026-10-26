# Configuration reference

Every option the plugin reads from `customDatastore.options` in
Temporal's YAML config. Unknown options are tolerated (warning, not
error) so forward-compatible configs survive plugin downgrades.

## Connection

| Option        | Type    | Default | Purpose                                       |
|---------------|---------|---------|-----------------------------------------------|
| `uri`         | string  | -       | Full MongoDB connection URI (SRV supported)   |
| `hosts`       | []string| -       | Host list, alternative to `uri`               |
| `replicaSet`  | string  | -       | Replica set name when using `hosts`           |
| `database`    | string  | *required* | Database name                              |
| `appName`     | string  | `temporal-mongodb-plugin` | Logged on server side for tracing |

`uri` and `hosts` are mutually exclusive. Supply one.

## Authentication

| Option             | Type    | Default | Purpose                                 |
|--------------------|---------|---------|-----------------------------------------|
| `username`         | string  | -       | Database user                           |
| `password`         | string  | -       | Inline password (discouraged)           |
| `passwordEnv`      | string  | -       | Env-var name holding the password       |
| `passwordCommand`  | string  | -       | Shell command whose stdout is the password |
| `passwordCommandTimeout` | int | 5000  | Milliseconds for passwordCommand to finish |
| `authSource`       | string  | -       | Authentication database                 |

## TLS

| Option                 | Type    | Default | Purpose                               |
|------------------------|---------|---------|---------------------------------------|
| `tlsCAFile`            | string  | -       | CA certificate path                   |
| `tlsCertFile`          | string  | -       | Client certificate path               |
| `tlsKeyFile`           | string  | -       | Client key path                       |
| `tlsInsecureSkipVerify`| bool    | false   | Skip TLS verification (dev only)      |

## Driver tuning

| Option                       | Type | Default  | Purpose                              |
|------------------------------|------|----------|--------------------------------------|
| `readPreference`             | str  | primary  | Read preference                      |
| `writeConcern`               | str  | majority | Write concern (non-majority warns)   |
| `readConcern`                | str  | majority | Read concern                         |
| `maxPoolSize`                | int  | driver default | Maximum connection pool size   |
| `minPoolSize`                | int  | 0        | Minimum connection pool size         |
| `maxConnecting`              | int  | driver default | Max concurrent connect attempts|
| `connectTimeoutMs`           | int  | driver default | Connection timeout             |
| `serverSelectionTimeoutMs`   | int  | driver default | Server selection timeout       |
| `socketTimeoutMs`            | int  | driver default | Per-operation timeout          |
| `heartbeatIntervalMs`        | int  | driver default | SDAM heartbeat cadence         |
| `retryWrites`                | bool | true     | Retryable writes                     |
| `retryReads`                 | bool | true     | Retryable reads                      |

## Visibility

| Option                  | Type   | Default      | Purpose                                 |
|-------------------------|--------|--------------|-----------------------------------------|
| `visibilityMode`        | string | aggregation  | `aggregation` or `atlas-search`         |
| `atlasSearchIndexName`  | string | `visibility_executions_search` | Atlas Search index name |

When `visibilityMode=atlas-search`, the plugin probes
`$listSearchIndexes` at startup and fails fast if unsupported.
It also polls the index status; reads return
`Unavailable` until READY. Writes always land.

## Schema checks

| Option              | Type | Default | Purpose                                 |
|---------------------|------|---------|-----------------------------------------|
| `numHistoryShards`  | int  | 0       | Must match `_schema_version` value; startup refuses mismatch |
| `skipSchemaCheck`   | bool | false   | Bypasses the startup schema-version check. Tests only — never set in production. |

## Metrics and observability

The plugin emits metrics under the `persistence_mongo_*` namespace
via Temporal's `metrics.Handler`. No per-plugin config option
affects metric emission; Temporal's own tally/otel exporter
configuration applies.

Tags:
- `store`: `shardstore`, `executionstore`, `taskstore`, …
- `operation`: method name, e.g., `UpdateShard`
- `category` (on errors only): `unavailable`, `internal`,
  `invalid_argument`, `resource_exhausted`, `not_found`, `unknown`

`namespace_id`, `shard_id`, `workflow_id`, `run_id` are NOT metric
tags (would explode cardinality). They are log tags.

## Example

```yaml
persistence:
  defaultStore: mongo-default
  visibilityStore: mongo-visibility
  numHistoryShards: 512
  datastores:
    mongo-default:
      customDatastore:
        name: mongodb
        options:
          uri: mongodb+srv://cluster0.example.mongodb.net/
          username: temporal_runtime
          passwordEnv: MONGO_TEMPORAL_PW
          database: temporal
          numHistoryShards: 512
          readPreference: primary
          writeConcern: majority
          maxPoolSize: 200
    mongo-visibility:
      customDatastore:
        name: mongodb
        indexName: temporal_visibility
        options:
          uri: mongodb+srv://cluster0.example.mongodb.net/
          username: temporal_runtime
          passwordEnv: MONGO_TEMPORAL_PW
          database: temporal          # the same database as mongo-default
          visibilityMode: atlas-search
```

Both stores share one database by default. To give the visibility store
its own database, see
[Optional: separate databases](deployment.md#optional-separate-databases).
