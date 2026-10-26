# Deployment

Minimum deployment shape for running Temporal on MongoDB with this plugin.

## Quickstarts

To try the plugin before planning a production deployment, use one of the
quickstarts under `quickstart/`:

- `quickstart/docker` — everything on one machine with Docker: MongoDB 8.2
  with search (`mongodb-atlas-local`), Temporal with the plugin in
  `atlas-search` visibility mode, the Temporal UI and the showcase app.
- `quickstart/aws` — an Atlas M10 cluster peered with an EC2 host on AWS
  that runs Temporal, the showcase and the UI, provisioned with the
  Terraform under `deploy/aws-qualify`.

Both run `examples/showcase`, a support-desk demo app built on Temporal
workflows and MongoDB search. `examples/e2e` is a separate Temporal
application that exercises the plugin end to end against a live server.

## Prerequisites

- MongoDB 6.0 or later, in one of the supported topologies:
  - A replica set (self-hosted or MongoDB Atlas dedicated M10+).
  - A sharded cluster.
- A Temporal server binary built with this plugin — `temporal-mongodb-server`.
- Network reachability between Temporal server processes and the MongoDB
  cluster on port 27017 (or Atlas SRV-resolved endpoints).
- TLS enabled (always, on Atlas; strongly recommended self-hosted).

Standalone `mongod` is NOT supported (no transaction support). The plugin
fails startup fast in that case.

### Self-hosted MongoDB on Linux kernel 6.19 or newer

Linux 6.19 changed `rseq` behaviour in a way that breaks the memory
allocator in MongoDB 8.0 and later, so these releases refuse to start on
affected kernels (SERVER-121912, SERVER-125742). Kernels 7.0.14+ and
7.1+ restore the old behaviour, but some MongoDB releases misread
Ubuntu's kernel version string (`7.0.0-…`) and still refuse
(SERVER-136848). On Ubuntu 26.04, or Ubuntu 24.04 with the HWE kernel:

| MongoDB | Starts |
|---|---|
| 7.0, 8.2 | yes |
| 8.0 up to 8.0.34, 8.3 up to 8.3.13 | no: exits with "Linux kernel versions 6.19 and newer has a known incompatibility" |
| 9.0.4 and later | yes |

MongoDB lists 8.0.35 and 8.3.14 as the fixed 8.0 and 8.3 releases. Until
you can run one of those, use 9.0.4+ or 8.2, or a host kernel older than
6.19. Atlas is not affected: Atlas manages the host kernel.

## Steps

### 1. Provision the MongoDB cluster

- Atlas: create an M10-or-higher dedicated cluster in the same region as
  the Temporal workers. Enable VPC peering or PrivateLink.
- Self-hosted: deploy a 3-node replica set; enable journaling and WT
  compression; provision storage for expected write IOPS + oplog headroom.

### 2. Create the database users

Both stores, Temporal's state and its visibility (workflow search),
live in one database, `temporal`. The Atlas Search index sits on the
visibility collection in that database: Atlas builds it from the
collection itself, with no separate search system to sync.

Two least-privilege users are recommended:

- `temporal_runtime` — `readWrite` on `temporal`.
- `temporal_admin`  — `dbAdmin` on `temporal`, used only for
  `setup-schema`/`update-schema` and Atlas Search index management.

### 3. Install the schema

```
temporal-mongodb-tool setup-schema --uri "$ADMIN_URI" --database temporal --num-history-shards 512
```

(Atlas Search visibility only) install the search index:

```
temporal-mongodb-tool setup-atlas-search --uri "$ADMIN_URI" --database temporal
```

### 4. Configure Temporal server

YAML snippet (see `docs/operator/config-reference.md` for every option):

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
          uri: mongodb+srv://temporal_runtime:${MONGO_PASSWORD}@cluster0.example.mongodb.net/
          database: temporal
    mongo-visibility:
      customDatastore:
        name: mongodb
        indexName: temporal_visibility
        options:
          uri: mongodb+srv://temporal_runtime:${MONGO_PASSWORD}@cluster0.example.mongodb.net/
          database: temporal
          visibilityMode: atlas-search
```

Both datastores name the same database. Each datastore opens its own
connection pool, so size `maxPoolSize` for two pools per Temporal
process.

#### Optional: separate databases

If you want database-level permissions, for example a reporting tool
with read-only access to workflow search but no access to workflow
state, give the visibility store its own database. Set up each
database with only its own store's collections:

```
temporal-mongodb-tool setup-schema --uri "$ADMIN_URI" --database temporal --stores core --num-history-shards 512
temporal-mongodb-tool setup-schema --uri "$ADMIN_URI" --database temporal_visibility --stores visibility --num-history-shards 512
temporal-mongodb-tool setup-atlas-search --uri "$ADMIN_URI" --database temporal_visibility
```

and set `database: temporal_visibility` on `mongo-visibility`. Grant
`temporal_runtime` `readWrite` on both databases. Restore the two
databases to the same point in time: restoring only one leaves
workflow search out of step with workflow state.

### 5. Start Temporal server

```
temporal-mongodb-server --config config.yaml
```

Expected startup log lines:

- "MongoDB connection established (topology=replicaset, transactions-enabled=true)"
- "Schema version check passed (current=<ver>)"

### 6. Verify

```
temporal-mongodb-tool verify --config config.yaml
```

Verifies schema version, required collections/indexes, Atlas Search index
status (if Search mode).

## Upgrades

1. Update `temporal-mongodb-tool` and `temporal-mongodb-server` binaries
   to the new plugin version.
2. Run `update-schema --uri ... --database temporal` against the cluster
   (each database, with separate databases, passing its `--stores`). It
   applies the latest schema version unless you pass `--schema-version`.
3. Roll Temporal server processes.

### Upgrading a two-database deployment

Both stores now share one database by default (ADR-011). A deployment
whose config names its databases explicitly is unaffected: keep
`database: temporal_visibility` on `mongo-visibility`.

The bundled configuration changed: `deploy/config/development.yaml`,
which the Docker images and Compose stacks use, now points the
visibility store at `temporal`. An existing stack started from it before
this change keeps its workflow search data in `temporal_visibility`, so
after upgrading, existing workflows would be missing from List and
Count. Either keep running the previous image, or start that stack
afresh (`down -v`) on the new one.

## Rollback

- Plugin versions guarantee forward compatibility within a major release.
  Down-rev rollback within a minor release is supported.
- Full schema rollback is NOT supported; backups + restore is the DR path.

## Further reading

- `docs/operator/sizing.md` — cluster sizing guidance.
- `docs/operator/security.md` — credentials, TLS, least privilege.
- `docs/operator/sharding.md` — sharded-cluster setup.
- `docs/operator/runbook.md` — common incident procedures.
