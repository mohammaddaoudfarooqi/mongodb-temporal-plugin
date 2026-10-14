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

## Steps

### 1. Provision the MongoDB cluster

- Atlas: create an M10-or-higher dedicated cluster in the same region as
  the Temporal workers. Enable VPC peering or PrivateLink.
- Self-hosted: deploy a 3-node replica set; enable journaling and WT
  compression; provision storage for expected write IOPS + oplog headroom.

### 2. Create databases and users

Two least-privilege users are recommended:

- `temporal_runtime` — `readWrite` on `temporal` and `temporal_visibility`.
- `temporal_admin`  — `dbAdmin` on the same databases, used only for
  `setup-schema`/`update-schema` and Atlas Search index management.

### 3. Install the schema

```
temporal-mongodb-tool setup-schema --config config.yaml
```

(Optional; Atlas Search only) install search indexes:

```
temporal-mongodb-tool setup-atlas-search --config config.yaml
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
          database: temporal_visibility
          visibilityMode: atlas-search
          atlasSearchIndexName: vis_default
```

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
2. Run `update-schema --target <version>` against the cluster.
3. Roll Temporal server processes.

## Rollback

- Plugin versions guarantee forward compatibility within a major release.
  Down-rev rollback within a minor release is supported.
- Full schema rollback is NOT supported; backups + restore is the DR path.

## Further reading

- `docs/operator/sizing.md` — cluster sizing guidance.
- `docs/operator/security.md` — credentials, TLS, least privilege.
- `docs/operator/sharding.md` — sharded-cluster setup.
- `docs/operator/runbook.md` — common incident procedures.
