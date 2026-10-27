# Security

Operator guide for securing the `mongodb-temporal-plugin` deployment.

## Secure the Temporal frontend first

The plugin is a storage layer: it trusts the Temporal server above it to
decide who may call it. **Out of the box, Temporal's frontend (gRPC
7233, HTTP 7243) and the Temporal UI accept every request.** Anyone who
reaches them can start, signal, terminate and read every workflow, and
through them read the data in MongoDB. The quickstarts keep these ports
on `127.0.0.1` (Docker) or behind an SSH tunnel (AWS) for exactly this
reason.

Before anyone but you can reach the frontend or the UI:

1. **Turn on TLS, with client certificates (mTLS),** for the frontend
   and between Temporal services (`global.tls.frontend` and
   `global.tls.internode` in Temporal's configuration).
2. **Turn on authorization:** a claim mapper and an authorizer
   (`global.authorization`), for example Temporal's JWT claim mapper with
   the default authorizer. Grant each client only the namespaces it needs.
3. **Put the Temporal UI behind your identity provider:**
   `TEMPORAL_AUTH_ENABLED=true` with an OIDC provider
   (`TEMPORAL_AUTH_PROVIDER_URL`, `TEMPORAL_AUTH_CLIENT_ID`,
   `TEMPORAL_AUTH_CLIENT_SECRET`, `TEMPORAL_AUTH_CALLBACK_URL`).
4. Keep the membership ports (6933–6939) and the metrics port (9090)
   reachable only between Temporal hosts.

See Temporal's documentation on
[security](https://docs.temporal.io/self-hosted-guide/security) for the
full configuration.

## What is stored in plaintext

The plugin stores workflow payloads (inputs, results, history events)
as opaque blobs. It never decodes them. If you encrypt them with a
Temporal payload codec, MongoDB only ever holds ciphertext.

**Search attributes, memo, workflow IDs, workflow types and task queue
names are different.** They are stored in plaintext in the visibility
database, and search attributes are also indexed (and, in `atlas-search`
mode, copied into the Atlas Search index) so that they can be queried.
Anyone who can read the database or query visibility can read them.

- Never put personal or confidential data (names, emails, account or
  card numbers, free-text customer content) into search attributes,
  memo or workflow IDs. Use opaque identifiers, such as an internal
  customer key, instead.
- Use a [payload codec](https://docs.temporal.io/payload-codec)
  (client-side encryption) for anything sensitive in workflow inputs and
  results.
- Retention: closed workflows and their visibility records are removed
  after the namespace's retention period (see `retention.md`).



The plugin supports every authentication mechanism the MongoDB Go
driver v2 supports:

- `SCRAM-SHA-256` — default for self-hosted and Atlas.
- `X.509` — certificate-based, recommended when the plugin and the
  database live in the same PKI.
- `MONGODB-AWS` — IAM-based, for deployments where Temporal workers
  run on AWS infrastructure.
- `MONGODB-OIDC` — for workload identity integrations (Azure AD,
  Okta, etc.).

Configure via the connection URI or the individual
`username`/`password`/`authSource` options. See
`docs/operator/config-reference.md`.

## Credentials

### Where credentials may appear

- **YAML config** at `customDatastore.options.password`. Prefer
  `passwordEnv` (env var reference) or `passwordCommand` (shell
  command) over inline secrets.
- **Connection URI**, inline: `mongodb+srv://user:pass@host/`. The
  plugin URL-escapes credential characters automatically.

### Where credentials MUST NEVER appear

- Log lines. The plugin redacts userinfo from every
  error message before surfacing it to Temporal's logger.
- Metric labels. See `persistence_mongo_*` in
  `docs/operator/config-reference.md`.
- Exported OpenTelemetry span attributes.

If you observe a credential in any of the above, report it as a
security bug.

### Rotation

- Atlas: use database user credential rotation via the Atlas UI or
  API. The plugin's `mongo.Client` reconnects automatically on
  auth failure.
- Self-hosted: rotate SCRAM passwords, restart Temporal server
  processes rolling to pick up the new credentials.

The plugin does NOT reload credentials from disk mid-run — a restart
is required.

## TLS

- Atlas enforces TLS on every connection; no configuration required.
- Self-hosted: set `tlsCAFile`, `tlsCertFile`, and `tlsKeyFile` in the
  customDatastore options. `tlsInsecureSkipVerify: true` is
  available for development only and emits a warning at startup.

Rotation: update the CA / cert / key files and restart Temporal. The
plugin does not watch the filesystem.

## Least-privilege database roles

Two users are recommended:

### `temporal_runtime` (used by Temporal server)

Required role: `readWrite` on the plugin's database.

```javascript
use temporal
db.createUser({
  user: "temporal_runtime",
  pwd: passwordPrompt(),
  roles: [
    { role: "readWrite", db: "temporal" }
  ]
})
```

With [separate databases](deployment.md#optional-separate-databases),
add the same role on the visibility database. That layout also lets you
grant a reporting tool `read` on the visibility database alone.

This role is deliberately narrow. It cannot create indexes, create
collections, or modify cluster-wide settings. If you see a
permission error pointing at those operations at runtime, it
indicates a schema drift — run `update-schema` under the elevated
`temporal_admin` role.

### `temporal_admin` (used by `temporal-mongodb-tool` only)

Required roles: `dbAdmin` on the plugin's database (each of them, with
separate databases), plus `atlasAdmin` on the project when using Atlas
Search.

```javascript
use temporal
db.createUser({
  user: "temporal_admin",
  pwd: passwordPrompt(),
  roles: [
    { role: "dbAdmin", db: "temporal" }
  ]
})
```

Only used during `setup-schema`, `update-schema`,
`setup-atlas-search`, and `drop-atlas-search` runs. Temporal's
runtime processes do NOT use this credential.

## Audit logging

MongoDB's audit log (Atlas has one, self-hosted requires enabling) is
the right place to track plugin access. Recommended filters:

- Authentication events on the plugin's runtime user.
- Any `createIndex` / `dropIndex` — should only come from
  `temporal_admin` sessions.
- Any `$where` or `$function` attempts — the plugin's visibility DSL
  never emits these, so an occurrence indicates an upstream
  compromise or a direct-cluster query.

## Server-side JavaScript

The visibility DSL translator does not emit `$where`, `$function`,
or `$accumulator` operators. An
`internal/security.RejectJSFilter` sanity check guards the query
path. If your operator policy requires blocking JavaScript at the
server level (defense in depth), set:

```
mongod --noscripting
```

on self-hosted, or contact Atlas support for the equivalent on
Atlas. The plugin does not depend on server-side JavaScript.

## Secrets management

Recommended patterns:

- Kubernetes: mount secrets as files, read via `passwordCommand`
  (e.g., `cat /secrets/mongo_password`) or `passwordEnv` (via
  projected env vars).
- Nomad/ECS: task-role credentials + `passwordEnv`.
- Bare metal: HashiCorp Vault agent → file → `passwordCommand`.

Inline passwords in YAML are supported but strongly discouraged.


## Configuration files

The Temporal configuration is trusted: `passwordCommand` runs a shell
command at startup, and file options such as `tlsCAFile` and
`tlsKeyFile` choose what the server reads. Anyone who can edit the
configuration can therefore run commands as the server.

- Make configuration files owned by the operator, and not writable by
  anyone else (for example mode 0640, owned by root with the server's
  group).
- Prefer `passwordEnv` (set by your service manager or secret store), or
  a `passwordCommand` that calls a secret manager, over passwords in the
  file.

## Quickstart and qualification scripts

The AWS quickstart and the release-qualification harness
(`deploy/aws-qualify`) create infrastructure with your credentials.

- **Credentials.**
  - Use a **dedicated Atlas project** and a project-level API key.
  - Use **short-lived AWS credentials**: the scripts need AWS access
    only while Terraform creates or deletes resources.
  - Keys are read from a gitignored `.env`. They never appear on
    command lines or in logs: the Atlas key goes into a 0600 curl
    config.
  - Revoke the Atlas key when you no longer need it.
- **Terraform state holds credentials.** The local state
  (`terraform/terraform.tfstate.d/`) contains the generated database
  passwords and connection strings.
  - Treat it like a secret: it is gitignored; keep it on an encrypted
    disk, and don't sync or share it.
  - For regular use, configure an encrypted remote backend with access
    control (for example S3 with SSE-KMS and a DynamoDB lock table).
  - `down` deletes the database users, which revokes the credentials.
- **Database users.** The AWS quickstart creates two users:
  - an admin user, used once to create the schema and indexes and
    never stored on the host;
  - a runtime user with `readWrite` on the Temporal and showcase
    databases only, used by the services.
- **The host.**
  - Temporal, the showcase worker and the showcase UI run as a
    dedicated `temporal` system user, with no login shell and no sudo.
  - The showcase UI, like the Temporal UI, has no authentication. Both
    listen on localhost only and are reached through the SSH tunnel;
    anyone with the tunnel can open, change and resolve tickets.
  - Inbound traffic is SSH only, from your address and the ranges you
    list.
  - IMDSv2 is required, and no IAM role is attached.
- **First SSH connection.** The scripts accept a new host's SSH key on
  first contact (`StrictHostKeyChecking=accept-new`, a per-run
  `known_hosts`).
  - The residual risk is a network attacker answering in the seconds
    after the host is created.
  - To remove it, compare the key with the fingerprints the instance
    prints to its console (`aws ec2 get-console-output`) before the
    first connection.
- **Audit.** AWS CloudTrail and the Atlas project's activity feed record
  every create, delete and failover. Every resource is tagged with its
  owner, purpose, expiry date and run ID.
