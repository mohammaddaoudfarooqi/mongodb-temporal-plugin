# Security

Operator guide for securing the `mongodb-temporal-plugin` deployment.

## Authentication

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

Required role: `readWrite` on the plugin's databases.

```javascript
use temporal
db.createUser({
  user: "temporal_runtime",
  pwd: passwordPrompt(),
  roles: [
    { role: "readWrite", db: "temporal" },
    { role: "readWrite", db: "temporal_visibility" }
  ]
})
```

This role is deliberately narrow. It cannot create indexes, create
collections, or modify cluster-wide settings. If you see a
permission error pointing at those operations at runtime, it
indicates a schema drift — run `update-schema` under the elevated
`temporal_admin` role.

### `temporal_admin` (used by `temporal-mongodb-tool` only)

Required roles: `dbAdmin` on both plugin databases, plus
`atlasAdmin` on the project when using Atlas Search.

```javascript
use temporal
db.createUser({
  user: "temporal_admin",
  pwd: passwordPrompt(),
  roles: [
    { role: "dbAdmin", db: "temporal" },
    { role: "dbAdmin", db: "temporal_visibility" }
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
