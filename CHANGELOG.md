# Changelog

## 0.1.0: first public release

- Temporal persistence on MongoDB for every store: shards, executions,
  history, task queues (classic and fair), namespaces, cluster metadata,
  Nexus endpoints and queues. Validated with Temporal's upstream
  persistence suites on Temporal 1.31 and 1.32.
- Visibility in two modes: `atlas-search` (Atlas Search, including
  full-text Text attributes) and `aggregation`.
- `temporal-mongodb-tool` for the versioned schema (`setup-schema`,
  `update-schema`, `verify`) and the Atlas Search index.
- Quickstarts for Docker (MongoDB 8.2 with search) and AWS (Atlas M10
  with VPC peering).
- The support-desk showcase, and end-to-end and load runners.
- An AWS + Atlas release-qualification harness: functional and load
  profiles, Atlas failover, node loss, soak, and a GO/NO-GO report.
