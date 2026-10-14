# Testing

How the test suite is organized and how to run each tier.

## Test tiers

| Tier          | Command               | External deps                | Runtime        |
|---------------|-----------------------|------------------------------|----------------|
| Unit          | `make unit`           | none                         | <30s           |
| Integration   | `make integration`    | Docker (testcontainers)      | 2–5 min        |
| Conformance   | `make conformance`    | Docker (Mongo replica set)   | ~30 min        |
| Live Atlas    | `make atlas-e2e`      | Atlas cluster env vars       | 5–10 min       |
| Load          | `make load`           | Docker + k6 or equivalent    | 30 min+        |

Build tags gate the tiers:

- `-tags=integration` — integration tests (testcontainers).
- `-tags=conformance` — upstream persistence conformance suite.
- `-tags=atlas` — tests that require a real Atlas Search index.

## Unit tests

Pure Go. No network, no disk outside `t.TempDir()`.

```bash
go test -run TestUpdateShard_StaleRangeID_ReturnsShardOwnershipLost \
    ./internal/shardstore/ -v
```

Unit tests fake the Mongo driver facade at `internal/mongo/`; they
are fast enough to run on every file save.

## Integration tests

`make integration` spins up a Mongo 6.0 replica set via
testcontainers-go. Tests tagged `integration` exercise real driver
paths (transactions, CAS, range queries, cursors).

Run a single integration test:

```bash
go test -tags=integration -run TestM2_ExecutionStore_Live \
    ./internal/executionstore/ -v -timeout=10m
```

If Docker is unavailable, integration tests skip with a clear
message — they do not silently pass.

## Conformance tests

`make conformance` runs the upstream Temporal persistence
conformance suite against a MongoDB replica set. This is the
acceptance gate for changes to persistence semantics.

Conformance output is verbose (~30 minutes). Prefer running it
before opening a PR that touches persistence semantics.

## Live Atlas tests

Requires these env vars:

```
MONGO_TEMPORAL_ATLAS_URI=mongodb+srv://...
MONGO_TEMPORAL_ATLAS_DB_DEFAULT=...
MONGO_TEMPORAL_ATLAS_DB_VISIBILITY=...
```

The test harness creates and drops its own indexes; never point it
at a production cluster.

## Load tests

`make load` runs against a local Mongo replica set and reports
STPS (sustained transactions per second) for the key operations.
See `docs/operator/sizing.md` for the baselines we target.

## Test data

- `testdata/` subdirectories under each package hold static
  fixtures (schema JSON, sample blobs).
- Large corpora (>10 MB) stay out of git; regenerate them via
  scripts under `test/fixtures/gen/`.

## Coverage

`make unit` writes `coverage.out`. `make coverage-gate` enforces a
60% unit-only floor; `make integration-coverage` followed by
`make coverage-combined-gate` enforces a 65% floor on the merged
unit + integration profile. The long-term target is 80% combined.

## Flaky-test policy

Flaky tests are real bugs. If a test fails intermittently:

1. Capture the failure (timestamp, seed, full log).
2. Open an issue labelled `area/testing` with the capture.
3. Quarantine the test via `t.Skip("flaky — issue #N")` only if it
   blocks a release branch. Do not quarantine silently.

A flaky test that is quarantined more than two weeks without root
cause becomes a release blocker.
