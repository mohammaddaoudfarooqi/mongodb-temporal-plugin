# Testing

How the test suite is organized and how to run each tier.

## Test tiers

| Tier | Command | External deps | Runtime |
|---|---|---|---|
| Unit | `make unit` | none | under 1 min |
| Integration | `make integration` | Docker (testcontainers) | 2–5 min |
| Conformance | `make conformance` | Docker (Mongo replica set) | about 30 min |
| Examples | `make examples` | none | under 1 min |
| Showcase UI | `make ui` | Node.js 20.19+ or 22.12+ | under 1 min |
| Live Atlas | `ci/scripts/atlas-e2e.sh` | `ATLAS_URI`, `ATLAS_DATABASE` | 5–10 min |
| Plugin load | see [Load tests](#load-tests) | Docker | about 1 min |
| Release qualification | `deploy/aws-qualify/scripts/qualify.sh` | AWS and Atlas | 1–6 h |

Build tags gate the tiers:

- `-tags=integration` — integration tests (testcontainers).
- `-tags=conformance` — upstream persistence conformance suite.

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

`ci/scripts/atlas-e2e.sh` runs the Atlas Search visibility lifecycle and
the schema tool's `setup-atlas-search` and `drop-atlas-search` against a
real Atlas cluster. It skips unless these are set:

```
ATLAS_URI=mongodb+srv://...      # a credential URI
ATLAS_DATABASE=...               # a scratch database
```

The tests create and drop their own indexes; never point them at a
production cluster.

## Load tests

The plugin load test measures the execution store's own overhead
against a local MongoDB replica set:

```
PLUGIN_LOADTEST_REPORT=1 go test -tags=integration -run TestLoad_UpdateWorkflow \
  ./internal/executionstore/ -args -loadtest.duration=30s -loadtest.concurrency=32
```

End-to-end throughput on Atlas comes from the release-qualification
harness ([`deploy/aws-qualify`](../../deploy/aws-qualify)); its results
are in the sizing guide's
[What was measured](../operator/sizing.md#what-was-measured).

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
