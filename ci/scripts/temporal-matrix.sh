#!/usr/bin/env bash
# Temporal server last-2-minor matrix check.
#
# Flips go.temporal.io/server to the version passed in TEMPORAL_VERSION
# and runs `go build ./... + go vet ./... + go test -short ./...` so
# we catch upstream interface drift the next minor introduces. Restores
# go.mod / go.sum on exit regardless of outcome.
#
# Usage:
#   TEMPORAL_VERSION=v1.30.2 ci/scripts/temporal-matrix.sh
#
# Exit codes:
#   0  build + vet + unit green on the pinned version
#   >0 drift detected; failing output shows which file / symbol broke.
#
# Scope:
#   - Unit tests only. Integration / conformance / atlas tiers use the
#     current pinned version (no point running them twice).
#   - Pins are expected to be tag-form (vMAJOR.MINOR.PATCH). Pseudo
#     versions work but are discouraged for stability pins.

set -euo pipefail

if [[ -z "${TEMPORAL_VERSION:-}" ]]; then
  echo "TEMPORAL_VERSION not set" >&2
  echo "Usage: TEMPORAL_VERSION=v1.30.2 $0" >&2
  exit 2
fi

echo "Running Temporal-matrix compat check against ${TEMPORAL_VERSION}"

# Snapshot go.mod + go.sum so we can restore on exit. The restore guard
# runs even on failure so developers don't end up with a half-mutated
# workspace.
TMP=$(mktemp -d)
cp go.mod "${TMP}/go.mod.bak"
cp go.sum "${TMP}/go.sum.bak"
restore() {
  mv "${TMP}/go.mod.bak" go.mod
  mv "${TMP}/go.sum.bak" go.sum
  rm -rf "${TMP}"
}
trap restore EXIT

# `go get` moves dependents consistently. `go mod tidy` is deliberately
# not used: it resolves any package missing from the requested release
# at the newest server version, silently testing a different minor.
go get "go.temporal.io/server@${TEMPORAL_VERSION}"
resolved="$(go list -m -f '{{.Version}}' go.temporal.io/server)"
if [[ "${resolved}" != "${TEMPORAL_VERSION}" ]]; then
  echo "FAIL: requested go.temporal.io/server ${TEMPORAL_VERSION}, module graph resolved ${resolved}" >&2
  exit 1
fi
export GOFLAGS=-mod=mod

echo "--- go build ./..."
go build ./...

echo "--- go vet ./..."
go vet ./...

echo "--- go test -short ./..."
go test -short ./...
echo "--- go test -tags=integration ./... (includes upstream persistence suites)"
go test -race -tags=integration ./...

echo "Temporal-matrix compat: PASS on ${TEMPORAL_VERSION}"
