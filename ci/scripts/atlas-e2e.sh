#!/usr/bin/env bash
# Atlas live-cluster end-to-end.
#
# Runs the M5 (Atlas Search visibility lifecycle) and M6 (CLI
# setup-atlas-search / drop-atlas-search) gates against a real Atlas
# cluster. Invoked by the scheduled `atlas-e2e` CI job; skips locally
# unless ATLAS_URI + ATLAS_DATABASE are set.
#
# Required env:
#   ATLAS_URI           mongodb+srv://... credential URI
#   ATLAS_DATABASE      visibility database name
# Optional env:
#   ATLAS_PUBLIC_KEY    Atlas Admin API public key (future matrix work)
#   ATLAS_PRIVATE_KEY   Atlas Admin API private key
#   ATLAS_PROJECT_ID    Atlas project ID for cluster provisioning
#
# Exit codes:
#   0   gates green, or ATLAS_URI unset (skip — CI treats as success)
#   >0  test failure; see log for detail
#
# Secrets hygiene:
#   - Never echo ATLAS_URI; every `set -x` in this script must stay off.
#   - testcontainers output stays — it does not include the URI.
#   - The tests themselves scrub credentials from error paths via
#     internal/security.RedactURI.

set -euo pipefail

if [[ -z "${ATLAS_URI:-}" || -z "${ATLAS_DATABASE:-}" ]]; then
  echo "ATLAS_URI / ATLAS_DATABASE not set — skipping Atlas e2e gate"
  exit 0
fi

: "${GO_TEST_TIMEOUT:=15m}"

echo "Running M5 Atlas Search visibility gate..."
go test -tags=integration \
    -run 'TestM5_AtlasLifecycle_Live|TestM5_AtlasProbe_RejectsPlainMongo' \
    -v -timeout="${GO_TEST_TIMEOUT}" \
    ./internal/testutil/

echo "Running M6 Atlas Search CLI gate..."
go test -tags=integration \
    -run 'TestM6_CLIAtlasSearch_Live' \
    -v -timeout="${GO_TEST_TIMEOUT}" \
    ./internal/testutil/

echo "Atlas e2e gates: PASS"
