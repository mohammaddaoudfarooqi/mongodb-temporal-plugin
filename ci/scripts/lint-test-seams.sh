#!/usr/bin/env bash
# Checks that no test-only seams leak into production code.
#
# Production Go files (non-_test.go) MUST NOT declare identifiers
# ending in "ForTest" or "ForTests" — those are test-only seams and
# belong in a _test.go file so they never ship in a production binary.
#
# Rationale: keeps the public API honest. If a test needs a seam, put
# it behind the `_test.go` suffix.

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT"

hits=$(grep -rEn 'func .*ForTests?\b' --include="*.go" \
           internal/ pkg/ cmd/ 2>/dev/null \
           | grep -v '_test.go:' || true)

if [ -n "$hits" ]; then
    echo "lint-test-seams: FAIL — production code declares test-only seams:"
    echo "$hits"
    echo
    echo "Move these symbols into a *_test.go file in the same package."
    exit 1
fi
echo "lint-test-seams: OK"
