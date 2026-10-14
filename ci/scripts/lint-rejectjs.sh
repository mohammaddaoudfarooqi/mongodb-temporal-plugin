#!/usr/bin/env bash
# Checks that every query dispatcher rejects server-side JavaScript.
#
# The visibility query path MUST pass every DSL-derived predicate
# through security.RejectJSFilter before dispatching to Mongo
#. This script guards against a future contributor
# adding a new query builder that forgets the call.
#
# Policy: every DSL query is lowered by Store.compile (Aggregation and
# Atlas Search modes alike), so that function must contain a
# RejectJSFilter call. Extend the caller set by listing more
# function-name prefixes in GUARDED_FUNCS below.

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT"

# Function-name prefixes that MUST invoke RejectJSFilter.
GUARDED_FUNCS=("compile")

fail=0
for fn in "${GUARDED_FUNCS[@]}"; do
    # Find the file(s) that define the function.
    while IFS= read -r file; do
        # Extract the function body (naive: grep func header, awk until
        # closing brace on column 0). Fine because we already lint
        # gofmt-clean code.
        body=$(awk -v fn="$fn" '
            /^func / && $0 ~ fn"\\(" { capture = 1 }
            capture { print }
            capture && /^}/ { capture = 0 }
        ' "$file")
        if [ -z "$body" ]; then
            continue
        fi
        if ! grep -q "RejectJSFilter" <<<"$body"; then
            echo "lint-rejectjs: $file defines '$fn' but does not call security.RejectJSFilter"
            fail=1
        fi
    done < <(grep -l "^func.* $fn(" internal/visibility/*.go 2>/dev/null || true)
done

if [ "$fail" -ne 0 ]; then
    echo "lint-rejectjs: FAIL — server-side JavaScript guard missing on at least one dispatch path."
    exit 1
fi
echo "lint-rejectjs: OK"
