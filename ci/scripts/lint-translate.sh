#!/usr/bin/env bash
# Checks that every index setup routes driver errors through Translate.
#
# Every EnsureIndexes / EnsureHistoryIndexes / EnsureTaskIndexes /
# EnsureDLQIndexes function in the store packages must route the
# index-creation error through internalmongo.Translate before
# returning. Raw `return err` leaks driver-specific error
# types past the classification layer.
#
# Policy: any function whose name matches ^Ensure.*Indexes?$ under
# internal/ must contain either an explicit "internalmongo.Translate"
# call OR return nil literally.

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT"

fail=0
# Scan every Go file under internal/ that defines an Ensure*Indexes
# function.
while IFS= read -r file; do
    # Skip test files.
    case "$file" in *_test.go) continue;; esac
    # testutil/conformance is a harness package that only composes
    # other stores' EnsureIndexes calls; by construction its errors
    # are already translated upstream.
    case "$file" in */testutil/conformance/*) continue;; esac
    while IFS= read -r fn; do
        # Extract the function body.
        body=$(awk -v fn="$fn" '
            /^func / && $0 ~ "\\<"fn"\\(" { capture = 1 }
            capture { print }
            capture && /^}/ { capture = 0 }
        ' "$file")
        if [ -z "$body" ]; then
            continue
        fi
        # A function that only delegates to other store Ensure* helpers
        # is in the translate chain transitively — accept.
        if grep -qE '\.Ensure(Indexes|HistoryIndexes|TaskIndexes|DLQIndexes)\>' <<<"$body"; then
            continue
        fi
        # Otherwise must contain a Translate call directly.
        if ! grep -q "internalmongo.Translate\|mongo.Translate" <<<"$body"; then
            echo "lint-translate: $file:$fn does not call internalmongo.Translate on the error path"
            fail=1
        fi
    done < <(grep -oE "^func Ensure\w*Indexes?" "$file" | awk '{print $2}')
done < <(grep -rlE "^func Ensure\w*Indexes?" --include="*.go" internal/ 2>/dev/null | grep -v _test.go)

if [ "$fail" -ne 0 ]; then
    echo "lint-translate: FAIL — at least one EnsureIndexes function returns raw driver errors."
    exit 1
fi
echo "lint-translate: OK"
