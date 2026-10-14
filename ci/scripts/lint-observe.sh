#!/usr/bin/env bash
# Checks that every store operation is observed (metrics and logs).
#
# Every public method declared on a store Stub / V2Stub / Store in
# the listed store packages must call observeOp (or delegate to
# another public method on the same receiver). Without the
# decorator, persistence_mongo_* metrics for that operation never
# fire — which is exactly what this lint prevents.
#
# Strategy:
#   1. grep for public-method declarations.
#   2. For each, scan the next 20 lines for `observeOp(`.
#   3. If not found, check for a same-receiver method delegation.
#   4. Anything left over is a violation.
#
# Exempts accessors that intentionally skip metrics.

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT"

STORES=(
    internal/shardstore
    internal/metadatastore
    internal/clustermetastore
    internal/executionstore
    internal/taskstore
    internal/nexusstore
    internal/queuestore
    internal/visibility
)

# Methods exempt from instrumentation — no I/O or deliberately
# side-effect-free accessors.
declare -A EXEMPT
for name in Close GetName GetClusterName GetIndexName \
            GetHistoryBranchUtil ValidateCustomSearchAttributes Init; do
    EXEMPT[$name]=1
done

fail=0
for pkg in "${STORES[@]}"; do
    for f in "$pkg"/*.go; do
        case "$f" in *_test.go) continue;; esac
        # Find every line defining a public method on *Stub|*V2Stub|*Store.
        while IFS=: read -r lineno decl; do
            # Extract just the method name — the character class right
            # after the closing `)` of the receiver.
            fn=$(echo "$decl" | sed -E 's/^func \([^)]+\) ([A-Z][A-Za-z0-9_]*)\(.*/\1/')
            if [[ -z "$fn" || "$fn" == "$decl" ]]; then
                continue
            fi
            if [[ -n "${EXEMPT[$fn]:-}" ]]; then
                continue
            fi
            # Scan the method body for observeOp.
            # Use awk for the window (not `tail | head` — that combo
            # races SIGPIPE on fast readers and `set -o pipefail`
            # promotes it to a script-level failure, which CI hit
            # with exit 141).
            # The window is the method body: from the signature to the
            # first closing brace at column 0, so a neighbouring method's
            # observeOp cannot satisfy the check.
            window=$(awk -v start="$lineno" \
                'NR >= start { print; if (/^}/) exit }' "$f")
            # Here-strings, not `echo | grep -q`: grep -q exits on the
            # first match, and under pipefail a SIGPIPE on the writer
            # turned a present observeOp into a spurious failure on CI.
            if grep -qE '\.observeOp\(|\.observeOpV2\(' <<<"$window"; then
                continue
            fi
            # Allow a pure delegation (body returns through another
            # public method on the same receiver — already observed
            # there, no double-count).
            if grep -qE 'return (s|v|f)\.[A-Z][A-Za-z0-9_]*\(' <<<"$window"; then
                continue
            fi
            echo "lint-observe: $f:$lineno: $fn lacks observeOp"
            fail=1
        done < <(grep -nE '^func \([a-z] \*(Stub|V2Stub|Store)\) [A-Z][A-Za-z0-9_]*\(' "$f" || true)
    done
done

if [ "$fail" -ne 0 ]; then
    echo "lint-observe: FAIL — at least one public store method lacks metrics instrumentation."
    exit 1
fi
echo "lint-observe: OK"
