#!/usr/bin/env bash
# coverage-merge.sh — merge unit + integration coverage profiles and
# enforce the combined >=80% coverage gate.
#
# Inputs (env):
#   UNIT_PROFILE         default: coverage.out          — from make unit
#   INTEGRATION_PROFILE  default: coverage-integration.out — from make integration-coverage
#   COVERED_PKGS         default: (same list as Makefile)
#   THRESHOLD            default: 80.0
#
# Output:
#   coverage-combined.out — merged go-cover profile
#   prints "coverage-combined OK" or "FAIL" and exits non-zero on miss.
#
# Go-cover profiles are line-based: `file.go:startLine.startCol,endLine.endCol numStmt count`.
# Merge strategy: keep the max `count` per unique block across both
# profiles. Blocks that never appear in unit/integration are treated
# as count=0 (uncovered) which is the conservative default.
set -euo pipefail

UNIT_PROFILE="${UNIT_PROFILE:-coverage.out}"
INTEGRATION_PROFILE="${INTEGRATION_PROFILE:-coverage-integration.out}"
OUT="${OUT:-coverage-combined.out}"
THRESHOLD="${THRESHOLD:-80.0}"

if [[ ! -s "$UNIT_PROFILE" ]]; then
  echo "coverage-merge: missing $UNIT_PROFILE; run 'make unit' first" >&2
  exit 1
fi
if [[ ! -s "$INTEGRATION_PROFILE" ]]; then
  echo "coverage-merge: missing $INTEGRATION_PROFILE; run 'make integration-coverage' first" >&2
  exit 1
fi

# Both profiles share the same mode header ("set"/"count"/"atomic").
# We keep whichever unit uses and assume integration matches — the
# `-covermode` flag in the Makefile is fixed.
head -n1 "$UNIT_PROFILE" > "$OUT"

# Merge: awk reads each profile's body (skip header), stores max count
# per block key (file+range+numStmt). Emit sorted for determinism.
#
# Coverage profile format (mode != "set"):
#   path/file.go:startLine.col,endLine.col numStmt count
# Key = field1 + field2 (range + numStmt). Value = max($3) across files.
awk '
  FNR == 1 { next } # skip mode header
  {
    key = $1 " " $2
    if (!(key in cnt) || $3+0 > cnt[key]+0) {
      cnt[key] = $3
    }
  }
  END {
    for (k in cnt) print k, cnt[k]
  }
' "$UNIT_PROFILE" "$INTEGRATION_PROFILE" | sort >> "$OUT"

total=$(go tool cover -func="$OUT" | awk '/^total:/ {gsub("%","",$3); print $3}')
if [[ -z "$total" ]]; then
  echo "coverage-merge: empty combined profile" >&2
  exit 1
fi

awk -v t="$total" -v th="$THRESHOLD" 'BEGIN {
  if (t+0 < th+0) {
    printf "coverage-combined FAIL: %s%% < %s%% (unit+integration)\n", t, th
    exit 1
  }
  printf "coverage-combined OK: %s%% >= %s%% (unit+integration)\n", t, th
}'
