#!/usr/bin/env bash
# Atlas live gates on the functional host: the Atlas Search lifecycle
# (M5), the schema CLI's Atlas Search commands (M6) and the Atlas
# conformance run, built from the shipped source with the shipped Go.
# Results go to results/phases/atlas-gates.json; a skipped gate fails,
# because a skip here means the gate could not see Atlas.
set -euo pipefail
# shellcheck source=common.sh
. "$(dirname "$0")/common.sh"

if [[ "${GATES_SKIP_UNPACK:-0}" != 1 ]]; then
  rm -rf "$ROOT/go" "$ROOT/src"; mkdir -p "$ROOT/src"
  tar -C "$ROOT" -xzf "$ROOT"/go*.linux-amd64.tar.gz
  tar -C "$ROOT/src" -xzf "$ROOT/source.tgz"
fi
GO=${GATES_GO:-$ROOT/go/bin/go}
# M6 builds the schema CLI with the go on PATH.
export PATH="$ROOT/go/bin:$PATH"
export GOPATH="$ROOT/gopath" GOCACHE="$ROOT/gocache" GOFLAGS=-mod=mod
ATLAS_URI=$(cat "$ROOT/mongo.uri"); export ATLAS_URI
export ATLAS_DATABASE=tmc_gates

out="$RESULTS/logs/atlas-gates.jsonl"
: > "$out"
cd "$ROOT/src"
# Every gate must report a result; a gate whose package fails to build
# or whose run dies reports none, and counts as failed.
EXPECTED="TestM5_AtlasLifecycle_Live TestM6_CLIAtlasSearch_Live TestConformance_Atlas"
rc=0 cmdfail='[]'
gate() { # gate PKG PATTERN TIMEOUT
  local code=0
  "$GO" test -tags=integration -json -timeout "$3" -run "$2" "$1" >> "$out" 2>>"$RESULTS/logs/atlas-gates.log" || code=$?
  if ((code != 0)); then
    rc=1
    cmdfail=$(jq -c --arg p "$1" --argjson c "$code" '. + [{name: ("go test " + $p), pass: false, duration_ms: 0, error: ("exited \($c); see atlas-gates.log")}]' <<<"$cmdfail")
  fi
}
gate ./internal/testutil/ '^(TestM5_AtlasLifecycle_Live|TestM6_CLIAtlasSearch_Live)$' 60m
gate ./internal/testutil/conformance/ '^TestConformance_Atlas$' 90m

# Top-level tests only; pass, fail, or skip (reported as a failure).
# Expected gates that never reported are added as failures.
jq -s --argjson cmd "$cmdfail" --arg expected "$EXPECTED" '
  [ .[] | select(.Test != null and (.Test | contains("/") | not))
    | select(.Action == "pass" or .Action == "fail" or .Action == "skip")
    | {name: .Test, pass: (.Action == "pass"), duration_ms: ((.Elapsed // 0) * 1000 | floor),
       error: (if .Action == "fail" then "failed; see atlas-gates.jsonl" elif .Action == "skip" then "skipped" else null end)}
    | with_entries(select(.value != null)) ] as $tests
  | ($tests | map(.name)) as $seen
  | {phase: "atlas-gates", scenarios: ($tests
      + [ $expected | split(" ")[] | select(. as $n | $seen | index($n) | not)
          | {name: ., pass: false, duration_ms: 0, error: "did not run"} ]
      + $cmd)}' "$out" > "$RESULTS/phases/atlas-gates.json"
jq -e 'all(.scenarios[]; .pass)' "$RESULTS/phases/atlas-gates.json" >/dev/null || rc=1
exit "$rc"
