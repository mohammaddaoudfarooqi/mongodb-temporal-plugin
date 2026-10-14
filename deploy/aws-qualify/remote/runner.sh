#!/usr/bin/env bash
# Runs one qualification phase with the e2e runner and leaves its results in
# results/phases/<phase>.json, even if the runner dies.
#
#   runner.sh PHASE [e2e flags...]
#   runner.sh --background PHASE [e2e flags...]   start and return
#   runner.sh --wait PHASE                        wait; exit with its code
#   runner.sh --fault PHASE REASON                record that PHASE's fault was not injected
#   runner.sh --skip PHASE REASON                 record that PHASE could not run
#   runner.sh --annotate PHASE TEXT               note the injected fault and recovery
set -euo pipefail
# shellcheck source=common.sh
. "$(dirname "$0")/common.sh"

mode="foreground"
case "${1:-}" in --background) mode="background"; shift ;; --wait) mode="wait"; shift ;; --fault) mode="fault"; shift ;; --skip) mode="skip"; shift ;; --annotate) mode="annotate"; shift ;; esac
phase=${1:?phase name required}; shift
json="$RESULTS/phases/$phase.json"
rcfile="$RUNDIR/$phase.rc"

if [[ "$mode" == annotate ]]; then
  [[ -s "$json" ]] || printf '{"phase":"%s"}\n' "$phase" > "$json"
  jq --arg t "${1:?text required}" '.fault = $t' "$json" > "$json.tmp" && mv "$json.tmp" "$json"
  exit 0
fi

if [[ "$mode" == skip ]]; then
  jq -n --arg p "$phase" --arg r "not run: ${1:?reason required}" '{phase: $p, setup_error: $r}' > "$json"
  exit 0
fi

if [[ "$mode" == fault ]]; then
  reason=${1:?reason required}
  [[ -s "$json" ]] || printf '{"phase":"%s"}\n' "$phase" > "$json"
  jq --arg r "$reason" '.fault_error = $r' "$json" > "$json.tmp" && mv "$json.tmp" "$json"
  exit 0
fi

if [[ "$mode" == wait ]]; then
  for ((i = 0; i < 2880; i++)); do # up to 4 h at the default poll
    [[ -f "$rcfile" ]] && exit "$(cat "$rcfile")"
    pause 5
    [[ "$POLL" == 0 ]] && sleep 0.2
  done
  echo "phase $phase did not finish" >&2; exit 1
fi

# The runner reaches the frontend locally on a server host, and server
# 1's private address from the load generator.
if [[ "$NODE" == server-* ]]; then frontend=127.0.0.1:7233; else frontend="$SERVER1_PRIVATE_IP:7233"; fi

run_phase() {
  rm -f "$json" "$rcfile"
  local rc=0
  # -timeout first: a later flag from the caller overrides it.
  "$ROOT/bin/e2e" -timeout 3h -address "$frontend" -namespace qualify \
    -phase "$phase" -json "$json" "$@" > "$RESULTS/logs/runner-$phase.log" 2>&1 || rc=$?
  if [[ ! -s "$json" ]]; then
    printf '{"phase":"%s","setup_error":"runner exited %d without writing results"}\n' "$phase" "$rc" > "$json"
    ((rc != 0)) || rc=1
  fi
  echo "$rc" > "$rcfile"
  return "$rc"
}

if [[ "$mode" == background ]]; then
  rm -f "$rcfile"
  (run_phase "$@" || true) </dev/null >/dev/null 2>&1 &
  disown || true
  exit 0
fi
run_phase "$@"
