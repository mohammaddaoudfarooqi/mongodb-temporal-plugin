# shellcheck shell=bash
# Shared by the host-side qualify scripts. QUALIFY_ROOT is the deploy
# directory (/opt/qualify on the hosts).
ROOT="${QUALIFY_ROOT:-/opt/qualify}"
POLL="${QUALIFY_POLL:-5}"
RESULTS="$ROOT/results"
RUNDIR="$ROOT/run"
mkdir -p "$RESULTS/phases" "$RESULTS/logs" "$RUNDIR"
# shellcheck disable=SC1091
. "$ROOT/node.env"

pause() { if [[ "$POLL" != 0 ]]; then sleep "$1"; fi; }
