#!/usr/bin/env bash
# Samples host CPU, memory and server RSS into results/samples.csv so
# the report can tell a host-bound result from a database-bound one
#.
#
#   sampler.sh start | stop | once
set -euo pipefail
# shellcheck source=common.sh
. "$(dirname "$0")/common.sh"

PROC=${PROC:-/proc}
CSV="$RESULTS/samples.csv"
PIDFILE="$RUNDIR/sampler.pid"
INTERVAL=${SAMPLE_INTERVAL:-15}

cpu_ticks() { awk '/^cpu / {idle=$5+$6; total=0; for (i=2; i<=NF; i++) total+=$i; print total, idle}' "$PROC/stat"; }

sample() {
  local t0 i0 t1 i1 cpu mem rss pid
  read -r t0 i0 < <(cpu_ticks)
  sleep "${1:-0}"
  read -r t1 i1 < <(cpu_ticks)
  cpu=$(awk -v t=$((t1 - t0)) -v i=$((i1 - i0)) 'BEGIN { if (t > 0) printf "%.1f", 100 * (t - i) / t; else printf "0.0" }')
  mem=$(awk '/^MemTotal:/ {t=$2} /^MemAvailable:/ {a=$2} END { printf "%.1f", 100 * (t - a) / t }' "$PROC/meminfo")
  rss=0
  pid=$(cat "$RUNDIR/server.pid" 2>/dev/null || true)
  if [[ -n "$pid" && -r "$PROC/$pid/status" ]]; then rss=$(awk '/^VmRSS:/ {print $2}' "$PROC/$pid/status"); fi
  [[ -s "$CSV" ]] || echo "ts,cpu_pct,mem_used_pct,server_rss_kb" > "$CSV"
  echo "$(date +%s),$cpu,$mem,${rss:-0}" >> "$CSV"
}

case "${1:-}" in
  once) sample 0 ;;
  start)
    [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null && exit 0
    (while true; do sample "$INTERVAL" || true; done) </dev/null >/dev/null 2>&1 &
    echo $! > "$PIDFILE"; disown || true ;;
  stop) kill "$(cat "$PIDFILE" 2>/dev/null)" 2>/dev/null || true; rm -f "$PIDFILE" ;;
  *) echo "usage: sampler.sh start|stop|once" >&2; exit 2 ;;
esac
