#!/usr/bin/env bash
# Samples host CPU, memory and server RSS into results/samples.csv so
# the report can tell a host-bound result from a database-bound one.
# With the server's profiler on (PPROF_PORT, localhost only), it also
# saves a heap profile and the goroutine count every PROFILE_INTERVAL
# seconds, into results/profiles/ and results/goroutines.csv, so memory
# growth over a soak can be attributed. Every TXN_INTERVAL seconds it
# appends the server's transaction counters (started, retried, failed,
# latency) per operation to results/txn.csv, so contention can be read
# per load step.
#
#   sampler.sh start | stop | once | profile | txn
set -euo pipefail
# shellcheck source=common.sh
. "$(dirname "$0")/common.sh"

PROC=${PROC:-/proc}
CSV="$RESULTS/samples.csv"
PIDFILE="$RUNDIR/sampler.pid"
INTERVAL=${SAMPLE_INTERVAL:-15}
PPROF_PORT=${PPROF_PORT:-7936}
PROFILE_INTERVAL=${PROFILE_INTERVAL:-600}
METRICS_ADDR=${METRICS_ADDR:-127.0.0.1:9090}
TXN_INTERVAL=${TXN_INTERVAL:-60}

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

# profile: one heap profile and goroutine count. A server without the
# profiler (or not yet up) records nothing.
profile() {
  local base="http://127.0.0.1:$PPROF_PORT/debug/pprof" ts n
  ts=$(date +%s)
  mkdir -p "$RESULTS/profiles"
  curl -fsS --max-time 60 -o "$RESULTS/profiles/heap-$ts.pb.gz" "$base/heap" 2>/dev/null ||
    { rm -f "$RESULTS/profiles/heap-$ts.pb.gz"; return 0; }
  n=$(curl -fsS --max-time 60 "$base/goroutine?debug=1" 2>/dev/null | awk '/^goroutine profile: total/ {print $4; exit}') || true
  [[ -s "$RESULTS/goroutines.csv" ]] || echo "ts,goroutines" > "$RESULTS/goroutines.csv"
  echo "$ts,${n:-0}" >> "$RESULTS/goroutines.csv"
}

# txn: one row per operation with transaction counters. Counters are
# cumulative; rates come from differences between rows.
txn() {
  local ts out
  ts=$(date +%s)
  out=$(curl -fsS --max-time 30 "http://$METRICS_ADDR/metrics" 2>/dev/null | awk -v ts="$ts" '
    /^persistence_mongo_(transactions_(started|retried|failed)_total|transaction_latency_ms_(sum|count))\{/ {
      name = $0; sub(/\{.*/, "", name)
      if (!match($0, /operation="[^"]*"/)) next
      op = substr($0, RSTART + 11, RLENGTH - 12)
      ops[op] = 1; v[op, name] += $NF
    }
    END {
      for (op in ops) printf "%s,%s,%d,%d,%d,%d,%d\n", ts, op,
        v[op, "persistence_mongo_transactions_started_total"], v[op, "persistence_mongo_transactions_retried_total"],
        v[op, "persistence_mongo_transactions_failed_total"], v[op, "persistence_mongo_transaction_latency_ms_sum"],
        v[op, "persistence_mongo_transaction_latency_ms_count"]
    }') || return 0
  [[ -n "$out" ]] || return 0
  [[ -s "$RESULTS/txn.csv" ]] || echo "ts,operation,started,retried,failed,latency_ms_sum,latency_count" > "$RESULTS/txn.csv"
  echo "$out" >> "$RESULTS/txn.csv"
}

case "${1:-}" in
  once) sample 0 ;;
  profile) profile ;;
  txn) txn ;;
  start)
    [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null && exit 0
    (
      next=0 next_txn=0
      while true; do
        if (( $(date +%s) >= next )); then profile || true; next=$(( $(date +%s) + PROFILE_INTERVAL )); fi
        if (( $(date +%s) >= next_txn )); then txn || true; next_txn=$(( $(date +%s) + TXN_INTERVAL )); fi
        sample "$INTERVAL" || true
      done
    ) </dev/null >/dev/null 2>&1 &
    echo $! > "$PIDFILE"; disown || true ;;
  stop) kill "$(cat "$PIDFILE" 2>/dev/null)" 2>/dev/null || true; rm -f "$PIDFILE" ;;
  *) echo "usage: sampler.sh start|stop|once|profile|txn" >&2; exit 2 ;;
esac
