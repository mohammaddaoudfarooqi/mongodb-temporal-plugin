#!/usr/bin/env bash
# Runs this host's Temporal server node.
#
#   server.sh start MODE DB_PREFIX [--schema]   MODE: aggregation | atlas-search
#   server.sh stop
#   server.sh settle                            wait for shard ownership to settle
#   server.sh schema MODE DB_PREFIX             only set up the schema (and search index)
#
# --schema (server 1 only) sets up the schema first, retrying until
# Atlas is reachable over the peering.
set -euo pipefail
# shellcheck source=common.sh
. "$(dirname "$0")/common.sh"

PIDFILE="$RUNDIR/server.pid"

stop() {
  local pid i
  pid=$(cat "$PIDFILE" 2>/dev/null || true)
  [[ -n "$pid" ]] || return 0
  kill "$pid" 2>/dev/null || true
  for ((i = 0; i < 20; i++)); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  kill -9 "$pid" 2>/dev/null || true
  rm -f "$PIDFILE"
}

setup_schema() {
  local mode=$1 prefix=$2 attempts=${SCHEMA_ATTEMPTS:-60} db i uri
  # SCHEMA_URI: an elevated, setup-only credential when the runtime one
  # is least-privilege (the quickstart).
  uri=${SCHEMA_URI:-$(cat "$ROOT/mongo.uri")}
  for db in "$prefix" "${prefix}_visibility"; do
    for ((i = 1; ; i++)); do
      if "$ROOT/bin/temporal-mongodb-tool" setup-schema --uri "$uri" --database "$db" \
        --schema-dir "$ROOT/schema/versioned" --num-history-shards "$NUM_HISTORY_SHARDS" \
        >>"$RESULTS/logs/schema.log" 2>&1; then break; fi
      ((i < attempts)) || { echo "schema setup for $db failed after $attempts attempts" >&2; return 1; }
      pause 10
    done
  done
  if [[ "$mode" == atlas-search ]]; then
    "$ROOT/bin/temporal-mongodb-tool" setup-atlas-search --uri "$uri" --database "${prefix}_visibility" \
      --atlas-schema-dir "$ROOT/schema/atlas_search" --timeout 1200 >>"$RESULTS/logs/schema.log" 2>&1 ||
      { echo "Atlas Search index setup failed" >&2; return 1; }
  fi
}

start() {
  local mode=$1 prefix=$2 schema=${3:-} port=${FRONTEND_PORT:-7233} i
  stop
  [[ "$schema" == --schema ]] && setup_schema "$mode" "$prefix"
  local addr=${BROADCAST_ADDRESS:-$(hostname -I 2>/dev/null | awk '{print $1}')}
  TEMPORAL_MONGO_URI=$(cat "$ROOT/mongo.uri") TEMPORAL_VISIBILITY_MODE="$mode" DB_PREFIX="$prefix" \
    NUM_HISTORY_SHARDS="$NUM_HISTORY_SHARDS" BROADCAST_ADDRESS="$addr" PROMETHEUS_ADDR=0.0.0.0:9090 \
    WORKDIR="$RUNDIR/cfg-$mode" SKIP_BUILD=1 SKIP_SCHEMA=1 FRONTEND_PORT="$port" \
    nohup bash "$ROOT/examples/e2e/scripts/run-native-server.sh" \
    >"$RESULTS/logs/server-$mode.log" 2>&1 </dev/null &
  echo $! > "$PIDFILE"
  for ((i = 0; i < 300; i++)); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then echo "server ($mode) is up"; return 0; fi
    kill -0 "$(cat "$PIDFILE")" 2>/dev/null || break
    sleep 1
  done
  echo "server ($mode) did not start; last log lines:" >&2
  tail -n 20 "$RESULTS/logs/server-$mode.log" | sed -E 's#(mongodb(\+srv)?://)[^@/ ]+@#\1<redacted>@#g' >&2
  return 1
}

# settle: done once no shard was acquired in the last 30 s.
settle() {
  local deadline=$((SECONDS + ${SETTLE_TIMEOUT:-600}))
  while true; do
    if python3 - "$RESULTS"/logs/server-*.log <<'PY'; then return 0; fi
import datetime, json, sys
last = None
for path in sys.argv[1:]:
    try:
        lines = open(path, errors="replace")
    except OSError:
        continue
    for line in lines:
        if '"Acquired shard"' not in line:
            continue
        try:
            ts = datetime.datetime.fromisoformat(json.loads(line)["ts"].replace("Z", "+00:00"))
        except (ValueError, KeyError):
            continue
        last = ts if last is None or ts > last else last
now = datetime.datetime.now(datetime.timezone.utc)
sys.exit(0 if last is None or (now - last).total_seconds() >= 30 else 1)
PY
    ((SECONDS < deadline)) || { echo "shard ownership did not settle" >&2; return 1; }
    pause 5
  done
}

case "${1:-}" in
  start) shift; start "$@" ;;
  stop) stop ;;
  settle) settle ;;
  schema) shift; setup_schema "$@" ;;
  *) echo "usage: server.sh start MODE DB_PREFIX [--schema] | stop | settle | schema MODE DB_PREFIX" >&2; exit 2 ;;
esac
