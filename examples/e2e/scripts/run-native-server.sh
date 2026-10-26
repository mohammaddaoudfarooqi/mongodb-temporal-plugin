#!/usr/bin/env bash
# Runs temporal-mongodb-server natively (no Docker) against any MongoDB
# deployment — useful for Atlas on macOS, where Docker Desktop's NAT
# breaks long-lived TLS connections (deploy/README.md).
#
# Env:
#   TEMPORAL_MONGO_URI        connection string (required; never printed)
#   TEMPORAL_VISIBILITY_MODE  aggregation (default) | atlas-search
#   DB_PREFIX                 database for both stores (default: temporal)
#   VISIBILITY_DB             a separate database for the visibility store
#                             (default: DB_PREFIX, i.e. one database)
#   WORKDIR                   where the rendered config lives
#                             (default: a new temp dir)
#   PORT_OFFSET               added to every server port (default 0;
#                             frontend gRPC = 7233 + offset)
#   NUM_HISTORY_SHARDS        history shards for a new database (default 4;
#                             fixed once the schema is set up)
#   BROADCAST_ADDRESS         address other server nodes reach this one on
#                             (default 127.0.0.1, i.e. a single node)
#   PROMETHEUS_ADDR           host:port to serve Prometheus metrics on
#   PPROF_PORT                serve Go's profiler (/debug/pprof) on
#                             127.0.0.1:<port>; never on other interfaces
#   SKIP_BUILD=1              use the binaries already in bin/
#   SKIP_SCHEMA=1             skip setup-schema (another node ran it)
#   RENDER_ONLY=1             render the config into WORKDIR and exit
#
# Usage: examples/e2e/scripts/run-native-server.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
: "${TEMPORAL_MONGO_URI:?TEMPORAL_MONGO_URI is required}"
export TEMPORAL_VISIBILITY_MODE="${TEMPORAL_VISIBILITY_MODE:-aggregation}"
DB_PREFIX="${DB_PREFIX:-temporal}"
VISIBILITY_DB="${VISIBILITY_DB:-$DB_PREFIX}"
PORT_OFFSET="${PORT_OFFSET:-0}"
NUM_HISTORY_SHARDS="${NUM_HISTORY_SHARDS:-4}"
WORKDIR="${WORKDIR:-$(mktemp -d)}"

if [ "${SKIP_BUILD:-0}" != "1" ] && [ "${RENDER_ONLY:-0}" != "1" ]; then
  (cd "$ROOT" && make build >/dev/null)
fi
mkdir -p "$WORKDIR/dynamicconfig"
sh "$ROOT/deploy/config/render-config.sh" "$ROOT/deploy/config/development.yaml" "$WORKDIR/development.yaml.tmp"
# Each datastore's database is set within its own block of the config.
awk -v core="$DB_PREFIX" -v vis="$VISIBILITY_DB" \
    '/^        mongo-default:/ { db = core } /^        mongo-visibility:/ { db = vis }
     /^ +database: temporal$/ { sub(/temporal$/, db) } { print }' \
    "$WORKDIR/development.yaml.tmp" |
  sed -e "s|numHistoryShards: 4|numHistoryShards: ${NUM_HISTORY_SHARDS}|" \
      -e "s|/etc/temporal/config/dynamicconfig/development.yaml|$WORKDIR/dynamicconfig/development.yaml|" \
      > "$WORKDIR/development.yaml"
rm "$WORKDIR/development.yaml.tmp"
if [ -n "${BROADCAST_ADDRESS:-}" ]; then
  sed -i.bak "s|broadcastAddress: \"127.0.0.1\"|broadcastAddress: \"${BROADCAST_ADDRESS}\"|" "$WORKDIR/development.yaml"
  rm -f "$WORKDIR/development.yaml.bak"
fi
if [ -n "${PROMETHEUS_ADDR:-}" ]; then
  # perl, not sed: a multi-line insertion that works with BSD and GNU.
  PROMETHEUS_ADDR="$PROMETHEUS_ADDR" perl -0pi -e \
    's/^global:\n/global:\n    metrics:\n        prometheus:\n            framework: "tally"\n            listenAddress: "$ENV{PROMETHEUS_ADDR}"\n            timerType: "histogram"\n/m' \
    "$WORKDIR/development.yaml"
fi
if [ -n "${PPROF_PORT:-}" ]; then
  case "$PPROF_PORT" in *[!0-9]*) echo "PPROF_PORT must be a port number" >&2; exit 2 ;; esac
  PPROF_PORT="$PPROF_PORT" perl -0pi -e \
    's/^global:\n/global:\n    pprof:\n        port: $ENV{PPROF_PORT}\n        host: "127.0.0.1"\n/m' \
    "$WORKDIR/development.yaml"
fi
if [ "$PORT_OFFSET" != "0" ]; then
  # perl, not sed: BSD sed has no \b word boundary.
  PORT_OFFSET="$PORT_OFFSET" perl -pi -e \
    's/\b(6933|6934|6935|6939|7233|7234|7235|7239|7243)\b/$1 + $ENV{PORT_OFFSET}/ge' \
    "$WORKDIR/development.yaml"
fi
cp "$ROOT/deploy/config/dynamicconfig/development.yaml" "$WORKDIR/dynamicconfig/"
if [ "${RENDER_ONLY:-0}" = "1" ]; then
  echo "config: $WORKDIR/development.yaml"
  exit 0
fi
frontend=$((7233 + PORT_OFFSET))
if (exec 3<>"/dev/tcp/127.0.0.1/$frontend") 2>/dev/null; then
  echo "port $frontend is already in use; set PORT_OFFSET" >&2
  exit 1
fi

tool="$ROOT/bin/temporal-mongodb-tool"
# One database gets every collection; separate databases get only their
# own store's collections.
if [ "$VISIBILITY_DB" = "$DB_PREFIX" ]; then set -- "$DB_PREFIX:all"; else set -- "$DB_PREFIX:core" "$VISIBILITY_DB:visibility"; fi
for spec in "$@"; do
  [ "${SKIP_SCHEMA:-0}" = "1" ] && break
  "$tool" setup-schema --uri "$TEMPORAL_MONGO_URI" --database "${spec%%:*}" --stores "${spec#*:}" \
    --schema-dir "$ROOT/schema/versioned" --num-history-shards "$NUM_HISTORY_SHARDS" >/dev/null
done
if [ "$TEMPORAL_VISIBILITY_MODE" = "atlas-search" ] && [ "${SKIP_SCHEMA:-0}" != "1" ]; then
  "$tool" setup-atlas-search --uri "$TEMPORAL_MONGO_URI" --database "$VISIBILITY_DB" \
    --atlas-schema-dir "$ROOT/schema/atlas_search" >/dev/null
fi
echo "config: $WORKDIR (core database ${DB_PREFIX}, visibility database ${VISIBILITY_DB}; visibility ${TEMPORAL_VISIBILITY_MODE})"
exec "$ROOT/bin/temporal-mongodb-server" --start --config "$WORKDIR" --env development \
  --services history,matching,frontend,worker
