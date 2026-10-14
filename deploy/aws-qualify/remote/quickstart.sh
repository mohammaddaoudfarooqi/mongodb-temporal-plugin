#!/usr/bin/env bash
# The AWS quickstart on its host: Temporal (with the MongoDB plugin), the
# showcase worker and the Temporal UI as services that survive reboots.
#
#   quickstart.sh start    reads KEY=VALUE settings on stdin (MONGODB_URI,
#                          SHOWCASE_EMBEDDING, VOYAGE_API_KEY, VOYAGE_MODEL,
#                          NUM_HISTORY_SHARDS), then installs and starts
#                          everything. Safe to re-run.
set -euo pipefail
# shellcheck source=common.sh
. "$(dirname "$0")/common.sh"

ETC=${QS_ETC:-/etc/temporal-quickstart}
UNITS=${QS_SYSTEMD_DIR:-/etc/systemd/system}
ME=$(id -un)
UI_IMAGE=${QS_UI_IMAGE:-temporalio/ui:2.37.4}

# put FILE: stdin into a 0600 file owned by this user, written with umask 077.
put() {
  local tmp; tmp=$(umask 077; mktemp)
  cat > "$tmp"
  sudo install -m 0600 -o "$ME" "$tmp" "$1"
  rm -f "$tmp"
}

# quote VALUE: single-quoted, which systemd's EnvironmentFile and the
# shell both read literally (the URI holds & and ?).
quote() { printf "'%s'" "${1//\'/}"; }

start() {
  local MONGODB_URI="" SHOWCASE_EMBEDDING=auto VOYAGE_API_KEY="" VOYAGE_MODEL=voyage-4 line
  while IFS= read -r line || [[ -n "$line" ]]; do
    case "$line" in
      MONGODB_URI=*) MONGODB_URI=${line#*=} ;;
      SHOWCASE_EMBEDDING=*) SHOWCASE_EMBEDDING=${line#*=} ;;
      VOYAGE_API_KEY=*) VOYAGE_API_KEY=${line#*=} ;;
      VOYAGE_MODEL=*) VOYAGE_MODEL=${line#*=} ;;
      NUM_HISTORY_SHARDS=*) NUM_HISTORY_SHARDS=${line#*=} ;;
    esac
  done
  [[ -n "$MONGODB_URI" ]] || { echo "quickstart: MONGODB_URI missing on stdin" >&2; exit 1; }
  sudo install -d -m 0755 "$ETC" "$UNITS"

  echo "schema and the visibility search index"
  bash "$(dirname "$0")/server.sh" schema atlas-search temporal

  local addr; addr=$(hostname -I 2>/dev/null | awk '{print $1}' || true); addr=${addr:-127.0.0.1}
  {
    echo "TEMPORAL_MONGO_URI=$(quote "$MONGODB_URI")"
    echo "TEMPORAL_VISIBILITY_MODE=atlas-search"
    echo "DB_PREFIX=temporal"
    echo "NUM_HISTORY_SHARDS=$NUM_HISTORY_SHARDS"
    echo "BROADCAST_ADDRESS=$addr"
    echo "PROMETHEUS_ADDR=127.0.0.1:9090"
    echo "WORKDIR=$ROOT/run/cfg-quickstart"
    echo "SKIP_BUILD=1"
    echo "SKIP_SCHEMA=1"
  } | put "$ETC/temporal.env"
  {
    echo "TEMPORAL_ADDRESS=127.0.0.1:${FRONTEND_PORT:-7233}"
    echo "MONGODB_URI=$(quote "$MONGODB_URI")"
    echo "SHOWCASE_EMBEDDING=$SHOWCASE_EMBEDDING"
    echo "VOYAGE_MODEL=$VOYAGE_MODEL"
    [[ -n "$VOYAGE_API_KEY" ]] && echo "VOYAGE_API_KEY=$(quote "$VOYAGE_API_KEY")"
    true
  } | put "$ETC/showcase.env"

  cat <<UNIT | sudo tee "$UNITS/temporal.service" >/dev/null
[Unit]
Description=Temporal server with the MongoDB persistence plugin
After=network-online.target
Wants=network-online.target

[Service]
User=$ME
EnvironmentFile=$ETC/temporal.env
ExecStart=/bin/bash $ROOT/examples/e2e/scripts/run-native-server.sh
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
UNIT
  cat <<UNIT | sudo tee "$UNITS/showcase-worker.service" >/dev/null
[Unit]
Description=Temporal + MongoDB showcase worker (support desk)
After=temporal.service
Wants=temporal.service

[Service]
User=$ME
EnvironmentFile=$ETC/showcase.env
ExecStart=$ROOT/bin/showcase worker
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT
  sudo systemctl daemon-reload
  sudo systemctl enable --now temporal.service

  local i
  for ((i = 0; i < 300; i++)); do
    (exec 3<>"/dev/tcp/127.0.0.1/${FRONTEND_PORT:-7233}") 2>/dev/null && break
    sleep 1
  done
  echo "showcase: namespace, search attributes, ticket indexes, seed tickets, schedule"
  # shellcheck disable=SC1091 # written above
  (set -a; . "$ETC/showcase.env"; set +a; "$ROOT/bin/showcase" setup)
  sudo systemctl enable --now showcase-worker.service

  echo "Temporal UI"
  sudo dnf install -y -q docker >/dev/null
  sudo systemctl enable --now docker
  sudo docker rm -f temporal-ui >/dev/null 2>&1 || true
  sudo docker run -d --name temporal-ui --restart unless-stopped --network host \
    -e TEMPORAL_ADDRESS=127.0.0.1:7233 -e TEMPORAL_UI_PORT=8080 -e TEMPORAL_DEFAULT_NAMESPACE=support "$UI_IMAGE" >/dev/null
  echo "quickstart: running"
}

case "${1:-}" in
  start) start ;;
  *) echo "usage: quickstart.sh start  (settings on stdin)" >&2; exit 2 ;;
esac
