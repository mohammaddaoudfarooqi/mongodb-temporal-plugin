#!/usr/bin/env bash
# Temporal + MongoDB 8.2 (full-text and vector search) + the showcase,
# on Docker.
#
#   ./quickstart.sh up       build the images and start everything
#   ./quickstart.sh tour     run the showcase's narrated end-to-end tour
#   ./quickstart.sh status   what is running, and where
#   ./quickstart.sh logs [service]
#   ./quickstart.sh down     stop (keeps data); down -v also deletes the data
#
# Optional: VOYAGE_API_KEY (in the environment or ./.env) turns on vector
# search; without it the showcase uses full-text search only.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
cd "$HERE"

say() { printf '\033[36m▸\033[0m %s\n' "$*"; }
ok() { printf '\033[32m✓\033[0m %s\n' "$*"; }
die() { printf '\033[31m✗ %s\033[0m\n' "$*" >&2; exit 1; }

command -v docker >/dev/null || die "docker is required"
docker compose version >/dev/null 2>&1 || die "Docker Compose v2 is required (docker compose)"
# docker compose reads ./.env (VOYAGE_API_KEY, port overrides) itself;
# the showcase turns on vector search when VOYAGE_API_KEY is set.
compose() { docker compose "$@"; }

up() {
  say "building the Temporal + MongoDB plugin image"
  docker build -q -t "${PLUGIN_IMAGE:-temporal-mongodb-plugin:quickstart}" "$REPO" >/dev/null
  say "building the showcase image"
  docker build -q -t "${SHOWCASE_IMAGE:-temporal-mongodb-showcase:quickstart}" "$REPO/examples/showcase" >/dev/null
  say "starting MongoDB 8.2 with search, Temporal and the showcase (first start takes a minute or two)"
  compose up -d
  say "waiting for the showcase setup to finish"
  local i
  for ((i = 0; i < 180; i++)); do
    case "$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$(compose ps -aq showcase-setup)" 2>/dev/null)" in
      "exited 0") break ;;
      "exited "*) compose logs showcase-setup schema-init | tail -40; die "setup failed (logs above)" ;;
    esac
    sleep 2
  done
  compose logs --no-log-prefix showcase-setup | grep -E '✓' || true
  ok "ready"
  printf '\033[33m!\033[0m development only: MongoDB has no authentication and Temporal accepts every request;\n  keep these ports on localhost (docs/operator/security.md)\n'
  status
  echo
  echo "Next: ./quickstart.sh tour"
}

status() {
  compose ps --format 'table {{.Service}}\t{{.State}}\t{{.Status}}'
  echo
  echo "  Temporal gRPC   localhost:${TEMPORAL_PORT:-7233}"
  echo "  Temporal UI     http://localhost:${TEMPORAL_UI_PORT:-8080}/namespaces/support/workflows"
  echo "  MongoDB         mongodb://localhost:${MONGO_PORT:-27017}/?directConnection=true"
  echo "  Vector search   on when VOYAGE_API_KEY is set (environment or ./.env); full-text search always"
}

case "${1:-}" in
  up) up ;;
  tour) compose run --rm showcase-setup tour ;;
  status) status ;;
  logs) shift; compose logs -f "$@" ;;
  down) shift; compose down "$@" ;;
  *) sed -n '2,13p' "$0"; exit 2 ;;
esac
