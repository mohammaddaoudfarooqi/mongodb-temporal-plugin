#!/usr/bin/env bash
# Temporal on MongoDB Atlas over VPC peering, on AWS (us-east-1).
#
#   ./quickstart.sh up [--ref <git-ref>]   create Atlas M10 + EC2, peer them, start everything
#   ./quickstart.sh tunnel                 support desk on http://localhost:8090, Temporal UI on
#                                          http://localhost:8080, gRPC on localhost:7233
#   ./quickstart.sh tour                   run the showcase's end-to-end tour on the host
#   ./quickstart.sh status                 the deployment's hosts
#   ./quickstart.sh ssh [-- <command>]     a shell (or one command) on the host
#   ./quickstart.sh down                   delete everything it created, and verify
#
# Credentials go in ./.env (copy .env.example). AWS credentials are needed
# only while `up` creates resources and `down` deletes them.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
Q="$REPO/deploy/aws-qualify/scripts/qualify.sh"
export QUALIFY_HOME="${QUALIFY_HOME:-$HERE}"

cmd=${1:-}; [[ -n "$cmd" ]] && shift
case "$cmd" in
  up)
    bash "$Q" up --profile quickstart "$@"
    bash "$Q" deploy --profile quickstart
    bash "$Q" start --profile quickstart
    cat <<'NEXT'

Temporal runs without authentication: keep SSH access to yourself, and
turn on mTLS and an authorizer before sharing it (docs/operator/security.md).

Next:
  ./quickstart.sh tunnel   # then open http://localhost:8090 (support desk) and http://localhost:8080 (Temporal UI)
  ./quickstart.sh tour     # a narrated demo that checks every feature
NEXT
    ;;
  tunnel) bash "$Q" tunnel --profile quickstart ;;
  tour) bash "$Q" ssh --profile quickstart -- \
    "sudo -u temporal bash -c 'set -a; . /etc/temporal-quickstart/showcase.env; set +a; /opt/qualify/bin/showcase tour'" ;;
  status) bash "$Q" status --profile quickstart ;;
  ssh) bash "$Q" ssh --profile quickstart "$@" ;;
  down) bash "$Q" down --profile quickstart "$@" ;;
  *) sed -n '2,13p' "$0"; exit 2 ;;
esac
