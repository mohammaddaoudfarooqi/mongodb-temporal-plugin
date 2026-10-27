#!/usr/bin/env bash
# Ephemeral AWS + Atlas release qualification for the MongoDB Temporal
# plugin, and the AWS quickstart deployment (see deploy/aws-qualify/README.md).
#
#   qualify.sh all --profile functional [--ref <git-ref>]
#   qualify.sh all --profile load [--ref <ref>] [--atlas-tier M50] [--servers 3] [--soak 2h]
#   qualify.sh up|deploy|run|collect|down|status [options]
#
# Options:
#   --profile functional|load   release-qualification test stacks
#   --profile quickstart        a lasting deployment (see quickstart/aws)
#   --ref REF                   git ref to test (default HEAD)
#   --atlas-tier SIZE           override the profile's Atlas tier
#   --mongodb VERSION           MongoDB major version (default 8.0)
#   --servers N                 load profile: Temporal server hosts
#   --soak DURATION             load profile: soak length (0 skips it)
#   --max-hours N               hosts terminate themselves N hours after
#                               boot (default 3 functional, 8 load); size it
#                               to cover every phase you will run
#   --keep                      all: leave the stack up for inspection
#   --yes                       down: no confirmation prompt
#
# Credentials come from deploy/aws-qualify/.env (see .env.example).
# Nothing here prints them, puts them on a command line, or writes them
# outside the gitignored .run/ directory.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
QUALIFY_HOME="${QUALIFY_HOME:-$HERE}"
# One run directory and one Terraform workspace per profile, so the
# functional and load profiles can run at the same time.
# RUN, LOG and TF_STATE are set once the profile is known.
RUNS="$QUALIFY_HOME/.run"
TF_DIR="$HERE/terraform"
POLL="${QUALIFY_POLL:-10}"
mkdir -p "$RUNS"
LOG=/dev/null
RUN="$RUNS/_none"

# shellcheck source=lib/log.sh
. "$HERE/scripts/lib/log.sh"
# shellcheck source=lib/atlas.sh
. "$HERE/scripts/lib/atlas.sh"
# shellcheck source=lib/remote.sh
. "$HERE/scripts/lib/remote.sh"

REQUIRED_VARS="MONGODB_ATLAS_PUBLIC_KEY MONGODB_ATLAS_PRIVATE_KEY ATLAS_PROJECT_ID OWNER_EMAIL SSH_PUBLIC_KEY_PATH SSH_PRIVATE_KEY_PATH"
REQUIRED_TOOLS="terraform aws jq ssh scp curl go git python3"

# pause SECONDS: wait between scheduled suite events. QUALIFY_POLL=0
# (tests) makes every wait immediate.
pause() { if [[ "$POLL" != 0 ]]; then sleep "$1"; fi; }

# ---------- arguments ----------
CMD="${1:-}"; [[ -n "$CMD" ]] && shift
# Stages of `all` re-run this script with the same options.
ORIG_ARGS=("$@")
REMOTE_ARGS=()
PROFILE="" REF="" ATLAS_TIER="" MONGODB="" SERVERS="" SOAK_OVERRIDE="" MAX_HOURS="" KEEP=0 YES=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile) PROFILE=$2; shift 2 ;;
    --ref) REF=$2; shift 2 ;;
    --atlas-tier) ATLAS_TIER=$2; shift 2 ;;
    --max-hours) MAX_HOURS=$2; shift 2
      [[ "$MAX_HOURS" =~ ^[1-9][0-9]*$ ]] || { echo "--max-hours takes a whole number of hours" >&2; exit 2; } ;;
    --mongodb) MONGODB=$2; shift 2 ;;
    --servers) SERVERS=$2; shift 2 ;;
    --soak) SOAK_OVERRIDE=$2; shift 2 ;;
    --keep) KEEP=1; shift ;;
    --yes|-y) YES=1; shift ;;
    -h|--help) sed -n '2,22p' "$0"; exit 0 ;;
    --) shift; REMOTE_ARGS=("$@"); break ;;
    *) die "unknown option: $1" ;;
  esac
done

# active_profiles: profiles with a run in progress.
active_profiles() {
  local f
  for f in "$RUNS"/*/run_id; do [[ -s "$f" ]] && basename "$(dirname "$f")"; done
  return 0
}

# Resolve the profile: --profile, or the only active run.
if [[ -z "$PROFILE" ]]; then
  ACTIVE=$(active_profiles | tr '\n' ' ')
  case "$(wc -w <<<"$ACTIVE" | tr -d ' ')" in
    1) PROFILE=${ACTIVE%% *} ;;
    0) ;;
    *) [[ "$CMD" == -h || "$CMD" == --help || -z "$CMD" ]] || die "runs are active for:${ACTIVE% }; pass --profile functional|load|quickstart" ;;
  esac
fi
if [[ -n "$PROFILE" ]]; then
  [[ "$PROFILE" == functional || "$PROFILE" == load || "$PROFILE" == quickstart ]] || die "--profile must be functional, load or quickstart"
  RUN="$RUNS/$PROFILE"
  mkdir -p "$RUN"
  LOG="$RUN/qualify.log"
  export TF_WORKSPACE="$PROFILE"
fi
# The workspace's local-backend state file.
TF_STATE="${QUALIFY_TF_STATE_DIR:-$TF_DIR/terraform.tfstate.d}/${PROFILE:-_none}/terraform.tfstate"

# ---------- environment ----------
# parse_env FILE: KEY=VALUE lines into shell variables, literally. The
# file is never executed, so a secret containing $, quotes or backticks
# is taken as written. Values are not exported: secrets reach only the
# processes they are handed to explicitly. Surrounding quotes are
# stripped; an unquoted value ends at " #".
parse_env() {
  local line key val
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]] || continue
    key=${BASH_REMATCH[1]} val=${BASH_REMATCH[2]}
    if [[ "$val" =~ ^\"(.*)\"[[:space:]]*$ || "$val" =~ ^\'(.*)\'[[:space:]]*$ ]]; then
      val=${BASH_REMATCH[1]}
    else
      val=${val%%[[:space:]]#*}
      val=${val%"${val##*[![:space:]]}"}
    fi
    printf -v "$key" '%s' "$val"
  done < "$1"
}

load_env() {
  local f="$QUALIFY_HOME/.env" missing="" v val
  [[ -f "$f" ]] || die "missing $f; copy .env.example to .env and fill it in"
  parse_env "$f"
  for v in $REQUIRED_VARS; do
    val="${!v:-}"
    if [[ -z "$val" || "$val" == "<"* ]]; then missing="$missing $v"; fi
  done
  [[ -z "$missing" ]] || die "missing .env values:$missing"
  SECRETS=("$MONGODB_ATLAS_PUBLIC_KEY" "$MONGODB_ATLAS_PRIVATE_KEY")
  AWS_REGION="${AWS_REGION:-us-east-1}"
  export AWS_REGION AWS_DEFAULT_REGION="$AWS_REGION"
  for v in AWS_PROFILE AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN; do
    [[ -n "${!v:-}" && "${!v}" != "<"* ]] && export "${v?}"
  done
  SSH_PUBLIC_KEY_PATH="${SSH_PUBLIC_KEY_PATH/#\~/$HOME}"
  SSH_PRIVATE_KEY_PATH="${SSH_PRIVATE_KEY_PATH/#\~/$HOME}"
  atlas_init
  remote_init
}

check_tools() {
  local t missing=""
  for t in $REQUIRED_TOOLS; do command -v "$t" >/dev/null || missing="$missing $t"; done
  [[ -z "$missing" ]] || die "missing tools:$missing"
}

load_profile() {
  [[ "$PROFILE" == functional || "$PROFILE" == load || "$PROFILE" == quickstart ]] || die "--profile functional|load|quickstart is required"
  # shellcheck disable=SC1090
  . "$HERE/profiles/$PROFILE.env"
  [[ -n "$SOAK_OVERRIDE" ]] && SOAK=$SOAK_OVERRIDE
  # The shared Atlas network container's range, used when the harness
  # has to create it.
  ATLAS_CIDR=${ATLAS_CIDR:-192.168.248.0/21}
  RUN_ID=$(cat "$RUN/run_id" 2>/dev/null || true)
}

# tf ARGS: terraform in the module dir with the Atlas key in its
# environment only.
tf() {
  TF_VAR_atlas_public_key="$MONGODB_ATLAS_PUBLIC_KEY" TF_VAR_atlas_private_key="$MONGODB_ATLAS_PRIVATE_KEY" \
    TF_IN_AUTOMATION=1 terraform -chdir="$TF_DIR" "$@"
}

# tf_out NAME: one non-sensitive output as JSON. Never a bare
# `output -json`, which would print the sensitive URI.
tf_out() { tf output -json "$1"; }

# state_resources: the resources in the Terraform state; exits 2 when the
# state cannot be read, so a read error never looks like an empty state.
# tf_init: init and create this profile's workspace. Both run with
# TF_WORKSPACE unset; everything else selects the workspace through it.
tf_init() {
  TF_WORKSPACE='' TF_IN_AUTOMATION=1 run_logged terraform -chdir="$TF_DIR" init -input=false || return 1
  if ! TF_WORKSPACE='' terraform -chdir="$TF_DIR" workspace list 2>/dev/null | tr -d ' *' | grep -qx "$PROFILE"; then
    TF_WORKSPACE='' run_logged terraform -chdir="$TF_DIR" workspace new "$PROFILE" || true
  fi
}

state_resources() {
  local out
  # No state file yet (a fresh checkout): real terraform exits 1 with
  # "No state file was found!" here, which is not an error for us. A
  # file that exists but cannot be listed still is.
  [[ -e "$TF_STATE" ]] || return 0
  out=$(tf state list 2>&1) || { printf 'cannot read the Terraform state: %s\n' "$(head -1 <<<"$out")" >&2; return 2; }
  printf '%s' "$out"
}

# ---------- up ----------
# admin_cidrs: the SSH sources as a JSON list. The operator's /32
# (ADMIN_CIDR, or detected) comes first, then ADMIN_CIDRS from .env
# (comma or space separated ranges). Every entry must be a valid
# IPv4 network; duplicates are dropped.
admin_cidrs() {
  local mine="${ADMIN_CIDR:-}" ip
  if [[ -z "$mine" ]]; then
    ip=$(curl -fsS --max-time 10 https://checkip.amazonaws.com 2>/dev/null | tr -d '[:space:]' || true)
    if [[ "$ip" =~ ^[0-9.]+$ ]]; then mine="$ip/32"
    elif [[ -z "${ADMIN_CIDRS:-}" ]]; then die "could not detect your public IP; set ADMIN_CIDR or ADMIN_CIDRS in .env"
    else warn "could not detect your public IP; SSH is limited to ADMIN_CIDRS"; fi
  fi
  python3 - "$mine" "${ADMIN_CIDRS:-}" <<'PY' || die "invalid admin CIDRs in .env"
import ipaddress, json, re, sys
entries = [e for e in [sys.argv[1]] + re.split(r"[,\s]+", sys.argv[2]) if e]
bad, out = [], []
for e in entries:
    try:
        n = ipaddress.IPv4Network(e, strict=False)
    except ValueError:
        bad.append(e)
        continue
    if n.prefixlen < 8:
        bad.append(e)
    elif str(n) not in out:
        out.append(str(n))
if bad:
    print("invalid or too wide (must be IPv4, /8 or narrower): " + " ".join(bad), file=sys.stderr)
    sys.exit(1)
print(json.dumps(out, separators=(",", ":")))
PY
}

preflight() {
  check_tools
  local sts
  sts=$(aws sts get-caller-identity 2>&1 >/dev/null) ||
    die "AWS credentials are not usable: $(grep -m1 -o 'An error occurred ([A-Za-z]*)[^:]*' <<<"$sts" || echo "aws sts get-caller-identity failed"). Temporary keys expire; refresh AWS_* in .env"
  [[ -f "$SSH_PUBLIC_KEY_PATH" && -f "$SSH_PRIVATE_KEY_PATH" ]] || die "SSH key pair not found at SSH_PUBLIC_KEY_PATH/SSH_PRIVATE_KEY_PATH"
  ensure_container
  # The VPC must overlap neither the container nor another
  # peering from it (the other profile's, or anything else's).
  local overlaps peers
  peers=$(atlas_get "$(atlas_project)/peers?providerName=AWS") || die "cannot list the project's peerings"
  overlaps=$( { echo "$CONTAINER_CIDR"; jq -r '.results[]? | .routeTableCidrBlock // empty' <<<"$peers"; } |
    python3 -c 'import ipaddress,sys
vpc = ipaddress.ip_network(sys.argv[1])
print(" ".join(c for c in (l.strip() for l in sys.stdin) if c and ipaddress.ip_network(c).overlaps(vpc)))' "$VPC_CIDR")
  [[ -z "$overlaps" ]] || die "the VPC CIDR $VPC_CIDR overlaps $overlaps in the Atlas project; change VPC_CIDR in profiles/$PROFILE.env"
}

# ensure_container: the region's Atlas network container. It is shared by
# every run, so the harness, not a stack, owns it: created here when
# missing (and remembered in .run/container.json), reused by each stack,
# deleted by the last down. Serialised with a lock so two
# parallel ups create it once.
ensure_container() {
  local containers region body tmp code i
  region=$(tr 'a-z-' 'A-Z_' <<<"$AWS_REGION")
  for ((i = 0; i < 120; i++)); do mkdir "$RUNS/.container.lock" 2>/dev/null && break; sleep 1; done
  containers=$(atlas_get "$(atlas_project)/containers?providerName=AWS") ||
    { rmdir "$RUNS/.container.lock"; die "the Atlas key cannot read project $ATLAS_PROJECT_ID (needs Project Owner)"; }
  jq -e . <<<"$containers" >/dev/null 2>&1 || { rmdir "$RUNS/.container.lock"; die "unexpected response listing the project's network containers"; }
  CONTAINER_ID=$(jq -r --arg r "$region" '[.results[]? | select(.regionName == $r)][0].id // empty' <<<"$containers")
  CONTAINER_CIDR=$(jq -r --arg r "$region" '[.results[]? | select(.regionName == $r)][0].atlasCidrBlock // empty' <<<"$containers")
  if [[ -z "$CONTAINER_ID" ]]; then
    tmp=$(mktemp "$RUN/atlas.XXXXXX")
    body=$(jq -nc --arg r "$region" --arg c "$ATLAS_CIDR" '{providerName: "AWS", regionName: $r, atlasCidrBlock: $c}')
    code=$(curl -sS -K "$ATLAS_RC" -X POST --max-time 60 -H "Accept: application/vnd.atlas.2023-01-01+json" \
      -H "Content-Type: application/json" -d "$body" -o "$tmp" -w '%{http_code}' "$ATLAS_BASE$(atlas_project)/containers") || code=000
    if [[ "$code" != 2* ]]; then rm -f "$tmp"; rmdir "$RUNS/.container.lock"; die "creating the Atlas network container failed (HTTP $code)"; fi
    CONTAINER_ID=$(jq -r .id "$tmp"); CONTAINER_CIDR=$ATLAS_CIDR; rm -f "$tmp"
    jq -n --arg id "$CONTAINER_ID" '{id: $id, created_by_harness: true}' > "$RUNS/container.json"
    ok "created the shared Atlas network container $CONTAINER_ID ($CONTAINER_CIDR)"
  fi
  rmdir "$RUNS/.container.lock"
  EXISTING_CTR_ID=$CONTAINER_ID EXISTING_CTR_CIDR=$CONTAINER_CIDR
}

# release_container: after the last run's teardown, delete the container
# if the harness created it and nothing peers from it any more.
release_container() {
  [[ -f "$RUNS/container.json" ]] && jq -e '.created_by_harness' "$RUNS/container.json" >/dev/null 2>&1 || return 0
  [[ -z "$(active_profiles)" ]] || { log "other runs remain; keeping the shared network container"; return 0; }
  local id peers tmp code
  id=$(jq -r .id "$RUNS/container.json")
  peers=$(atlas_get "$(atlas_project)/peers?providerName=AWS") || { warn "cannot list peerings; keeping container $id"; return 1; }
  if jq -e --arg id "$id" '[.results[]? | select(.containerId == $id or .containerId == null)] | length > 0' <<<"$peers" >/dev/null; then
    warn "peerings still use container $id; keeping it"; return 0
  fi
  tmp=$(mktemp "$RUN/atlas.XXXXXX")
  code=$(atlas_api DELETE "$(atlas_project)/containers/$id" 2023-01-01 "$tmp" || echo 000); rm -f "$tmp"
  case "$code" in
    2*|404) rm -f "$RUNS/container.json"; ok "deleted the shared Atlas network container $id" ;;
    *) warn "deleting the Atlas network container $id returned HTTP $code; delete it in the Atlas UI"; return 1 ;;
  esac
}

apply_with_retry() {
  local attempt=1
  while true; do
    if run_logged tf apply -auto-approve -input=false "$@"; then return 0; fi
    if ((attempt < 3)) && tail -n 40 "$LOG" | grep -qiE "CANNOT_.*_YET|PEER.*PENDING|Throttling|RequestLimitExceeded|timeout|try again"; then
      warn "apply hit a transient error; retrying ($attempt/3)"
      sleep $((3 * POLL * attempt)); attempt=$((attempt + 1)); continue
    fi
    return 1
  done
}

cmd_up() {
  load_env; load_profile; preflight
  REF=${REF:-HEAD}
  local cidrs; cidrs=$(admin_cidrs) || exit 1
  tf_init || die "terraform init failed; see $LOG"
  # One run at a time, judged by the state itself, not by
  # .run/ metadata that may be missing.
  local existing
  existing=$(state_resources) || die "refusing to start: the Terraform state cannot be read"
  if [[ -n "$existing" ]]; then
    die "the $PROFILE run ${RUN_ID:-<unknown: .run/$PROFILE/run_id is missing>} still has resources in the Terraform state; run 'qualify.sh down --profile $PROFILE' first (one run per profile)"
  fi
  RUN_ID="$(date +%m%d-%H%M)-$(od -An -N2 -tx1 /dev/urandom | tr -d ' \n')"
  echo "$RUN_ID" > "$RUN/run_id"; echo "$REF" > "$RUN/ref"
  date -u +%Y-%m-%dT%H:%M:%SZ > "$RUN/started_at"
  log "run $RUN_ID: profile $PROFILE, ref $REF"
  local expire vars=()
  expire=$(python3 -c 'import datetime; print((datetime.date.today()+datetime.timedelta(days=1)).isoformat())')
  vars=(-var-file="$HERE/profiles/$PROFILE.tfvars" -var "run_id=$RUN_ID" -var "profile=$PROFILE"
    -var "atlas_project_id=$ATLAS_PROJECT_ID" -var "owner_email=$OWNER_EMAIL" -var "expire_on=$expire"
    -var "admin_cidrs=$cidrs" -var "public_key_path=$SSH_PUBLIC_KEY_PATH" -var "aws_region=$AWS_REGION"
    -var "vpc_cidr=$VPC_CIDR" -var "subnet_cidr=$SUBNET_CIDR")
  [[ -n "$ATLAS_TIER" ]] && vars+=(-var "atlas_instance_size=$ATLAS_TIER")
  [[ -n "$MAX_HOURS" ]] && vars+=(-var "max_hours=$MAX_HOURS")
  [[ -n "$MONGODB" ]] && vars+=(-var "mongodb_version=$MONGODB")
  [[ -n "$SERVERS" ]] && vars+=(-var "server_count=$SERVERS")
  # Whether the container pre-existed is decided once, here,
  # and reused by every retry and by destroy.
  vars+=(-var "existing_container_id=${EXISTING_CTR_ID:-}" -var "existing_container_cidr=${EXISTING_CTR_CIDR:-}")
  printf '%s\n' "${vars[@]}" > "$RUN/tfvars.args"
  log "creating the stack (Atlas cluster creation takes 10-20 min)"
  apply_with_retry "${vars[@]}" || die "terraform apply failed; see $LOG. Run 'qualify.sh down' to clean up."
  tf_out vpc_peering_connection_id | jq -r . > "$RUN/pcx"
  tf_out vpc_id | jq -r . > "$RUN/vpc_id"
  wait_peering
  load_hosts
  local h
  for h in $(hosts_public); do
    log "waiting for host $h"
    wait_host "$h" || die "host $h did not finish booting"
  done
  ok "stack $RUN_ID is up"
}

# wait_peering: until Atlas reports the peering AVAILABLE. Uses the Atlas
# key, not AWS: temporary AWS credentials may have expired by the time
# the cluster finishes building.
wait_peering() {
  local pcx i s
  pcx=$(cat "$RUN/pcx")
  for ((i = 0; i < 60; i++)); do
    s=$(atlas_get "$(atlas_project)/peers?providerName=AWS" 2>/dev/null |
      jq -r --arg p "$pcx" '.results[]? | select(.connectionId == $p) | .statusName' 2>/dev/null || true)
    [[ "$s" == AVAILABLE ]] && { ok "peering $pcx active"; return 0; }
    pause "$POLL"
  done
  die "peering $pcx did not become active"
}

# load_hosts: reads the hosts from Terraform once, failing loudly, into
# HOSTS ("<name> <public ip>" per line) and SERVER1_PRIVATE. Callers loop
# over HOSTS, so a failed or empty read can't turn into a loop over zero
# hosts that reports success.
load_hosts() {
  local srv lg priv i=1 ip
  srv=$(tf_out server_public_ips) || die "cannot read the server hosts from Terraform"
  lg=$(tf_out loadgen_public_ips) || die "cannot read the load generator hosts from Terraform"
  priv=$(tf_out server_private_ips) || die "cannot read the server private addresses from Terraform"
  jq -e 'type == "array" and length > 0' <<<"$srv" >/dev/null || die "Terraform lists no server hosts"
  HOSTS=""
  for ip in $(jq -r '.[]' <<<"$srv"); do HOSTS="${HOSTS}server-$i $ip"$'\n'; i=$((i + 1)); done
  SERVER_COUNT=$((i - 1))
  for ip in $(jq -r '.[]' <<<"$lg"); do HOSTS="${HOSTS}loadgen $ip"$'\n'; done
  HOSTS=${HOSTS%$'\n'}
  SERVER1_PRIVATE=$(jq -r '.[0] // empty' <<<"$priv")
  [[ -n "$SERVER1_PRIVATE" ]] || die "Terraform lists no private address for server-1"
}
hosts_public() { awk '{print $2}' <<<"$HOSTS"; }
server_ip() { awk -v n="server-$1" '$1 == n {print $2}' <<<"$HOSTS"; }
runner_ip() {
  local lg; lg=$(awk '$1 == "loadgen" {print $2; exit}' <<<"$HOSTS")
  if [[ -n "$lg" ]]; then echo "$lg"; else server_ip 1; fi
}

# ---------- deploy ----------
go_tarball() {
  if [[ -n "${QUALIFY_GO_TARBALL:-}" ]]; then echo "$QUALIFY_GO_TARBALL"; return; fi
  local ver cache sum want
  ver=$(awk '/^toolchain go/ {sub("toolchain go",""); print; exit} /^go [0-9]/ {print $2; exit}' "$REPO/go.mod")
  cache="${XDG_CACHE_HOME:-$HOME/.cache}/aws-qualify/go$ver.linux-amd64.tar.gz"
  if [[ ! -f "$cache" ]]; then
    mkdir -p "$(dirname "$cache")"
    curl -fsSL --max-time 300 -o "$cache.part" "https://go.dev/dl/go$ver.linux-amd64.tar.gz"
    want=$(curl -fsSL "https://go.dev/dl/?mode=json&include=all" |
      jq -r --arg f "go$ver.linux-amd64.tar.gz" '.[].files[] | select(.filename == $f) | .sha256')
    sum=$(shasum -a 256 "$cache.part" | cut -d' ' -f1)
    [[ -n "$want" && "$sum" == "$want" ]] || { rm -f "$cache.part"; die "Go toolchain checksum mismatch"; }
    mv "$cache.part" "$cache"
  fi
  echo "$cache"
}

build_bundle() {
  local src="$RUN/src" b="$RUN/bundle"
  rm -rf "$b"; mkdir -p "$b/bin" "$b/remote" "$b/deploy" "$b/examples/e2e/scripts"
  git -C "$REPO" worktree remove --force "$src" >/dev/null 2>&1 || true
  run_logged git -C "$REPO" worktree add --detach "$src" "$REF" || die "cannot check out $REF"
  git -C "$src" rev-parse HEAD > "$RUN/commit"
  log "building linux/amd64 binaries at $(cut -c1-12 "$RUN/commit")"
  local env=(env GOOS=linux GOARCH=amd64 CGO_ENABLED=0)
  run_logged "${env[@]}" go build -C "$src" -o "$b/bin/temporal-mongodb-server" ./cmd/temporal-mongodb-server || die "build failed"
  run_logged "${env[@]}" go build -C "$src" -o "$b/bin/temporal-mongodb-tool" ./cmd/temporal-mongodb-tool || die "build failed"
  run_logged "${env[@]}" go build -C "$src/examples/e2e" -o "$b/bin/e2e" ./cmd/e2e || die "build failed"
  run_logged "${env[@]}" go build -C "$src/examples/showcase" -o "$b/bin/showcase" ./cmd/showcase || die "build failed"
  cp -R "$src/schema" "$b/" 2>/dev/null || true
  cp -R "$src/deploy/config" "$b/deploy/" 2>/dev/null || true
  cp "$src/examples/e2e/scripts/run-native-server.sh" "$b/examples/e2e/scripts/" 2>/dev/null || true
  cp "$HERE"/remote/*.sh "$b/remote/" 2>/dev/null || true
  # No macOS extended attributes: GNU tar on the hosts warns about each.
  COPYFILE_DISABLE=1 tar --no-xattrs -C "$b" -czf "$RUN/bundle.tgz" .
  if [[ "$PROFILE" == functional ]]; then
    # The Atlas gate M6 builds the schema CLI from source.
    run_logged git -C "$REPO" archive --format=tar.gz --output="$RUN/source.tgz" "$(cat "$RUN/commit")" || die "git archive failed"
  fi
  git -C "$REPO" worktree remove --force "$src" >/dev/null 2>&1 || true
}

# node_env INDEX: the non-secret settings one host's scripts read.
node_env() {
  cat <<ENV
PROFILE=$PROFILE
RUN_ID=$RUN_ID
NODE=$1
SERVER1_PRIVATE_IP=$SERVER1_PRIVATE
NUM_HISTORY_SHARDS=$SHARDS
ENV
}

cmd_deploy() {
  load_env; load_profile
  [[ -n "$RUN_ID" ]] || die "no run; run 'qualify.sh up' first"
  # The ref saved by up, unless deploy overrides it; the
  # saved ref always names what was deployed.
  local saved; saved=$(cat "$RUN/ref" 2>/dev/null || echo HEAD)
  if [[ -z "$REF" ]]; then REF=$saved
  elif [[ "$REF" != "$saved" ]]; then warn "deploying $REF instead of $saved"; echo "$REF" > "$RUN/ref"; fi
  load_hosts
  build_bundle
  local gotar=""
  [[ "$PROFILE" == functional ]] && gotar=$(go_tarball)
  local name ip
  while read -r name ip; do
    log "deploying to $name ($ip)"
    rcp "$RUN/bundle.tgz" "ec2-user@$ip:/opt/qualify/bundle.tgz" || die "copy to $name failed"
    rsh "$ip" 'cd /opt/qualify && tar -xzf bundle.tgz && rm bundle.tgz && mkdir -p results/phases results/logs' </dev/null ||
      die "unpacking the bundle on $name failed"
    if [[ "$PROFILE" == functional && "$name" == server-1 ]]; then
      rcp "$RUN/source.tgz" "$gotar" "ec2-user@$ip:/opt/qualify/" || die "copy of the gate toolchain failed"
    fi
    # URI over stdin into a 0600 file.
    tf output -raw mongodb_uri | rsh "$ip" 'umask 077; cat > /opt/qualify/mongo.uri' ||
      die "delivering the connection string to $name failed"
    node_env "$name" | rsh "$ip" 'cat > /opt/qualify/node.env' || die "delivering node settings to $name failed"
  done <<<"$HOSTS"
  ok "deployed $(cut -c1-12 "$RUN/commit") to every host"
}

# ---------- run ----------
PHASE_FAILED=0

# phase NAME HOST ARGS...: one runner phase on HOST; failures are
# recorded and the suite continues so later phases still report.
phase() {
  local name=$1 host=$2; shift 2
  log "phase $name"
  if phase_bg "$name" "$host" "$@" && [[ "$(phase_rc "$name" "$host")" == 0 ]]; then
    ok "phase $name passed"
  else
    warn "phase $name failed"; PHASE_FAILED=1
  fi
}

# phase_rc NAME HOST: the phase's exit code once it has finished. Polls
# with short SSH calls: a session held open for an hours-long phase can
# drop, and dropping it could stop the phase. 10 failed polls in a row,
# or PHASE_MAX_WAIT seconds, give 1.
phase_rc() {
  local name=$1 host=$2 out fails=0 i
  for ((i = 0; i < ${PHASE_MAX_WAIT:-28800} / 30; i++)); do
    if out=$(rsh "$host" "/opt/qualify/remote/runner.sh --status $name" </dev/null 2>>"$LOG"); then
      fails=0
      [[ "$out" =~ ^[0-9]+$ ]] && { echo "$out"; return 0; }
    else
      ((++fails < 10)) || { echo 1; return 0; }
    fi
    pause 30
  done
  echo 1
}

phase_bg() { local name=$1 host=$2; shift 2; rsh "$host" "/opt/qualify/remote/runner.sh --background $name $*" </dev/null >>"$LOG" 2>&1; }

# fault_failed PHASE HOST REASON: the phase's fault was not injected;
# record it in the phase results so the report cannot call it a pass.
fault_failed() {
  warn "phase $1: $3"; PHASE_FAILED=1
  rsh "$2" "/opt/qualify/remote/runner.sh --fault $1 $(printf '%q' "$3")" </dev/null >>"$LOG" 2>&1 ||
    warn "could not record the fault failure in $1's results"
}

# cluster_healthy: Atlas reports the cluster IDLE with one primary and at
# least two secondaries.
cluster_healthy() {
  local cluster st procs
  cluster=$(tf_out cluster_name | jq -r .) || return 1
  st=$(atlas_get "$(atlas_project)/clusters/$cluster" | jq -r '.stateName // empty') || return 1
  [[ "$st" == IDLE ]] || return 1
  procs=$(atlas_get "$(atlas_project)/processes") || return 1
  jq -e --arg c "$cluster" '[.results[]? | select((.userAlias // .hostname // "") | startswith($c))] as $p
    | ($p | map(select(.typeName == "REPLICA_PRIMARY")) | length) == 1
      and ($p | map(select(.typeName == "REPLICA_SECONDARY")) | length) >= 2' <<<"$procs" >/dev/null
}

# wait_cluster_healthy: until cluster_healthy holds on two consecutive
# checks; prints the seconds waited. Fails after QUALIFY_HEALTH_ATTEMPTS.
wait_cluster_healthy() {
  local start=$SECONDS i streak=0 attempts=${QUALIFY_HEALTH_ATTEMPTS:-180}
  for ((i = 0; i < attempts; i++)); do
    if cluster_healthy; then streak=$((streak + 1)); else streak=0; fi
    ((streak >= 2)) && { echo $((SECONDS - start)); return 0; }
    pause 10
  done
  return 1
}

# after_failover PHASE HOST: record the fault and, unless it was never
# injected, wait for the cluster to recover before anything else is
# measured. Fails when it does not recover.
after_failover() {
  local phase=$1 host=$2 waited
  if [[ -n "$FAULT_ERR" ]]; then fault_failed "$phase" "$host" "$FAULT_ERR"; return 0; fi
  log "waiting for the cluster to recover from the failover"
  if waited=$(wait_cluster_healthy); then
    ok "cluster healthy ${waited}s after the failover phase"
    rsh "$host" "/opt/qualify/remote/runner.sh --annotate $phase $(printf '%q' "Atlas failover test accepted; cluster healthy ${waited}s after the phase ended")" \
      </dev/null >>"$LOG" 2>&1 || true
    return 0
  fi
  warn "the cluster did not report healthy after the failover"; PHASE_FAILED=1
  rsh "$host" "/opt/qualify/remote/runner.sh --annotate $phase $(printf '%q' "Atlas failover test accepted; cluster not healthy afterwards")" \
    </dev/null >>"$LOG" 2>&1 || true
  return 1
}

phase_wait() {
  local name=$1 host=$2
  if [[ "$(phase_rc "$name" "$host")" == 0 ]]; then ok "phase $name passed"
  else warn "phase $name failed"; PHASE_FAILED=1; fi
}

# failover: restart the cluster's primary while a load phase runs. Sets
# FAULT_ERR when Atlas did not accept the request.
FAULT_ERR=""
trigger_failover() {
  local tmp code cluster
  FAULT_ERR=""
  cluster=$(tf_out cluster_name | jq -r .)
  tmp=$(mktemp "$RUN/atlas.XXXXXX")
  local why
  code=$(atlas_api POST "$(atlas_project)/clusters/$cluster/restartPrimaries" 2023-02-01 "$tmp") || code=000
  # Atlas explains a refusal in the body (for example a short oplog window).
  why=$(jq -r '[.errorCode, .detail] | map(select(. != null and . != "")) | join(": ")' "$tmp" 2>/dev/null || true)
  rm -f "$tmp"
  if [[ "$code" == 2* ]]; then ok "Atlas failover test started on $cluster"
  else FAULT_ERR="Atlas restartPrimaries returned HTTP $code${why:+ ($why)}"; fi
}

servers_do() { local i; for ((i = 1; i <= SERVER_COUNT; i++)); do rsh "$(server_ip "$i")" "$@" </dev/null >>"$LOG" 2>&1 || return 1; done; }

suite_functional() {
  local s1; s1=$(server_ip 1)
  servers_do /opt/qualify/remote/sampler.sh start || true
  rsh "$s1" /opt/qualify/remote/server.sh start aggregation tmc_agg --schema </dev/null >>"$LOG" 2>&1 || warn "server start (aggregation) failed"
  phase smoke-aggregation "$s1" -scenarios all
  rsh "$s1" /opt/qualify/remote/server.sh stop </dev/null >>"$LOG" 2>&1 || true
  rsh "$s1" /opt/qualify/remote/server.sh start atlas-search tmc_search --schema </dev/null >>"$LOG" 2>&1 || warn "server start (atlas-search) failed"
  phase smoke-atlas-search "$s1" -scenarios all
  log "phase atlas-gates"
  if rsh "$s1" /opt/qualify/remote/gates.sh </dev/null >>"$LOG" 2>&1; then ok "phase atlas-gates passed"; else warn "phase atlas-gates failed"; PHASE_FAILED=1; fi
  # The sanity load measures a healthy cluster, so it runs before the
  # failover, which leaves the cluster recovering for minutes.
  phase sanity-load "$s1" -scenarios none -load "$SANITY_LOAD" -concurrency "$SANITY_CONCURRENCY"
  phase_bg failover "$s1" -scenarios none -concurrency "$FAILOVER_CONCURRENCY" -duration "$FAILOVER_DURATION"
  pause "$((FAILOVER_AT))"
  trigger_failover
  phase_wait failover "$s1"
  after_failover failover "$s1" || true
}

# skip_phases HOST REASON PHASE...: record phases that could not run, so
# the report shows them as failed rather than missing.
skip_phases() {
  local host=$1 reason=$2 p; shift 2
  PHASE_FAILED=1
  for p in "$@"; do
    warn "phase $p not run: $reason"
    rsh "$host" "/opt/qualify/remote/runner.sh --skip $p $(printf '%q' "$reason")" </dev/null >>"$LOG" 2>&1 ||
      warn "could not record that $p was not run"
  done
}

suite_load() {
  # The load worker's pollers (WORKER_POLLERS in the profile); none keeps
  # the SDK default.
  local pol=()
  [[ -n "${WORKER_POLLERS:-}" ]] && pol=(-workflow-pollers "$WORKER_POLLERS" -activity-pollers "$WORKER_POLLERS")
  local s1 lg n i c not_ready=""
  s1=$(server_ip 1); lg=$(runner_ip); n=$SERVER_COUNT
  for h in $(hosts_public); do rsh "$h" /opt/qualify/remote/sampler.sh start </dev/null >>"$LOG" 2>&1 || true; done
  rsh "$s1" "/opt/qualify/remote/server.sh start $VISIBILITY_MODE tmc_load --schema" </dev/null >>"$LOG" 2>&1 ||
    not_ready="server-1 did not start"
  for ((i = 2; i <= n; i++)); do
    rsh "$(server_ip "$i")" "/opt/qualify/remote/server.sh start $VISIBILITY_MODE tmc_load" </dev/null >>"$LOG" 2>&1 ||
      not_ready="${not_ready:+$not_ready; }server-$i did not start"
  done
  # No measured phase runs on an unsettled cluster.
  if [[ -z "$not_ready" ]] && ! servers_do /opt/qualify/remote/server.sh settle; then
    not_ready="shard ownership did not settle"
  fi
  phase smoke "$lg" -scenarios all
  if [[ -n "$not_ready" ]]; then
    skip_phases "$lg" "$not_ready" ladder failover node-loss soak
    return 0
  fi
  phase ladder "$lg" -scenarios none -ladder "$LADDER" -duration "$STEP_DURATION" -max-p99 "$MAX_P99" -max-error-rate "$MAX_ERROR_RATE" ${pol[@]+"${pol[@]}"}
  c=$(rsh "$lg" "jq -r '.ladder.max_sustainable // 0' /opt/qualify/results/phases/ladder.json" </dev/null 2>/dev/null || echo 0)
  c=$((c * LOAD_FRACTION / 100)); ((c > 0)) || c=${LADDER%%,*}
  log "sustained phases run at concurrency $c ($LOAD_FRACTION% of the ladder maximum)"
  if phase_bg failover "$lg" -scenarios none -concurrency "$c" -duration "$FAILOVER_DURATION" ${pol[@]+"${pol[@]}"}; then
    pause "$((FAILOVER_AT))"
    trigger_failover
    phase_wait failover "$lg"
  else
    warn "phase failover failed: it did not start"; PHASE_FAILED=1
  fi
  if ! after_failover failover "$lg"; then
    skip_phases "$lg" "the cluster did not recover from the failover" node-loss soak
    return 0
  fi
  local last; last=$(server_ip "$n")
  local fault=""
  if phase_bg node-loss "$lg" -scenarios none -concurrency "$c" -duration "$NODE_LOSS_DURATION" ${pol[@]+"${pol[@]}"}; then
    pause "$((NODE_LOSS_STOP_AT))"
    log "stopping server-$n"
    rsh "$last" /opt/qualify/remote/server.sh stop </dev/null >>"$LOG" 2>&1 || fault="stopping server-$n failed"
    pause "$((NODE_LOSS_RESTART_AT - NODE_LOSS_STOP_AT))"
    log "restarting server-$n"
    rsh "$last" "/opt/qualify/remote/server.sh start $VISIBILITY_MODE tmc_load" </dev/null >>"$LOG" 2>&1 ||
      fault="${fault:+$fault; }restarting server-$n failed"
    phase_wait node-loss "$lg"
  else
    warn "phase node-loss failed: it did not start"; PHASE_FAILED=1
  fi
  [[ -z "$fault" ]] || fault_failed node-loss "$lg" "$fault"
  if [[ "$SOAK" != 0 ]] && ! servers_do /opt/qualify/remote/server.sh settle; then
    skip_phases "$lg" "shard ownership did not settle after the node loss" soak
  elif [[ "$SOAK" != 0 ]]; then
    # Poll for as long as the runner may run the soak (its timeout).
    PHASE_MAX_WAIT=$(soak_timeout "$SOAK" | tr -d s) phase soak "$lg" -scenarios none -concurrency "$c" -duration "$SOAK" -timeout "$(soak_timeout "$SOAK")" ${pol[@]+"${pol[@]}"}
  fi
}

# soak_timeout DURATION: the runner deadline, soak length plus 30 min.
soak_timeout() { python3 -c 'import re,sys; s=sys.argv[1]; m={"h":3600,"m":60,"s":1}; t=sum(int(n)*m[u] for n,u in re.findall(r"(\d+)([hms])",s)); print(f"{t+1800}s")' "$1"; }

cmd_run() {
  [[ "$PROFILE" != quickstart ]] || die "the quickstart profile has no test suite; use quickstart/aws/quickstart.sh tour"
  load_env; load_profile
  [[ -n "$RUN_ID" ]] || die "no run; run 'qualify.sh up' first"
  load_hosts
  log "running the $PROFILE suite"
  "suite_$PROFILE"
  local h
  for h in $(hosts_public); do rsh "$h" '/opt/qualify/remote/sampler.sh stop; /opt/qualify/remote/server.sh stop' </dev/null >>"$LOG" 2>&1 || true; done
  ((PHASE_FAILED == 0)) || { warn "one or more phases failed"; return 1; }
  ok "suite finished"
}

# ---------- collect ----------
required_phases() {
  if [[ "$PROFILE" == load && "${SOAK:-0}" == 0 ]]; then echo "${REQUIRED_PHASES/ soak/}"; else echo "$REQUIRED_PHASES"; fi
}

cmd_collect() {
  load_env; load_profile
  [[ -n "$RUN_ID" ]] || die "no run to collect"
  local dir="$QUALIFY_HOME/artifacts/$PROFILE-$RUN_ID" name ip tmp tier mdb n=2
  # A later session on the same stack gets its own directory, so a second
  # collect never mixes with or overwrites an earlier report.
  if [[ -e "$dir" ]]; then while [[ -e "$dir-$n" ]]; do n=$((n + 1)); done; dir="$dir-$n"; fi
  load_hosts
  tier=$(tf_out atlas_tier | jq -r .) || die "cannot read the Atlas tier from Terraform"
  # The version Atlas reports, so a run after an in-place upgrade is
  # labelled with what it ran on; Terraform's setting otherwise.
  mdb=$(atlas_get "$(atlas_project)/clusters/$(tf_out cluster_name | jq -r .)" 2>/dev/null | jq -r '.mongoDBVersion // empty') || mdb=""
  [[ -n "$mdb" ]] || mdb=$(tf_out mongodb_version | jq -r .) || die "cannot read the MongoDB version from Terraform"
  mkdir -p "$dir/phases" "$dir/hosts" "$dir/atlas"
  while read -r name ip; do
    log "collecting from $name"
    # Mask any URI credentials in logs before they leave the host.
    rsh "$ip" '/opt/qualify/remote/sampler.sh stop >/dev/null 2>&1; find /opt/qualify/results -name "*.log" -exec sed -i -E "s#(mongodb(\+srv)?://)[^@/ ]+@#\1<redacted>@#g" {} +' </dev/null >>"$LOG" 2>&1 || true
    tmp="$RUN/collect-$name"; rm -rf "$tmp"
    rcp "ec2-user@$ip:/opt/qualify/results" "$tmp" || { warn "could not copy results from $name"; continue; }
    mkdir -p "$dir/hosts/$name"
    if [[ -d "$tmp/phases" ]]; then cp "$tmp"/phases/*.json "$dir/phases/" 2>/dev/null || true; fi
    if [[ -f "$tmp/samples.csv" ]]; then cp "$tmp/samples.csv" "$dir/hosts/$name/"; fi
    if [[ -f "$tmp/goroutines.csv" ]]; then cp "$tmp/goroutines.csv" "$dir/hosts/$name/"; fi
    if [[ -f "$tmp/txn.csv" ]]; then cp "$tmp/txn.csv" "$dir/hosts/$name/"; fi
    if [[ -d "$tmp/profiles" ]]; then cp -R "$tmp/profiles" "$dir/hosts/$name/"; fi
    if [[ -d "$tmp/logs" ]]; then cp -R "$tmp/logs" "$dir/hosts/$name/"; fi
    rm -rf "$tmp"
  done <<<"$HOSTS"
  collect_atlas_metrics "$dir/atlas" || warn "could not fetch Atlas metrics"
  jq -n --arg profile "$PROFILE" --arg run "$RUN_ID" --arg ref "$(cat "$RUN/ref" 2>/dev/null)" \
    --arg commit "$(cat "$RUN/commit" 2>/dev/null)" --arg tier "$tier" \
    --arg mdb "$mdb" --argjson servers "$SERVER_COUNT" \
    --argjson shards "$SHARDS" --arg required "$(required_phases)" \
    --arg started "$(cat "$RUN/started_at" 2>/dev/null)" --arg finished "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    '{profile:$profile, run_id:$run, ref:$ref, commit:$commit, atlas_tier:$tier, mongodb_version:$mdb,
      server_count:$servers, history_shards:$shards, required_phases:($required|split(" ")|map(select(.!=""))),
      started_at:$started, finished_at:$finished}' > "$dir/meta.json"
  cp "$LOG" "$dir/qualify.log" 2>/dev/null || true
  log "rendering the report"
  if (cd "$REPO/examples/e2e" && go run ./cmd/qualify-report -dir "$dir") 2>&1 | tee -a "$LOG"; then
    ok "artifacts in $dir"
  else
    warn "report verdict NO-GO; artifacts in $dir"; return 1
  fi
}

collect_atlas_metrics() {
  local out=$1 procs id cluster
  cluster=$(tf_out cluster_name | jq -r .)
  procs=$(atlas_get "$(atlas_project)/processes") || return 1
  for id in $(jq -r --arg c "$cluster" '.results[]? | select(.userAlias // .hostname | startswith($c)) | .id' <<<"$procs"); do
    atlas_get "$(atlas_project)/processes/$id/measurements?granularity=PT1M&period=PT12H&m=SYSTEM_NORMALIZED_CPU_USER&m=SYSTEM_NORMALIZED_CPU_KERNEL&m=OPCOUNTER_UPDATE&m=OPCOUNTER_INSERT&m=OPCOUNTER_QUERY&m=CONNECTIONS" \
      > "$out/${id//[:\/]/_}.json" || true
  done
}

# ---------- down ----------
cmd_down() {
  [[ -n "$PROFILE" ]] || { ok "nothing to tear down"; return 0; }
  load_env
  RUN_ID=$(cat "$RUN/run_id" 2>/dev/null || true)
  [[ -n "$RUN_ID" ]] || { ok "nothing to tear down"; return 0; }
  if ((YES == 0)); then
    read -r -p "Destroy every resource of run $RUN_ID? Type 'destroy': " a
    [[ "$a" == destroy ]] || die "aborted"
  fi
  local vars=() line
  if [[ -f "$RUN/tfvars.args" ]]; then while IFS= read -r line; do vars+=("$line"); done < "$RUN/tfvars.args"; fi
  tf_init || die "terraform init failed; see $LOG"
  log "destroying run $RUN_ID"
  if ! run_logged tf destroy -auto-approve -input=false ${vars[@]+"${vars[@]}"}; then
    warn "destroy failed; retrying once"; sleep "$((3 * POLL))"
    run_logged tf destroy -auto-approve -input=false ${vars[@]+"${vars[@]}"} || warn "destroy failed again"
  fi
  local pcx
  for pcx in $(aws ec2 describe-vpc-peering-connections --filters "Name=tag:qualify-run,Values=$RUN_ID" \
      "Name=status-code,Values=active,pending-acceptance,provisioning" \
      --query 'VpcPeeringConnections[].VpcPeeringConnectionId' --output text 2>/dev/null || true); do
    aws ec2 delete-vpc-peering-connection --vpc-peering-connection-id "$pcx" >/dev/null 2>&1 || true
  done
  verify_teardown
}

# Prove nothing of the run is left. A query that errors is
# a failed verification, never an empty result.
verify_teardown() {
  local left="" r rc tag="Name=tag:qualify-run,Values=$RUN_ID" tmp code cluster="tmc-$RUN_ID" kind
  if r=$(state_resources 2>&1); then
    [[ -z "$r" ]] || left="$left
  terraform state: $(tr '\n' ' ' <<<"$r")"
  else
    left="$left
  could not verify the Terraform state: $r"
  fi
  rc=0; r=$(aws ec2 describe-instances --filters "$tag" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query 'Reservations[].Instances[].[InstanceId,State.Name]' --output text 2>&1) || rc=$?
  if ((rc != 0)); then left="$left
  could not verify EC2 instances: $(head -1 <<<"$r")"
  elif [[ -n "$r" ]]; then left="$left
  EC2 instances: $r"; fi
  for kind in vpcs:Vpcs[].VpcId security-groups:SecurityGroups[].GroupId key-pairs:KeyPairs[].KeyName; do
    rc=0; r=$(aws ec2 "describe-${kind%%:*}" --filters "$tag" --query "${kind#*:}" --output text 2>&1) || rc=$?
    if ((rc != 0)); then left="$left
  could not verify ${kind%%:*}: $(head -1 <<<"$r")"
    elif [[ -n "$r" ]]; then left="$left
  ${kind%%:*}: $r"; fi
  done
  tmp=$(mktemp "$RUN/atlas.XXXXXX")
  code=$(atlas_api GET "$(atlas_project)/clusters/$cluster" 2023-01-01 "$tmp" || echo 000)
  case "$code" in
    404) ;;
    2*) left="$left
  Atlas cluster $cluster still exists" ;;
    *) left="$left
  could not verify Atlas cluster $cluster (HTTP $code)" ;;
  esac
  if [[ -f "$RUN/pcx" ]]; then
    code=$(atlas_api GET "$(atlas_project)/peers?providerName=AWS" 2023-01-01 "$tmp" || echo 000)
    if [[ "$code" != 2* ]]; then left="$left
  could not verify Atlas peerings (HTTP $code)"
    elif jq -e --arg p "$(cat "$RUN/pcx")" '.results[]? | select(.connectionId == $p)' "$tmp" >/dev/null; then left="$left
  Atlas peering $(cat "$RUN/pcx")"; fi
  fi
  code=$(atlas_api GET "$(atlas_project)/accessList" 2023-01-01 "$tmp" || echo 000)
  if [[ "$code" != 2* ]]; then left="$left
  could not verify the Atlas access list (HTTP $code)"
  elif jq -e --arg c "$cluster" '.results[]? | select(.comment == $c)' "$tmp" >/dev/null; then left="$left
  Atlas access list entry $cluster"; fi
  rm -f "$tmp"
  [[ -z "$left" ]] || die "teardown of $RUN_ID is not verified; run metadata kept so 'down' can be re-run:$left"
  ok "teardown of $RUN_ID verified: no AWS or Atlas resources remain"
  rm -f "$RUN/run_id" "$RUN/pcx" "$RUN/vpc_id" "$RUN/tfvars.args" "$RUN/bundle.tgz" "$RUN/source.tgz"
  release_container || die "teardown of $RUN_ID is verified, but the shared network container could not be removed"
}

# ---------- quickstart: a lasting deployment ----------

# cmd_start: install Temporal, the showcase worker and the Temporal UI as
# services on the host. The connection string and any Voyage key go on
# SSH stdin; nothing secret is on a command line.
cmd_start() {
  [[ "$PROFILE" == quickstart ]] || die "start is for the quickstart profile"
  load_env; load_profile
  [[ -n "$RUN_ID" ]] || die "no deployment; run up and deploy first"
  load_hosts
  local host emb=auto
  host=$(server_ip 1)
  [[ -n "${VOYAGE_API_KEY:-}" ]] && { emb=voyage; SECRETS+=("$VOYAGE_API_KEY"); }
  log "installing Temporal, the showcase worker and the Temporal UI on $host (embeddings: $emb)"
  {
    printf 'MONGODB_URI='; tf output -raw mongodb_uri; printf '\n'
    # Setup-only (schema, indexes); used once on the host, never stored.
    printf 'MONGODB_ADMIN_URI='; tf output -raw mongodb_admin_uri; printf '\n'
    printf 'SHOWCASE_EMBEDDING=%s\nVOYAGE_MODEL=%s\nNUM_HISTORY_SHARDS=%s\n' "$emb" "${VOYAGE_MODEL:-voyage-4}" "$SHARDS"
    [[ -n "${VOYAGE_API_KEY:-}" ]] && printf 'VOYAGE_API_KEY=%s\n' "$VOYAGE_API_KEY"
    true
  } | rsh "$host" '/opt/qualify/remote/quickstart.sh start' >>"$LOG" 2>&1 || die "starting the services failed; see $LOG"
  ok "Temporal, the showcase worker and the Temporal UI are running on $host"
}

# cmd_tunnel: forward the Temporal UI (8080) and gRPC (7233) to localhost.
cmd_tunnel() {
  load_env; load_profile; load_hosts
  local host; host=$(server_ip 1)
  log "tunnel open: Temporal UI http://localhost:8080, gRPC localhost:7233 (Ctrl-C to close)"
  ssh "${SSH_OPTS[@]}" -N -L 8080:127.0.0.1:8080 -L 7233:127.0.0.1:7233 "ec2-user@$host" </dev/null
}

# cmd_ssh: run a command on the host (after --), or open a shell.
cmd_ssh() {
  load_env; load_profile; load_hosts
  local host; host=$(server_ip 1)
  if ((${#REMOTE_ARGS[@]} > 0)); then rsh "$host" "${REMOTE_ARGS[@]}"
  else ssh "${SSH_OPTS[@]}" -t "ec2-user@$host"; fi
}

cmd_status() {
  [[ -n "$PROFILE" ]] || { echo "no active run"; return 0; }
  load_env; load_profile
  [[ -n "$RUN_ID" ]] || { echo "no active run"; return 0; }
  load_hosts
  echo "run $RUN_ID ($PROFILE)"; echo "$HOSTS"
}

# ---------- all ----------
# Each stage runs as its own process: inside a function or subshell on
# the left of || bash ignores set -e, and a failing step would not stop
# its stage. all tears down only a run it created itself,
# never one that was already there.
OWNED=0
stage() { bash "$0" "$1" ${ORIG_ARGS[@]+"${ORIG_ARGS[@]}"}; }

on_exit() {
  local rc=$?
  if ((OWNED == 1)); then
    if ((KEEP == 1)); then warn "--keep: stack $(cat "$RUN/run_id" 2>/dev/null) left up; run 'qualify.sh down' when done"
    else bash "$0" down --yes --profile "$PROFILE" || rc=1; fi
  fi
  atlas_cleanup
  exit "$rc"
}
trap on_exit EXIT

cmd_all() {
  local prior rc=0
  prior=$(cat "$RUN/run_id" 2>/dev/null || true)
  if ! stage up; then
    [[ "$(cat "$RUN/run_id" 2>/dev/null || true)" != "$prior" ]] && OWNED=1
    return 1
  fi
  OWNED=1
  stage deploy || return 1
  stage run || rc=1
  stage collect || rc=1
  return "$rc"
}

case "$CMD" in
  up|deploy|run|collect|down|status|all|start|tunnel|ssh) "cmd_$CMD" ;;
  *) sed -n '2,22p' "$0"; exit 2 ;;
esac
