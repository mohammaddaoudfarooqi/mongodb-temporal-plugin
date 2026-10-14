#!/usr/bin/env bats
# Orchestrator tests. External tools are stubs that
# record argv/stdin; see stubs/. Real-tool parity is proven by the live
# runs.

PUB=SECRETPUBKEY1
PRIV=SECRETPRIVKEY2
URI='mongodb+srv://tmc-t:URIPASSWORD3@tmc-t.abcde.mongodb.net/?w=majority'

setup() {
  CT="$BATS_TEST_DIRNAME/../scripts/qualify.sh"
  export QUALIFY_HOME="$BATS_TEST_TMPDIR/home"
  mkdir -p "$QUALIFY_HOME"
  export STUB_FIXTURES="$BATS_TEST_TMPDIR/fixtures" CALLS="$BATS_TEST_TMPDIR/calls"
  mkdir -p "$STUB_FIXTURES"; : > "$CALLS"; : > "$CALLS.stdin"
  export PATH="$BATS_TEST_DIRNAME/stubs:$PATH"
  export QUALIFY_TF_STATE_DIR="$BATS_TEST_TMPDIR/tfstate.d"
  export URI QUALIFY_POLL=0 QUALIFY_GO_TARBALL="$BATS_TEST_TMPDIR/go.tar.gz"
  : > "$QUALIFY_GO_TARBALL"
  ssh-keygen -q -t ed25519 -N '' -f "$BATS_TEST_TMPDIR/id" >/dev/null
  cat > "$QUALIFY_HOME/.env" <<ENV
MONGODB_ATLAS_PUBLIC_KEY=$PUB
MONGODB_ATLAS_PRIVATE_KEY=$PRIV
ATLAS_PROJECT_ID=0123456789abcdef01234567
AWS_REGION=us-east-1
OWNER_EMAIL=owner@example.com
SSH_PUBLIC_KEY_PATH=$BATS_TEST_TMPDIR/id.pub
SSH_PRIVATE_KEY_PATH=$BATS_TEST_TMPDIR/id
ADMIN_CIDR=203.0.113.7/32
ENV
  default_fixtures
}

default_fixtures() {
  # terraform: per-workspace state (TF_WORKSPACE, as real terraform keeps
  # it under terraform.tfstate.d/<ws>/; see tf_parity.bats). Only named
  # outputs exist: a bare `output -json` would print sensitive values.
  cat > "$STUB_FIXTURES/terraform.sh" <<'SH'
args=" $* "
ws=${TF_WORKSPACE:-default}
echo "tfws $ws $*" >> "$CALLS"
STATE="$QUALIFY_TF_STATE_DIR/$ws/terraform.tfstate"
shape=$ws; [ -n "${FIXTURE_PROFILE:-}" ] && shape=$FIXTURE_PROFILE
out() {
  case "$1" in
    server_public_ips) [ -n "${FAIL_HOSTS:-}" ] && exit 1
      if [ "$shape" = load ]; then echo '["198.51.100.1","198.51.100.2"]'; else echo '["198.51.100.1"]'; fi ;;
    server_private_ips) echo '["10.40.1.10","10.40.1.11"]' ;;
    loadgen_public_ips) if [ "$shape" = load ]; then echo '["198.51.100.9"]'; else echo '[]'; fi ;;
    loadgen_private_ips) echo '[]' ;;
    cluster_name) echo '"tmc-t"' ;; atlas_tier) echo '"M10"' ;; mongodb_version) echo '"8.0"' ;;
    vpc_peering_connection_id) echo "\"pcx-$ws\"" ;; vpc_id) echo "\"vpc-$ws\"" ;;
    *) echo "unknown output $1" >&2; exit 1 ;;
  esac
}
case "$args" in
  # State semantics match real terraform (tf_parity.bats): no state file
  # -> "No state file was found!" exit 1; empty -> nothing, exit 0;
  # corrupt -> exit 1.
  *" workspace "*) exit 0 ;;
  *" apply "*) mkdir -p "$(dirname "$STATE")"; echo aws_vpc.main > "$STATE"
    rm -f "$BATS_TEST_TMPDIR/destroyed-$ws"; : > "$BATS_TEST_TMPDIR/applied-$ws" ;;
  *" destroy "*) mkdir -p "$(dirname "$STATE")"; : > "$STATE"; : > "$BATS_TEST_TMPDIR/destroyed-$ws" ;;
  *" state list "*)
    [ -e "$STATE" ] || { echo "No state file was found!" >&2; exit 1; }
    grep -q '^garbage' "$STATE" && { echo "Failed to load state" >&2; exit 1; }
    cat "$STATE" ;;
  *" output -raw mongodb_uri "*) printf '%s' "$URI" ;;
  *" output -json "?*) out "${@: -1}" ;;
  *" output -json "*) echo "bare output -json would leak sensitive values" >&2; exit 97 ;;
esac
exit "${TF_EXIT:-0}"
SH
  cat > "$STUB_FIXTURES/aws.sh" <<'SH'
if [ -n "${AWS_DEAD_AFTER_APPLY:-}" ] && ls "$BATS_TEST_TMPDIR"/applied-* >/dev/null 2>&1; then
  echo "An error occurred (ExpiredToken) when calling the operation: The security token included in the request is expired" >&2; exit 254
fi
case "$*" in
  *get-caller-identity*) echo '{"Account":"123456789012","Arn":"arn:aws:iam::123456789012:user/t"}' ;;
  *describe-vpc-peering-connections*vpc-peering-connection-ids*) echo active ;;
  *describe-instances*) printf '%s' "${LEFTOVER_INSTANCE:-}" ;;
  *) : ;;
esac
SH
  # curl: body to -o, status code on stdout; record the -K file's mode.
  # Containers, peerings and clusters follow the applied/destroyed
  # workspaces, like the real project would.
  cat > "$STUB_FIXTURES/curl.sh" <<'SH'
out=""; url=""; cfg=""; method=GET; prev=""
for a in "$@"; do
  case "$prev" in -o) out=$a ;; -K) cfg=$a ;; -X) method=$a ;; esac
  case "$a" in http*) url=$a ;; esac
  prev=$a
done
[ -n "$cfg" ] && ls -l "$cfg" | cut -c1-10 >> "$BATS_TEST_TMPDIR/curlrc.modes"
T=$BATS_TEST_TMPDIR
body='{}'; code=200
case "$method $url" in
  "POST "*/containers)
    echo '{"results":[{"id":"ctr-new","regionName":"US_EAST_1","atlasCidrBlock":"192.168.248.0/21","providerName":"AWS"}]}' > "$T/containers.json"
    body='{"id":"ctr-new","regionName":"US_EAST_1","atlasCidrBlock":"192.168.248.0/21"}'; code=201 ;;
  "DELETE "*/containers/*) rm -f "$T/containers.json"; code=204 ;;
  "GET "*/containers*) body='{"results":[]}'
    [ -f "$T/containers.json" ] && body=$(cat "$T/containers.json")
    [ -n "${CONTAINERS_JSON:-}" ] && body=$CONTAINERS_JSON ;;
  *"/clusters/tmc-"*/restartPrimaries) code=${RESTART_CODE:-200} ;;
  *"/clusters/tmc-"*) if ls "$T"/destroyed-* >/dev/null 2>&1 && [ -z "${LEFTOVER_CLUSTER:-}" ]; then code=404
    else body="{\"name\":\"tmc-t\",\"stateName\":\"${CLUSTER_STATE:-IDLE}\"}"; fi ;;
  *"/peers"*) items=""
    for f in "$T"/applied-*; do
      [ -e "$f" ] || continue; w=${f##*/applied-}; [ -e "$T/destroyed-$w" ] && continue
      c=10.40.0.0/16; [ "$w" = load ] && c=10.41.0.0/16
      items="$items${items:+,}{\"connectionId\":\"pcx-$w\",\"statusName\":\"${PEER_STATUS:-AVAILABLE}\",\"routeTableCidrBlock\":\"$c\"}"
    done
    body="{\"results\":[$items]}"; [ -n "${PEERS_JSON:-}" ] && body=$PEERS_JSON ;;
  *"/accessList"*) body='{"results":[]}' ;;
  *"/processes") body='{"results":[{"id":"tmc-t-shard-00-00:27017","userAlias":"tmc-t-shard-00-00","typeName":"REPLICA_PRIMARY"},{"id":"tmc-t-shard-00-01:27017","userAlias":"tmc-t-shard-00-01","typeName":"REPLICA_SECONDARY"},{"id":"tmc-t-shard-00-02:27017","userAlias":"tmc-t-shard-00-02","typeName":"REPLICA_SECONDARY"}]}' ;;
  *"/processes"*) body='{"measurements":[]}' ;;
esac
[ -n "$out" ] && printf '%s' "$body" > "$out"
printf '%s' "$code"
SH
  # scp from a host: materialise a results tree in the destination.
  cat > "$STUB_FIXTURES/scp.sh" <<'SH'
dst="${@: -1}"
case "$*" in
  *:/opt/qualify/results*) mkdir -p "$dst/phases"; echo '{"phase":"smoke-aggregation"}' > "$dst/phases/smoke-aggregation.json"; echo "ts,cpu_pct,mem_used_pct,server_rss_kb" > "$dst/samples.csv" ;;
esac
SH
}

# state_of PROFILE: that profile's state file (its workspace).
state_of() { mkdir -p "$QUALIFY_TF_STATE_DIR/$1"; echo "$QUALIFY_TF_STATE_DIR/$1/terraform.tfstate"; }

secrets_absent() {
  for s in "$PUB" "$PRIV" URIPASSWORD3; do
    if grep -rq "$s" "$1"; then echo "secret $s found in $1"; return 1; fi
  done
}

@test "missing .env variables are listed by name and up stops" {
  sed -i.bak '/ATLAS_PROJECT_ID/d; /MONGODB_ATLAS_PRIVATE_KEY/d' "$QUALIFY_HOME/.env"
  run bash "$CT" up --profile functional
  [ "$status" -ne 0 ]
  [[ "$output" == *ATLAS_PROJECT_ID* ]]
  [[ "$output" == *MONGODB_ATLAS_PRIVATE_KEY* ]]
  ! grep -q "terraform.* apply" "$CALLS"
}

@test "a project container overlapping the VPC CIDR stops preflight" {
  export CONTAINERS_JSON='{"results":[{"id":"c1","regionName":"US_EAST_1","atlasCidrBlock":"10.40.0.0/21","providerName":"AWS"}]}'
  run bash "$CT" up --profile functional
  [ "$status" -ne 0 ]
  [[ "$output" == *overlap* ]]
  ! grep -q "terraform.* apply" "$CALLS"
}

@test "a full run leaks no secret to output, log, argv; URI goes over ssh stdin" {
  run bash "$CT" all --profile functional --yes
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  secrets_absent <(printf '%s' "$output")
  secrets_absent "$QUALIFY_HOME/.run"
  secrets_absent "$CALLS"
  grep -q URIPASSWORD3 "$CALLS.stdin"
  grep -q -- "curl .*-K" "$CALLS"
  ! grep -v -- "-rw-------" "$BATS_TEST_TMPDIR/curlrc.modes"
}

@test "all tears down even when a phase fails" {
  cat > "$STUB_FIXTURES/ssh.sh" <<'SH'
case "$*" in *runner.sh*) exit 1 ;; esac
exit 0
SH
  run bash "$CT" all --profile functional --yes
  [ "$status" -ne 0 ]
  grep -q "terraform .*destroy" "$CALLS"
}

@test "--keep leaves the stack up" {
  run bash "$CT" all --profile functional --yes --keep
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  ! grep -q "terraform .*destroy" "$CALLS"
}

@test "down fails and names a leftover AWS instance" {
  bash "$CT" up --profile functional >/dev/null
  export LEFTOVER_INSTANCE="i-0abc123 running"
  run bash "$CT" down --yes --profile functional
  [ "$status" -ne 0 ]
  [[ "$output" == *i-0abc123* ]]
}

@test "down fails and names a leftover Atlas cluster" {
  bash "$CT" up --profile functional >/dev/null
  export LEFTOVER_CLUSTER=1
  run bash "$CT" down --yes --profile functional
  [ "$status" -ne 0 ]
  [[ "$output" == *tmc-* ]]
}

@test "a clean down verifies and succeeds" {
  bash "$CT" up --profile functional >/dev/null
  run bash "$CT" down --yes --profile functional
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  [[ "$output" == *verified* ]]
}

@test "up refuses while another run's state exists" {
  mkdir -p "$QUALIFY_HOME/.run/functional"; echo other-run > "$QUALIFY_HOME/.run/functional/run_id"; echo aws_vpc.main > "$(state_of functional)"
  run bash "$CT" up --profile functional
  [ "$status" -ne 0 ]
  [[ "$output" == *other-run* ]]
  ! grep -q "terraform.* apply" "$CALLS"
}

@test "collect writes meta and per-host results under artifacts/<profile>-<run>" {
  bash "$CT" up --profile functional >/dev/null
  run_id=$(cat "$QUALIFY_HOME/.run/functional/run_id")
  run bash "$CT" collect --profile functional
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  dir="$QUALIFY_HOME/artifacts/functional-$run_id"
  [ -f "$dir/meta.json" ]
  grep -q '"profile": "functional"' "$dir/meta.json"
  grep -q 'smoke-aggregation' "$dir/meta.json"
  [ -f "$dir/phases/smoke-aggregation.json" ]
  [ -f "$dir/hosts/server-1/samples.csv" ]
}


@test ".env is parsed literally, never executed" {
  cat >> "$QUALIFY_HOME/.env" <<'ENV'
# a comment
MONGODB_ATLAS_PRIVATE_KEY="pr$iv'k`ey"
EMPTY_OPTIONAL=
ENV
  echo 'touch "$BATS_TEST_TMPDIR/executed"' >> "$QUALIFY_HOME/.env"
  run bash "$CT" status --profile functional
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  [ ! -e "$BATS_TEST_TMPDIR/executed" ]
  grep -qF "pr\$iv'k\`ey" "$QUALIFY_HOME/.run/functional/atlas.curlrc" 2>/dev/null || [ ! -e "$QUALIFY_HOME/.run/functional/atlas.curlrc" ]
}

@test "the shipped .env.example loads and reports every empty value" {
  cp "$BATS_TEST_DIRNAME/../.env.example" "$QUALIFY_HOME/.env"
  run bash "$CT" up --profile functional
  [ "$status" -ne 0 ]
  for v in MONGODB_ATLAS_PUBLIC_KEY MONGODB_ATLAS_PRIVATE_KEY ATLAS_PROJECT_ID OWNER_EMAIL; do
    [[ "$output" == *$v* ]] || { echo "missing $v in: $output"; return 1; }
  done
  [[ "$output" != *"syntax error"* ]]
}

# --- review fixes ---

@test "an all run refused at up never tears down the existing stack" {
  mkdir -p "$QUALIFY_HOME/.run/functional"; echo existing-run > "$QUALIFY_HOME/.run/functional/run_id"; echo aws_vpc.main > "$(state_of functional)"
  run bash "$CT" all --profile functional --yes
  [ "$status" -ne 0 ]
  [[ "$output" == *existing-run* ]]
  ! grep -q "terraform .*destroy" "$CALLS"
  [ "$(cat "$QUALIFY_HOME/.run/functional/run_id")" = existing-run ]
}

@test "up refuses when Terraform state exists even without .run/run_id" {
  echo aws_vpc.main > "$(state_of functional)"
  run bash "$CT" up --profile functional
  [ "$status" -ne 0 ] || { echo "$output"; return 1; }
  [[ "$output" == *"state"* ]]
  ! grep -q "terraform.* apply" "$CALLS"
}

@test "a failing step inside a stage fails all (URI delivery)" {
  cat > "$STUB_FIXTURES/ssh.sh" <<'SH'
case "$*" in *mongo.uri*) exit 55 ;; esac
exit 0
SH
  run bash "$CT" all --profile functional --yes
  [ "$status" -ne 0 ]
  [[ "$output" != *"deployed"* ]]
  ! grep -q "runner.sh" "$CALLS"
  grep -q "terraform .*destroy" "$CALLS"
}

@test "a verification query that errors fails down and keeps the run metadata" {
  bash "$CT" up --profile functional >/dev/null
  cat > "$STUB_FIXTURES/aws.sh" <<'SH'
case "$*" in
  *get-caller-identity*) echo '{}' ;;
  *describe-vpc-peering-connections*vpc-peering-connection-ids*) echo active ;;
  *describe-*) echo "An error occurred (UnauthorizedOperation)" >&2; exit 254 ;;
esac
SH
  run bash "$CT" down --yes --profile functional
  [ "$status" -ne 0 ]
  [[ "$output" == *"could not verify"* ]]
  [ -f "$QUALIFY_HOME/.run/functional/run_id" ]
}

@test "a failed failover trigger is recorded in the phase results" {
  export RESTART_CODE=500
  run bash "$CT" all --profile functional --yes
  [ "$status" -ne 0 ]
  grep -q "runner.sh --fault failover" "$CALLS"
}

@test "deploy without --ref uses the ref saved by up" {
  run bash "$CT" up --profile functional --ref v1.2.0
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  run bash "$CT" deploy --profile functional
  grep -q "worktree add --detach .* v1.2.0" "$CALLS"
}

@test "a container found at preflight is fixed into the run's variables for apply and destroy" {
  export CONTAINERS_JSON='{"results":[{"id":"ctr-9","regionName":"US_EAST_1","atlasCidrBlock":"192.168.240.0/21","providerName":"AWS"}]}'
  run bash "$CT" up --profile functional
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  grep -qx 'existing_container_id=ctr-9' "$QUALIFY_HOME/.run/functional/tfvars.args"
  grep -q 'apply .*existing_container_id=ctr-9' "$CALLS"
  unset CONTAINERS_JSON   # Atlas now also lists nothing new; destroy must still use the saved decision
  run bash "$CT" down --yes --profile functional
  grep -q 'destroy .*existing_container_id=ctr-9' "$CALLS"
}

# --- follow-up review ---

@test "a fresh checkout with no state file starts; a corrupt state still refuses" {
  [ ! -e "$QUALIFY_TF_STATE_DIR/functional/terraform.tfstate" ]
  run bash "$CT" up --profile functional
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  bash "$CT" down --yes --profile functional >/dev/null
  echo garbage > "$(state_of functional)"
  : > "$CALLS"
  run bash "$CT" up --profile functional
  [ "$status" -ne 0 ]
  [[ "$output" == *"cannot be read"* ]]
  ! grep -q "terraform.* apply" "$CALLS"
}

@test "deploy fails when the host list cannot be read" {
  bash "$CT" up --profile functional >/dev/null
  export FAIL_HOSTS=1
  run bash "$CT" deploy --profile functional
  [ "$status" -ne 0 ]
  [[ "$output" != *"to every host"* ]]
}

@test "failed shard settling skips the measured load phases and fails the run" {
  export FIXTURE_PROFILE=load
  bash "$CT" up --profile load >/dev/null
  cat > "$STUB_FIXTURES/ssh.sh" <<'SH'
case "$*" in *"server.sh settle"*) exit 1 ;; esac
exit 0
SH
  run bash "$CT" run --profile load
  [ "$status" -ne 0 ]
  ! grep -qE "runner.sh (--background )?(ladder|failover|node-loss|soak)( |$)" "$CALLS"
  for p in ladder failover node-loss soak; do grep -q "runner.sh --skip $p" "$CALLS" || { echo "no skip record for $p"; return 1; }; done
}

@test "ADMIN_CIDRS from .env and the operator's /32 all reach the SSH rule" {
  echo 'ADMIN_CIDRS=198.51.100.64/29, 203.0.113.128/29' >> "$QUALIFY_HOME/.env"
  run bash "$CT" up --profile functional
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  line=$(grep '^admin_cidrs=' "$QUALIFY_HOME/.run/functional/tfvars.args")
  [ "$line" = 'admin_cidrs=["203.0.113.7/32","198.51.100.64/29","203.0.113.128/29"]' ] || { echo "$line"; return 1; }
}

@test "an invalid ADMIN_CIDRS entry stops preflight" {
  echo 'ADMIN_CIDRS=198.51.100.64/29,300.1.1.1/8' >> "$QUALIFY_HOME/.env"
  run bash "$CT" up --profile functional
  [ "$status" -ne 0 ]
  [[ "$output" == *300.1.1.1/8* ]]
  ! grep -q "terraform.* apply" "$CALLS"
}

# --- no measurement on a cluster still recovering ---

@test "functional runs the sanity load before the failover" {
  bash "$CT" up --profile functional >/dev/null
  run bash "$CT" run --profile functional
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  s=$(grep -n "runner.sh sanity-load" "$CALLS" | head -1 | cut -d: -f1)
  f=$(grep -n "runner.sh --background failover" "$CALLS" | head -1 | cut -d: -f1)
  [ -n "$s" ] && [ -n "$f" ] && [ "$s" -lt "$f" ]
}

@test "after a failover the suite waits for a healthy cluster and records the recovery" {
  bash "$CT" up --profile functional >/dev/null
  run bash "$CT" run --profile functional
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  grep -q "clusters/tmc-.*restartPrimaries" "$CALLS"
  grep -q "runner.sh --annotate failover" "$CALLS"
}

@test "load skips node loss and soak when the cluster never recovers" {
  export FIXTURE_PROFILE=load CLUSTER_STATE=REPAIRING QUALIFY_HEALTH_ATTEMPTS=3
  bash "$CT" up --profile load >/dev/null
  run bash "$CT" run --profile load
  [ "$status" -ne 0 ]
  ! grep -qE "runner.sh (--background )?(node-loss|soak)( |$)" "$CALLS"
  grep -q "runner.sh --skip node-loss" "$CALLS"
  grep -q "runner.sh --skip soak" "$CALLS"
}

@test "functional fails when the cluster does not recover after the failover" {
  export CLUSTER_STATE=REPAIRING QUALIFY_HEALTH_ATTEMPTS=3
  bash "$CT" up --profile functional >/dev/null
  run bash "$CT" run --profile functional
  [ "$status" -ne 0 ]
  grep -q "runner.sh --annotate failover .*not\\\\ healthy" "$CALLS"
}

@test "an AWS credential failure shows AWS's reason, never the key" {
  cat > "$STUB_FIXTURES/aws.sh" <<'SH'
echo "An error occurred (ExpiredToken) when calling the GetCallerIdentity operation: The security token included in the request is expired" >&2
exit 254
SH
  run bash "$CT" up --profile functional
  [ "$status" -ne 0 ]
  [[ "$output" == *ExpiredToken* ]]
  secrets_absent <(printf '%s' "$output")
}

# --- functional and load can run at the same time ---

@test "both profiles come up side by side, each in its own workspace and run dir" {
  run bash "$CT" up --profile functional
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  run bash "$CT" up --profile load
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  [ -s "$QUALIFY_HOME/.run/functional/run_id" ] && [ -s "$QUALIFY_HOME/.run/load/run_id" ]
  [ "$(cat "$QUALIFY_HOME/.run/functional/run_id")" != "$(cat "$QUALIFY_HOME/.run/load/run_id")" ]
  grep -q "^tfws functional .* apply " "$CALLS"
  grep -q "^tfws load .* apply " "$CALLS"
  ! grep -q "^tfws default .* apply " "$CALLS"
}

@test "down of one profile leaves the other run untouched" {
  bash "$CT" up --profile functional >/dev/null
  bash "$CT" up --profile load >/dev/null
  run bash "$CT" down --yes --profile functional
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  [ -s "$QUALIFY_HOME/.run/load/run_id" ]
  grep -q aws_vpc.main "$QUALIFY_TF_STATE_DIR/load/terraform.tfstate"
  ! grep -q "^tfws load .* destroy " "$CALLS"
}

@test "a command without --profile is refused while both runs exist" {
  bash "$CT" up --profile functional >/dev/null
  bash "$CT" up --profile load >/dev/null
  run bash "$CT" status
  [ "$status" -ne 0 ]
  [[ "$output" == *functional* && "$output" == *load* ]]
}

@test "the profiles get non-overlapping VPC ranges" {
  bash "$CT" up --profile functional >/dev/null
  bash "$CT" up --profile load >/dev/null
  grep -qx 'vpc_cidr=10.40.0.0/16' "$QUALIFY_HOME/.run/functional/tfvars.args"
  grep -qx 'vpc_cidr=10.41.0.0/16' "$QUALIFY_HOME/.run/load/tfvars.args"
}

@test "preflight refuses a VPC range that overlaps an existing peering" {
  export PEERS_JSON='{"results":[{"connectionId":"pcx-x","statusName":"AVAILABLE","routeTableCidrBlock":"10.40.0.0/16"}]}'
  run bash "$CT" up --profile functional
  [ "$status" -ne 0 ]
  [[ "$output" == *overlap* ]]
  ! grep -q " apply " "$CALLS"
}

# --- the shared network container is the harness's, not a stack's ---

@test "the harness creates the container once, both stacks reuse it, the last down deletes it" {
  bash "$CT" up --profile functional >/dev/null
  bash "$CT" up --profile load >/dev/null
  [ "$(grep -c -- '-X POST .*containers' "$CALLS")" -eq 1 ]
  grep -qx 'existing_container_id=ctr-new' "$QUALIFY_HOME/.run/functional/tfvars.args"
  grep -qx 'existing_container_id=ctr-new' "$QUALIFY_HOME/.run/load/tfvars.args"
  bash "$CT" down --yes --profile functional >/dev/null
  ! grep -q -- '-X DELETE .*containers/ctr-new' "$CALLS"
  run bash "$CT" down --yes --profile load
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  grep -q -- '-X DELETE .*containers/ctr-new' "$CALLS"
}

@test "a container that existed before the harness is never deleted" {
  echo '{"results":[{"id":"ctr-old","regionName":"US_EAST_1","atlasCidrBlock":"192.168.240.0/21","providerName":"AWS"}]}' > "$BATS_TEST_TMPDIR/containers.json"
  bash "$CT" up --profile functional >/dev/null
  run bash "$CT" down --yes --profile functional
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  ! grep -q -- '-X POST .*containers' "$CALLS"
  ! grep -q -- '-X DELETE .*containers' "$CALLS"
}

# --- up needs AWS credentials only while Terraform runs ---

@test "up completes when AWS credentials expire right after the apply" {
  export AWS_DEAD_AFTER_APPLY=1
  run bash "$CT" up --profile functional
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  [[ "$output" == *"peering"*"active"* ]]
}

# --- the AWS quickstart profile (quickstart/aws) ---

@test "quickstart up/deploy/start ships the showcase and sends secrets only on stdin" {
  echo 'VOYAGE_API_KEY=VOYAGESECRET4' >> "$QUALIFY_HOME/.env"
  run bash "$CT" up --profile quickstart
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  grep -qx 'vpc_cidr=10.42.0.0/16' "$QUALIFY_HOME/.run/quickstart/tfvars.args"
  run bash "$CT" deploy --profile quickstart
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  grep -q "go build -C .*examples/showcase -o .*bin/showcase" "$CALLS"
  run bash "$CT" start --profile quickstart
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  grep -q "remote/quickstart.sh start" "$CALLS"
  grep -q "URIPASSWORD3" "$CALLS.stdin"
  grep -q "VOYAGE_API_KEY=VOYAGESECRET4" "$CALLS.stdin"
  grep -q "SHOWCASE_EMBEDDING=voyage" "$CALLS.stdin"
  ! grep -q "VOYAGESECRET4\|URIPASSWORD3" "$CALLS"
  secrets_absent <(printf '%s' "$output")
}

@test "without a Voyage key the quickstart uses Atlas automated embedding" {
  bash "$CT" up --profile quickstart >/dev/null
  run bash "$CT" start --profile quickstart
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  grep -q "SHOWCASE_EMBEDDING=auto" "$CALLS.stdin"
}

@test "tunnel forwards the UI and gRPC to localhost" {
  bash "$CT" up --profile quickstart >/dev/null
  run bash "$CT" tunnel --profile quickstart
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  grep -q "ssh .*-N .*-L 8080:127.0.0.1:8080 -L 7233:127.0.0.1:7233 .*ec2-user@198.51.100.1" "$CALLS"
}

@test "the test suite refuses the quickstart profile" {
  bash "$CT" up --profile quickstart >/dev/null
  run bash "$CT" run --profile quickstart
  [ "$status" -ne 0 ]
  [[ "$output" == *"no test suite"* ]]
}
