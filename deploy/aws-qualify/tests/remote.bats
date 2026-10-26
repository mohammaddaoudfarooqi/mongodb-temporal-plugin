#!/usr/bin/env bats
# Host-side scripts: schema-setup retries, shard settling, phase result
# files, host samples and the quickstart services. Each test builds a fake /opt/qualify tree with stub
# binaries; QUALIFY_ROOT points the scripts at it.

setup() {
  R="$BATS_TEST_TMPDIR/opt"
  export QUALIFY_ROOT="$R" QUALIFY_POLL=0
  mkdir -p "$R/bin" "$R/results/phases" "$R/results/logs" "$R/run" "$R/examples/e2e/scripts"
  cp "$BATS_TEST_DIRNAME"/../remote/*.sh "$BATS_TEST_TMPDIR/"
  REMOTE="$BATS_TEST_TMPDIR"
  printf 'mongodb+srv://u:REMOTESECRET@h/\n' > "$R/mongo.uri"
  cat > "$R/node.env" <<ENV
PROFILE=functional
RUN_ID=t1
NODE=server-1
SERVER1_PRIVATE_IP=10.40.1.10
NUM_HISTORY_SHARDS=8
ENV
}

# --- runner.sh ---

fake_e2e() { # $1: exit code; writes -json results like the real runner
  cat > "$R/bin/e2e" <<SH
#!/usr/bin/env bash
echo "e2e \$*" >> "$BATS_TEST_TMPDIR/e2e.calls"
out=""; phase=""; prev=""
for a in "\$@"; do case "\$prev" in -json) out=\$a ;; -phase) phase=\$a ;; esac; prev=\$a; done
[ "$1" = crash ] && exit 3
printf '{"phase":"%s","scenarios":[{"name":"x","pass":true,"duration_ms":1}]}\n' "\$phase" > "\$out"
exit $1
SH
  chmod +x "$R/bin/e2e"
}

@test "runner.sh writes the phase results and log, and targets the local frontend on server-1" {
  fake_e2e 0
  run bash "$REMOTE/runner.sh" smoke -scenarios all
  [ "$status" -eq 0 ]
  [ -f "$R/results/phases/smoke.json" ]
  [ -f "$R/results/logs/runner-smoke.log" ]
  grep -q -- "-address 127.0.0.1:7233" "$BATS_TEST_TMPDIR/e2e.calls"
  grep -q -- "-phase smoke" "$BATS_TEST_TMPDIR/e2e.calls"
}

@test "runner.sh on the load generator targets server-1's private address" {
  sed -i.bak 's/^NODE=.*/NODE=loadgen/' "$R/node.env"
  fake_e2e 0
  run bash "$REMOTE/runner.sh" smoke
  grep -q -- "-address 10.40.1.10:7233" "$BATS_TEST_TMPDIR/e2e.calls"
}

@test "a runner that dies without results still leaves a failing phase file" {
  fake_e2e crash
  run bash "$REMOTE/runner.sh" failover -duration 1s
  [ "$status" -ne 0 ]
  grep -q '"setup_error"' "$R/results/phases/failover.json"
}

@test "runner.sh --background then --wait returns the phase's exit code" {
  fake_e2e 1
  run bash "$REMOTE/runner.sh" --background failover -duration 1s
  [ "$status" -eq 0 ]
  run bash "$REMOTE/runner.sh" --wait failover
  [ "$status" -eq 1 ]
  [ -f "$R/results/phases/failover.json" ]
}

# --- server.sh ---

fake_tool() { # fails the first $1 calls, then succeeds
  echo 0 > "$BATS_TEST_TMPDIR/tool.count"
  cat > "$R/bin/temporal-mongodb-tool" <<SH
#!/usr/bin/env bash
n=\$(( \$(cat "$BATS_TEST_TMPDIR/tool.count") + 1 )); echo \$n > "$BATS_TEST_TMPDIR/tool.count"
echo "tool \$*" >> "$BATS_TEST_TMPDIR/tool.calls"
[ \$n -gt $1 ]
SH
  chmod +x "$R/bin/temporal-mongodb-tool"
}

fake_server_script() { # records its environment, then listens like a frontend
  cat > "$R/examples/e2e/scripts/run-native-server.sh" <<SH
#!/usr/bin/env bash
env | grep -E '^(SKIP_SCHEMA|SKIP_BUILD|BROADCAST_ADDRESS|PROMETHEUS_ADDR|PPROF_PORT|DB_PREFIX|TEMPORAL_VISIBILITY_MODE|NUM_HISTORY_SHARDS)=' > "$BATS_TEST_TMPDIR/server.env"
[ -n "\$TEMPORAL_MONGO_URI" ] && echo uri-present >> "$BATS_TEST_TMPDIR/server.env"
exec python3 -c 'import os,socket,time; s=socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(("127.0.0.1", int(os.environ["FRONTEND_PORT"]))); s.listen(); time.sleep(30)'
SH
}

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }

@test "start --schema retries schema setup until Atlas is reachable" {
  fake_tool 2; fake_server_script
  export FRONTEND_PORT=$(free_port) BROADCAST_ADDRESS=10.40.1.10
  run bash "$REMOTE/server.sh" start aggregation tmc_agg --schema
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  [ "$(cat "$BATS_TEST_TMPDIR/tool.count")" -eq 3 ]   # 2 failures + 1 database
  grep -q -- '--database tmc_agg ' "$BATS_TEST_TMPDIR/tool.calls"
  ! grep -q '_visibility' "$BATS_TEST_TMPDIR/tool.calls"
  grep -q 'SKIP_SCHEMA=1' "$BATS_TEST_TMPDIR/server.env"
  grep -q 'BROADCAST_ADDRESS=10.40.1.10' "$BATS_TEST_TMPDIR/server.env"
  grep -q 'NUM_HISTORY_SHARDS=8' "$BATS_TEST_TMPDIR/server.env"
  grep -q 'PPROF_PORT=7936' "$BATS_TEST_TMPDIR/server.env"
  grep -q 'uri-present' "$BATS_TEST_TMPDIR/server.env"
  ! grep -rq REMOTESECRET "$R/results"
  bash "$REMOTE/server.sh" stop
}

@test "start --schema gives up when Atlas never becomes reachable" {
  fake_tool 1000; fake_server_script
  export FRONTEND_PORT=$(free_port) SCHEMA_ATTEMPTS=3
  run bash "$REMOTE/server.sh" start aggregation tmc_agg --schema
  [ "$status" -ne 0 ]
  [[ "$output" == *schema* ]]
}

@test "atlas-search start also creates the Atlas Search index" {
  fake_tool 0; fake_server_script
  export FRONTEND_PORT=$(free_port)
  run bash "$REMOTE/server.sh" start atlas-search tmc_s --schema
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  grep -q 'setup-atlas-search' "$BATS_TEST_TMPDIR/tool.calls"
  bash "$REMOTE/server.sh" stop
}

shard_log() { # $1: seconds ago of the last "Acquired shard" line
  python3 - "$1" > "$R/results/logs/server-aggregation.log" <<'PY'
import datetime, json, sys
ago = int(sys.argv[1])
ts = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=ago)).isoformat()
print(json.dumps({"level": "info", "ts": ts, "msg": "Acquired shard", "shard-id": 3}))
PY
}

@test "settle succeeds once no shard was acquired for 30s" {
  shard_log 45
  run bash "$REMOTE/server.sh" settle
  [ "$status" -eq 0 ]
}

@test "settle times out while shards keep moving" {
  shard_log 1
  export SETTLE_TIMEOUT=0
  run bash "$REMOTE/server.sh" settle
  [ "$status" -ne 0 ]
}

# --- sampler.sh ---

@test "a sample records CPU, memory and server RSS" {
  P="$BATS_TEST_TMPDIR/proc"; mkdir -p "$P/4242"
  echo "cpu  100 0 100 800 0 0 0 0 0 0" > "$P/stat"
  printf 'MemTotal:       8000000 kB\nMemAvailable:   2000000 kB\n' > "$P/meminfo"
  printf 'VmRSS:\t  123456 kB\n' > "$P/4242/status"
  echo 4242 > "$R/run/server.pid"
  PROC="$P" run bash "$REMOTE/sampler.sh" once
  [ "$status" -eq 0 ]
  head -1 "$R/results/samples.csv" | grep -q '^ts,cpu_pct,mem_used_pct,server_rss_kb$'
  tail -1 "$R/results/samples.csv" | grep -Eq '^[0-9]+,[0-9.]+,75\.0,123456$'
}

fake_pprof() { # $1: goroutine total; a curl that serves the profiler's endpoints
  mkdir -p "$BATS_TEST_TMPDIR/pbin"
  cat > "$BATS_TEST_TMPDIR/pbin/curl" <<SH
#!/usr/bin/env bash
echo "curl \$*" >> "$BATS_TEST_TMPDIR/curl.calls"
out=""; url=""; prev=""
for a in "\$@"; do [ "\$prev" = "-o" ] && out=\$a; case "\$a" in http*) url=\$a ;; esac; prev=\$a; done
[ "$1" = none ] && exit 7
case "\$url" in
  */debug/pprof/heap*) printf 'HEAPPROFILE' > "\$out" ;;
  */debug/pprof/goroutine*) echo "goroutine profile: total $1"; echo "1 @ 0x1" ;;
esac
SH
  chmod +x "$BATS_TEST_TMPDIR/pbin/curl"
}

@test "a profile snapshot saves the heap profile and the goroutine count" {
  fake_pprof 812
  PATH="$BATS_TEST_TMPDIR/pbin:$PATH" PPROF_PORT=7936 run bash "$REMOTE/sampler.sh" profile
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  ls "$R"/results/profiles/heap-*.pb.gz >/dev/null
  [ "$(cat "$R"/results/profiles/heap-*.pb.gz)" = HEAPPROFILE ]
  head -1 "$R/results/goroutines.csv" | grep -q '^ts,goroutines$'
  tail -1 "$R/results/goroutines.csv" | grep -Eq '^[0-9]+,812$'
  grep -q 'http://127.0.0.1:7936/debug/pprof/heap' "$BATS_TEST_TMPDIR/curl.calls"
}

@test "a profile snapshot without a reachable profiler records nothing and succeeds" {
  fake_pprof none
  PATH="$BATS_TEST_TMPDIR/pbin:$PATH" PPROF_PORT=7936 run bash "$REMOTE/sampler.sh" profile
  [ "$status" -eq 0 ]
  ! ls "$R"/results/profiles/heap-*.pb.gz 2>/dev/null
  [ ! -f "$R/results/goroutines.csv" ]
}

@test "the native server config enables the profiler on localhost when PPROF_PORT is set" {
  repo="$BATS_TEST_DIRNAME/../../.."
  W="$BATS_TEST_TMPDIR/cfg"
  RENDER_ONLY=1 WORKDIR="$W" TEMPORAL_MONGO_URI=mongodb://h/ PPROF_PORT=7936 \
    run bash "$repo/examples/e2e/scripts/run-native-server.sh"
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
  python3 - "$W/development.yaml" <<'PY'
import re, sys
s = open(sys.argv[1]).read()
g = re.search(r"^global:\n((?:    .*\n|\n)*)", s, re.M).group(1)
assert re.search(r"^    pprof:\n        port: 7936\n        host: \"127\.0\.0\.1\"\n", g, re.M), g
PY
  RENDER_ONLY=1 WORKDIR="$W.off" TEMPORAL_MONGO_URI=mongodb://h/ \
    run bash "$repo/examples/e2e/scripts/run-native-server.sh"
  ! grep -q pprof "$W.off/development.yaml"
}

# --- gates.sh ---

@test "atlas gates map go test results to the phase file; a skip fails" {
  mkdir -p "$BATS_TEST_TMPDIR/gobin"
  cat > "$BATS_TEST_TMPDIR/gobin/go" <<'SH'
#!/usr/bin/env bash
[ -n "$ATLAS_URI" ] || exit 9
case "$*" in
  *conformance*) echo '{"Action":"skip","Test":"TestConformance_Atlas","Elapsed":0}' ;;
  *) echo '{"Action":"pass","Test":"TestM5_AtlasLifecycle_Live","Elapsed":12.5}'
     echo '{"Action":"pass","Test":"TestM5_AtlasLifecycle_Live/sub","Elapsed":1}'
     echo '{"Action":"fail","Test":"TestM6_CLIAtlasSearch_Live","Elapsed":3}' ;;
esac
SH
  chmod +x "$BATS_TEST_TMPDIR/gobin/go"
  mkdir -p "$R/src"
  export GATES_GO="$BATS_TEST_TMPDIR/gobin/go" GATES_SKIP_UNPACK=1
  run bash "$REMOTE/gates.sh"
  [ "$status" -ne 0 ]
  f="$R/results/phases/atlas-gates.json"
  [ "$(jq '.scenarios | length' "$f")" -eq 3 ]
  [ "$(jq -r '.scenarios[] | select(.name=="TestM5_AtlasLifecycle_Live") | .pass' "$f")" = true ]
  [ "$(jq -r '.scenarios[] | select(.name=="TestM6_CLIAtlasSearch_Live") | .pass' "$f")" = false ]
  [ "$(jq -r '.scenarios[] | select(.name=="TestConformance_Atlas") | .error' "$f")" = skipped ]
}

@test "a gate invocation that fails before any test runs is recorded, and every expected gate is required" {
  mkdir -p "$BATS_TEST_TMPDIR/gobin"
  cat > "$BATS_TEST_TMPDIR/gobin/go" <<'SH'
#!/usr/bin/env bash
case "$*" in
  *conformance*) echo "conformance/x.go:1: syntax error" >&2; exit 2 ;;
  *) echo '{"Action":"pass","Test":"TestM5_AtlasLifecycle_Live","Elapsed":1}'
     echo '{"Action":"pass","Test":"TestM6_CLIAtlasSearch_Live","Elapsed":1}' ;;
esac
SH
  chmod +x "$BATS_TEST_TMPDIR/gobin/go"; mkdir -p "$R/src"
  export GATES_GO="$BATS_TEST_TMPDIR/gobin/go" GATES_SKIP_UNPACK=1
  run bash "$REMOTE/gates.sh"
  [ "$status" -ne 0 ]
  f="$R/results/phases/atlas-gates.json"
  [ "$(jq -r '.scenarios[] | select(.name=="TestConformance_Atlas") | .pass' "$f")" = false ]
  jq -e 'any(.scenarios[]; .pass == false)' "$f" >/dev/null
}

@test "runner.sh --fault marks the phase failed with the reason" {
  echo '{"phase":"failover","load":{"workflows":5,"completed":5,"failed":0}}' > "$R/results/phases/failover.json"
  run bash "$REMOTE/runner.sh" --fault failover "Atlas restartPrimaries returned HTTP 500"
  [ "$status" -eq 0 ]
  [ "$(jq -r .fault_error "$R/results/phases/failover.json")" = "Atlas restartPrimaries returned HTTP 500" ]
}

@test "runner.sh --skip records a phase that could not run" {
  run bash "$REMOTE/runner.sh" --skip ladder "shard ownership did not settle"
  [ "$status" -eq 0 ]
  [ "$(jq -r .setup_error "$R/results/phases/ladder.json")" = "not run: shard ownership did not settle" ]
}

@test "runner.sh --annotate adds a fault note without failing the phase" {
  echo '{"phase":"failover","load":{"workflows":5,"completed":5,"failed":0}}' > "$R/results/phases/failover.json"
  run bash "$REMOTE/runner.sh" --annotate failover "cluster healthy after 312s"
  [ "$status" -eq 0 ]
  [ "$(jq -r .fault "$R/results/phases/failover.json")" = "cluster healthy after 312s" ]
  [ "$(jq -r '.fault_error // "none"' "$R/results/phases/failover.json")" = none ]
}

# --- quickstart.sh: the lasting deployment on one host ---

quickstart_stubs() {
  S="$BATS_TEST_TMPDIR/sbin"; mkdir -p "$S"
  for t in systemctl docker dnf; do printf '#!/bin/sh\necho "%s $*" >> "%s/qs.calls"\n' "$t" "$BATS_TEST_TMPDIR" > "$S/$t"; chmod +x "$S/$t"; done
  # sudo runs the command (dropping -u USER); install ignores -o/-g so
  # tests can name a service user that does not exist on this machine.
  cat > "$S/sudo" <<'SH'
#!/bin/sh
echo "sudo $*" >> "$QS_CALLS"
if [ "$1" = -u ]; then shift 2; fi
exec "$@"
SH
  cat > "$S/install" <<'SH'
#!/bin/sh
args=""; while [ $# -gt 0 ]; do case "$1" in -o|-g) shift 2 ;; *) args="$args '$1'"; shift ;; esac; done
eval "exec /usr/bin/install $args"
SH
  cat > "$S/useradd" <<SH
#!/bin/sh
echo "useradd \$*" >> "$BATS_TEST_TMPDIR/qs.calls"
SH
  printf '#!/bin/sh\necho "chown $*" >> "%s/qs.calls"\n' "$BATS_TEST_TMPDIR" > "$S/chown"
  chmod +x "$S/sudo" "$S/install" "$S/useradd" "$S/chown"
  export QS_CALLS="$BATS_TEST_TMPDIR/qs.calls" QS_SERVICE_USER=qs-nonexistent-user
  cat > "$R/bin/showcase" <<SH
#!/bin/sh
case "\$MONGODB_URI" in *ADMIN*) u=admin ;; "") u="" ;; *) u=set ;; esac
echo "showcase \$* MONGODB_URI=\$u EMB=\$SHOWCASE_EMBEDDING" >> "$BATS_TEST_TMPDIR/qs.calls"
SH
  chmod +x "$R/bin/showcase"
  cat > "$R/examples/e2e/scripts/run-native-server.sh" <<'SH'
#!/bin/sh
exit 0
SH
  export PATH="$S:$PATH" QS_SYSTEMD_DIR="$BATS_TEST_TMPDIR/systemd" QS_ETC="$BATS_TEST_TMPDIR/etc" FRONTEND_PORT=$(free_port)
  python3 -c 'import os,socket,time
s=socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(("127.0.0.1", int(os.environ["FRONTEND_PORT"]))); s.listen(); time.sleep(20)' &
  LISTENER=$!
}

@test "quickstart start installs services, env files 0600, no secret in a unit file" {
  fake_tool 0; quickstart_stubs
  printf 'MONGODB_URI=mongodb+srv://u:QSSECRET@h/\nSHOWCASE_EMBEDDING=auto\n' | bash "$REMOTE/quickstart.sh" start
  kill "$LISTENER" 2>/dev/null || true
  for u in temporal.service showcase-worker.service; do [ -f "$QS_SYSTEMD_DIR/$u" ] || { echo "missing $u"; return 1; }; done
  grep -q "EnvironmentFile=$QS_ETC/temporal.env" "$QS_SYSTEMD_DIR/temporal.service"
  ! grep -rq QSSECRET "$QS_SYSTEMD_DIR"
  [ "$(stat -f %Lp "$QS_ETC/temporal.env" 2>/dev/null || stat -c %a "$QS_ETC/temporal.env")" = 600 ]
  grep -q "TEMPORAL_VISIBILITY_MODE=atlas-search" "$QS_ETC/temporal.env"
  grep -q "systemctl enable --now temporal.service" "$BATS_TEST_TMPDIR/qs.calls"
  grep -q "systemctl enable --now showcase-worker.service" "$BATS_TEST_TMPDIR/qs.calls"
  grep -q "docker run .*temporalio/ui" "$BATS_TEST_TMPDIR/qs.calls"
  grep -q "showcase setup MONGODB_URI=set EMB=auto" "$BATS_TEST_TMPDIR/qs.calls"
  grep -q "setup-atlas-search" "$BATS_TEST_TMPDIR/tool.calls"
}


@test "quickstart start runs services as a dedicated user and never stores the admin URI" {
  fake_tool 0; quickstart_stubs
  printf 'MONGODB_URI=mongodb+srv://rt:RUNTIMEPW@h/\nMONGODB_ADMIN_URI=mongodb+srv://ad:ADMINPW@h/\nSHOWCASE_EMBEDDING=auto\n' |
    bash "$REMOTE/quickstart.sh" start
  kill "$LISTENER" 2>/dev/null || true
  grep -q "useradd --system .*qs-nonexistent-user" "$BATS_TEST_TMPDIR/qs.calls"
  grep -q "^User=qs-nonexistent-user$" "$QS_SYSTEMD_DIR/temporal.service"
  grep -q "^User=qs-nonexistent-user$" "$QS_SYSTEMD_DIR/showcase-worker.service"
  grep -q RUNTIMEPW "$QS_ETC/temporal.env"
  grep -q RUNTIMEPW "$QS_ETC/showcase.env"
  ! grep -rq ADMINPW "$QS_ETC" "$QS_SYSTEMD_DIR" "$R"
  grep -q "setup-schema --uri mongodb+srv://ad:ADMINPW@h/" "$BATS_TEST_TMPDIR/tool.calls"
  grep -q "showcase setup MONGODB_URI=admin" "$BATS_TEST_TMPDIR/qs.calls"
}
