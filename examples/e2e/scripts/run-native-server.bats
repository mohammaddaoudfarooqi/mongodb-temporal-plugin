#!/usr/bin/env bats
# Tests for run-native-server.sh.
# Run: bats examples/e2e/scripts/run-native-server.bats

setup() {
  SCRIPT="$BATS_TEST_DIRNAME/run-native-server.sh"
  ROOT="$(cd "$BATS_TEST_DIRNAME/../../.." && pwd)"
  WORK="$BATS_TEST_TMPDIR/cfg"
  STUBS="$BATS_TEST_TMPDIR/stubs"
  mkdir -p "$STUBS"
  # make must not run when SKIP_BUILD=1 or RENDER_ONLY=1.
  printf '#!/bin/sh\necho "make $*" >> "%s/calls"\n' "$BATS_TEST_TMPDIR" > "$STUBS/make"
  chmod +x "$STUBS/make"
  export PATH="$STUBS:$PATH"
  export TEMPORAL_MONGO_URI='mongodb+srv://u:s3cr3t-pw@example.invalid/?w=majority'
}

render() {
  env WORKDIR="$WORK" RENDER_ONLY=1 SKIP_BUILD=1 "$@" bash "$SCRIPT"
}

@test "RENDER_ONLY renders the config and runs no build or schema step" {
  run render
  [ "$status" -eq 0 ]
  [ -f "$WORK/development.yaml" ]
  [ ! -e "$BATS_TEST_TMPDIR/calls" ]
}

@test "the URI password never reaches stdout or stderr" {
  run render
  [[ "$output" != *s3cr3t-pw* ]]
}

@test "BROADCAST_ADDRESS sets the membership broadcast address" {
  run render BROADCAST_ADDRESS=10.40.1.5
  [ "$status" -eq 0 ]
  grep -q 'broadcastAddress: "10.40.1.5"' "$WORK/development.yaml"
  ! grep -q 'broadcastAddress: "127.0.0.1"' "$WORK/development.yaml"
}

@test "PROMETHEUS_ADDR exposes Prometheus metrics" {
  run render PROMETHEUS_ADDR=0.0.0.0:9090
  [ "$status" -eq 0 ]
  grep -q 'listenAddress: "0.0.0.0:9090"' "$WORK/development.yaml"
  grep -q 'prometheus:' "$WORK/development.yaml"
}

@test "SKIP_BUILD=1 does not call make" {
  run env WORKDIR="$WORK" RENDER_ONLY=1 SKIP_BUILD=1 bash "$SCRIPT"
  [ ! -e "$BATS_TEST_TMPDIR/calls" ]
}

@test "without the new variables the rendered config is unchanged" {
  run render
  [ "$status" -eq 0 ]
  expected="$BATS_TEST_TMPDIR/expected.yaml"
  TEMPORAL_VISIBILITY_MODE=aggregation sh "$ROOT/deploy/config/render-config.sh" \
    "$ROOT/deploy/config/development.yaml" "$expected.tmp"
  sed -e "s|database: temporal_visibility|database: temporal_visibility|" \
      -e "s|database: temporal$|database: temporal|" \
      -e "s|numHistoryShards: 4|numHistoryShards: 4|" \
      -e "s|/etc/temporal/config/dynamicconfig/development.yaml|$WORK/dynamicconfig/development.yaml|" \
      "$expected.tmp" > "$expected"
  diff -u "$expected" "$WORK/development.yaml"
}
