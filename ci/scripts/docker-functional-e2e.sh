#!/usr/bin/env bash
# Functional end-to-end against the Docker image.
#
# For each MongoDB backend we start a replica set (via docker compose
# network) or connect to Atlas, then drive the containerised
# temporal-mongodb-tool through setup-schema → verify. Green here means
# the plugin binary in the published image can bootstrap a real Mongo
# backend end-to-end.
#
# Usage:
#   ci/scripts/docker-functional-e2e.sh <image-tag>
#
# Env:
#   MONGO_VERSION       6.0 / 7.0 / 8.0 / 8.2 / 8.3 / 9.0 (for the container-based tier)
#   ATLAS_URI           full Atlas connection URI (skips Atlas tier if unset)
#   NETWORK             Docker network name (default: tor-func)
#   LOG_DIR             where to write per-backend logs (default: /tmp/tor_logs)

set -euo pipefail

IMAGE="${1:-temporal-mongodb-plugin:latest}"
LOG_DIR="${LOG_DIR:-/tmp/tor_logs}"
NETWORK="${NETWORK:-tor-func}"
mkdir -p "$LOG_DIR"

# Image-hardening precondition. Run before spinning Mongo
# containers so a misconfigured image fails fast.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$SCRIPT_DIR/docker-image-lint.sh" "$IMAGE"

docker network inspect "$NETWORK" >/dev/null 2>&1 || docker network create "$NETWORK" >/dev/null

# run_against runs setup-schema + verify against the Mongo at $uri,
# writing to the given log file. Returns non-zero on failure.
run_against() {
    local tag="$1" uri="$2" db="$3" out="$4"
    echo "=== Functional e2e: $tag ===" | tee "$out"
    # setup-schema: point schema flags at the image's copy of schema/.
    docker run --rm --network "$NETWORK" --entrypoint /usr/local/bin/temporal-mongodb-tool \
        "$IMAGE" setup-schema \
        -uri "$uri" \
        -database "$db" \
        -num-history-shards 4 \
        -schema-dir /etc/temporal-mongodb/schema/versioned \
        -atlas-schema-dir /etc/temporal-mongodb/schema/atlas_search \
        2>&1 | tee -a "$out"
    # verify
    docker run --rm --network "$NETWORK" --entrypoint /usr/local/bin/temporal-mongodb-tool \
        "$IMAGE" verify \
        -uri "$uri" \
        -database "$db" \
        2>&1 | tee -a "$out"
    echo "=== PASS: $tag ===" | tee -a "$out"
}

# Container-based tier: start a replica set, join it to the network,
# and drive the tool from inside the same network so hostname
# resolution works.
run_mongo_tier() {
    local version="$1"
    local container="mongo-tor-${version//./}"
    local db="temporal_func_${version//./}"
    local out="$LOG_DIR/functional_mongo${version%.*}.log"

    docker rm -f "$container" >/dev/null 2>&1 || true
    docker run -d --network "$NETWORK" --name "$container" \
        "mongo:${version}" \
        --replSet rs0 --bind_ip_all >/dev/null

    # Wait for listener; a 30s budget is generous for 6/7/8 cold-start.
    for _ in $(seq 1 30); do
        docker exec "$container" mongosh --quiet --eval 'db.runCommand({ping:1}).ok' \
            2>/dev/null | grep -q '^1$' && break
        sleep 1
    done

    # Initiate the single-node replica set.
    docker exec "$container" mongosh --quiet --eval \
        'rs.initiate({_id: "rs0", members: [{_id: 0, host: "'"$container"':27017"}]})' >/dev/null
    # Wait for primary.
    for _ in $(seq 1 30); do
        docker exec "$container" mongosh --quiet --eval 'rs.status().myState' \
            2>/dev/null | grep -q '^1$' && break
        sleep 1
    done

    run_against "mongo-$version" "mongodb://${container}:27017/?replicaSet=rs0" "$db" "$out"

    docker rm -f "$container" >/dev/null 2>&1 || true
}

# Atlas tier: only when ATLAS_URI is set.
run_atlas_tier() {
    if [[ -z "${ATLAS_URI:-}" ]]; then
        echo "ATLAS_URI not set — Atlas functional tier skipped"
        return
    fi
    local db="${ATLAS_DATABASE:-temporal_func_atlas_$(date +%s)}"
    local out="$LOG_DIR/functional_atlas.log"
    run_against "atlas" "$ATLAS_URI" "$db" "$out"
}

for v in 6.0 7.0 8.0 8.2 8.3 9.0; do
    run_mongo_tier "$v"
done
run_atlas_tier

docker network rm "$NETWORK" >/dev/null 2>&1 || true
echo "Functional e2e: ALL PASS"
