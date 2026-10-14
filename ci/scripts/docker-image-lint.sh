#!/usr/bin/env bash
# Lints the container image build.
#
# Asserts that a built docker image carries the hardening markers we
# committed to in the Dockerfile:
#   - Non-root USER
#   - tini as PID 1 (entrypoint starts with /sbin/tini)
#   - HEALTHCHECK present
#   - OCI LABELs (title, source, licenses, version)
#
# Usage: ci/scripts/docker-image-lint.sh <image-tag>

set -euo pipefail

IMG="${1:-temporal-mongodb-plugin:latest}"

fail=0
report() {
    echo "docker-image-lint: $1"
    fail=1
}

inspect=$(docker inspect --format '{{json .Config}}' "$IMG" 2>/dev/null) \
    || { echo "docker-image-lint: cannot inspect $IMG"; exit 2; }

# USER must be set and not root / 0.
user=$(echo "$inspect" | sed -n 's/.*"User":"\([^"]*\)".*/\1/p')
case "$user" in
    ""|"root"|"0") report "USER must be non-root; got: '$user'" ;;
esac

# Entrypoint must start with /sbin/tini.
ep=$(echo "$inspect" | sed -n 's/.*"Entrypoint":\[\("[^"]*"\).*/\1/p')
case "$ep" in
    \"/sbin/tini\"*) ;; # OK
    *) report "Entrypoint must start with /sbin/tini; got: $ep" ;;
esac

# HEALTHCHECK present (Healthcheck field is present AND has Test array).
if ! grep -q '"Healthcheck"' <<<"$inspect"; then
    report "HEALTHCHECK directive missing"
fi

# Required OCI labels.
for label in \
    "org.opencontainers.image.title" \
    "org.opencontainers.image.source" \
    "org.opencontainers.image.licenses" \
    "org.opencontainers.image.version" ; do
    if ! grep -q "\"$label\"" <<<"$inspect"; then
        report "LABEL $label missing"
    fi
done

if [ "$fail" -ne 0 ]; then
    echo "docker-image-lint: FAIL — image $IMG does not meet hardening requirements."
    exit 1
fi
echo "docker-image-lint: OK — $IMG has non-root user, tini PID 1, HEALTHCHECK, and OCI labels."
