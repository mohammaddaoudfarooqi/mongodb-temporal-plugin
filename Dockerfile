# Multi-stage Dockerfile for the temporal-mongodb-plugin binaries.
# Produces a single image carrying both temporal-mongodb-server (the
# consumer binary that wires the plugin into Temporal server) and the
# temporal-mongodb-tool CLI for schema setup / update / verify.

FROM golang:1.26-alpine@sha256:3082400e369fa24d5fc60bca20edab3f6d604e0c5a690ec66b295eff4dd87ade AS build
WORKDIR /src

# Cache the module download layer.
COPY go.mod go.sum ./
RUN go mod download

# Everything else.
COPY . .

# Static, trim-pathed release builds. buildvcs=false keeps the binary
# deterministic across repeat builds from the same tag (cf.
# docs/developer/supply-chain.md). --version embeds VERSION into the
# binary when supplied; defaults to "dev".
ARG VERSION=dev
ENV CGO_ENABLED=0 GOFLAGS="-trimpath -buildvcs=false"
RUN go build -ldflags "-X main.version=${VERSION}" \
        -o /out/temporal-mongodb-server ./cmd/temporal-mongodb-server \
    && go build -ldflags "-X main.version=${VERSION}" \
        -o /out/temporal-mongodb-tool ./cmd/temporal-mongodb-tool

FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc

# tini is PID 1: it reaps zombies and forwards signals (SIGTERM in
# particular) cleanly to the server process. Matters when the container
# runs in a pod manager that signals on shutdown.
RUN apk add --no-cache ca-certificates tini \
    && addgroup -S temporal && adduser -S temporal -G temporal

COPY --from=build /out/temporal-mongodb-server /usr/local/bin/temporal-mongodb-server
COPY --from=build /out/temporal-mongodb-tool /usr/local/bin/temporal-mongodb-tool
COPY --from=build /src/schema /etc/temporal-mongodb/schema

USER temporal

# OCI image metadata. Consumers rendering "where does this image come
# from" and policy engines that check for source/license labels read
# these directly.
ARG VERSION=dev
LABEL org.opencontainers.image.title="temporal-mongodb-plugin" \
      org.opencontainers.image.description="Temporal server with MongoDB persistence plugin" \
      org.opencontainers.image.source="https://github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.vendor="MongoDB, Inc."

# HEALTHCHECK: the frontend gRPC port must accept connections (busybox
# nc), and when MONGO_HEALTH_URI + MONGO_HEALTH_DB are set the schema
# must also verify. Running the server binary as a probe would only
# print help and exit 0, reporting a dead server as healthy.
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD sh -c 'nc -z 127.0.0.1 7233 || exit 1; \
              if [ -n "$MONGO_HEALTH_URI" ] && [ -n "$MONGO_HEALTH_DB" ]; then \
                  /usr/local/bin/temporal-mongodb-tool verify \
                      -uri "$MONGO_HEALTH_URI" \
                      -database "$MONGO_HEALTH_DB" \
                      -timeout 5 > /dev/null 2>&1; \
              fi'

ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/temporal-mongodb-server"]
