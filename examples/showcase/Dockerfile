# The showcase CLI (worker, ui, setup, tour, ...). Build context: examples/showcase.

# The browser UI, built into web/dist for the Go binary to embed.
FROM node:22-alpine@sha256:fe4f6d74e20370acb04ad889077d14d0f64a9c8ea6a42bc60cddf1e844699914 AS ui
WORKDIR /src/ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY ui/ ./
RUN npm run build

FROM golang:1.26-alpine@sha256:3082400e369fa24d5fc60bca20edab3f6d604e0c5a690ec66b295eff4dd87ade AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=ui /src/web/dist ./web/dist
ENV CGO_ENABLED=0 GOFLAGS="-trimpath -buildvcs=false"
RUN go build -o /out/showcase ./cmd/showcase

FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc
RUN apk add --no-cache ca-certificates tini && addgroup -S app && adduser -S app -G app
COPY --from=build /out/showcase /usr/local/bin/showcase
USER app
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/showcase"]
