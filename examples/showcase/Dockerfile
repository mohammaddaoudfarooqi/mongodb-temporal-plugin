# The showcase CLI (worker, setup, tour, ...). Build context: examples/showcase.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0 GOFLAGS="-trimpath -buildvcs=false"
RUN go build -o /out/showcase ./cmd/showcase

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tini && addgroup -S app && adduser -S app -G app
COPY --from=build /out/showcase /usr/local/bin/showcase
USER app
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/showcase"]
