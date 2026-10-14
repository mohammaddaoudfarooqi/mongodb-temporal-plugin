# The showcase CLI (worker, setup, tour, ...). Build context: examples/showcase.
FROM golang:1.26-alpine@sha256:3082400e369fa24d5fc60bca20edab3f6d604e0c5a690ec66b295eff4dd87ade AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0 GOFLAGS="-trimpath -buildvcs=false"
RUN go build -o /out/showcase ./cmd/showcase

FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc
RUN apk add --no-cache ca-certificates tini && addgroup -S app && adduser -S app -G app
COPY --from=build /out/showcase /usr/local/bin/showcase
USER app
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/showcase"]
