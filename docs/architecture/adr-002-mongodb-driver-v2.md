# ADR-002: MongoDB Go driver v2 only

- Status: Accepted
- Date: 2026-04-30
- Deciders: plugin architecture review

## Context

The MongoDB Go driver has two supported major versions:

- `go.mongodb.org/mongo-driver` — v1, maintenance mode.
- `go.mongodb.org/mongo-driver/v2` — v2, current development line.

Both can satisfy our needs functionally. Choosing both (import
paths differ, so they can coexist) is plausible but doubles the
dependency surface.

## Decision

Use driver v2 exclusively. All internal code imports
`go.mongodb.org/mongo-driver/v2/...`.

## Alternatives considered

- **Driver v1 only** — rejected: moving off v1 is a matter of time;
  starting on v2 avoids a re-migration within the plugin's first
  18 months.
- **Support both** — rejected: two transaction APIs, two BSON
  codec surfaces, two error types. Cost dwarfs any benefit for
  consumers (who pick up the plugin by import path, not by driver
  version).

## Consequences

- The plugin can only be used with consumer binaries that also use
  driver v2 (no shared `mongo.Client` across v1/v2 boundaries).
- BSON codecs, transaction semantics, and error types are locked
  to v2's conventions. Future driver breakage is ours to absorb.
- Driver v2's `WithTransaction` is the canonical retry loop; we do not wrap it.
