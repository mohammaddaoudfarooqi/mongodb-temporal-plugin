# ADR-001: Out-of-tree plugin via AbstractDataStoreFactory

- Status: Accepted
- Date: 2026-04-30
- Deciders: plugin architecture review

## Context

Temporal server exposes two extension points for persistence:

1. **In-tree**: add a new store under `common/persistence/...` in the
   Temporal repo. Requires forking or upstreaming.
2. **Out-of-tree**: implement the `AbstractDataStoreFactory` and
   `VisibilityStoreFactory` interfaces, compile a consumer binary
   that wires the factory via `temporal.WithCustomDataStoreFactory`.

We need to ship a MongoDB backend without asking the Temporal
maintainers to merge ~10 kLOC of domain-specific code, and without
taking on the maintenance burden of a long-lived fork.

## Decision

Ship as an out-of-tree Go module. Consumers import
`github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/pkg/plugin`
and wire the factory in their own `temporal-server` main.

## Alternatives considered

- **Upstream into `common/persistence/mongodb/`** — rejected:
  requires ongoing coordination with upstream maintainers; ties
  release cadence to theirs (their repo, their review constraints).
- **Fork `temporalio/temporal`** — rejected: fork maintenance is
  expensive; consumers must run our fork instead of upstream.

## Consequences

- No upstream Temporal source changes are required.
- Consumer binary is a small glue layer; the plugin itself is a
  library, testable in isolation.
- Tracks `go.temporal.io/server` as a versioned dependency. We
  commit to supporting the last two minor versions.
- No ability to modify core Temporal code paths. If upstream
  changes an interface in a breaking way, we bump our major.
