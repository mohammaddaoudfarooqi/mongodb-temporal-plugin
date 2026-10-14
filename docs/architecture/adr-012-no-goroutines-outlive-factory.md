# ADR-012: No goroutines outlive the factory

- Status: Accepted
- Date: 2026-04-30
- Deciders: plugin architecture review

## Context

A persistence plugin is a library hosted by `temporal-server`.
Any background goroutine the plugin starts becomes part of the
host's lifecycle. If those goroutines survive `Factory.Close()`,
shutdown hangs, tests leak, and process lifecycles become
unpredictable.

Temporal itself is careful about this; we need to be too.

## Decision

Every goroutine the plugin starts must be owned by a context that
the factory cancels in `Close()`. `Close()` drains the owned
goroutines within 10 seconds; if any goroutine is still live after
the drain, `Close()` returns an error.

Concretely:

- No "background retrier" loops — the driver handles retries.
- No "healthcheck pinger" — health is observed on request,
  returned via a gauge.
- No "metrics flusher" — metrics go through the host's
  `metrics.Handler`.

## Alternatives considered

- **Library-owned health worker** — rejected: crosses the
  lifecycle boundary; host code already has health orchestration.
- **Unbounded retry goroutine** — rejected: same issue; the
  driver's built-in retries are sufficient and respect context
  cancellation.

## Consequences

- Simpler shutdown: `Close()` cancels the factory's root context;
  in-flight requests drain naturally.
- Host-side testing is easier — no goroutines survive the test
  that owns them.
- Operator-visible: stopping the plugin always releases its
  database connections within 10s.
