# Contributing

Welcome. This document covers how to make a change land.

## Prerequisites

- Go toolchain matching `go.mod`. Check with `go version`.
- Docker (for testcontainers-driven integration tests).
- A MongoDB Atlas project if you want to run the live Atlas Search
  tests; otherwise the container-based tests are enough.

## DCO sign-off

Every commit must include `Signed-off-by: Name <email>`. Use
`git commit -s`. CI fails PRs without sign-off. Never bypass hooks
(`--no-verify`, `--no-gpg-sign`) — fix the underlying issue instead.

## TDD discipline

Each change begins with a failing-test commit,
then the minimum code to pass, then a refactor. Reviewers look for
the test commit explicitly. PRs that add implementation without
exercising tests do not merge.

Tests are co-located: `foo.go` next to `foo_test.go`. Naming:
`TestName_Scenario_Expected`, e.g.
`TestUpdateShard_StaleRangeID_ReturnsShardOwnershipLost`.

## Commit style

```
<type>(<scope>): <subject>

<body: why, not what>

Signed-off-by: ...
```

- Types: `feat`, `fix`, `refactor`, `test`, `docs`, `build`, `ci`,
  `chore`.
- Scope: store or area — `shardstore`, `executionstore`,
  `visibility`, `tool`, `ci`.
- Link the related GitHub issue in the body or the PR description.

## Pull requests

1. Branch from `main`; keep PRs small and single-purpose.
2. Ensure `make vet staticcheck unit integration dco`
   passes locally.
3. PR description template:
   - **What** — 1–2 sentences.
   - **Why** — the problem being solved; link the issue if there is one.
   - **Test evidence** — the failing-test commit and its
     passing-commit.
4. CI matrix (unit → integration → conformance → DCO)
   must be green.
5. At least one reviewer approval.

## Dependency updates

Adding or upgrading a dependency touches three files that must
stay consistent, or CI rejects the PR:

- `go.mod`
- `go.sum`
- `NOTICE` (license attribution)

New dependencies must be Apache 2.0 / MIT / BSD licensed. If unsure,
check the dependency's `LICENSE` file and add the project's
copyright to `NOTICE` in the same PR.

## Issue triage

- Bugs affecting production: label `priority/high`. Include the
  minimal reproduction.
- Feature requests: label `kind/feature`; describe the use case.
- Documentation drift: label `docs`; open with a pointer to the
  divergent implementation.
