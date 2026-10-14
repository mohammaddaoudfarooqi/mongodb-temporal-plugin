# Developer conventions

## Go style

- Go version: pinned in `go.mod` to match the targeted Temporal Server tag.
- `gofmt`, `go vet`, `staticcheck` clean on every PR.
- Errors: wrap with `fmt.Errorf("%s: %w", context, err)` when adding
  context; keep wrap chains short (≤3).
- Log: structured, Temporal logger passed by the factory. No `log.Printf`.
- Panics are bugs. Guard every goroutine entry point with `defer recover()`
  and convert to a returned error.
- Public types in `pkg/plugin/`; implementation in `internal/`.
- Tests co-located with source: `foo.go` + `foo_test.go`.

## Naming

- Package names: lower-case single word. `executionstore`, `shardstore`.
- File names: lower-case with underscores only where meaningful
  (`transfer_task.go`). Prefer single words.
- Exported identifiers: `CamelCase`. Unexported: `camelCase`.
- Test names: `TestName_Scenario_Expected`, e.g.,
  `TestUpdateShard_StaleRangeID_ReturnsShardOwnershipLost`.
- BSON field names: `snake_case` consistently throughout the schema.

## Document shape conventions

- `_id` is always a string composite key `part1:part2:part3` where parts
  come from the natural primary key of the entity.
- Every document has `update_time` of BSON type DateTime.
- Blobs are `{data: BinData, encoding: string}`.
- Integer IDs/versions are BSON Int64.
- Enumerations (status, state) are stored as their upstream protobuf integer
  values.

## TDD discipline

Every change begins with a failing-test commit, then the minimum code to
pass, then a refactor. Reviewers will ask for the test commit if it is
missing.

## Commit messages

```
<type>(<scope>): <subject>

<body: why, not what>

Signed-off-by: ...
```

Types: feat, fix, refactor, test, docs, build, ci, chore.
Scope: store or area, e.g., `shardstore`, `visibility`, `tool`.

## Dependency discipline

- Adding a dep: update `go.mod`, `go.sum`, AND `NOTICE`. CI rejects PRs
  where these three are out of sync.
- New deps must be Apache 2.0, MIT, or BSD. Other licenses require a
  CODEOWNERS-approved exception.
