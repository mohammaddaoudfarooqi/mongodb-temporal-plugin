# Contributing

Thanks for your interest. This is a reference implementation (see the
note in the [README](README.md)). Issues and pull requests are welcome.

## Before you start

- For anything beyond a small fix, open an issue first, so we can agree
  on the approach.
- Write code yourself or base it on sources whose licence allows it.
  Don't copy code from other Temporal persistence implementations.
  Reading Temporal's public interfaces and documentation to understand
  the contract is fine.
- Dependencies must be Apache 2.0, MIT or BSD licensed. When you add one,
  update `go.mod`, `go.sum` **and** `NOTICE` together; CI checks that
  they agree.

## Making a change

1. Fork, then branch from `main`.
2. Add a test that fails without your change, then make it pass. Tests
   sit next to the code (`foo.go` and `foo_test.go`). Test names follow
   `TestName_Scenario_Expected`.
3. Run the checks:

   ```bash
   make vet unit lint      # quick, no external services
   make integration        # needs Docker
   ```

   Run `make conformance` too if you changed a store's behaviour.
4. Commit with a DCO sign-off (`git commit -s`). CI rejects commits
   without `Signed-off-by:`. Messages follow
   `<type>(<scope>): <subject>`, for example
   `fix(executionstore): keep the current-run pointer on reset`.
5. Open a pull request that says what changed and why, and how you
   tested it.

## Conventions

- BSON field names are `snake_case`.
- Composite `_id` values join the natural key with colons.
- Every document carries `update_time`.
- Workflow state stays an opaque blob: the plugin never decodes
  Temporal's protobuf payloads.
- Wrap errors with `fmt.Errorf("context: %w", err)`, and log through the
  Temporal logger the factory receives.

More detail: [`docs/developer/conventions.md`](docs/developer/conventions.md)
and [`docs/developer/testing.md`](docs/developer/testing.md).
