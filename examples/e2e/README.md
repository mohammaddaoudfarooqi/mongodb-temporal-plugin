# End-to-end example: workflows that exercise the MongoDB plugin

A small Temporal application (Go SDK) plus a runner that drives it against
a live Temporal server backed by `mongodb-temporal-plugin` and checks the
outcomes the plugin is responsible for. It is a separate Go module so the
SDK never becomes a dependency of the plugin.

## What it covers

| Scenario | Workflow | Plugin paths exercised |
|---|---|---|
| `order_saga` | `OrderWorkflow` | activity tasks + retries (flaky payment), heartbeats, signal, query, validated update, approval timer, typed custom search attributes |
| `order_compensation` | `OrderWorkflow` | non-retryable activity failure, compensation on a disconnected context |
| `signal_with_start` | `OrderWorkflow` | signal-with-start (signal buffered before the first workflow task) |
| `fan_out_children` | `FanOutWorkflow` | 10 child workflow executions |
| `continue_as_new` | `CounterWorkflow` | continue-as-new chain of 5 runs; the current-run pointer must name the last, completed run |
| `cancellation` | `LongTimerWorkflow` | request-cancel, timer cancellation, cleanup activity |
| `durable_timer` | `LongTimerWorkflow` | timer task fires after a real delay |
| `big_history` | `BigHistoryWorkflow` | 300 activities → multi-page history reads, full history read-back, deterministic replay |
| `failure` | `FailingWorkflow` | failure propagation |
| `reset` | `BigHistoryWorkflow` | `ResetWorkflowExecution` (conflict resolution), new current run |
| `visibility` | `OrderWorkflow` ×3 | custom search-attribute filters (Keyword, Int, Double), `ORDER BY StartTime DESC`, `COUNT … GROUP BY ExecutionStatus` |
| `schedule` | `ScheduledWorkflow` | schedules (the scheduler runs in its own namespace division), scheduled runs found via `TemporalScheduledById` |
| `-load N` | `OrderWorkflow` ×N | sustained end-to-end throughput and latency |

Workflow logic is unit tested with the SDK test environment
(`go test ./app/`), so a failing live scenario points at the server or the
plugin, not the workflow code.

## Run it

Against the local Docker stack (MongoDB replica set in a container):

```bash
cd deploy && ./deploy.sh            # Temporal + plugin + MongoDB
cd ../examples/e2e
go run ./cmd/e2e -address localhost:7233
go run ./cmd/e2e -scenarios none -load 500 -concurrency 50
```

For load tests, raise the load worker's task pollers: with the Go SDK's
defaults (two workflow and two activity pollers) the worker, not the
server, caps throughput. On Atlas M40, 16 of each doubled throughput:

```bash
go run ./cmd/e2e -scenarios none -concurrency 200 -duration 10m \
  -workflow-pollers 16 -activity-pollers 16
```

Against any running Temporal server backed by the plugin (for example a
native `temporal-mongodb-server` pointed at Atlas, see
`scripts/run-native-server.sh`):

```bash
go run ./cmd/e2e -address <host:port> -namespace <ns>
```

The runner registers the namespace and the custom search attributes
(`CustomerId`, `OrderTotal`, `Priority`, `OrderStatus`) if they are
missing, runs an in-process worker on task queue `mongodb-plugin-e2e`,
prints one line per scenario and exits non-zero if anything fails.
Workflow IDs are prefixed with `e2e-<unix time>` so repeated runs don't
collide.

## What it has found

The first runs against a real server surfaced three plugin bugs, each
fixed test-first: custom search attributes could not be registered
, scheduled runs were unqueryable by `TemporalScheduledById`
, and the classic and fair task backlogs fenced each other
out under load (3.2 → 24.5 workflows/s on the load mode once fixed).
