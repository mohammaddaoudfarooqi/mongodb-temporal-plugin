# Showcase: a support desk on Temporal and MongoDB

A small, complete application that shows what running Temporal on
MongoDB gives you. **One cluster** does three jobs:

1. **Temporal's persistence.** Every ticket is a durable
   `TicketWorkflow`, with its history, timers and state stored in
   MongoDB through the plugin.
2. **Temporal visibility.** Operators find workflows with queries the
   plugin answers from MongoDB. In `atlas-search` mode this uses
   Atlas Search, including full-text matching on Text search attributes.
3. **Application data and search.** The tickets live in the same
   cluster, with a full-text (`$search`) and a vector (`$vectorSearch`)
   index. A new ticket gets suggested fixes from resolved ones through
   hybrid search.

## What it demonstrates

| Feature | Where |
|---|---|
| Durable workflow per business entity | [`app/workflows.go`](app/workflows.go) `TicketWorkflow` |
| Activities with retries; a failing search doesn't block the ticket | `Triage`, `StoreTicket`, `FindSimilar`, `Resolve` |
| Signals: `assign`, `resolve` | human-in-the-loop steps |
| Validated update: `comment` | rejects empty comments and comments on resolved tickets before they reach history |
| Query: `ticket` | the live ticket state and its suggestions |
| Durable timer | SLA per priority; if nobody responds in time, the ticket escalates |
| Search attributes | `CustomerId`, `Category`, `TicketStatus` (Keyword), `Priority` (Int), `Summary` (**Text**, full-text) |
| Visibility queries | `Category = 'billing'`, `Priority >= 3`, `Summary = 'unavailable'`, `ORDER BY StartTime DESC`, `COUNT … GROUP BY ExecutionStatus` |
| Schedule | `sla-report` runs `SLAReportWorkflow` every 5 minutes, built from visibility counts |
| Hybrid search | [`kb/store.go`](kb/store.go): `$search` + `$vectorSearch`, fused with Reciprocal Rank Fusion ([`kb/fuse.go`](kb/fuse.go)) |
| A learning loop | a resolved ticket's fix is written back and suggested for later, similar tickets |

## Embeddings

`SHOWCASE_EMBEDDING` chooses how vector search gets its embeddings:

| Mode | When | How |
|---|---|---|
| `auto` | Atlas (M10+, storage auto-scaling) | **Atlas automated embedding**: the vector index has an `autoEmbed` field with a Voyage AI model; the app stores text and queries with text. Preview feature. |
| `voyage` | Any MongoDB 8 with search (e.g. the Docker quickstart) | The app calls the Voyage AI API (`VOYAGE_API_KEY`) and stores vectors |
| `none` | No key | Full-text search only |

Without `SHOWCASE_EMBEDDING`, the mode is `voyage` when `VOYAGE_API_KEY`
is set, and `none` otherwise.

**`auto` and `voyage` send ticket text to Voyage AI**, a third party:
through Atlas in `auto` mode, directly in `voyage` mode. The showcase
is for demo data; use `none` if no text may leave your deployment.

## Run it

The quickest way is a [quickstart](../../quickstart), which starts
MongoDB, Temporal, the UI and this showcase:

```bash
cd quickstart/docker && ./quickstart.sh up && ./quickstart.sh tour
```

Against your own Temporal server (with the MongoDB plugin) and MongoDB:

```bash
export TEMPORAL_ADDRESS=localhost:7233 MONGODB_URI='mongodb://localhost:27017/?directConnection=true'
go run ./cmd/showcase setup     # namespace, search attributes, indexes, seed tickets, schedule
go run ./cmd/showcase worker &  # the workflows and activities
go run ./cmd/showcase tour      # every feature, each one checked
```

Other commands: `open`, `assign`, `comment`, `resolve`, `show`, `find`,
`search` and `report`. Run `go run ./cmd/showcase` for usage.

## Tests

```bash
go test ./...                       # unit: workflows (SDK test env), fusion, index definitions, Voyage client
go test -tags=integration ./kb/     # real $search and $vectorSearch on MongoDB 8.2 (Docker)
```

`showcase tour` is the end-to-end test: it exits non-zero if any feature
does not behave as described.
