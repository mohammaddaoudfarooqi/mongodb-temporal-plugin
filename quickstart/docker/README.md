# Quickstart: Temporal on MongoDB, on Docker

One command starts a complete local stack:

- **MongoDB 8.2 with search** (`mongodb/mongodb-atlas-local`): a single
  replica set that stores Temporal's state, answers Temporal visibility
  queries through Atlas Search, and holds the showcase's data with
  full-text and vector search indexes.
- **Temporal server** with the MongoDB persistence plugin, visibility in
  `atlas-search` mode.
- **Temporal Web UI**.
- **The showcase**, a support desk (see [`examples/showcase`](../../examples/showcase)).

> **Development only.** MongoDB runs without authentication, and the
> Temporal frontend and UI accept every request, so every port binds to
> `127.0.0.1`. Don't publish these ports or run this stack on a shared
> machine. To run Temporal for others, follow the
> [security guide](../../docs/operator/security.md).

## Requirements

Docker with Compose v2, about 4 GB of free memory and 5 GB of disk.

## Run it

```bash
cd quickstart/docker
./quickstart.sh up      # builds the images and starts everything
./quickstart.sh tour    # a narrated demo that checks every feature
```

Open the Temporal UI at <http://localhost:8080/namespaces/support/workflows>.

| Command | What it does |
|---|---|
| `./quickstart.sh up` | Build the images and start the stack |
| `./quickstart.sh tour` | Run the showcase's end-to-end tour |
| `./quickstart.sh status` | Show the services and their addresses |
| `./quickstart.sh logs [service]` | Follow logs |
| `./quickstart.sh down` | Stop the stack and keep the data (`down -v` also deletes it) |

## Vector search

Full-text search always works, and nothing leaves your machine. For
semantic (vector) search the showcase embeds ticket text with
[Voyage AI](https://www.voyageai.com/). **That sends the ticket text to
Voyage AI**, so use demo data only. Put your key in
`quickstart/docker/.env`:

```bash
VOYAGE_API_KEY=your-key
```

Then run `./quickstart.sh down -v && ./quickstart.sh up`, so the vector
index is created with the right dimensions.

## Ports

Everything binds to `127.0.0.1` only, because MongoDB runs without
authentication. To change a port, set it in `.env`:

| Variable | Default |
|---|---|
| `TEMPORAL_PORT` | 7233 |
| `TEMPORAL_UI_PORT` | 8080 |
| `MONGO_PORT` | 27017 |

## Try it yourself

With the stack up, use the showcase CLI through Docker:

```bash
run() { docker compose run --rm showcase-setup "$@"; }
id=$(run open -customer acme -subject "Charged twice" -body "Refund my duplicate invoice charge")
run show "$id"                          # live state and suggested fixes
run comment "$id" "Looking into it"     # a validated update
run resolve "$id" "Refunded the duplicate charge"
run find "Category = 'billing' AND TicketStatus = 'resolved'"
run search "duplicate charge refund"
```

This stack is for local development and demos only. For a cloud
deployment, see [`quickstart/aws`](../aws).
