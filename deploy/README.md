# One-command deploy

Brings up a full Temporal stack (server + Web UI) backed by this
plugin and a MongoDB 8.0 replica set — or a MongoDB Atlas cluster on
Linux.

## Platform compatibility

| Platform | Local MongoDB | MongoDB Atlas | Notes |
|----------|:---:|:---:|-------|
| **Linux** (native Docker) | Yes | Yes | No VM layer — container-to-internet TCP is reliable |
| **macOS** (Docker Desktop) | Yes | **No** | VM NAT bridge corrupts long-lived TCP connections |
| **Windows** (Docker Desktop / WSL2) | Yes | **No** | Same VM limitation as macOS |

**Why Atlas fails on Docker Desktop:** Docker Desktop runs containers
inside a lightweight Linux VM. All outbound traffic passes through a
NAT bridge in that VM. Long-lived TLS connections to external hosts
(like Atlas) get corrupted at the TCP layer, causing:

- `incomplete read of message header` errors on MongoDB operations
- Task queue managers stopping with `InitError`
- Workflow tasks never reaching workers (`poll_workflow_activation` hangs)

This is a Docker Desktop architectural limitation, not a MongoDB or
plugin bug. The `deploy.sh` script auto-detects the platform and
blocks `--atlas` on Docker Desktop with a clear error message.

## Prerequisites

- Docker with the `compose` v2 plugin (`docker compose version` must work).
- ~2 GB free RAM and ~2 GB free disk for the Mongo volume (local mode).
- For Atlas mode: a Linux host with native Docker.

No Go toolchain required — the deploy script builds the plugin
container image directly from the repo Dockerfile.

## Quick start — self-hosted Mongo (all platforms)

```bash
cd deploy/
./deploy.sh --namespace my-namespace
```

After ~1-2 minutes (first run includes the image build):

- Temporal Web UI → `http://localhost:8080` (auto-picks a free port)
- Temporal gRPC → `localhost:7233`
- Temporal HTTP API → `http://localhost:7243`
- MongoDB → `mongodb://localhost:27017` (replica set `rs0`)
- Namespace `my-namespace` registered and set as UI default

All ports auto-increment if occupied. Pin specific ports via env vars
(see `.env.example`).

## Quick start — MongoDB Atlas (Linux only)

```bash
cd deploy/
./deploy.sh --atlas \
    "mongodb+srv://USER:PASS@cluster0.example.mongodb.net/?retryWrites=true&w=majority" \
    --namespace my-namespace
```

When you pass `--atlas`:

- The local `mongo` + `mongo-init` containers are skipped.
- Visibility mode defaults to `aggregation` (override with
  `TEMPORAL_VISIBILITY_MODE=atlas-search` if you have created Atlas
  Search indexes).
- The Atlas cluster must be reachable from the Docker network and
  the DB user must have `readWrite` + `dbAdmin` on the `temporal`
  database, which holds both stores.

To create Atlas Search indexes (required for `atlas-search` visibility
mode):

```bash
docker run --rm "$PLUGIN_IMAGE_TAG" temporal-mongodb-tool setup-atlas-search \
    --uri "$ATLAS_URI" --database temporal
```

On macOS/Windows, `deploy.sh` blocks `--atlas` with an explanatory
message. To override at your own risk:

```bash
DEPLOY_FORCE_ATLAS=true ./deploy.sh --atlas '<URI>'
```

## What the stack contains

| Service | Purpose |
|---------|---------|
| `mongo` | MongoDB 8.0 community, replica set `rs0` (local mode only) |
| `mongo-init` | Runs `rs.initiate()` once, then exits |
| `schema-init` | Runs `temporal-mongodb-tool setup-schema` on both DBs, exits |
| `temporal` | `temporal-mongodb-server` — Temporal server + MongoDB plugin |
| `temporal-ui` | `temporalio/ui:latest` wired to the server |

## Commands

```bash
# --- Lifecycle ---
./deploy.sh                              # start with local Mongo (default)
./deploy.sh --namespace my-namespace    # start + register namespace
./deploy.sh --atlas '<URI>' --namespace ns  # start with Atlas (Linux)
./deploy.sh --down                       # stop + remove containers + volume

# --- Diagnostics ---
./deploy.sh --status                     # show service status + URLs
./deploy.sh --health                     # run connectivity + error checks
./deploy.sh --logs                       # tail temporal server logs
./deploy.sh --logs ui                    # tail UI logs
./deploy.sh --logs mongo                 # tail MongoDB logs
./deploy.sh --logs all                   # tail everything
```

## Connecting a consumer app

Your own applications connect to the Temporal server
from a separate compose stack. The key points:

1. **Join the plugin's Docker network** so the container name `temporal`
   resolves:

   ```yaml
   networks:
       plugin_temporal:
           name: deploy_default
           external: true
   ```

2. **Point workers at `temporal:7233`** (the container's internal gRPC
   port, not the host-mapped port):

   ```yaml
   environment:
       TEMPORAL_ADDRESS: temporal:7233
       TEMPORAL_NAMESPACE: my-namespace
       TEMPORAL_TASK_QUEUE: my-task-queue
   ```

3. **Start the plugin stack first**, then the consumer stack:

   ```bash
   # Terminal 1: plugin stack
   cd mongodb-temporal-plugin/deploy
   ./deploy.sh --namespace my-namespace

   # Terminal 2: your application's stack
   cd my-app
   docker compose up -d
   ```

4. **If the worker fails with "Namespace not found"**: the namespace
   wasn't registered yet when the worker started. Either restart the
   worker container (`docker restart <worker>`) or pass `--namespace`
   to `deploy.sh`.

## Customizing the Temporal config

`deploy/config/development.yaml` is the Temporal server config. The
compose file mounts it into the `temporal` container and renders the
`${TEMPORAL_MONGO_URI}` / `${TEMPORAL_VISIBILITY_MODE}` placeholders
with `sed` at startup.

### Configuration warnings

**Do NOT set `socketTimeoutMs` in `development.yaml` or in the MongoDB
URI.** The MongoDB Go driver v2 interprets this as a Client-Side
Operation Timeout (CSOT). When Temporal cancels a context (normal during
task queue drain cycles), CSOT transforms the clean cancellation into a
destructive mid-read socket tear, producing `incomplete read of message
header` errors even with local MongoDB. The plugin's default timeouts
(driven by Temporal's own context deadlines) are correct for all
deployments.

**Do NOT use `directConnection=true` with `replicaSet=` in the URI.**
The MongoDB Go driver v2 cannot reconcile these options. The driver
enters replica-set topology mode but `directConnection` prevents server
discovery, resulting in `ReplicaSetNoPrimary` errors on startup.

## Troubleshooting

### `temporal` container restarts every 30s

Usually means `schema-init` didn't finish. Run `./deploy.sh --down`
then start again. The first boot sometimes loses the race with
replica-set initialization on slow hosts.

### `setup-schema` fails on Atlas

Confirm the DB user has `dbAdmin` on `temporal`. `readWrite` alone
can't create indexes.

### UI shows "cluster unreachable"

Check `./deploy.sh --logs` for a stack trace. Common cause: the
MongoDB URI in `deploy/.env` is wrong or unreachable from the Docker
network.

### `incomplete read of message header` errors

Run `./deploy.sh --health` to check. This error means TCP connections
to MongoDB are being corrupted.

| Scenario | Cause | Fix |
|----------|-------|-----|
| Docker Desktop + Atlas | VM NAT bridge corrupts TCP | Switch to local MongoDB |
| Docker Desktop + local Mongo | `socketTimeoutMs` set in config | Remove `socketTimeoutMs` from `development.yaml` |
| Linux + Atlas | Rare — network instability | Check Atlas cluster health, retry |

### Task queue managers stopping with `InitError`

Downstream effect of `incomplete read` errors. Fix the underlying
MongoDB connectivity issue (see above).

### Worker's `poll_workflow_activation()` hangs (workflow tasks never arrive)

The worker is connected and polling, but the server can't complete
`RecordWorkflowTaskStarted` because MongoDB persistence is failing.
Check server logs for persistence errors:

```bash
./deploy.sh --logs | grep -i "error\|incomplete\|InitError"
```

### Namespace not found after fresh deploy

Namespace propagation in Temporal is eventually consistent. If the
worker starts before propagation completes, restart it:

```bash
docker restart <worker-container-name>
```

Or pass `--namespace` to `deploy.sh`, which registers the namespace
and waits for the server's HTTP API to be ready before returning.

## Architecture notes

### Why local MongoDB works everywhere

Container-to-container networking stays entirely inside the Docker VM's
virtual bridge. Traffic never traverses the NAT layer that causes
problems with external hosts. Round-trip time is sub-millisecond, and
there is no TLS overhead or TCP corruption.

### CSOT and context cancellation

The MongoDB Go driver v2 replaced `SocketTimeout` with Client-Side
Operation Timeout (CSOT). When enabled via `SetTimeout()`, the driver
installs a deadline watcher on every socket read. If Temporal cancels
a context while a MongoDB read is in flight, the driver force-closes
the socket mid-read and reports `incomplete read of message header`.
Without CSOT, context cancellation produces a clean `context.Canceled`
error that the plugin's error translator handles correctly.

The plugin's `development.yaml` intentionally omits `socketTimeoutMs`
to avoid enabling CSOT. Temporal's own context deadlines govern all
operation timeouts.

### MaxIdleTimeMs

The plugin defaults to a 30-second `maxConnIdleTime` for the MongoDB
connection pool. This prevents idle connections from accumulating
during low-traffic periods without being so aggressive that connections
churn during normal Temporal background operations (task queue lease
renewals, shard range checks, etc.).
