# Context Intelligence Server

An event-driven telemetry platform for [Amplifier](https://github.com/microsoft/amplifier) sessions. Captures session events as structured data and builds a property graph in Neo4j.

## How It Works

```
Amplifier CLI sessions
       |
       |  hook-context-intelligence (thin forwarder)
       |  POST /events {event, workspace, data}  ->  202 Accepted (persist-then-202)
       v
+------------------------------------------+        +----------------------+
| Ingestion Server (:8000)                 |        | Neo4j                |
| - Durable per-session append-log (queue) |        | :7687  bolt/driver   |
| - Async drainer -> batched Neo4j flush   | bolt   | :7474  browser UI    |
|   under a global write semaphore         |------->| Property graph       |
| - Retry + dead-letter + crash recovery   |        | 5 node / 8 edge types|
| - Blob storage (large payloads to disk)  |        +----------------------+
| - Dashboard + API docs + Cypher proxy    |
+------------------------------------------+
```

`POST /events` appends the raw event to a durable per-session append-log and returns `202`
immediately; an async single drainer per session processes batches and flushes them to Neo4j
under a global write semaphore, retrying transient/deadlock failures and dead-lettering poison
events. See [docs/architecture/05-durable-ingest-queue.png](docs/architecture/05-durable-ingest-queue.png)
for the full ingest/drain flow.

---

## Running with Docker Compose

### Prerequisites

- Docker and Docker Compose

### 1. Clone the repository

```bash
git clone https://github.com/microsoft/amplifier-context-intelligence.git
cd amplifier-context-intelligence
```

### 2. Start the stack (first run)

On first run, use the `start.sh` script to generate credentials and start the stack:

```bash
./start.sh
```

This generates credentials (`credentials.yaml` + `neo4j-auth.env`), then calls `docker compose up -d` to start the services.

On first run, `start.sh` prints your API token **once**, behind a `SAVE THIS TOKEN — it will NOT be shown again` banner. Capture it then — `credentials.yaml` stores only its SHA-256 digest (under `api_keys`), so you cannot grep the raw token back later. If you lose it, rotate to issue a new one (see [docs/managing-api-keys.md](docs/managing-api-keys.md)).

### 2a. Restart the stack (subsequent runs)

On subsequent restarts, the credentials already exist, so you can use `docker compose` directly:

```bash
docker compose up -d
```

### Services

The stack runs 2 services:

| Service | Port | Description |
|---------|------|-------------|
| **Ingestion server** | [localhost:8000](http://localhost:8000) | Event processing, dashboard, API |
| **Neo4j** | browser [localhost:7474](http://localhost:7474) · bolt `:7687` | Property graph (auth enabled) |

All services are configured with `restart: unless-stopped` — they automatically restart on crash or Docker daemon restart. They only stay down if you explicitly stop them with `docker compose stop` or `docker compose down`.

### 3. Access the dashboard

Open [http://localhost:8000](http://localhost:8000) — this is the single navigation hub for the system.

| Route | Content |
|-------|---------| 
| `/` | Landing page with navigation cards |
| `/dashboard` | Live session monitoring, event history, log stream, and an in-page Queues tab |
| `/docs` | Swagger API documentation |

The home page and dashboard both show:
- **Neo4j status** (Connected / Disconnected) polled every few seconds
- **Neo4j Bolt URL** — the exact value of `neo4j_url` from `server-config.yaml`
- **Neo4j Browser URL** — the exact value of `neo4j_browser_url` from `server-config.yaml`, as a clickable link

Both URLs are displayed verbatim from the configuration. If Neo4j is on a remote host, the displayed values reflect that remote address — not `localhost`.

When authentication is configured, the dashboard shows an API key prompt on first visit — enter the raw API token you saved at first run (it is **not** in `credentials.yaml`, which stores only the digest).

The dashboard has an in-page Queues tab (Overview | Queues) showing the pipeline conservation invariant, totals, and dead-letter management (Replay/Purge). The dashboard's single auth overlay gates everything; the `/queues/*` data endpoints require a Bearer token. There is no separate `/queues` page.

---

## Neo4j Plugins (APOC)

The Docker Compose stack ships with the **APOC** plugin (Awesome Procedures On
Cypher) enabled on the Neo4j service. APOC adds ~190 procedures and ~246
functions used for graph maintenance, migrations, and richer Cypher queries.

### How it is enabled

APOC is enabled by a single environment variable on the `neo4j` service in
`docker-compose.yml`:

```yaml
neo4j:
  image: neo4j:5.26.22-community
  environment:
    NEO4J_PLUGINS: '["apoc"]'
```

On startup Neo4j 5.x auto-installs the bundled `apoc-core` jar from
`/var/lib/neo4j/labs` into `/var/lib/neo4j/plugins` and applies APOC's default
configuration (which includes `dbms.security.procedures.unrestricted=apoc.*`).

- **No volume mount or manual jar download is required.** The jar lives on the
  image layer and is re-installed on every container start, so it survives
  rebuilds automatically.
- **APOC Core only.** `neo4j:5.26.22-community` bundles APOC Core. APOC
  Extended is not included and is not needed.

### Verify APOC is loaded

```bash
docker compose exec neo4j \
  cypher-shell -u neo4j -p "<password>" "RETURN apoc.version();"
# → "5.26.22"  (matches the Neo4j version)
```

> If you run a **standalone** Neo4j container (the `docker run` examples in this
> README and in [docs/service-setup.md](docs/service-setup.md)), add
> `-e 'NEO4J_PLUGINS=["apoc"]'` to the `docker run` command to get the same
> behavior. **Hosted Neo4j AuraDB** already has APOC Core preinstalled — no
> action needed there.

### Air-gapped / offline deployments

Some environments block the Neo4j container's internet egress entirely. APOC
must then be **provisioned locally** — it cannot be downloaded. Two facts make
this straightforward:

1. **The default `NEO4J_PLUGINS=["apoc"]` path is already offline-safe for APOC
   Core.** On Neo4j 5.x the APOC Core jar ships *inside* the official image at
   `/var/lib/neo4j/labs/`; the startup installer copies it from there into
   `/var/lib/neo4j/plugins/` — it does **not** reach the internet. (Verified by
   running the container with networking fully disabled: the jar still installs
   from the local labs dir.)
2. **For an air-tight guarantee, bake the jar into the image** and skip the
   installer entirely. This repo ships that path:
   - [`neo4j.Dockerfile`](neo4j.Dockerfile) — copies the bundled APOC Core jar
     into `/var/lib/neo4j/plugins/` at **build time** and sets
     `NEO4J_dbms_security_procedures_unrestricted=apoc.*`. The jar becomes an
     immutable image layer; there is nothing to download at run time.
   - [`docker-compose.airgap.yml`](docker-compose.airgap.yml) — an override that
     builds Neo4j from `neo4j.Dockerfile` and disables `NEO4J_PLUGINS`.

   ```bash
   docker compose -f docker-compose.yml -f docker-compose.airgap.yml up -d --build
   ```

> **Base image must be pre-loaded too.** On a fully disconnected host you also
> cannot `docker pull neo4j:5.26.22-community`. Pre-load it on a connected
> machine and transfer it — `docker save neo4j:5.26.22-community -o neo4j.tar`
> then `docker load -i neo4j.tar` on the air-gapped host — or pull from an
> internal registry mirror. The base image carries the bundled APOC Core jar the
> build copies, so everything else then runs with zero internet access.

This air-gapped path has been validated in an isolated environment with the
Neo4j container cut off from the internet (no default route; outbound to
`dist.neo4j.org:443` blocked): `RETURN apoc.version()` returns `5.26.22`, APOC
procedures run, built-in `db.*` procedures are unaffected, and no plugin
download occurs.

---

## Running with Docker (single container)

Build the image then run with explicit port and volume mounts:

```bash
docker build -t context-intelligence-server .
docker run -d \
  --name context-intelligence-server \
  -p 8000:8000 \
  -v "$HOME/amplifier-context-intelligence-server-data-store:/data" \
  -e AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_API_KEY=<your-api-key> \
  -e AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_URL=bolt://your-neo4j:7687 \
  -e AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_PASSWORD=<password> \
  context-intelligence-server
```

The entrypoint prints the generated API token **once** on first run, behind a `SAVE THIS TOKEN` banner. Read it from the container's first-run logs and save it — `/data/credentials.yaml` stores only the token's SHA-256 digest (under `api_keys`), not the token itself:

```bash
docker logs context-intelligence-server   # look for the "SAVE THIS TOKEN" banner
```

> Passing `-e AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_API_KEY=<...>` above uses the legacy single-key mode instead, with a token you choose. See [docs/managing-api-keys.md](docs/managing-api-keys.md) for both modes.

---

## First-Run Setup (Standalone)

There is **no `init` subcommand.** To run the server standalone, write a
`server-config.yaml` by hand (copy `server-config.example.yaml` and edit — see
[Running Without Docker](#running-without-docker) below) and add authentication
yourself.

For auth, choose one of:

- **Legacy single key** — set `api_key: "<your-secret>"` in the config. Clients
  send it as `Authorization: Bearer <your-secret>`.
- **Per-contributor keystore** — generate a token, derive its SHA-256 digest, and
  add an `api_keys` entry. Send the **raw token** to the client.

  ```bash
  TOKEN=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
  python3 -c "import hashlib,sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest())" "$TOKEN"
  echo "raw token (give to client): $TOKEN"
  ```

- **Microsoft Entra JWT** (`auth_mode=entra`) — clients authenticate with Azure AD
  bearer tokens instead of pre-shared keys; the server validates the JWT and maps the
  token's `oid` to a contributor. See [docs/entra-auth-setup.md](docs/entra-auth-setup.md).

The server verifies every request by hashing the presented token (`sha256(token)`)
and matching the digest. The full guide — adding/revoking/rotating peers, the
empty-`{}` hard-error rule, and the raw-token-vs-digest guardrail — is in
[docs/managing-api-keys.md](docs/managing-api-keys.md). Use the client token as
`context_intelligence_api_key` in your bundle config.

> Running under Docker instead? `./start.sh` (or the container entrypoint)
> bootstraps an `api_keys` keystore and prints the token once — no manual steps.

---

## Running Without Docker

Run the server as a plain Python process against any Neo4j instance — useful for local development, custom deployments, or environments where Docker is unavailable.

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv)
- A running Neo4j instance (see below)

### 1. Install dependencies

```bash
git clone https://github.com/microsoft/amplifier-context-intelligence.git
cd amplifier-context-intelligence
uv sync
```

### 2. Start a Neo4j instance

**Option A — Docker (easiest):**

```bash
docker run -d --name neo4j-ci \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=none \
  -e 'NEO4J_PLUGINS=["apoc"]' \
  neo4j:5.26.22-community
```

The `NEO4J_PLUGINS=["apoc"]` line enables the APOC plugin (see
[Neo4j Plugins (APOC)](#neo4j-plugins-apoc) below). It is optional for the
server to run today, but keeps standalone instances consistent with the Docker
Compose stack.

**Option B — Neo4j Desktop / existing instance:**

Use any Neo4j 5.x instance. Note the bolt URL, username, and password — you will need them in the next step.

### 3. Configure the server

The server accepts configuration from a **YAML file**, **environment variables**, or both. Environment variables always take precedence over the YAML file.

#### Option A — YAML configuration file (recommended)

Copy the example file and edit it:

```bash
cp server-config.example.yaml server-config.yaml
```

Edit `server-config.yaml`:

```yaml
# Neo4j connection
neo4j_url: neo4j://localhost:7687          # bolt/driver URL (used for graph operations)
neo4j_browser_url: http://localhost:7474   # browser UI URL (clickable link in web UI)
neo4j_user: neo4j
neo4j_password: ""          # empty string for NEO4J_AUTH=none instances

# Storage — directories are created automatically
blob_path: /home/you/.local/share/ci-server/blobs
log_path:  /home/you/.local/share/ci-server/logs/server.jsonl

# Server bind address — 0.0.0.0 accepts connections from all interfaces,
# including Docker/Incus container bridges.  Use 127.0.0.1 only if you are
# certain no containers need to reach this server.
server_host: 0.0.0.0
server_port: 8000
```

The server looks for `server-config.yaml` in the **working directory** by default. To use a file at a different path, set the `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE` environment variable:

```bash
AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE=/etc/ci-server/config.yaml \
  uvicorn context_intelligence_server.main:app
```

#### Option B — Environment variables

Pass settings directly on the command line or export them in your shell:

```bash
AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_URL=neo4j://localhost:7687 \
AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_BROWSER_URL=http://localhost:7474 \
AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_PASSWORD="" \
AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_BLOB_PATH=/tmp/ci-blobs \
AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_LOG_PATH=/tmp/ci-logs/server.jsonl \
  uvicorn context_intelligence_server.main:app --reload
```

#### Option C — Mix both

YAML provides the baseline; environment variables override individual values at runtime. This is handy for secrets or per-machine overrides:

```yaml
# server-config.yaml — checked into version control
neo4j_url: neo4j://localhost:7687
blob_path: /data/ci-blobs
log_path:  /data/ci-logs/server.jsonl
```

```bash
# Override only the password at runtime (e.g. from a secrets manager)
AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_PASSWORD=hunter2 \
  uvicorn context_intelligence_server.main:app
```

### 4. Start the server

```bash
# With auto-reload (development)
uvicorn context_intelligence_server.main:app --reload

# Production — bind explicitly
uvicorn context_intelligence_server.main:app \
  --host 0.0.0.0 \
  --port 8000 \
  --workers 1
```

Open [http://localhost:8000](http://localhost:8000) to confirm the server is running.

---

## Running as a System Service

To run the server as an auto-starting background service on Linux (systemd)
or macOS (launchd), see [docs/service-setup.md](docs/service-setup.md).

---

## Deploying to Azure

See [docs/azure-deployment.md](docs/azure-deployment.md) for a full guide to deploying as an Azure Container App with automatic HTTPS, persistent storage, and Neo4j on AuraDB.

---

## Sharing with Trusted Peers

To let a few trusted people on **other networks** send their sessions to your server
— privately, exposing only the `/events` endpoint and nothing else — see
[docs/remote-access-sharing.md](docs/remote-access-sharing.md). It covers loopback
binding, exposing a single path over Tailscale, and per-peer access scoping
(including a common ACL pitfall that silently grants too much). Hand your peers
[docs/peer-onboarding.md](docs/peer-onboarding.md) to get them connected.

---

## Network Access and Security

The server defaults to `server_host: 0.0.0.0`, which binds on **all network interfaces** — loopback, LAN, and any container bridges (Docker, Incus). This is intentional: worker containers and DTU containers use the host's bridge gateway IP to reach the server, and they cannot reach `127.0.0.1` (the host's loopback) from inside a container. On a single-user development machine behind a NAT router or firewall, binding to 0.0.0.0 is safe. If you deploy the server in an environment where port 8000 (or your configured `server_port`) is reachable from untrusted networks, restrict access with a firewall rule to trusted source IPs. Use `server_host: 127.0.0.1` only when you are certain no container processes will ever need to reach the server.

---

## Feeding Events into the Server

The server receives events from [amplifier-bundle-context-intelligence](https://github.com/microsoft/amplifier-bundle-context-intelligence) — a thin-forwarder hook that captures every Amplifier session event and dispatches it to the server over HTTP.

### Install the bundle

```bash
amplifier bundle add git+https://github.com/microsoft/amplifier-bundle-context-intelligence@main --name context-intelligence --app
```

The `--app` flag makes the bundle always active across all sessions — no need to run `amplifier bundle use`.

### Configure the server URL

Add the hook configuration to `~/.amplifier/settings.yaml`:

```yaml
overrides:
  hook-context-intelligence:
    config:
      context_intelligence_server_url: "http://localhost:8000"
```

### How it works

When `context_intelligence_server_url` is configured, the hook:

1. Writes every event to local JSONL (always, regardless of server)
2. Fire-and-forgets `POST /events` to the server for each event (5s timeout, failures logged as warnings)
3. Registers `blob_list` and `blob_dump` tools for querying server-stored blobs

The local JSONL is the durable record. The server dispatch is best-effort and never blocks the Amplifier session. If the server is down, the session continues unaffected.

### Hook settings

All settings live in `~/.amplifier/settings.yaml` under `overrides.hook-context-intelligence.config`:

| Setting | Default | Description |
|---------|---------|-------------|
| `context_intelligence_server_url` | *(empty — disabled)* | Server URL to forward events to |
| `context_intelligence_api_key` | *(empty)* | Raw bearer token for server auth. The server verifies it by computing `sha256(token)` and matching the digest in its keystore (a legacy `api_key`, or an `api_keys` entry). Send this raw token to clients; never the digest. |
| `workspace` | *(auto-resolved)* | Workspace scope for graph data |

---

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/events` | Ingest a session event (returns 202 immediately) |
| `GET` | `/status` | Server health, active sessions, completed history, error counts, `neo4j_connected`, `neo4j_url`, `neo4j_browser_url` |
| `GET` | `/` | Landing page with navigation cards |
| `GET` | `/dashboard` | Live monitoring dashboard |
| `GET` | `/docs` | Swagger API docs |
| `GET` | `/logs/stream` | Server-Sent Events — live structured log tail |
| `GET` | `/blobs/{session_id}` | List all blob URIs for a session |
| `GET` | `/blobs/{session_id}/{key}` | Retrieve a stored blob |
| `POST` | `/cypher` | Proxy a Cypher query to Neo4j |
| `GET` | `/queues/dead-letter` | List dead-letter queues — `worker_key`, `item_count`, `last_error`, `last_ts` (requires `Authorization: Bearer`) |
| `POST` | `/queues/dead-letter/{worker_key}/replay` | Re-enqueue a worker's dead-letter records then purge; returns count re-enqueued (requires `Authorization: Bearer`) |
| `POST` | `/queues/dead-letter/{worker_key}/purge` | Permanently delete a worker's dead-letter records; returns count purged (requires `Authorization: Bearer`) |

### Event payload

```json
{
  "event": "tool:pre",
  "workspace": "my-project",
  "data": {
    "session_id": "abc-123",
    "timestamp": "2026-03-14T12:00:00+00:00",
    "tool_name": "bash",
    "tool_call_id": "tc-001"
  }
}
```

### Cypher proxy

```json
{
  "query": "MATCH (s:Session {workspace: $workspace}) RETURN s.node_id, s.status",
  "params": {},
  "workspace": "my-project"
}
```

Use `"workspace": "*"` to query across all workspaces.

---

## Configuration

### Settings resolution order

Values are resolved with this priority (highest first):

1. **Environment variables** — `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_*`
2. **YAML configuration file** — `server-config.yaml` in the working directory, or the path in `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE`
3. **Built-in defaults**

### All settings

| Environment variable | YAML key | Default | Description |
|----------------------|----------|---------|-------------|
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE` | *(env only)* | `server-config.yaml` | Path to the YAML config file |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_API_KEY` | `api_key` | *(empty — auth disabled)* | Legacy single bearer token (folds to contributor id `owner`). When set, all API endpoints except `/status` and static routes require `Authorization: Bearer <value>`. The server verifies a request by computing `sha256(token)` and matching it. Coexists with `api_keys`. See [docs/managing-api-keys.md](docs/managing-api-keys.md). |
| *(YAML only)* | `api_keys` | *(empty — disabled)* | Per-contributor keystore: a map of `sha256_hex(token) -> {id: <contributor>}`. The server stores only digests; the peer sends the **raw** token and the server hashes it to look up the contributor. `api_keys: {}` (empty map) is a **hard startup error** — omit or use `null` to disable auth. The matched `id` is recorded as `created_by` on graph nodes. See [docs/managing-api-keys.md](docs/managing-api-keys.md). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AUTH_MODE` | `auth_mode` | `static` | Selects the active resolver: `static` (sha256 keystore, the `api_key`/`api_keys` path above) or `entra` (Microsoft Entra JWT validation via JWKS). Exactly one mode is active. `entra` requires the three fields below. See [docs/entra-auth-setup.md](docs/entra-auth-setup.md). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AZURE_CLIENT_ID` | `azure_client_id` | *(empty)* | App Registration (client) GUID. **Required when `auth_mode=entra`** (startup refuses otherwise). See [docs/entra-auth-setup.md](docs/entra-auth-setup.md). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AZURE_TENANT_ID` | `azure_tenant_id` | *(empty)* | Azure AD tenant GUID. **Required when `auth_mode=entra`**. See [docs/entra-auth-setup.md](docs/entra-auth-setup.md). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ENTRA_IDENTITIES` (JSON) | `entra_identities` | *(empty)* | Identity map `oid -> {id: <contributor>}` (oids are Azure Object IDs — **PII**, never commit real values). **Required (non-empty) when `auth_mode=entra`**; the matched `id` is recorded as `created_by`. See [docs/entra-auth-setup.md](docs/entra-auth-setup.md). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ALLOW_UNAUTHENTICATED` | `allow_unauthenticated` | `false` | Opt-out of the fail-closed startup gate so the server can boot with no auth configured (every request passes through). **TEST/DEV ONLY — never set in production.** |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_WEB_UI_ENABLED` | `web_ui_enabled` | `true` | When `false`, locks down to API-only: no OpenAPI schema / Swagger UI, and the index, dashboard, static assets, and `/logs/stream` routes are unregistered and removed from the auth-exempt set (`/logs/stream` becomes auth-gated). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_URL` | `neo4j_url` | `neo4j://neo4j:7687` | Neo4j bolt/driver URL used for all graph operations. **Displayed verbatim in the web UI.** May point to a remote host — `bolt://db.internal:7687` is valid. Use `bolt://` scheme for Community Edition single-node installs. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_BROWSER_URL` | `neo4j_browser_url` | `http://localhost:7474` | Neo4j Browser HTTP UI URL. **Displayed verbatim as a clickable link in the web UI.** Set to the address reachable from your browser — not necessarily `localhost` if Neo4j is on a remote machine. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_USER` | `neo4j_user` | `neo4j` | Neo4j username |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_PASSWORD` | `neo4j_password` | `password` | Neo4j password |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_BLOB_PATH` | `blob_path` | `/data/blobs` | Blob storage root directory |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_QUEUES_PATH` | `queues_path` | `/data/queues` | Directory for the durable per-session append-logs (persist-then-202 ingest); mirrors `blob_path`. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_WRITE_CONCURRENCY` | `write_concurrency` | `8` | Max concurrent Neo4j write flushes across all session drainers (starvation guard). |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_MAX_DELIVERY_ATTEMPTS` | `max_delivery_attempts` | `5` | Flush retries for one batch before its offending line is dead-lettered. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_LOG_PATH` | `log_path` | `/data/logs/server.jsonl` | Structured log file path |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_LOG_LEVEL` | `log_level` | `INFO` | Log level (`DEBUG`/`INFO`/`WARNING`/`ERROR`) |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_SERVER_HOST` | `server_host` | `0.0.0.0` | Bind host |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_SERVER_PORT` | `server_port` | `8000` | Bind port |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_DASHBOARD_INACTIVE_TIMEOUT` | `dashboard_inactive_timeout` | `1800.0` | Seconds before a session is hidden from the dashboard (30 min) |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_STALE_SESSION_TIMEOUT` | `stale_session_timeout` | `432000.0` | Seconds before a session worker is reaped (5 days) |

> **Note:** `CONFIG_FILE` is resolved before any other setting and cannot itself be set from the YAML file — only from the environment.

---

## Data Persistence

The Docker Compose stack uses bind mounts under `$HOME/amplifier-context-intelligence-server-data-store/` for all persistent data. When running without Docker, the paths are whatever you configure.

| Data | Docker Compose path | Description |
|------|--------------------| ------------|
| Neo4j graph | `~/amplifier-context-intelligence-server-data-store/neo4j` | Property graph database |
| Blobs | `~/amplifier-context-intelligence-server-data-store/blobs` | Event blob JSON files |
| Logs | `~/amplifier-context-intelligence-server-data-store/logs` | Rotating JSONL log files |
| Queues | `~/amplifier-context-intelligence-server-data-store/queues` | Durable per-session append-logs (`.log`, `.offset`, `.dead.jsonl`) |

Graph data, blob data, and the durable per-session queues survive container rebuilds and restarts. On startup the server replays any unprocessed queue lines and re-seeds its conservation counters (accepted/written/in-queue/dead) from disk, so in-flight events are recovered rather than lost across a restart. The durable per-session logs — not just Neo4j — are the record for events that have been accepted but not yet written to the graph.

**Safe operations:**
```bash
docker compose restart <service>           # Preserves all data
docker compose up -d --build <service>     # Preserves all data
```

**Destructive operations (use only to intentionally wipe data):**
```bash
docker compose down -v                     # Deletes ALL volumes
docker volume rm <name>                    # Deletes specific volume
```

---

## Neo4j Graph Model

All nodes carry a `workspace` property for multi-workspace isolation.

### Node types

| Label | Created by | Key properties |
|-------|-----------|----------------|
| `Session` + `RootSession`/`SubSession`/`ForkedSession` | `session:start`, `session:fork` | `node_id`, `status`, `started_at` |
| `Session` + `IncompleteSession` | `session:end` with no prior start/fork | `node_id`, `has_terminal: false`; health signal — spike indicates upstream event loss; WARNING logged at ingest; not a terminal type |
| `ToolCall` | `tool:pre` | `node_id` (session__tool_call__tool_call_id), `tool_name`, `tool_call_id` |
| `Event` + derived label | unclaimed events | `node_id`, `event_type` |

### Edge types

`SUBSESSION_OF` | `HAS_FORK` (session:fork parent→child) | `HAS_EVENT` | `HAS_TOOL_CALL` (Session→ToolCall, has started_at/ended_at)

### Example queries

```cypher
-- All sessions in a workspace
MATCH (s:Session {workspace: "my-project"})
RETURN s ORDER BY s.started_at DESC

-- Full session graph
MATCH path = (s:Session)-[*1..4]->(n)
WHERE s.node_id CONTAINS "my-session-id"
RETURN path

-- Find all events for a session
MATCH (s:Session {node_id: "your-session-id"})-[:HAS_EVENT]->(e:Event)
RETURN labels(e), e.occurred_at
ORDER BY e.occurred_at
```

---

## Development

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv)
- Docker (for running Neo4j during tests, or the full stack)

### Setup

```bash
git clone https://github.com/microsoft/amplifier-context-intelligence.git
cd amplifier-context-intelligence
uv sync
```

### Run tests

```bash
uv run pytest tests/ -q
```

### Project structure

```
amplifier-context-intelligence/
├── context_intelligence_server/         # Ingestion server (FastAPI)
│   ├── main.py                          # App factory, lifespan, static files
│   ├── config.py                        # Pydantic Settings + YAML source
│   ├── queue_manager.py                 # Durable per-session append-log (persist-then-202)
│   ├── registry.py                      # Per-session drainers (drain_worker, write semaphore, retry/dead-letter)
│   ├── services.py                      # Service wiring / lifecycle
│   ├── pipeline.py                      # Per-event dispatch spine (invoked by the drainer)
│   ├── neo4j_store.py                   # Neo4jGraphStore (managed-tx writes)
│   ├── graph_store.py                   # Graph store protocol / abstraction
│   ├── blob_store.py                    # AsyncDiskBlobStore
│   ├── idempotency.py                   # Idempotent MERGE / dedupe helpers
│   ├── auth.py                          # Bearer-token API authentication
│   ├── dashboard.py                     # Dashboard SSE stream
│   ├── routers/                         # API routers: queues.py, skills.py, version.py
│   ├── handlers/                        # Event handlers: data_layer_1/2/3/ + field_lifters/
│   └── web/                             # Dashboard HTML + static assets
├── server-config.example.yaml           # Configuration file template
├── docker-compose.yml                   # 2-service stack (server + neo4j)
└── Dockerfile                           # Ingestion server image
```

---

## Related

- [amplifier-bundle-context-intelligence](https://github.com/microsoft/amplifier-bundle-context-intelligence) — Amplifier bundle that forwards session events to this server
- [amplifier](https://github.com/microsoft/amplifier) — The Amplifier framework


## Contributing

> [!NOTE]
> This project is not currently accepting external contributions, but we're actively working toward opening this up. We value community input and look forward to collaborating in the future. For now, feel free to fork and experiment!

Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit [Contributor License Agreements](https://cla.opensource.microsoft.com).

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft
trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
