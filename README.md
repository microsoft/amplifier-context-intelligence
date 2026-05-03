# Context Intelligence Server

An event-driven telemetry platform for [Amplifier](https://github.com/microsoft/amplifier) sessions. Captures session events as structured data and builds a property graph in Neo4j.

## How It Works

```
Amplifier CLI sessions
       |
       |  hook-context-intelligence (thin forwarder)
       |  POST /events {event, workspace, data}
       v
+------------------------------------------+        +----------------------+
| Ingestion Server (:8000)                 | bolt   | Neo4j                |
| - Event processing pipeline              |------->| :7687  bolt/driver   |
| - Blob storage (large payloads to disk)  |        | :7474  browser UI    |
| - Dashboard + API docs                   |        | Property graph       |
| - Cypher proxy                           |        | 5 node / 8 edge types|
+------------------------------------------+        +----------------------+
```

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

To retrieve your API key after the first run:

```bash
grep api_key ~/amplifier-context-intelligence-server-data-store/credentials.yaml
```

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
| `/dashboard` | Live session monitoring, event history, log stream |
| `/docs` | Swagger API documentation |

The home page and dashboard both show:
- **Neo4j status** (Connected / Disconnected) polled every few seconds
- **Neo4j Bolt URL** — the exact value of `neo4j_url` from `server-config.yaml`
- **Neo4j Browser URL** — the exact value of `neo4j_browser_url` from `server-config.yaml`, as a clickable link

Both URLs are displayed verbatim from the configuration. If Neo4j is on a remote host, the displayed values reflect that remote address — not `localhost`.

When `api_key` is configured, the dashboard shows an API key prompt on first visit — enter the key from `credentials.yaml`.

---

## First-Run Setup (Standalone)

Before starting the server for the first time outside Docker, run the init command to generate credentials:

```bash
context-intelligence-server init \
  --neo4j-url         bolt://localhost:7687 \
  --neo4j-browser-url http://localhost:7474 \
  --neo4j-user        neo4j
```

You will be prompted for the Neo4j password. The command writes `server-config.yaml` with all required fields including a generated `api_key`. The generated API key is printed to stdout — copy it to your bundle config as `context_intelligence_api_key`.

`--neo4j-url` is the bolt driver URL (used for all graph operations). `--neo4j-browser-url` is the Neo4j Browser HTTP URL (displayed as a clickable link in the web UI). If Neo4j is on a remote host, use that host in both values.

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
  neo4j:5.26.22-community
```

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

# Server bind address
server_host: 127.0.0.1
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
| `context_intelligence_api_key` | *(empty)* | Bearer token for server auth. Must match the server's `api_key`. |
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
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_API_KEY` | `api_key` | *(empty — auth disabled)* | Bearer token. When set, all API endpoints except `/status` and static routes require `Authorization: Bearer <value>`. Generate with `context-intelligence-server init`. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_URL` | `neo4j_url` | `neo4j://neo4j:7687` | Neo4j bolt/driver URL used for all graph operations. **Displayed verbatim in the web UI.** May point to a remote host — `bolt://db.internal:7687` is valid. Use `bolt://` scheme for Community Edition single-node installs. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_BROWSER_URL` | `neo4j_browser_url` | `http://localhost:7474` | Neo4j Browser HTTP UI URL. **Displayed verbatim as a clickable link in the web UI.** Set to the address reachable from your browser — not necessarily `localhost` if Neo4j is on a remote machine. |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_USER` | `neo4j_user` | `neo4j` | Neo4j username |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_PASSWORD` | `neo4j_password` | `password` | Neo4j password |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_BLOB_PATH` | `blob_path` | `/data/blobs` | Blob storage root directory |
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

Graph data and blob data survive container rebuilds and restarts. The ingestion server's in-memory counters (completed sessions, recent events) reset on process restart — the Neo4j graph is the durable record.

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
│   ├── main.py                          # Routes, lifespan, static files
│   ├── config.py                        # Pydantic Settings + YAML source
│   ├── pipeline.py                      # Event dispatch pipeline
│   ├── neo4j_store.py                   # Neo4jGraphStore (buffered writes)
│   ├── blob_store.py                    # AsyncDiskBlobStore
│   ├── handlers/                        # DefaultHandler, SessionHandler, ToolCallHandler + field_lifters/
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
