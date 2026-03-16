# Context Intelligence Server

An event-driven telemetry platform for [Amplifier](https://github.com/microsoft/amplifier) sessions. Captures session events as structured data, builds a property graph in Neo4j, and provides an AI-driven exploration interface.

## How It Works

```
Amplifier CLI sessions
       |
       |  hook-context-intelligence (thin forwarder)
       |  POST /events {event, workspace, data}
       v
+------------------------------------------+        +-------------------+
| Ingestion Server (:8000)                 | bolt   | Neo4j (:7474)     |
| - Event processing pipeline              |------->| Property graph    |
| - Blob storage (large payloads to disk)  |        | 5 node types      |
| - Dashboard + API docs                   |        | 8 edge types      |
| - Cypher proxy                           |        +-------------------+
+------------------------------------------+
       |
       | iframe at /explorer
       v
+------------------------------------------+
| Intelligence Service (:8100)             |
| - Amplifier-powered AI sessions          |
| - WebSocket A2UI protocol                |
| - Graph query + blob reader tools        |
+------------------------------------------+
       |
       | WebSocket /ws (proxied by nginx)
       v
+------------------------------------------+
| Frontend (:3000)                         |
| - Lit 3 SPA with 6 A2UI components      |
| - Cytoscape.js, Plotly, Graphviz         |
| - Embedded at :8000/explorer via iframe  |
+------------------------------------------+
```

---

## Prerequisites

- Docker and Docker Compose
- A GitHub personal access token with repo scope (for private bundle repos)
- At least one LLM provider API key (Anthropic, OpenAI, or Google)

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/colombod/amplifier-context-intelligence.git
cd amplifier-context-intelligence
```

### 2. Configure secrets

Create `config/secrets.env` with your API keys and GitHub token:

```bash
cat > config/secrets.env << 'EOF'
ANTHROPIC_API_KEY=sk-ant-...
OPENAI_API_KEY=sk-...
GOOGLE_API_KEY=AI...
GH_TOKEN=ghp_...
EOF
```

The `GH_TOKEN` is required for the intelligence service to clone private bundle repos during startup. Provider API keys enable the AI-powered exploration features.

### 3. Start the stack

```bash
docker compose up -d
```

This starts 4 services:

| Service | Port | Description |
|---------|------|-------------|
| **Ingestion server** | [localhost:8000](http://localhost:8000) | Event processing, dashboard, API |
| **Intelligence service** | :8100 | Amplifier AI sessions (WebSocket) |
| **Frontend** | :3000 | A2UI SPA (embedded at :8000/explorer) |
| **Neo4j** | [localhost:7474](http://localhost:7474) | Property graph (bolt on :7687, no auth) |

All services are configured with `restart: unless-stopped` -- they automatically restart on crash or Docker daemon restart. They only stay down if you explicitly stop them with `docker compose stop` or `docker compose down`.

The intelligence service has a 180-second startup period on first boot while it downloads and prepares Amplifier modules.

### 4. Access the dashboard

Open [http://localhost:8000](http://localhost:8000) -- this is the single navigation hub for the entire system.

| Route | Content |
|-------|---------|
| `/` | Landing page with navigation cards |
| `/dashboard` | Live session monitoring, event history, log stream |
| `/explorer` | AI-driven graph exploration (iframe embedding the frontend) |
| `/docs` | Swagger API documentation |

Neo4j browser is linked directly from the navigation bar (it cannot be iframed due to its Content-Security-Policy).

---

## Feeding Events into the Server

The server receives events from [amplifier-bundle-context-intelligence](https://github.com/colombod/amplifier-bundle-context-intelligence) -- a thin-forwarder hook that captures every Amplifier session event and dispatches it to the server over HTTP.

### Install the bundle

```bash
amplifier bundle add git+https://github.com/colombod/amplifier-bundle-context-intelligence@main --name context-intelligence
amplifier bundle use context-intelligence
```

### Configure the server URL

Set the environment variable before running Amplifier:

```bash
export AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_URL=http://localhost:8000
```

Or configure it in `~/.amplifier/settings.yaml`:

```yaml
overrides:
  hook-context-intelligence:
    config:
      context_intelligence_server_url: "http://localhost:8000"
```

### How it works

When `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_URL` is set, the hook:

1. Writes every event to local JSONL (always, regardless of server)
2. Fire-and-forgets `POST /events` to the server for each event (5s timeout, failures logged as warnings)
3. Registers `blob_list` and `blob_dump` tools for querying server-stored blobs

The local JSONL is the durable record. The server dispatch is best-effort and never blocks the Amplifier session. If the server is down, the session continues unaffected.

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_URL` | *(empty -- disabled)* | Server URL to forward events to |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_WORKSPACE` | *(auto-resolved)* | Workspace scope for graph data |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_LOG_LEVEL` | `INFO` | Hook log level |

---

## Server Bundle Dependency

The intelligence service loads [amplifier-bundle-context-intelligence-server](https://github.com/colombod/amplifier-bundle-context-intelligence-server) at startup. This bundle provides the AI agent's tools for graph exploration:

| Tool | Description |
|------|-------------|
| `graph_query` | Execute Cypher queries against the Neo4j property graph |
| `blob_reader` | Read event blob data from disk storage |
| `render_surface` | Render interactive A2UI visualizations to the frontend |
| `update_viz` | Incrementally update live visualizations |

The server bundle is pre-baked into the intelligence service Docker image at build time (`COPY amplifier-bundle-context-intelligence-server/` in `Dockerfile.intelligence`). No runtime download is needed for the bundle itself, but its transitive dependencies (the CI bundle, foundation, providers) are resolved on first startup.

---

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/events` | Ingest a session event (returns 202 immediately) |
| `GET` | `/status` | Server health, active sessions, completed history, error counts |
| `GET` | `/` | Landing page with navigation cards |
| `GET` | `/dashboard` | Live monitoring dashboard |
| `GET` | `/explorer` | Exploration UI (iframe) |
| `GET` | `/docs` | Swagger API docs |
| `GET` | `/logs/stream` | Server-Sent Events -- live structured log tail |
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

### Ingestion server

All settings use the `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_` prefix:

| Variable | Default | Description |
|----------|---------|-------------|
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_URL` | `neo4j://neo4j:7687` | Neo4j bolt URI |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_USER` | `neo4j` | Neo4j username |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_PASSWORD` | *(empty)* | Neo4j password |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_BLOB_PATH` | `/data/blobs` | Blob storage root |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_LOG_PATH` | `/data/logs/server.jsonl` | Structured log file |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_LOG_LEVEL` | `INFO` | Log level |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_SERVER_HOST` | `0.0.0.0` | Bind host |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_SERVER_PORT` | `8000` | Bind port |

### Intelligence service

All settings use the `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_` prefix:

| Variable | Default | Description |
|----------|---------|-------------|
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_BUNDLE_PATH` | *(empty)* | Path to bundle.md (empty = stub mode) |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_ROUTING_MATRIX` | `balanced` | Provider routing strategy |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_RUNTIME_STATE_PATH` | `/data/intelligence-runtime` | Runtime state root |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_INGESTION_URL` | `http://context-intelligence-server:8000` | Ingestion server URL |
| `AMPLIFIER_CONTEXT_INTELLIGENCE_WORKSPACE` | `intelligence-runtime` | Session workspace name |

---

## Data Persistence

The stack uses Docker named volumes for all persistent data:

| Volume | Mount | Contains |
|--------|-------|----------|
| `neo4j_data` | `/data` (neo4j) | Graph database |
| `blob_data` | `/data/blobs` (ingestion server, rw) | Event blob JSON files |
| `log_data` | `/data/logs` (ingestion server) | Rotating JSONL log files |
| `intelligence_runtime_state` | `/data/intelligence-runtime` (intelligence service) | Amplifier module cache and runtime state |

Graph data and blob data survive container rebuilds and restarts. The ingestion server's in-memory counters (completed sessions, recent events) reset on process restart -- the Neo4j graph is the durable record.

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
| `Session` + `Root`/`Subsession` | `session:start` | `node_id`, `status`, `started_at` |
| `OrchestratorRun` | `execution:start` | `node_id`, `status`, `run_number` |
| `Step` + `PromptStep`/`AssistantStep` | `prompt:submit`, `provider:request` | `node_id`, `model`, `input_tokens` |
| `ToolExecution` | `tool:pre` | `node_id`, `tool_name`, `status` |
| `Event` + derived label | unclaimed events | `node_id`, `event_type` |

### Edge types

`HAS_RUN` | `HAS_STEP` | `NEXT` | `TRIGGERED` | `PARALLEL_WITH` | `SPAWNED` | `SUBSESSION_OF` | `HAS_EVENT`

### Example queries

```cypher
-- All sessions in a workspace
MATCH (s:Session {workspace: "my-project"})
RETURN s ORDER BY s.started_at DESC

-- Full session graph
MATCH path = (s:Session)-[*1..4]->(n)
WHERE s.node_id CONTAINS "my-session-id"
RETURN path

-- Token usage per run
MATCH (s:Session)-[:HAS_RUN]->(r:OrchestratorRun)-[:HAS_STEP]->(step:Step)
WHERE s.workspace = "my-project"
RETURN r.node_id,
       sum(step.input_tokens) AS input_tokens,
       sum(step.output_tokens) AS output_tokens
```

---

## Development

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv)
- Docker and Docker Compose
- Node.js 22+ (for frontend development)

### Setup

```bash
git clone https://github.com/colombod/amplifier-context-intelligence.git
cd amplifier-context-intelligence
uv venv && source .venv/bin/activate
uv sync
```

### Run tests

```bash
# Ingestion server (612 tests)
.venv/bin/pytest tests/ --ignore=tests/intelligence_service -v

# Intelligence service (101 tests)
.venv/bin/pytest tests/intelligence_service/ -v

# Frontend (70 tests)
cd frontend && npx vitest run

# All Python tests
.venv/bin/pytest tests/ -v
```

### Run locally without Docker

```bash
# Start Neo4j
docker run -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=none neo4j:5.26.22-community

# Start the ingestion server
AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_URL=neo4j://localhost:7687 \
AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_BLOB_PATH=/tmp/ci-blobs \
AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_LOG_PATH=/tmp/ci-logs/server.jsonl \
uvicorn context_intelligence_server.main:app --reload
```

### Project structure

```
amplifier-context-intelligence/
├── context_intelligence_server/         # Ingestion server (FastAPI)
│   ├── main.py                          # Routes, lifespan, static files
│   ├── config.py                        # Pydantic Settings
│   ├── pipeline.py                      # Event dispatch pipeline
│   ├── neo4j_store.py                   # Neo4jGraphStore (buffered writes)
│   ├── blob_store.py                    # AsyncDiskBlobStore
│   ├── handlers/                        # 7 event handlers
│   └── web/                             # Dashboard HTML + static assets
├── intelligence_service/                # Intelligence service (FastAPI + WebSocket)
│   ├── app.py                           # Routes, WebSocket endpoint, lifespan
│   ├── config.py                        # Pydantic Settings
│   ├── amplifier_app.py                 # Bundle lifecycle manager
│   ├── amplifier_session_manager.py     # Session management
│   └── a2ui_bridge.py                   # A2UI wire format codec
├── frontend/                            # Exploration UI (Lit 3 SPA)
│   ├── src/app-shell.ts                 # Main application component
│   ├── src/a2ui-client.ts               # WebSocket client
│   ├── src/a2ui-renderer.ts             # A2UI surface renderer
│   ├── src/catalog/                     # 6 visualization components
│   └── nginx.conf                       # Production proxy config
├── docs/dot/                            # Architecture diagrams (Graphviz DOT)
├── docker-compose.yml                   # 4-service stack
├── Dockerfile                           # Ingestion server image
├── Dockerfile.intelligence              # Intelligence service image
└── Dockerfile.frontend                  # Frontend image (Node -> nginx)
```

---

## Architecture Diagrams

DOT diagrams in `docs/dot/`:

| File | Description |
|------|-------------|
| `system-architecture.dot` | Full 4-service Docker Compose stack |
| `container-initialization.dot` | Intelligence service startup sequence |
| `data-access.dot` | Data access paths (graph query, blob read, event ingestion) |
| `event-pipeline.dot` | POST /events -> handler dispatch -> Neo4j |
| `graph-schema.dot` | Neo4j property graph schema |

---

## Related

- [amplifier-bundle-context-intelligence](https://github.com/colombod/amplifier-bundle-context-intelligence) -- Amplifier bundle that forwards session events to this server
- [amplifier-bundle-context-intelligence-server](https://github.com/colombod/amplifier-bundle-context-intelligence-server) -- Server-side bundle with graph query and visualization tools
- [amplifier](https://github.com/microsoft/amplifier) -- The Amplifier framework
