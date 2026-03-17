# Context Intelligence Server

An event-driven telemetry platform for [Amplifier](https://github.com/microsoft/amplifier) sessions. Captures session events as structured data and builds a property graph in Neo4j.

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
```

---

## Prerequisites

- Docker and Docker Compose

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/colombod/amplifier-context-intelligence.git
cd amplifier-context-intelligence
```

### 2. Start the stack

```bash
docker compose up -d
```

This starts 2 services:

| Service | Port | Description |
|---------|------|-------------|
| **Ingestion server** | [localhost:8000](http://localhost:8000) | Event processing, dashboard, API |
| **Neo4j** | [localhost:7474](http://localhost:7474) | Property graph (bolt on :7687, no auth) |

All services are configured with `restart: unless-stopped` -- they automatically restart on crash or Docker daemon restart. They only stay down if you explicitly stop them with `docker compose stop` or `docker compose down`.

### 3. Access the dashboard

Open [http://localhost:8000](http://localhost:8000) -- this is the single navigation hub for the system.

| Route | Content |
|-------|---------|
| `/` | Landing page with navigation cards |
| `/dashboard` | Live session monitoring, event history, log stream |
| `/docs` | Swagger API documentation |

Neo4j browser is linked directly from the navigation bar (it cannot be iframed due to its Content-Security-Policy).

---

## Feeding Events into the Server

The server receives events from [amplifier-bundle-context-intelligence](https://github.com/colombod/amplifier-bundle-context-intelligence) -- a thin-forwarder hook that captures every Amplifier session event and dispatches it to the server over HTTP.

### Install the bundle

```bash
amplifier bundle add git+https://github.com/colombod/amplifier-bundle-context-intelligence@main --name context-intelligence --app
```

The `--app` flag makes the bundle always active across all sessions -- no need to run `amplifier bundle use`.

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
| `context_intelligence_server_url` | *(empty -- disabled)* | Server URL to forward events to |
| `workspace` | *(auto-resolved)* | Workspace scope for graph data |

---

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/events` | Ingest a session event (returns 202 immediately) |
| `GET` | `/status` | Server health, active sessions, completed history, error counts |
| `GET` | `/` | Landing page with navigation cards |
| `GET` | `/dashboard` | Live monitoring dashboard |
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

---

## Data Persistence

The stack uses Docker named volumes for all persistent data:

| Volume | Mount | Contains |
|--------|-------|----------|
| `neo4j_data` | `/data` (neo4j) | Graph database |
| `blob_data` | `/data/blobs` (ingestion server, rw) | Event blob JSON files |
| `log_data` | `/data/logs` (ingestion server) | Rotating JSONL log files |

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

### Setup

```bash
git clone https://github.com/colombod/amplifier-context-intelligence.git
cd amplifier-context-intelligence
uv venv && source .venv/bin/activate
uv sync
```

### Run tests

```bash
uv run pytest tests/ -q
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
├── docker-compose.yml                   # 2-service stack
└── Dockerfile                           # Ingestion server image
```

---

## Related

- [amplifier-bundle-context-intelligence](https://github.com/colombod/amplifier-bundle-context-intelligence) -- Amplifier bundle that forwards session events to this server
- [amplifier](https://github.com/microsoft/amplifier) -- The Amplifier framework
