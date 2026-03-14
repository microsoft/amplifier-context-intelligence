# Context Intelligence Server

A standalone server that receives session events from [Amplifier](https://github.com/microsoft/amplifier) instances, stores them as a property graph in Neo4j, manages blob storage, and exposes a real-time dashboard with live log streaming.

Replaces the in-process `GraphDataHook` in [`amplifier-bundle-context-intelligence`](https://github.com/colombod/amplifier-bundle-context-intelligence) — all graph processing, blob management, and session state moves server-side.

---

## Quick Start

```bash
# Start the full stack (Neo4j + server)
docker compose up

# Server:      http://localhost:8000
# Dashboard:   http://localhost:8000/
# Neo4j browser: http://localhost:7474  (bolt://localhost:7687, no auth)
```

---

## What It Does

When an Amplifier session runs, the bundle's `LoggingHandler` sends each event to this server over HTTP. The server:

1. **Ingests events** — `POST /events` enqueues each event to a per-session async worker (returns 202 immediately, never blocks the Amplifier session)
2. **Processes the graph** — workers run the full handler pipeline (SessionHandler, OrchestratorRunHandler, StepHandler, ToolExecutionHandler, RecipeHandler, DefaultHandler) and write to Neo4j
3. **Manages blobs** — large event fields (`raw`, `result`, `messages`, `mount_plan`, etc.) are offloaded to disk and served over HTTP
4. **Cleans up** — workers self-terminate after `session:end`, closing the Neo4j driver and recording a completion summary
5. **Streams logs** — all operational logs are written to a rotating JSONL file and streamed live over SSE

---

## API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/events` | Ingest a session event — returns 202 immediately |
| `GET` | `/status` | Server health, active sessions, completed session history, error counts |
| `GET` | `/` | Live dashboard — session stats, completed sessions with Neo4j expand, SSE log viewer |
| `GET` | `/logs/stream` | Server-Sent Events — live structured log tail (200-line backfill on connect) |
| `GET` | `/blobs/{session_id}/{key}` | Retrieve a stored blob by key |
| `GET` | `/blobs/{session_id}` | List all blob URIs for a session |
| `POST` | `/cypher` | Proxy a Cypher query to Neo4j — scoped by `workspace` |

### Event Payload

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

- `workspace` — groups multiple sessions under one scope (maps to the `workspace` property on all Neo4j nodes and edges)
- `session_id` is extracted from `data` server-side

### Cypher Proxy

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

All settings use the `CI_SERVER_` environment variable prefix:

| Variable | Default | Description |
|----------|---------|-------------|
| `CI_SERVER_NEO4J_URL` | `neo4j://neo4j:7687` | Neo4j bolt URI |
| `CI_SERVER_NEO4J_USER` | `neo4j` | Neo4j username (empty = no auth) |
| `CI_SERVER_NEO4J_PASSWORD` | *(empty)* | Neo4j password (empty = no auth) |
| `CI_SERVER_BLOB_PATH` | `/data/blobs` | Blob storage root |
| `CI_SERVER_LOG_PATH` | `/data/logs/server.jsonl` | Structured log file |
| `CI_SERVER_LOG_LEVEL` | `INFO` | Log level |
| `CI_SERVER_SERVER_HOST` | `0.0.0.0` | Bind host |
| `CI_SERVER_SERVER_PORT` | `8000` | Bind port |

---

## Docker Compose

The stack runs two services:

```
context-intelligence-server  — FastAPI app on port 8000
neo4j                         — Neo4j 5.x, browser UI on port 7474
```

Three named volumes provide persistence:

| Volume | Mount | Contains |
|--------|-------|----------|
| `blob_data` | `/data/blobs` | Blob JSON files (`ci-blob://` URIs) |
| `neo4j_data` | `/data` (neo4j) | Graph database |
| `log_data` | `/data/logs` | Rotating JSONL log files (10MB × 5) |

Neo4j bolt (7687) is exposed to the host for direct client connections and the Neo4j browser UI. All programmatic Cypher access from code should go through `POST /cypher`.

---

## Bundle Integration

In [`amplifier-bundle-context-intelligence`](https://github.com/colombod/amplifier-bundle-context-intelligence), configure the `LoggingHandler` with the server URL and workspace:

```yaml
hooks:
  - module: hook-context-intelligence
    config:
      server_url: "http://localhost:8000"
      workspace: "my-project"
```

When `server_url` is set, the `LoggingHandler` fires `POST /events` in the background for every Amplifier event, in parallel with writing the local `events.jsonl`. If the server is unreachable, a warning is logged and the session continues unaffected.

The `BlobTool` (`blob_list`, `blob_dump`) automatically uses the server's HTTP endpoints when `server_url` is configured.

> **Note:** Bundle changes are on the `feat/server-dispatch` branch of `amplifier-bundle-context-intelligence`. Not yet merged to `main`.

---

## Neo4j Graph Model

All nodes and edges carry a `workspace` property for multi-workspace isolation.

### Node Types

| Label | Created by | Key properties |
|-------|-----------|----------------|
| `Session` + `Root`/`Subsession`/`ForkedSession` | `session:start` | `node_id`, `status`, `started_at` |
| `OrchestratorRun` | `execution:start` | `node_id`, `status`, `run_number` |
| `Step` + `PromptStep`/`AssistantStep` | `prompt:submit`, `provider:request` | `node_id`, `model`, `input_tokens`, `output_tokens` |
| `ToolExecution` | `tool:pre` | `node_id`, `tool_name`, `status` |
| `Event` + derived label | unclaimed events | `node_id`, `event_type` |

### Edge Types

`HAS_RUN` · `HAS_STEP` · `NEXT` · `TRIGGERED` · `PARALLEL_WITH` · `SPAWNED` · `SUBSESSION_OF` · `HAS_EVENT`

### Example Queries

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
- [uv](https://github.com/astral-sh/uv) (recommended) or pip
- Docker + Docker Compose

### Setup

```bash
git clone https://github.com/colombod/amplifier-context-intelligence.git
cd amplifier-context-intelligence
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
```

### Run Tests

```bash
pytest tests/ -v          # full suite (~530 tests, no Neo4j required)
pytest tests/ -q          # quiet mode
pytest tests/integration/ # integration tests only
```

### Run Locally Without Docker

```bash
# Start Neo4j
docker run -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=none neo4j:5

# Start the server
CI_SERVER_NEO4J_URL=neo4j://localhost:7687 \
CI_SERVER_BLOB_PATH=/tmp/ci-blobs \
CI_SERVER_LOG_PATH=/tmp/ci-logs/server.jsonl \
uvicorn context_intelligence_server.main:app --reload
```

### Project Structure

```
context_intelligence_server/
├── main.py              # FastAPI app, routes, dashboard HTML
├── config.py            # Pydantic Settings (CI_SERVER_* env vars)
├── logging_config.py    # Structured logging (stdout + RotatingFileHandler)
├── registry.py          # SessionRegistry, SessionWorker, CompletedSession
├── pipeline.py          # Event dispatch pipeline
├── dashboard.py         # Status response builder, ring buffers
├── services.py          # HookStateService, SessionCursors, GraphState
├── neo4j_store.py       # Neo4jGraphStore — buffered writes, UNWIND batch flush
├── blob_store.py        # AsyncDiskBlobStore — ci-blob:// URI scheme
├── blob_processor.py    # In-place blob field offloading
├── graph_store.py       # GraphStore protocol
├── protocol.py          # EventHandler protocol, HookResult
├── utils.py             # make_node_id, make_edge_id, logging helpers
└── handlers/
    ├── session.py           # session:start/fork/end
    ├── orchestrator_run.py  # prompt:submit, execution:start/end, orchestrator:complete
    ├── step.py              # provider:request, llm:request/response, content_block:*
    ├── tool_execution.py    # tool:pre/post/error, delegate:*
    ├── recipe.py            # recipe:*
    ├── event.py             # context:compaction, cancel:* (no-op claim)
    └── default.py           # all unclaimed events
```

---

## Architecture

```
Amplifier instances (multiple concurrent)
       │
       │  POST /events {event, workspace, data}
       ▼
┌──────────────────────────────────────────┐
│       context-intelligence-server        │
│                                          │
│  SessionRegistry                         │
│  ├── SessionWorker (per session)         │
│  │   ├── asyncio.Queue                   │
│  │   ├── drain coroutine (self-termin.)  │
│  │   │   ├── ensure_session_node         │
│  │   │   ├── blob_processor (in-place)  │
│  │   │   └── handler dispatch           │
│  │   └── Neo4jGraphStore (per session)  │
│  └── CompletedSession ring (last 100)   │
│                                          │
│  GET /logs/stream ──► SSE log tail       │
│  POST /cypher ──────► Neo4j proxy        │
│  GET /blobs/... ────► volume read        │
│  GET / ─────────────► dashboard          │
└──────────────┬───────────────────────────┘
               │ bolt (internal network)
               ▼
          Neo4j :7687
     (persisted graph data)
```

---

## Design Documents

Full design and implementation plans are in [`docs/plans/`](docs/plans/):

- [`2026-03-13-context-intelligence-server-design.md`](docs/plans/2026-03-13-context-intelligence-server-design.md) — initial server architecture
- [`2026-03-14-cleanup-dashboard-logging-design.md`](docs/plans/2026-03-14-cleanup-dashboard-logging-design.md) — worker cleanup, log persistence, dashboard enhancement

---

## Related

- [`amplifier-bundle-context-intelligence`](https://github.com/colombod/amplifier-bundle-context-intelligence) — the Amplifier bundle that sends events to this server
- [`amplifier`](https://github.com/microsoft/amplifier) — the Amplifier framework
