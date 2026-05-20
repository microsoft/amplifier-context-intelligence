# Context Intelligence Server

Event-driven telemetry platform for [Amplifier](https://github.com/microsoft/amplifier) sessions.
Captures session events as structured data and builds a property graph in Neo4j.

## Quick Overview

```
Amplifier CLI → hook (POST /events) → Ingestion Server (:8000) → Neo4j graph + blob storage
```

See [README.md](README.md) for full setup instructions.

---

## Project Structure

```
context_intelligence_server/      # FastAPI ingestion server
├── main.py                       # App factory, routes, lifespan
├── config.py                     # Settings (YAML + env vars via Pydantic)
├── pipeline.py                   # Event dispatch pipeline
├── neo4j_store.py                # Buffered Neo4j writes
├── blob_store.py                 # Async disk blob storage
├── handlers/                     # Event handlers (data_layer_1/2/3)
│   ├── data_layer_1/             # Session/tool-call handlers
│   ├── data_layer_2/             # Graph enrichment handlers
│   └── data_layer_3/             # High-level insight handlers
├── auth.py                       # API key authentication
├── dashboard.py                  # Dashboard SSE stream
├── models.py                     # Pydantic request/response models
└── web/                          # Dashboard HTML + static assets

docs/
├── architecture/                 # DOT diagrams: pipeline, handlers, graph model
└── service-setup.md              # Running as a system service

tests/
├── handlers/                     # Handler unit tests
├── integration/                  # Pipeline integration tests
└── neo4j/                        # Tests requiring a live Neo4j instance
```

---

## Running Tests

Requires Python 3.11+ and [uv](https://github.com/astral-sh/uv).

```bash
uv sync
uv run pytest tests/ -q                    # All tests (no Neo4j required)
uv run pytest tests/neo4j/ -q              # Neo4j tests (requires running instance)
```

Most tests run against in-memory fakes. The `tests/neo4j/` suite requires a live Neo4j 5.x instance — see `tests/neo4j/conftest.py` for connection details.

---

## Running the Server Locally

```bash
# 1. Start Neo4j (Docker, easiest)
docker run -d --name neo4j-ci \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=none \
  neo4j:5.26.22-community

# 2. Configure and start the server
cp server-config.example.yaml server-config.yaml
# Edit server-config.yaml with your Neo4j connection details

# 3. Start
uvicorn context_intelligence_server.main:app --reload
```

Or use Docker Compose to run everything together:

```bash
./start.sh
```

---

## Key Concepts

- **Event pipeline** — `POST /events` queues work; handlers process events asynchronously. Each handler is a Python class in `handlers/data_layer_*/`.
- **Graph model** — 5 node types, 8 edge types. See `docs/architecture/03-graph-model.dot` for the diagram and `docs/architecture/README.md` for the legend.
- **Blob storage** — Large event payloads are written to disk and referenced by URI to avoid graph bloat.
- **Configuration** — Pydantic Settings reads from `server-config.yaml` first, then environment variables. See `config.py`.

---

## Making Changes

- **New handler**: Add a class to the appropriate `handlers/data_layer_*/` directory, register it in `handlers/__init__.py`.
- **New API endpoint**: Add a route to `main.py` or a new router under `routers/`.
- **Configuration**: Add fields to `ServerConfig` in `config.py`. Keep defaults conservative.
- **Tests**: Every handler should have a unit test in `tests/handlers/`. Integration tests live in `tests/integration/`.

Run `uv run pytest tests/ -q` to verify before committing.
