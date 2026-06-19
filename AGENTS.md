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
├── queue_manager.py              # Durable per-session append-log (persist-then-202)
├── registry.py                   # Per-session drainers (drain_worker, write semaphore, retry/dead-letter)
├── pipeline.py                   # Per-event dispatch spine (invoked by the drainer)
├── neo4j_store.py                # Managed-transaction Neo4j writes
├── blob_store.py                 # Async disk blob storage
├── handlers/                     # Event handlers (data_layer_1/2/3)
│   ├── data_layer_1/             # Session/tool-call handlers
│   ├── data_layer_2/             # Graph enrichment handlers
│   └── data_layer_3/             # High-level insight handlers
├── routers/                      # API routers (queues.py = dead-letter inspect/replay/purge)
├── auth.py                       # API key authentication
├── dashboard.py                  # Dashboard SSE stream
├── models.py                     # Pydantic request/response models
└── web/                          # Dashboard HTML + static assets

docs/                             # ⚠️ PRODUCT DOCUMENTATION ONLY
├── architecture/                 # DOT diagrams: pipeline, handlers, graph model
└── service-setup.md              # Running as a system service

tests/
├── handlers/                     # Handler unit tests
├── integration/                  # Pipeline integration tests
└── neo4j/                        # Tests requiring a live Neo4j instance
```

**`docs/` is product documentation only.** Architecture diagrams and operational guides
that ship with the server. Plans, fix designs, and Superpowers-generated documents go in
the **workspace root `docs/`** (one level up from this repo), never here.

---

## Running Tests

Requires Python 3.11+ and [uv](https://github.com/astral-sh/uv).

```bash
uv sync
uv run pytest tests/ -q                    # All tests (no Neo4j required)
uv run pytest tests/neo4j/ -q              # Neo4j tests (requires running instance)
```

Most tests run against in-memory fakes. The `tests/neo4j/` suite requires a live Neo4j instance (the calendar `2026.x` line that this project tracks, or `5.26` LTS) — see `tests/neo4j/conftest.py` for connection details.

---

## Running the Server Locally

```bash
# 1. Start Neo4j (Docker, easiest) — NEO4J_PLUGINS enables APOC (see "Neo4j / APOC setup" below)
docker run -d --name neo4j-ci \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=none \
  -e 'NEO4J_PLUGINS=["apoc"]' \
  neo4j:2026.05.0-community

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

## Key Conventions

### Neo4j / APOC setup

Neo4j runs with the **APOC** plugin enabled. Whenever you provision or document a
Neo4j instance for this server — Docker Compose, a standalone `docker run`, a
systemd-managed container, or a test fixture — APOC must be turned on the same way:

```yaml
# docker-compose.yml — neo4j service
environment:
  NEO4J_PLUGINS: '["apoc"]'
```

```bash
# standalone docker run
-e 'NEO4J_PLUGINS=["apoc"]'
```

Neo4j auto-installs the bundled `apoc-core` jar from `/var/lib/neo4j/labs`
into `/var/lib/neo4j/plugins` at every startup and applies APOC's default config
(including `dbms.security.procedures.unrestricted=apoc.*`). **No volume mount and
no manual jar download are required** — the jar lives on the image layer and
re-installs on each container start. The image bundles APOC Core; APOC Extended
is neither included nor needed. **APOC tracks the Neo4j version automatically**
(the bundled jar matches the image), so there is no separate APOC version to
manage. Hosted **AuraDB** already has APOC Core preinstalled.

This project pins the **latest Neo4j Community release** — currently
`2026.05.0-community` (the calendar line, what `neo4j:latest` resolves to), not
the moving `latest` tag. Do not confuse it with the `5.26` LTS line; `2026.05.0`
is newer. To bump, change the tag in `docker-compose.yml`, `neo4j.Dockerfile`,
`docker-compose.airgap.yml`, `docs/service-setup.md`, and `test/airgap/` — APOC
follows automatically.

Verify: `cypher-shell -u neo4j -p <pw> "RETURN apoc.version();"` → matches the
Neo4j version (e.g. `2026.05.0`). Canonical setup docs: `README.md`
("Neo4j Plugins (APOC)" / "Neo4j version policy") and `docs/service-setup.md` (Step 2).

**Air-gapped / offline provisioning.** When provisioning Neo4j in an environment
with no internet egress, do NOT rely on a download. Two facts:

1. `NEO4J_PLUGINS=["apoc"]` is already offline-safe for APOC **Core** — Neo4j
   copies the jar from the in-image `/var/lib/neo4j/labs/` dir, never the
   network. (Confirmed: the install happens even with networking disabled.)
2. For an air-tight guarantee that skips the installer entirely, use the baked
   image: **`neo4j.Dockerfile`** (copies the bundled APOC Core jar into
   `/var/lib/neo4j/plugins/` at build time + sets `unrestricted=apoc.*`) via the
   **`docker-compose.airgap.yml`** override:
   `docker compose -f docker-compose.yml -f docker-compose.airgap.yml up -d --build`.

On a fully disconnected host, also pre-load the **base image** itself
(`docker save neo4j:2026.05.0-community` → `docker load`, or an internal registry
mirror) — you cannot `docker pull` it either. Do NOT set
`dbms.security.procedures.allowlist=apoc.*`; that blocks built-in `db.*`/`dbms.*`
procedures. Only `unrestricted=apoc.*` is needed. This path was validated in an
isolated environment with the Neo4j container cut off from the internet.

### Temporal properties are `ZONED DATETIME`

All temporal properties are stored as native Neo4j ZONED DATETIME, not ISO strings.
The single source of truth is the `TEMPORAL_PROPS` frozenset in `neo4j_store.py`.
Adding a temporal property to any handler requires adding its name to `TEMPORAL_PROPS` — forget
and the value lands as a plain string silently; no error, no warning; temporal predicates
`WHERE`/`duration.between` then behave inconsistently.

`last_updated` (on Session) is the only temporal field not ending in `_at` and is listed
deliberately. Do not replace the registry with a `*_at` suffix heuristic — it silently misses
`last_updated`. Edge `occurred_at` on `HAS_EVENT`, `HAS_SUBSESSION`, `FORKED` is covered by the
same registry and conversion path; no special-casing needed.

### `neo4j_store.py` is the type boundary

`neo4j.time` driver types (`DateTime`, etc.) must never leave `neo4j_store.py`.
`_convert_temporal_props` converts Python `datetime` → ZONED DATETIME on write.
`_normalize_temporal` converts `neo4j.time.DateTime` → Python `datetime` on read.
`services.py`, `pipeline.py`, and handlers deal in Python stdlib types only and never import
or reference `neo4j.time`.

---

## Key Concepts

- **Event pipeline** — `POST /events` persists the raw event to a durable per-session append-log (`queue_manager.py`) and returns `202` immediately (persist-then-202). A single drainer per session (`registry.py`) processes batches and flushes them to Neo4j under a global write semaphore, with transient/deadlock retry, dead-letter isolation of poison events, and crash recovery (replay + counter re-seed) on startup. Each handler invoked by the per-event dispatch spine is a Python class in `handlers/data_layer_*/`.
- **Graph model** — session sub-labels: `RootSession`, `SubSession`, `ForkedSession`, `IncompleteSession` (health marker; not a terminal). Full schema with all node and edge types: see `docs/architecture/03-graph-model.dot` and `docs/architecture/README.md`.
- **Blob storage** — Large event payloads are written to disk and referenced by URI to avoid graph bloat.
- **Configuration** — Pydantic Settings reads from `server-config.yaml` first, then environment variables. See `config.py`.

---

## Making Changes

- **New handler**: Add a class to the appropriate `handlers/data_layer_*/` directory, register it in `handlers/__init__.py`.
- **New API endpoint**: Add a route to `main.py` or a new router under `routers/`.
- **Configuration**: Add fields to `ServerConfig` in `config.py`. Keep defaults conservative.
- **Tests**: Every handler should have a unit test in `tests/handlers/`. Integration tests live in `tests/integration/`.

Run `uv run pytest tests/ -q` to verify before committing.
