# Context Intelligence Service Application Layer Design

## Goal

Revise the Intelligence Service from Phase 1 (which assumed CLI-based Amplifier initialization) into a fully bespoke Amplifier application that uses the programmatic API directly — importing `amplifier-foundation` and `amplifier-core` as libraries, managing `PreparedBundle` lifecycle, creating real Amplifier sessions per WebSocket connection, using the routing matrix for model selection, and capturing its own telemetry via `hook-context-intelligence` for the self-improvement loop.

## Background

Phase 1 delivered an `intelligence_service/` Python package with a `StubSessionManager`, `a2ui_bridge`, drain logic, and test infrastructure. The Phase 1 Dockerfile assumed `uv tool install amplifier` and CLI-based initialization. This design replaces the CLI approach with direct library usage, which is simpler, faster, and gives full control over the Amplifier lifecycle.

The amplifier-expert research revealed:

- **Programmatic API:** `load_bundle()` → `compose()` → `prepare()` → `create_session()` → `execute()`
- **`AMPLIFIER_HOME` env var** controls where cache, registry, and session transcripts are stored
- **Hot-reload:** re-load bundle from local path, re-compose, re-prepare, swap `PreparedBundle` singleton
- **Routing matrix** comes through foundation bundle includes; `hooks-routing` module resolves `model_role` → provider+model
- **`PreparedBundle`** is the key singleton: expensive to create (cold start 1-3 min), cheap to create sessions from (~milliseconds)
- **Local bundle paths** fully supported (`file://` URIs, relative paths)

## Approach

Completely bespoke service. **NOT** using `amplifier-app-cli`. The Intelligence Service is a standalone Python application that imports `amplifier-foundation` and `amplifier-core` as libraries. It uses `uv` for dependency management. The routing matrix approach matches what `amplifier-app-cli` does — agents declare `model_role` in their frontmatter and the `hooks-routing` module resolves to the right provider/model.

## Architecture

### Bundle Composition at Startup

```
amplifier-bundle-context-intelligence-server (local path, pre-baked in image)
├── includes: amplifier-foundation (brings routing-matrix, tools, etc.)
├── includes: amplifier-bundle-context-intelligence (the hook!)
│   └── hook-context-intelligence captures ALL events from this service
│       → pushes to the same Neo4j graph
│       → workspace set to "context-intelligence-service" (its own identity)
├── routing overlay composed programmatically:
│   └── hooks-routing config: { default_matrix: "balanced" }
└── runtime overlay composed programmatically:
    └── API keys injected from environment variables
```

### Lifecycle Overview

**Container startup (once):**

```
load_bundle(BUNDLE_PATH)                    → Local path, instant read
→ compose(routing_overlay)                  → Inject matrix selection (balanced default)
→ prepare()                                 → Downloads modules on cold start (1-3 min)
                                            → Uses cache on warm start (5-15s)
→ PreparedBundle singleton held in memory
```

**Per WebSocket connection (cheap, ~milliseconds):**

```
prepared.create_session(
    session_id=conversation_id,
    session_cwd="/data/workspace",
    approval_system=AutoApproveSystem(),     → Headless: auto-approve everything
    display_system=NullDisplaySystem(),      → Or route to WebSocket
)
→ session.execute(user_prompt)              → Full agentic loop
→ Tool results inspected for A2UI payloads
→ A2UI JSON forwarded over WebSocket
```

**Hot-reload (no container restart):**

```
POST /admin/reload-bundle triggers:
→ Re-load bundle from local path (file re-read, instant)
→ Re-compose with routing overlay
→ Re-prepare (downloads any new/changed modules)
→ Swap PreparedBundle singleton
→ New sessions use new config
→ Existing sessions continue unaffected
```

### Self-Telemetry via Workspace Identity

The workspace identity `"context-intelligence-service"` is what makes the self-improvement loop work. All events from this service are tagged with that workspace. The self-improver can query `WHERE workspace = "context-intelligence-service"` to find its own past behavior.

### Routing Matrix Integration

The routing matrix works exactly like `amplifier-app-cli`: the server bundle includes `amplifier-bundle-routing-matrix` via foundation, and we compose a routing overlay that selects the `"balanced"` matrix. Agent definitions in the server bundle declare their `model_role` (e.g., `model_role: coding`) and the `hooks-routing` module resolves that to the right provider and model at session start.

## Components

### Dependency Management (`pyproject.toml`)

The Intelligence Service is a standalone Python package managed with `uv`. Its `pyproject.toml` declares direct dependencies on `amplifier-foundation`, `amplifier-core`, all 7 provider modules, and the modules needed for sessions:

```toml
[project]
name = "context-intelligence-service"
version = "0.1.0"
requires-python = ">=3.11"

dependencies = [
    # Web framework
    "fastapi>=0.115.0",
    "uvicorn[standard]>=0.30.0",
    "websockets>=13.0",
    "pydantic-settings>=2.0.0",

    # Amplifier core (programmatic API)
    "amplifier-core @ git+https://github.com/microsoft/amplifier-core@main",
    "amplifier-foundation @ git+https://github.com/microsoft/amplifier-foundation@main",

    # All 7 providers
    "amplifier-module-provider-anthropic @ git+https://github.com/microsoft/amplifier-module-provider-anthropic@main",
    "amplifier-module-provider-openai @ git+https://github.com/microsoft/amplifier-module-provider-openai@main",
    "amplifier-module-provider-gemini @ git+https://github.com/microsoft/amplifier-module-provider-gemini@main",
    "amplifier-module-provider-azure-openai @ git+https://github.com/microsoft/amplifier-module-provider-azure-openai@main",
    "amplifier-module-provider-github-copilot @ git+https://github.com/microsoft/amplifier-module-provider-github-copilot@main",
    "amplifier-module-provider-ollama @ git+https://github.com/microsoft/amplifier-module-provider-ollama@main",
    "amplifier-module-provider-vllm @ git+https://github.com/microsoft/amplifier-module-provider-vllm@main",

    # Orchestrator + context
    "amplifier-module-loop-basic @ git+https://github.com/microsoft/amplifier-module-loop-basic@main",
    "amplifier-module-context-simple @ git+https://github.com/microsoft/amplifier-module-context-simple@main",
]

[dependency-groups]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.24.0",
    "httpx>=0.27.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

The `uv.lock` file pins exact versions for reproducible builds. The Dockerfile uses `uv sync --frozen` to install from the lock file.

### Dockerfile (`Dockerfile.intelligence`)

The Phase 1 Dockerfile assumed CLI-based initialization. The new design is pure Python, no CLI:

```dockerfile
FROM python:3.13-slim

# Install uv (Amplifier ecosystem standard)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Build tools for amplifier-core's Rust bindings
RUN apt-get update && apt-get install -y --no-install-recommends \
    git build-essential pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy and install intelligence service (deps first for layer caching)
COPY intelligence_service/pyproject.toml intelligence_service/uv.lock \
     /app/intelligence_service/
WORKDIR /app/intelligence_service
RUN uv sync --frozen --no-dev --no-install-project

# Copy service source code
COPY intelligence_service/ /app/intelligence_service/
RUN uv sync --frozen --no-dev

# Pre-bake the server bundle (read-only, local path reference)
COPY amplifier-bundle-context-intelligence-server/ \
     /app/bundles/context-intelligence-server/

# Runtime config
ENV AMPLIFIER_HOME=/data/context-intelligence-service
ENV BUNDLE_PATH=/app/bundles/context-intelligence-server/bundle.md
EXPOSE 8100

HEALTHCHECK --interval=10s --timeout=5s --retries=60 --start-period=180s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8100/health')"

CMD ["uv", "run", "uvicorn", "intelligence_service.app:app", \
     "--host", "0.0.0.0", "--port", "8100"]
```

**Key differences from Phase 1:**

- No `uv tool install amplifier` — no CLI at all
- No entrypoint shell script — pure Python startup via uvicorn
- Dependencies installed via `uv sync` from `pyproject.toml` + `uv.lock`
- Layer caching done right: deps copied and installed before source code
- Bundle pre-baked at `/app/bundles/` (read-only)
- `AMPLIFIER_HOME` points to the volume for runtime data
- Cold start is longer (180s `start_period`) because `prepare()` downloads modules on first run

### Application Startup Lifecycle (`app.py`)

The `app.py` lifespan handler manages the full Amplifier lifecycle programmatically:

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Load bundle from pre-baked local path
    bundle = await load_bundle(os.environ["BUNDLE_PATH"])

    # 2. Compose routing matrix overlay (balanced default)
    routing_overlay = Bundle(
        name="routing-config",
        hooks=[{
            "module": "hooks-routing",
            "config": {
                "default_matrix": os.environ.get("ROUTING_MATRIX", "balanced"),
            }
        }]
    )
    composed = bundle.compose(routing_overlay)

    # 3. Prepare (downloads modules on cold start, uses cache on warm)
    app.state.prepared = await composed.prepare()

    # 4. Configure workspace identity for self-telemetry
    app.state.workspace = "context-intelligence-service"

    # 5. Initialize session manager with the PreparedBundle
    app.state.session_manager = AmplifierSessionManager(
        prepared=app.state.prepared,
        workspace=app.state.workspace,
    )

    try:
        yield
    finally:
        # Drain active sessions on shutdown
        await app.state.drain_manager.start_drain()
        await app.state.session_manager.close_all()
```

### `AmplifierSessionManager` (replaces `StubSessionManager`)

```python
class AmplifierSessionManager:
    """Maps conversation IDs to real Amplifier sessions."""

    async def create_session(self, conversation_id: str) -> str:
        session = await self.prepared.create_session(
            session_id=conversation_id,
            session_cwd="/data/workspace",
            approval_system=AutoApproveSystem(),
            display_system=NullDisplaySystem(),
        )
        self.sessions[conversation_id] = session
        return conversation_id

    async def execute(self, conversation_id: str, prompt: str) -> dict:
        session = self.sessions[conversation_id]
        response = await session.execute(prompt)
        a2ui_messages = extract_a2ui_from_response(response)
        return {"text": response.text, "a2ui": a2ui_messages}

    async def reset_session(self, conversation_id: str) -> str:
        await self.sessions[conversation_id].close()
        del self.sessions[conversation_id]
        new_id = str(uuid4())
        await self.create_session(new_id)
        return new_id
```

### Hot-Reload Endpoint

```python
@app.post("/admin/reload-bundle")
async def reload_bundle():
    bundle = await load_bundle(os.environ["BUNDLE_PATH"])
    composed = bundle.compose(build_routing_overlay())
    new_prepared = await composed.prepare()
    app.state.prepared = new_prepared
    app.state.session_manager.prepared = new_prepared
    return {"status": "reloaded"}
```

## Data Flow

### Docker Compose (4 Services)

```yaml
services:
  # Existing: event ingestion + graph writes + operational dashboard
  context-intelligence-server:
    build: .
    ports: ["8000:8000"]
    depends_on:
      neo4j: { condition: service_healthy }
    volumes:
      - blob_data:/data/blobs
      - log_data:/data/logs
    environment:
      CONTEXT_INTELLIGENCE_NEO4J_URL: neo4j://neo4j:7687
      CONTEXT_INTELLIGENCE_BLOB_PATH: /data/blobs
      PYTHONUNBUFFERED: "1"
    networks: [context-intelligence]
    labels:
      com.context-intelligence.component: server

  # NEW: AI-powered graph exploration (bespoke Amplifier application)
  intelligence-service:
    build:
      context: .
      dockerfile: Dockerfile.intelligence
    ports: ["8100:8100"]
    depends_on:
      context-intelligence-server: { condition: service_healthy }
    volumes:
      # Amplifier runtime data (cache, registry, session transcripts)
      - context_intelligence_service_data:/data/context-intelligence-service
      # Blob access (shared with ingestion server, read-only)
      - blob_data:/data/blobs:ro
    env_file: config/secrets.env
    environment:
      AMPLIFIER_HOME: /data/context-intelligence-service
      BUNDLE_PATH: /app/bundles/context-intelligence-server/bundle.md
      ROUTING_MATRIX: balanced
      INTEL_SERVICE_INGESTION_URL: http://context-intelligence-server:8000
      PYTHONUNBUFFERED: "1"
    networks: [context-intelligence]
    labels:
      com.context-intelligence.component: intelligence
    healthcheck:
      test: ["CMD", "python", "-c",
        "import urllib.request; urllib.request.urlopen('http://localhost:8100/health')"]
      interval: 10s
      timeout: 5s
      retries: 60
      start_period: 180s

  # NEW: A2UI web frontend
  frontend:
    build:
      context: .
      dockerfile: Dockerfile.frontend
    ports: ["3000:80"]
    depends_on:
      intelligence-service: { condition: service_healthy }
    networks: [context-intelligence]
    labels:
      com.context-intelligence.component: frontend

  # Graph database
  neo4j:
    image: neo4j:5.26.22-community
    ports: ["7474:7474", "7687:7687"]
    environment:
      NEO4J_AUTH: none
    volumes:
      - neo4j_data:/data
    networks: [context-intelligence]
    labels:
      com.context-intelligence.component: neo4j
    healthcheck:
      test: ["CMD", "wget", "-O", "-", "http://localhost:7474"]
      interval: 10s
      timeout: 5s
      retries: 30

volumes:
  blob_data:
  neo4j_data:
  log_data:
  context_intelligence_service_data:

networks:
  context-intelligence:
    driver: bridge
```

### Volume Strategy

| Volume | Purpose | Mounted In |
|--------|---------|------------|
| `context_intelligence_service_data` | All Amplifier runtime data (cache, registry, session transcripts) | `intelligence-service` at `/data/context-intelligence-service` |
| `blob_data` | Event blob storage | `context-intelligence-server` (rw), `intelligence-service` (ro) |
| `neo4j_data` | Graph database persistence | `neo4j` |
| `log_data` | Server logs | `context-intelligence-server` |

### Environment Variables

| Variable | Service | Purpose |
|----------|---------|---------|
| `AMPLIFIER_HOME` | intelligence-service | Where Amplifier stores cache, registry, session transcripts |
| `BUNDLE_PATH` | intelligence-service | Path to the pre-baked server bundle inside the image |
| `ROUTING_MATRIX` | intelligence-service | Which routing matrix to use (default: `balanced`) |
| `INTEL_SERVICE_INGESTION_URL` | intelligence-service | URL to reach the ingestion server's `/cypher` endpoint |
| `CONTEXT_INTELLIGENCE_NEO4J_URL` | context-intelligence-server | Neo4j bolt connection URL |
| `CONTEXT_INTELLIGENCE_BLOB_PATH` | context-intelligence-server | Where to store event blobs |

## Error Handling

- **Cold start timeout:** 180s `start_period` in the healthcheck accommodates `prepare()` downloading modules on first run. Warm starts (cache hit) take 5-15s.
- **Graceful shutdown:** `drain.py` logic from Phase 1 is unchanged. The lifespan `finally` block drains active sessions before closing.
- **Hot-reload failure:** If `reload_bundle()` fails at any step, the old `PreparedBundle` remains in place. New sessions continue using the previous configuration.
- **Session errors:** Individual session failures don't affect other sessions or the `PreparedBundle` singleton.

## Testing Strategy

Phase 1's test infrastructure is extended, not replaced:

- **Unit tests:** `AmplifierSessionManager` with mocked `PreparedBundle` — verify session creation, execution, reset, and A2UI extraction
- **Lifespan tests:** Mock `load_bundle()` / `compose()` / `prepare()` chain — verify startup and shutdown sequences
- **Hot-reload tests:** Verify `PreparedBundle` singleton swap, verify new sessions use new config, verify existing sessions unaffected
- **Integration tests:** Full Docker Compose stack — verify end-to-end WebSocket flow with real Amplifier sessions
- **Config tests:** Verify `ROUTING_MATRIX`, `AMPLIFIER_HOME`, `BUNDLE_PATH` env vars are correctly wired

## Phase 1 Revision Summary

Phase 1 was implemented with CLI-based assumptions. This design changes the foundation:

| Phase 1 Assumed | This Design Replaces With |
|-----------------|---------------------------|
| `uv tool install amplifier` in Dockerfile | `uv sync` from `pyproject.toml` with direct library deps |
| Shell entrypoint script (config overlay, CLI commands) | Pure Python startup via uvicorn, lifespan handler |
| `StubSessionManager` (placeholder) | `AmplifierSessionManager` using `PreparedBundle.create_session()` |
| `AMPLIFIER_HOME` not considered | `/data/context-intelligence-service/` volume with `AMPLIFIER_HOME` env var |
| No context-intelligence hook | Bundle includes `amplifier-bundle-context-intelligence` for self-telemetry |
| No routing matrix | Routing matrix composed at startup, `balanced` default |
| `a2ui_bridge` only handled message framing | `a2ui_bridge` also extracts A2UI payloads from tool results |
| Docker Compose had no `context_intelligence_service_data` volume | Single volume for all Amplifier runtime data |

**What stays from Phase 1:**

- `config.py` (Pydantic settings with `INTEL_SERVICE_` prefix) — add new fields
- `drain.py` — unchanged, graceful shutdown logic is correct
- `app.py` structure (FastAPI, lifespan, flat endpoints) — revise internals
- Test patterns and test infrastructure — extend, don't replace
- `a2ui_bridge.py` message parsing — extend with A2UI extraction

The implementation plan for this phase will be a revision of Phase 1's `intelligence_service/` code, not a rewrite. The architecture (thin bridge) is correct; the internals change from stubs to real Amplifier integration.

## Open Questions

- The exact API surface of `amplifier-foundation`'s `load_bundle`, `Bundle.compose`, and `PreparedBundle.create_session` may vary from what the amplifier-expert described — the implementation plan should validate against the actual source code
- Whether `hook-context-intelligence` needs explicit workspace configuration passed at session creation or if it reads it from the session context automatically
- Whether `prepare()` can be called with a custom `AMPLIFIER_HOME` or if it always uses the env var
- The exact format of tool results that contain A2UI payloads — this depends on how `render_surface` and `update_viz` tools are implemented (deferred to agent design phase)

## References

- Amplifier `APPLICATION_INTEGRATION_GUIDE.md` — canonical 7-step headless session lifecycle
- `amplifier-foundation` `BundleRegistry` source (`registry.py`) — cache lifecycle, load/save semantics
- `amplifier-bundle-routing-matrix` — `hooks-routing` module, matrix YAML schema, `model_role` resolution
- David Koleczek's self-improvement repo — container patterns (though we diverge from CLI-based approach)
- Phase 1 implementation — `intelligence_service/` package structure, test patterns, drain logic
