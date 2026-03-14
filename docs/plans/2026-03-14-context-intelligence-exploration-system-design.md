# Context Intelligence Exploration System Design

## Goal

Add an Amplifier-based agentic system to the context-intelligence platform that allows users to ask natural language questions about the telemetry graph and receive rich, AI-driven visual responses using Google's A2UI protocol — where the AI agent controls what the user sees, choosing between graph visualizations, charts, timelines, tables, and metrics based on query results. The system is also self-improving: it captures its own telemetry, inspects its own sessions, and files improvement opportunities as GitHub Issues against its own repo.

## Background

The context-intelligence platform currently ingests Amplifier telemetry events into a Neo4j graph and provides an operational dashboard for monitoring pipeline health and browsing sessions. What it lacks is an intelligent exploration layer — users cannot ask questions like "show me all delegations that failed in the last hour" or "what's the most common tool call pattern across sessions today" and get meaningful visual answers.

The ingestion server's operational dashboard (landing page at `/`, dashboard at `/dashboard`) handles monitoring well: live stat chips, active/completed session lists, SSE log streaming, glassmorphic design with OKLCH color tokens. But monitoring is not exploration. Exploration requires an AI agent that understands the graph schema, can compose Cypher queries, select appropriate visualizations, and progressively disclose detail as the user drills in.

## Approach

**Agent-native A2UI (Approach C):** The Amplifier agent itself emits A2UI messages directly as tool calls (`render_surface`, `update_viz`). The service layer is a thin WebSocket-to-Amplifier bridge. Maximum intelligence in the bundle, minimum in the service. All interesting logic — visualization selection, UI composition, user interaction handling — lives in the bundle where it benefits from LLM reasoning.

This approach was chosen over alternatives (server-side rendering, hybrid approaches) because it keeps the bridge stateless and dumb, makes the intelligence layer fully portable as a bundle, and allows the LLM to reason about what visualization best answers the user's question rather than hard-coding mapping rules.

## Architecture

### System Topology

The system expands from a 2-service stack to a 4-service Docker Compose deployment:

```
┌────────────────────────┐  ┌─────────────────────────────┐
│  Ingestion Server      │  │  Intelligence Service        │
│  Port 8000             │  │  Port 8100                   │
│                        │  │                              │
│  Serves its own UI:    │  │  WebSocket bridge only       │
│  GET /         (landing)│  │  WS /ws  (A2UI sessions)    │
│  GET /dashboard        │  │  GET /health                 │
│  GET /status   (JSON)  │  │  GET /admin/reload-bundle    │
│  GET /logs/stream (SSE)│  │                              │
│  POST /cypher          │  │  No UI served from here      │
│  POST /events          │  │                              │
└────────────────────────┘  └──────────────────────────────┘

┌────────────────────────┐  ┌─────────────────────────────┐
│  Frontend (nginx)      │  │  Neo4j                       │
│  Port 3000             │  │  Bolt 7687 / Browser 7474    │
│                        │  │                              │
│  A2UI SPA:             │  │  Graph database              │
│  Lit renderer          │  │  Accessed by ingestion       │
│  Custom viz catalog    │  │  server for writes           │
│  WebSocket → :8100     │  │                              │
└────────────────────────┘  └──────────────────────────────┘
```

### Data Flow

- Frontend talks ONLY to Intelligence Service via WebSocket (A2UI protocol)
- Intelligence Service queries the graph through Ingestion Server's `/cypher` endpoint (REST)
- Intelligence Service reads blobs directly from shared `blob_data` volume (filesystem, read-only)
- Intelligence Service reads `events.jsonl` from shared `projects` volume (read-only)
- Neo4j is accessed directly only by Ingestion Server for writes

### Shared Volumes

| Volume | Ingestion Server | Intelligence Service | Neo4j |
|--------|-----------------|---------------------|-------|
| `blob_data` | read/write | read-only | — |
| `neo4j_data` | — | — | read/write |
| `projects` | — | read-only (JSONL + session files) | — |

The Intelligence Service is a sidecar, allowing independent restart and management without affecting the ingestion pipeline.

## Components

### Intelligence Service Container

The container is a full Amplifier installation with all providers, routing matrix, and the server bundle.

**Dockerfile pattern** (based on David Koleczek's self-improvement repo patterns):

- Base: `python:3.13-slim` with uv, git, build-essential
- Bundle pre-baking: COPY bundle into image, `sed` rewrite `git+https://` to `file:///` for air-gapped operation, pre-install Python modules
- Install Amplifier via `uv tool install`
- Copy the thin WebSocket bridge service code

**Entrypoint pattern:**

- Config overlay: mount config at `/config:ro`, copy with `cp -n` (no-overwrite) to `/data` at startup, so user edits persist across restarts
- Apply `settings.yaml` with routing matrix and all providers
- Install/update the server bundle
- Start the WebSocket bridge

**Full provider list (all 7):**

- provider-anthropic
- provider-openai
- provider-gemini
- provider-azure-openai
- provider-github-copilot
- provider-ollama
- provider-vllm

**API key management** — `secrets.env` with pass-through from host environment (bare variable names):

- `ANTHROPIC_API_KEY`
- `OPENAI_API_KEY`
- `GOOGLE_API_KEY`
- `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT`
- `GITHUB_TOKEN`
- `OLLAMA_HOST`
- `VLLM_BASE_URL`

**Settings** — mounted as `/config/settings.yaml`, containing routing matrix configuration and all provider module declarations with their `git+https://` sources.

**Health check** — generous `start_period` (120s) for first-time module installation, Python urllib check against `/health` endpoint.

**Update model (layered):**

- Hot-reload: `POST /admin/reload-bundle` triggers bundle update, new sessions pick up changes, existing sessions unaffected
- Graceful restart: rebuild container image, drain active WebSocket sessions with timeout, stop, start with full re-initialization

### WebSocket Bridge Service

The Intelligence Service is a thin bridge. Minimal custom Python code:

```
intelligence_service/
├── __main__.py          → Entry point (uvicorn)
├── app.py               → FastAPI/Starlette: GET /health, GET /admin/reload-bundle, WS /ws
├── session_manager.py   → Maps WebSocket connections to Amplifier sessions (create, destroy, reset)
├── a2ui_bridge.py       → Amplifier tool result → A2UI JSON → WebSocket; WebSocket action → Amplifier input
├── drain.py             → Graceful shutdown: stop accepting, wait for drain timeout
└── config.py            → Pydantic settings (ingestion server URL, bundle name, drain timeout, max sessions)
```

The bridge does NOT contain:

- Any LLM logic (that's in the bundle)
- Any graph query logic (that's in bundle tools)
- Any visualization selection logic (that's the agent's job)
- Any A2UI component construction (that's the agent via tools)

All intelligence lives in `amplifier-bundle-context-intelligence-server`.

### Frontend and A2UI Custom Catalog

**Tech stack:**

- Lit renderer (A2UI v0.8 stable, most mature web renderer)
- TypeScript with Vite build
- Custom A2UI component catalog

**Custom catalog (6 components):**

| Component | Wraps | Use Case |
|-----------|-------|----------|
| `NetworkGraph` | Cytoscape.js | Session trees, delegation chains, tool call flows, parallel execution graphs |
| `TimeseriesChart` | Plotly.js | Event rates over time, session durations, latency distributions |
| `StatChart` | Plotly.js | Bar charts (tool usage), pie charts (event distribution), histograms |
| `DotDiagram` | @hpcc-js/wasm (Graphviz) | Architecture diagrams, pipeline flows, system's own DOT files |
| `DataTable` | Lit native (no heavy library) | Event lists, session metadata, Cypher query results |
| `MetricCard` | Lit native (no heavy library) | KPI summaries (session count, error rate, avg duration) |

**Large graph handling (3 layers):**

Layer 1 — Agent-side query scoping: The agent's `graph_query` tool always applies limits (default 200 nodes). The agent decomposes broad queries into scoped views — top-level session nodes, not full trees. "Show all sessions today" returns session-level nodes; "Drill into session X" loads that session's orchestrator runs and steps.

Layer 2 — Progressive disclosure in the visualization: `NetworkGraph` supports collapsed/expanded groups. Sessions render as single nodes until clicked. Each expansion triggers a new agent query via A2UI action → `updateDataModel` for incremental UI update. `DataTable` supports server-side pagination via agent. `TimeseriesChart` uses aggregation buckets, not raw events.

Layer 3 — Client-side performance for large renders: Cytoscape.js with WebGL renderer (`cytoscape-webgl`) for 500+ nodes. Virtual scrolling in `DataTable`. Plotly.js WebGL traces for large timeseries. `DotDiagram` pre-renders server-side via Graphviz WASM, sends SVG string in the data model (no client-side layout computation).

**The agent is the gatekeeper.** If a user asks a question that would produce 5000 nodes, the agent returns a summarized view ("47 sessions, 12 had failures, here are the 12 failure clusters") and lets the user drill in.

### Bundle Dependency Chain

The Intelligence Service depends on BOTH bundles:

- `amplifier-bundle-context-intelligence` (existing) — provides `hook-context-intelligence` which captures the Intelligence Service's OWN sessions into the graph
- `amplifier-bundle-context-intelligence-server` (new) — provides agents, tools, skills for graph exploration and visualization

```
amplifier-bundle-context-intelligence-server (new, private, colombod)
├── includes: amplifier-bundle-context-intelligence (existing)
│   ├── hook-context-intelligence (captures own events → graph)
│   ├── context-intelligence-analyst agent
│   └── skills: neo4j-search, session-navigation
├── agents: (deferred — deep research needed)
├── tools: graph_query, blob_reader, render_surface, update_viz
├── skills: (deferred — deep research needed)
└── context: A2UI catalog schema, graph model reference
```

## Data Flow

### User Query Flow

```
User types question in Frontend
  → WebSocket message to Intelligence Service (:8100)
    → a2ui_bridge injects as user input to Amplifier session
      → Agent reasons about the question
        → Agent calls graph_query tool
          → Tool sends Cypher to Ingestion Server POST /cypher (:8000)
            → Ingestion Server proxies to Neo4j (Bolt :7687)
          → Results returned to agent
        → Agent selects visualization strategy
        → Agent calls render_surface tool
          → createSurface { surfaceId, catalogId: "context-intelligence" }
          → updateComponents { layout tree with chosen visualization components }
          → updateDataModel { query results mapped to component data paths }
      → a2ui_bridge translates tool results to A2UI JSON
    → WebSocket sends A2UI messages to Frontend
  → Lit renderer instantiates components from custom catalog
→ User sees interactive visualization
```

### User Interaction Flow (Drill-Down)

```
User clicks node in NetworkGraph
  → A2UI action message sent via WebSocket
    → a2ui_bridge translates to agent input
      → Agent receives action context (which node, what type)
        → Agent queries more detail for that node
        → Agent calls update_viz tool
          → updateDataModel with expanded data
          → updateComponents if layout needs to change
      → a2ui_bridge sends incremental A2UI update
    → WebSocket sends update to Frontend
  → Lit renderer incrementally updates affected components
```

### Session Lifecycle

```
Browser connects via WebSocket
  → session_manager.create_session()
    → Amplifier session starts with context-intelligence-server bundle
    → Session ID returned to client

Client sends message
  → a2ui_bridge injects as user input → agent runs → A2UI response

Client sends "new_session"
  → session_manager.reset_session()
    → Current session ends, new one created

Client disconnects
  → session_manager.destroy_session() → resources freed

Service restart
  → drain.py: stop accepting new connections
    → Wait for active sessions to complete (or drain timeout)
    → SIGTERM remaining sessions
```

## Self-Improvement Feedback Loop

### The 5-Phase Lifecycle

**Phase 1 — OPERATE:** User asks question → Agent runs → `hook-context-intelligence` captures events into the graph. The system's own operations become observable data.

**Phase 2 — OBSERVE:** Self-improver agent queries its own past sessions:

- Slow queries, wrong visualization choices, unhelpful results
- User interaction patterns (what got drilled into, what was changed, what was abandoned)
- Tool usage efficiency (redundant Cypher calls, etc.)

**Phase 3 — CAPTURE:** Each improvement opportunity becomes a GitHub Issue in the `amplifier-bundle-context-intelligence-server` repo. Issue types:

- `skill-improvement`: "Add Cypher pattern for common delegation-depth queries"
- `tool-improvement`: "Cache workspace session lists to avoid repeated traversals"
- `context-improvement`: "Add heuristic: use TimeseriesChart when query involves temporal range"
- `agent-strategy`: "Decompose multi-hop graph questions into sequential single-hop queries"

Each issue includes evidence from telemetry (session IDs, metrics), proposed change, and expected impact.

**Phase 4 — DEVELOP:** Issues are developed in isolation — branch, reason, test, PR, review, merge.

**Phase 5 — DEPLOY:** Hot-reload for skills/context changes, graceful restart for tool/agent structural changes. New sessions benefit from improvements. Loop back to OPERATE.

Every step of this loop is itself captured in the graph — the system's improvement decisions are observable, traceable, and reversible.

Self-improvement is triggered manually/on-demand for now. Automation will be explored later.

## Architectural Documentation as DOT Files

All architectural and operational flows are captured as DOT files, consistent with the existing 14 DOT files in the bundle's `context/` directory. These DOTs serve double duty: human documentation AND renderable by the `DotDiagram` A2UI component.

### System Architecture DOTs

Location: `amplifier-context-intelligence/docs/dot/`

- `system-architecture.dot` — 4-service Docker Compose topology, shared volumes, ports, protocols
- `container-initialization.dot` — Startup sequence, provider initialization, health check readiness
- `data-access.dot` — How Intelligence Service reads data (`/cypher`, shared volumes, JSONL)

### Operational Flow DOTs

Location: `amplifier-bundle-context-intelligence-server/context/dot/`

- `user-query-flow.dot` — User question → WebSocket → Amplifier session → graph query → visualization → A2UI render
- `a2ui-message-flow.dot` — createSurface → updateComponents → updateDataModel → action → incremental update
- `self-improvement-lifecycle.dot` — OPERATE → OBSERVE → CAPTURE → DEVELOP → DEPLOY → loop
- `session-lifecycle.dot` — WebSocket connect → session → conversation → disconnect/new session/drain
- `update-flow.dot` — Hot-reload path and restart path with config overlay
- `bundle-dependencies.dot` — Full dependency chain

## Repository Structure

### Three Repos in the Workspace

1. **`amplifier-context-intelligence/`** (colombod) — Ingestion server + `intelligence_service/` + `frontend/` + Docker Compose
2. **`amplifier-bundle-context-intelligence/`** (colombod, existing) — Hook, analyst agent, skills
3. **`amplifier-bundle-context-intelligence-server/`** (colombod, NEW, private) — Server-side intelligence bundle

### The Two UIs

The two UIs complement each other and are not redundant:

- **Operational Dashboard** (port 8000, served by ingestion server): Monitor pipeline health, active/completed sessions, live log viewer via SSE, glassmorphic design with OKLCH tokens. Logs are visible here only — not in the exploration frontend.
- **Exploration Frontend** (port 3000, nginx + SPA): AI-driven graph exploration, conversational, A2UI + Cytoscape/Plotly/Graphviz.

Cross-linking: operational dashboard landing page gets a navigation card to the exploration UI. Exploration frontend can link back to the dashboard for monitoring. Both share design tokens for visual consistency.

### Directory Layout

**`amplifier-context-intelligence/` (main repo):**

```
context_intelligence_server/           (existing ingestion server)
├── web/                               (existing dashboard: index.html, dashboard.html, static/)
├── main.py, pipeline.py, handlers/    (existing)
└── dashboard.py                       (EventRingBuffer, status)

intelligence_service/                  (NEW: WebSocket bridge)
├── __main__.py
├── app.py
├── session_manager.py
├── a2ui_bridge.py
├── drain.py
└── config.py

frontend/                              (NEW: A2UI SPA)
├── src/
│   ├── catalog/
│   │   ├── network-graph.ts           (Cytoscape.js wrapper)
│   │   ├── timeseries-chart.ts        (Plotly.js wrapper)
│   │   ├── stat-chart.ts              (Plotly.js wrapper)
│   │   ├── dot-diagram.ts             (@hpcc-js/wasm wrapper)
│   │   ├── data-table.ts              (Lit native)
│   │   └── metric-card.ts             (Lit native)
│   ├── a2ui-client.ts                 (WebSocket + A2UI message handler)
│   ├── session-controls.ts            (new session, connection status)
│   └── app.ts                         (shell: header, A2UI surface area)
├── catalog.json                       (A2UI custom catalog definition)
├── package.json
├── vite.config.ts
└── index.html

config/                                (NEW: intelligence service config)
├── settings.yaml
└── secrets.env

docker-compose.yml                     (UPDATED: 4 services)
Dockerfile                             (existing ingestion server)
Dockerfile.intelligence                (NEW)
Dockerfile.frontend                    (NEW)
docs/plans/                            (existing + new)
docs/dot/                              (NEW: architecture DOTs)
tests/                                 (existing + new)
```

### Docker Compose (4 Services)

| Service | Port | Depends On | Volumes |
|---------|------|-----------|---------|
| `context-intelligence-server` | 8000 | neo4j (healthy) | blob_data (rw) |
| `intelligence-service` | 8100 | context-intelligence-server (healthy) | blob_data (ro), projects (ro), config (ro) |
| `frontend` | 3000 | intelligence-service (healthy) | — |
| `neo4j` | 7474, 7687 | — | neo4j_data |

The `intelligence-service` also uses `env_file: secrets.env` for API key pass-through.

## Error Handling

### Service-Level

- **Intelligence Service health check** fails → Docker restarts the container. Generous `start_period` (120s) prevents false failures during first-time module installation.
- **WebSocket disconnect** → `session_manager.destroy_session()` frees resources. No dangling Amplifier sessions.
- **Ingestion Server unreachable** → Agent's `graph_query` tool returns error. Agent communicates the issue to the user via A2UI (e.g., MetricCard showing "Graph service unavailable").
- **Graceful shutdown** → `drain.py` stops accepting new connections, waits for active sessions to complete or timeout, then terminates.

### Agent-Level

- **Large result sets** → Agent-side query scoping (200 node default limit) prevents runaway queries. Progressive disclosure for drill-down.
- **100k+ token JSONL lines** → Agent tools MUST use safe extraction patterns (`jq`, line-number indexing). Never `cat` or `grep` that outputs full lines.
- **Blob URI resolution** → `context-intelligence-blob://` URIs resolve to filesystem paths. Both services must mount `blob_data` at the same path.

### Frontend-Level

- **WebSocket reconnection** → Client detects disconnect, shows connection status indicator, attempts reconnect.
- **Large graph rendering** → WebGL renderer for 500+ nodes, virtual scrolling in tables, SVG pre-rendering for DOT diagrams.

## Testing Strategy

Testing strategy details will be finalized during implementation planning. Key areas:

- **Intelligence Service** — Unit tests for session_manager, a2ui_bridge, drain logic. Integration tests for WebSocket lifecycle.
- **Frontend** — Component tests for each catalog component with mock data. A2UI message flow tests.
- **End-to-end** — Docker Compose up, send question via WebSocket, verify A2UI response contains expected visualization structure.
- **Bundle** — Agent/tool testing deferred to the research phase alongside agent design.

## Deferred Design Work — Agent Research Phase

The detailed agent/skills/tools design is explicitly deferred. It requires deep research into:

1. **`amplifier-bundle-context-intelligence` analyst agent** — Its capabilities, skills (`neo4j-search` with 12+ Cypher patterns, `session-navigation` with safe extraction), large data handling, tool constraints.

2. **`foundation:session-analyst` agent** — The foundation's approach to safely managing and exploring large JSONL files using bash tools and jq. Battle-tested patterns for 100k+ token event lines without crashing context windows.

3. **A2UI agent-side patterns** — Python SDK, tool call structure for A2UI message emission, catalog schema integration into LLM system prompts.

**The research phase will produce:**

- Agent definitions (graph-explorer, visualization-driver, self-improver)
- Tool implementations (`graph_query`, `blob_reader`, `render_surface`, `update_viz`)
- Skills (Cypher patterns, safe extraction, A2UI catalog knowledge, visualization selection heuristics)
- Context files (graph model reference, A2UI catalog schema)
- Agent strategies for the self-improvement loop

This research phase is a prerequisite before implementation of the server bundle. It should be its own brainstorm/design cycle.

## Open Questions

### Resolved Decisions

- **Auth** — Internal tooling for now, deferred to later
- **Logs** — Operational dashboard only, not in exploration frontend
- **Data access** — Agent service accesses blob data and JSONL through shared Docker Compose volumes (no mapping to `~/.amplifier/`)
- **A2UI version** — v0.8 stable
- **Self-improvement trigger** — Manual/on-demand for now, automation explored later
- **Neo4j Browser link** — Parameterized to point to wherever the services are running, sitting alongside other context-intelligence links on the ingestion server landing page

### Known Constraints

- Ingestion server's ring buffer is in-process memory only — not shared across services
- Blob URIs (`context-intelligence-blob://`) resolve to filesystem paths — both services must mount the same volume at the same path
- `events.jsonl` lines can be 100k+ tokens — agent tools MUST use safe extraction patterns (`jq`, line-number indexing), never `cat` or `grep` that outputs full lines
- A2UI custom catalog components must handle large datasets gracefully (progressive disclosure, pagination, WebGL rendering for 500+ node graphs)

## References

- [Google A2UI](https://github.com/google/A2UI) — v0.8 stable, Apache-2.0
- [David Koleczek's self-improvement patterns](https://github.com/DavidKoleczek/self-improvement) — Container setup, bundle pre-baking, config overlay, secrets pass-through
- Existing context-intelligence server: `amplifier-context-intelligence/` submodule
- Existing context-intelligence bundle: `amplifier-bundle-context-intelligence/` submodule
