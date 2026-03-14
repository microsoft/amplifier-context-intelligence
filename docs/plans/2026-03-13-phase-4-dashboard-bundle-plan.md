# Context Intelligence Server — Phase 4: Dashboard, Observability & Bundle Changes

> **Execution:** Use the subagent-driven-development workflow to implement this plan.

**Goal:** Add operational dashboard, enriched status endpoint, event ring buffer, and structured logging to the server; enhance the bundle's `LoggingHandler` with fire-and-forget server dispatch and update `BlobTool` to use HTTP endpoints.

**Architecture:** Server gets a `dashboard.py` module with an in-memory ring buffer (last 50 events) and enriched status builder. `GET /` serves a vanilla HTML dashboard polling `/status` every 3 seconds. Bundle changes (on a **local-only feature branch**) add `asyncio.create_task` HTTP dispatch to `LoggingHandler` and convert `BlobTool` from `DiskBlobStore` to pure HTTP client.

**Tech Stack:** Python 3.11, FastAPI, httpx, pytest-asyncio, Docker Compose

---

## Repo Locations

| Repo | Root Path | Branch |
|------|-----------|--------|
| **Server** | `/home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence/` | current branch (main) |
| **Bundle** | `/home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence/` | `feat/server-dispatch` (created in Task 1) |

**All relative paths in tasks are relative to the repo root shown above.**

---

## Starting State (after Phases 1–3)

The server repo has this structure:

```
.
├── pyproject.toml
├── Dockerfile
├── docker-compose.yml
├── context_intelligence_server/
│   ├── __init__.py
│   ├── config.py          # Settings with CI_SERVER_ env prefix
│   ├── models.py          # EventRequest, EventResponse, StatusResponse
│   ├── registry.py        # SessionRegistry, SessionWorker, drain_worker()
│   ├── pipeline.py        # process_event() — handler dispatch (Phase 2)
│   ├── main.py            # FastAPI app: POST /events, GET /status, GET /blobs/*, POST /cypher
│   └── ...                # handlers/, blob_store, neo4j_store, services, etc. (Phases 2–3)
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── test_config.py
    ├── test_models.py
    ├── test_registry.py
    ├── test_main.py
    └── test_pipeline.py
```

Key conventions from Phase 1:
- `asyncio_mode = "auto"` — all async test functions run automatically
- Test client uses `httpx.ASGITransport` + `AsyncClient`
- `SessionWorker` is a `@dataclass` with `session_id`, `workspace`, `queue`, `task` fields
- `drain_worker()` is a module-level function in `registry.py`
- `SessionRegistry` has `get_or_create()`, `remove()`, `active_count()`, `active_sessions()` methods
- Structured JSON logging via `logging.basicConfig` in `main.py`

The bundle module is at:
```
modules/hook-context-intelligence/amplifier_module_hook_context_intelligence/
├── handlers/
│   └── logging_handler.py   # LoggingHandler class
├── __init__.py               # mount() entry point
├── blob_tool.py              # BlobTool (currently wraps DiskBlobStore)
├── blob_store.py             # DiskBlobStore, BlobStore protocol
├── config_resolver.py        # ConfigResolver
└── ...
```

Bundle test conventions:
- `asyncio_mode = "auto"`, `asyncio_default_fixture_loop_scope = "function"`
- Test classes group related tests (e.g., `TestBlobList`, `TestBlobDump`)
- Fixtures use `tmp_path` for filesystem isolation
- Mock coordinators built with `MagicMock` / `AsyncMock`

---

## Final Directory Structure (new/modified files only)

### Server (new files in bold):
```
context_intelligence_server/
└── dashboard.py          # NEW — EventRingBuffer, build_status_response()
```

### Bundle (modifications only, on feat/server-dispatch):
```
amplifier_module_hook_context_intelligence/
├── handlers/
│   └── logging_handler.py    # MODIFIED — add server dispatch
├── __init__.py               # MODIFIED — wire BlobTool
├── blob_tool.py              # MODIFIED — HTTP client
└── config_resolver.py        # MODIFIED — add server_url property
```

---

## Task 1: Create Feature Branch in Bundle Repo

**Files:** None — git operation only.

### Step 1: Create and verify the branch

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence
git checkout -b feat/server-dispatch
```

### Step 2: Verify

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence
git branch
```

Expected: Output shows `* feat/server-dispatch`.

> **⚠️ CRITICAL: DO NOT run `git push` on this branch.** This branch exists for local migration work only. The push decision is deferred until the server is fully validated end-to-end.

No tests, no commit — this is a branch creation only.

---

## Task 2: SessionWorker Activity Tracking

**Files:**
- Modify: `context_intelligence_server/registry.py`
- Modify: `tests/test_registry.py`

### Step 1: Write the failing tests

Append these tests to the end of `tests/test_registry.py`:

```python
import time


async def test_worker_tracking_fields_initialized(registry: SessionRegistry):
    """New worker starts with zeroed tracking fields."""
    worker = registry.get_or_create("sess-track", "ws")
    assert worker.last_event == ""
    assert worker.last_event_time == 0.0
    assert worker.events_processed == 0


async def test_worker_tracking_updated_after_drain(registry: SessionRegistry):
    """After drain processes an event, tracking fields are updated."""
    worker = registry.get_or_create("sess-track", "ws")
    await worker.queue.put(("tool:pre", "ws", {"session_id": "sess-track"}))

    # Wait for drain to process
    import asyncio
    await asyncio.wait_for(worker.queue.join(), timeout=5.0)

    assert worker.last_event == "tool:pre"
    assert worker.last_event_time > 0.0
    assert worker.events_processed == 1


async def test_worker_events_processed_increments(registry: SessionRegistry):
    """events_processed increments on each event."""
    worker = registry.get_or_create("sess-inc", "ws")
    await worker.queue.put(("tool:pre", "ws", {"session_id": "sess-inc"}))
    await worker.queue.put(("tool:post", "ws", {"session_id": "sess-inc"}))

    import asyncio
    await asyncio.wait_for(worker.queue.join(), timeout=5.0)

    assert worker.events_processed == 2
    assert worker.last_event == "tool:post"
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_registry.py::test_worker_tracking_fields_initialized -v
```

Expected: **FAIL** — `AttributeError: 'SessionWorker' has no attribute 'last_event'`

### Step 3: Update the SessionWorker dataclass

In `context_intelligence_server/registry.py`, add three fields to the `SessionWorker` dataclass. Find the existing dataclass:

```python
@dataclass
class SessionWorker:
    """Holds per-session state: an asyncio queue and a background drain task.

    The ``task`` field is ``None`` until the drain loop is started by
    ``SessionRegistry.get_or_create``.
    """

    session_id: str
    workspace: str
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    task: asyncio.Task | None = None
```

Replace it with:

```python
@dataclass
class SessionWorker:
    """Holds per-session state: an asyncio queue and a background drain task.

    The ``task`` field is ``None`` until the drain loop is started by
    ``SessionRegistry.get_or_create``.
    """

    session_id: str
    workspace: str
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    task: asyncio.Task | None = None
    last_event: str = ""
    last_event_time: float = 0.0
    events_processed: int = 0
```

### Step 4: Update the drain loop to set tracking fields

In `context_intelligence_server/registry.py`, find the `drain_worker` function. Add `import time` at the top of the file if not already present. Then update the drain loop's `try` block to set the tracking fields after the existing log statement.

Find this block inside `drain_worker`:

```python
        try:
            event, workspace, data = await worker.queue.get()
            session_id = data.get("session_id", "unknown")
            logger.info(
                "event_received: event=%s session_id=%s workspace=%s",
                event,
                session_id,
                workspace,
            )
```

Replace with:

```python
        try:
            event, workspace, data = await worker.queue.get()
            session_id = data.get("session_id", "unknown")
            logger.info(
                "event_received: event=%s session_id=%s workspace=%s",
                event,
                session_id,
                workspace,
            )
            worker.last_event = event
            worker.last_event_time = time.time()
            worker.events_processed += 1
```

> **Note:** If Phase 2 replaced the placeholder drain loop with a `process_event()` call, the tracking update should go right after that call instead. The key invariant is: tracking updates happen after each event is processed, inside the `try` block, before `task_done()`.

### Step 5: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_registry.py -v
```

Expected: **ALL PASS** — original tests plus the 3 new tracking tests.

### Step 6: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/registry.py tests/test_registry.py
git commit -m "feat: phase 4 — session worker activity tracking"
```

---

## Task 3: Event Ring Buffer and Status Builder

**Files:**
- Create: `context_intelligence_server/dashboard.py`
- Create: `tests/test_dashboard.py`

### Step 1: Write the failing tests

Create file `tests/test_dashboard.py`:

```python
"""Tests for dashboard module — event ring buffer and status builder."""

from __future__ import annotations

import time

from context_intelligence_server.dashboard import (
    EventRecord,
    EventRingBuffer,
    build_status_response,
)
from context_intelligence_server.registry import SessionRegistry


# ---------------------------------------------------------------------------
# EventRingBuffer
# ---------------------------------------------------------------------------
class TestEventRingBuffer:
    """Ring buffer stores recent events with a max length."""

    def test_add_and_recent(self):
        """Adding a record makes it appear in recent()."""
        buf = EventRingBuffer(maxlen=10)
        rec = EventRecord(
            timestamp=time.time(),
            event="tool:pre",
            session_id="s1",
            workspace="ws",
            result="ok",
        )
        buf.add(rec)
        assert len(buf.recent()) == 1
        assert buf.recent()[0].event == "tool:pre"

    def test_newest_first(self):
        """Most recent event is first in the list."""
        buf = EventRingBuffer(maxlen=10)
        buf.add(EventRecord(
            timestamp=1.0, event="first", session_id="s1",
            workspace="ws", result="ok",
        ))
        buf.add(EventRecord(
            timestamp=2.0, event="second", session_id="s1",
            workspace="ws", result="ok",
        ))
        recent = buf.recent()
        assert recent[0].event == "second"
        assert recent[1].event == "first"

    def test_maxlen_respected(self):
        """Adding beyond maxlen drops the oldest entry."""
        buf = EventRingBuffer(maxlen=3)
        for i in range(5):
            buf.add(EventRecord(
                timestamp=float(i), event=f"evt-{i}", session_id="s1",
                workspace="ws", result="ok",
            ))
        recent = buf.recent()
        assert len(recent) == 3
        # Most recent events kept: evt-4, evt-3, evt-2
        assert recent[0].event == "evt-4"
        assert recent[2].event == "evt-2"

    def test_error_record(self):
        """Error records carry error message."""
        buf = EventRingBuffer(maxlen=10)
        buf.add(EventRecord(
            timestamp=time.time(), event="tool:pre", session_id="s1",
            workspace="ws", result="error", error="something broke",
        ))
        assert buf.recent()[0].result == "error"
        assert buf.recent()[0].error == "something broke"

    def test_empty_buffer(self):
        """Empty buffer returns empty list."""
        buf = EventRingBuffer(maxlen=10)
        assert buf.recent() == []


# ---------------------------------------------------------------------------
# build_status_response
# ---------------------------------------------------------------------------
class TestBuildStatusResponse:
    """build_status_response returns enriched status dict."""

    def test_empty_registry(self):
        """Empty registry returns zero sessions and empty lists."""
        registry = SessionRegistry()
        start_time = time.time() - 10.0
        result = build_status_response(registry, start_time)
        assert result["status"] == "ok"
        assert result["uptime_seconds"] >= 10.0
        assert result["active_sessions"] == 0
        assert result["sessions"] == []
        assert result["recent_events"] == []

    def test_with_active_session(self):
        """Active session appears in sessions list with correct fields."""
        registry = SessionRegistry()
        worker = registry.get_or_create("sess-1", "my-workspace")
        worker.last_event = "tool:pre"
        worker.last_event_time = time.time()
        worker.events_processed = 5
        start_time = time.time() - 30.0
        result = build_status_response(registry, start_time)
        assert result["active_sessions"] == 1
        assert len(result["sessions"]) == 1
        sess = result["sessions"][0]
        assert sess["session_id"] == "sess-1"
        assert sess["workspace"] == "my-workspace"
        assert sess["queue_depth"] == 0
        assert sess["last_event"] == "tool:pre"
        assert sess["events_processed"] == 5

    def test_includes_recent_events(self):
        """Recent events from ring buffer appear in response."""
        registry = SessionRegistry()
        start_time = time.time()
        # Import the module-level ring buffer and add a record
        from context_intelligence_server.dashboard import ring_buffer
        ring_buffer._buffer.clear()  # reset for test isolation
        ring_buffer.add(EventRecord(
            timestamp=time.time(), event="session:start", session_id="s1",
            workspace="ws", result="ok",
        ))
        result = build_status_response(registry, start_time)
        assert len(result["recent_events"]) == 1
        assert result["recent_events"][0]["event"] == "session:start"
        ring_buffer._buffer.clear()  # cleanup
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_dashboard.py -v
```

Expected: **FAIL** — `ModuleNotFoundError: No module named 'context_intelligence_server.dashboard'`

### Step 3: Write the implementation

Create file `context_intelligence_server/dashboard.py`:

```python
"""Dashboard support — event ring buffer and enriched status builder."""

from __future__ import annotations

import time
from collections import deque
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from context_intelligence_server.registry import SessionRegistry


@dataclass
class EventRecord:
    """A single processed event record for the ring buffer."""

    timestamp: float
    event: str
    session_id: str
    workspace: str
    result: str  # "ok" | "error"
    error: str = ""


class EventRingBuffer:
    """Fixed-size buffer of recent processed events (newest first)."""

    def __init__(self, maxlen: int = 50) -> None:
        self._buffer: deque[EventRecord] = deque(maxlen=maxlen)

    def add(self, record: EventRecord) -> None:
        """Add a record to the front of the buffer."""
        self._buffer.appendleft(record)

    def recent(self) -> list[EventRecord]:
        """Return all records, newest first."""
        return list(self._buffer)


# Module-level singleton — shared by pipeline and status endpoint
ring_buffer = EventRingBuffer()


def build_status_response(
    registry: SessionRegistry,
    start_time: float,
) -> dict[str, Any]:
    """Build the enriched status response dict.

    Returns a dict suitable for JSON serialization with:
    - status, uptime_seconds, active_sessions (top-level)
    - sessions: list of per-session detail dicts
    - recent_events: list of recent EventRecord dicts
    """
    sessions = []
    for session_id in registry.active_sessions():
        worker = registry.get_or_create(session_id, "")
        sessions.append({
            "session_id": worker.session_id,
            "workspace": worker.workspace,
            "queue_depth": worker.queue.qsize(),
            "last_event": worker.last_event,
            "last_event_time": worker.last_event_time,
            "events_processed": worker.events_processed,
        })

    return {
        "status": "ok",
        "uptime_seconds": time.time() - start_time,
        "active_sessions": registry.active_count(),
        "sessions": sessions,
        "recent_events": [asdict(r) for r in ring_buffer.recent()],
    }
```

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_dashboard.py -v
```

Expected: **ALL PASS** (8 tests)

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/dashboard.py tests/test_dashboard.py
git commit -m "feat: phase 4 — event ring buffer and status builder"
```

---

## Task 4: Wire Ring Buffer into Pipeline

**Files:**
- Modify: `context_intelligence_server/pipeline.py` (or the drain loop in `registry.py`)
- Modify: `tests/test_pipeline.py` (or `tests/test_registry.py`)

> **Note for implementer:** Phase 2 may have created a `pipeline.py` with a `process_event()` function, or the drain logic may still live in `registry.py:drain_worker()`. Read the actual code to determine where event processing happens. The ring buffer emission goes at the end of that processing, in a `finally` block.

### Step 1: Write the failing tests

If `tests/test_pipeline.py` exists, append to it. Otherwise append to `tests/test_registry.py`. The test below uses the registry approach — adapt the import if `process_event` lives in `pipeline.py`.

```python
from context_intelligence_server.dashboard import ring_buffer


async def test_ring_buffer_receives_record_after_event():
    """Ring buffer gets a record after the drain loop processes an event."""
    from context_intelligence_server.registry import SessionRegistry
    import asyncio

    ring_buffer._buffer.clear()
    reg = SessionRegistry()
    worker = reg.get_or_create("ring-test", "ws-ring")
    await worker.queue.put(("tool:pre", "ws-ring", {"session_id": "ring-test"}))
    await asyncio.wait_for(worker.queue.join(), timeout=5.0)

    recent = ring_buffer.recent()
    assert len(recent) >= 1
    record = recent[0]
    assert record.event == "tool:pre"
    assert record.session_id == "ring-test"
    assert record.workspace == "ws-ring"
    assert record.result == "ok"

    # Cleanup
    reg.remove("ring-test")
    ring_buffer._buffer.clear()
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_registry.py::test_ring_buffer_receives_record_after_event -v
```

Expected: **FAIL** — ring buffer is empty because nothing emits to it yet.

### Step 3: Add ring buffer emission to the drain loop

In the file where events are processed (either `registry.py:drain_worker()` or `pipeline.py:process_event()`), add the ring buffer import and emission.

**If processing is in `registry.py:drain_worker()`:**

Add this import near the top of `context_intelligence_server/registry.py`:

```python
from context_intelligence_server.dashboard import ring_buffer, EventRecord
```

Then wrap the drain loop body in a try/except/finally that emits to the ring buffer. Find the existing `drain_worker` function body and restructure it. The current structure looks like:

```python
async def drain_worker(worker: SessionWorker) -> None:
    while True:
        try:
            event, workspace, data = await worker.queue.get()
            session_id = data.get("session_id", "unknown")
            logger.info(...)
            worker.last_event = event
            worker.last_event_time = time.time()
            worker.events_processed += 1
        except Exception:
            logger.exception(...)
        finally:
            worker.queue.task_done()
```

Replace with:

```python
async def drain_worker(worker: SessionWorker) -> None:
    """Drain loop: dequeue events and process them."""
    while True:
        result = "ok"
        error = ""
        event = ""
        try:
            event, workspace, data = await worker.queue.get()
            session_id = data.get("session_id", "unknown")
            logger.info(
                "event_received: event=%s session_id=%s workspace=%s",
                event,
                session_id,
                workspace,
            )
            worker.last_event = event
            worker.last_event_time = time.time()
            worker.events_processed += 1
        except Exception as exc:
            result = "error"
            error = str(exc)
            logger.exception("drain_worker error for session=%s", worker.session_id)
        finally:
            if event:
                ring_buffer.add(EventRecord(
                    timestamp=time.time(),
                    event=event,
                    session_id=data.get("session_id", "") if "data" in dir() else "",
                    workspace=worker.workspace,
                    result=result,
                    error=error,
                ))
            worker.queue.task_done()
```

> **If Phase 2 created a `process_event()` function in `pipeline.py`:** The ring buffer emission wraps that call instead. Add the same import to `pipeline.py` and wrap the `await process_event(...)` call in a similar try/except/finally pattern. The important thing is: every processed event (success or error) emits an `EventRecord` to the ring buffer.

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_registry.py -v
```

Expected: **ALL PASS** — all existing tests plus the new ring buffer test.

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/registry.py tests/test_registry.py
git commit -m "feat: phase 4 — wire ring buffer into drain loop"
```

---

## Task 5: Enrich GET /status and Add GET / Dashboard

**Files:**
- Modify: `context_intelligence_server/main.py`
- Modify: `tests/test_main.py`

### Step 1: Write the failing tests

Append to `tests/test_main.py`:

```python
async def test_status_includes_sessions_list(client: AsyncClient):
    """GET /status includes sessions list and recent_events list."""
    resp = await client.get("/status")
    body = resp.json()
    assert "sessions" in body
    assert isinstance(body["sessions"], list)
    assert "recent_events" in body
    assert isinstance(body["recent_events"], list)


async def test_status_session_detail_after_event(client: AsyncClient):
    """After posting an event, GET /status shows session detail."""
    payload = {
        "event": "session:start",
        "workspace": "test-ws",
        "data": {"session_id": "status-detail-test"},
    }
    await client.post("/events", json=payload)

    import asyncio
    await asyncio.sleep(0.5)  # allow drain loop to process

    resp = await client.get("/status")
    body = resp.json()
    assert body["active_sessions"] >= 1
    sessions = body["sessions"]
    match = [s for s in sessions if s["session_id"] == "status-detail-test"]
    assert len(match) == 1
    assert match[0]["workspace"] == "test-ws"
    assert "queue_depth" in match[0]
    assert "events_processed" in match[0]


async def test_dashboard_returns_html(client: AsyncClient):
    """GET / returns 200 with HTML content."""
    resp = await client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "Context Intelligence Server" in resp.text
    assert "setInterval" in resp.text  # polling JS present
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_main.py::test_status_includes_sessions_list tests/test_main.py::test_dashboard_returns_html -v
```

Expected: **FAIL** — `/status` returns old `StatusResponse` without `sessions`/`recent_events`; `GET /` returns 404.

### Step 3: Update main.py

In `context_intelligence_server/main.py`, make three changes:

**3a. Add imports** near the top:

```python
from fastapi.responses import HTMLResponse

from context_intelligence_server.dashboard import build_status_response
```

**3b. Replace the GET /status handler.** Find:

```python
@app.get("/status", response_model=StatusResponse)
async def status() -> StatusResponse:
    """Health check endpoint. Used as Docker Compose healthcheck."""
    return StatusResponse(
        status="ok",
        uptime_seconds=time.time() - _start_time,
        active_sessions=registry.active_count(),
    )
```

Replace with:

```python
@app.get("/status")
async def status() -> dict:
    """Health check endpoint with enriched session data. Used as Docker Compose healthcheck."""
    return build_status_response(registry, _start_time)
```

**3c. Add the GET / dashboard endpoint.** Add this after the `/status` route:

```python
_DASHBOARD_HTML = """<!DOCTYPE html>
<html>
<head>
  <title>Context Intelligence Server</title>
  <style>
    body { font-family: monospace; padding: 20px; background: #1a1a2e; color: #eee; }
    h1 { color: #4cc9f0; }
    h2 { color: #7b8cde; margin-top: 24px; }
    .metric { display: inline-block; margin: 10px; padding: 10px; background: #16213e; border-radius: 6px; }
    table { width: 100%; border-collapse: collapse; margin: 10px 0; }
    th, td { padding: 8px; text-align: left; border-bottom: 1px solid #333; }
    th { color: #4cc9f0; }
    .ok { color: #4ade80; }
    .error { color: #f87171; }
  </style>
</head>
<body>
  <h1>Context Intelligence Server</h1>
  <div id="health"></div>
  <h2>Active Sessions</h2>
  <table id="sessions"><tr><th>Session</th><th>Workspace</th><th>Queue</th><th>Last Event</th><th>Processed</th></tr></table>
  <h2>Recent Events</h2>
  <table id="events"><tr><th>Time</th><th>Event</th><th>Session</th><th>Workspace</th><th>Result</th></tr></table>
  <script>
    async function refresh() {
      try {
        const r = await fetch('/status');
        const d = await r.json();
        document.getElementById('health').innerHTML =
          '<span class="metric">Uptime: ' + d.uptime_seconds.toFixed(0) + 's</span>' +
          '<span class="metric">Active Sessions: ' + d.active_sessions + '</span>';
        document.getElementById('sessions').innerHTML =
          '<tr><th>Session</th><th>Workspace</th><th>Queue</th><th>Last Event</th><th>Processed</th></tr>' +
          d.sessions.map(function(s) {
            return '<tr><td>' + s.session_id.slice(0,8) + '\\u2026</td><td>' + s.workspace + '</td><td>' + s.queue_depth + '</td><td>' + s.last_event + '</td><td>' + s.events_processed + '</td></tr>';
          }).join('');
        document.getElementById('events').innerHTML =
          '<tr><th>Time</th><th>Event</th><th>Session</th><th>Workspace</th><th>Result</th></tr>' +
          d.recent_events.map(function(e) {
            return '<tr><td>' + new Date(e.timestamp * 1000).toISOString().slice(11,19) + '</td><td>' + e.event + '</td><td>' + e.session_id.slice(0,8) + '\\u2026</td><td>' + e.workspace + '</td><td class="' + e.result + '">' + e.result + '</td></tr>';
          }).join('');
      } catch (err) {
        console.error('Dashboard refresh failed:', err);
      }
    }
    refresh();
    setInterval(refresh, 3000);
  </script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> str:
    """Minimal operational dashboard — polls /status every 3 seconds."""
    return _DASHBOARD_HTML
```

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_main.py -v
```

Expected: **ALL PASS** — old tests still pass (enriched `/status` is a superset of old fields), new tests pass.

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/main.py tests/test_main.py
git commit -m "feat: phase 4 — enriched /status and HTML dashboard"
```

---

## Task 6: Enhance LoggingHandler with Server Dispatch (Bundle)

**Working directory:** `/home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence/`
**Branch:** `feat/server-dispatch` (verify with `git branch` before editing)

**Files:**
- Modify: `modules/hook-context-intelligence/amplifier_module_hook_context_intelligence/handlers/logging_handler.py`
- Create: `modules/hook-context-intelligence/tests/test_logging_handler_server_dispatch.py`

### Step 1: Write the failing tests

Create file `modules/hook-context-intelligence/tests/test_logging_handler_server_dispatch.py`:

```python
"""Tests for LoggingHandler server dispatch (fire-and-forget HTTP).

These tests cover the optional server_url dispatch path added for the
Context Intelligence Server migration.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from amplifier_core.models import HookResult


# ---------------------------------------------------------------------------
# _FakeResolver with server_url support
# ---------------------------------------------------------------------------
class _FakeResolver:
    """Minimal resolver adapter with optional server_url."""

    def __init__(
        self,
        base_path: Path,
        project_slug: str,
        server_url: str | None = None,
        workspace: str | None = None,
    ) -> None:
        self.base_path = base_path
        self.project_slug = project_slug
        self.server_url = server_url
        self.workspace = workspace

    def session_dir(self, session_id: str) -> Path:
        return (
            self.base_path
            / self.project_slug
            / "sessions"
            / session_id
            / "context-intelligence"
        )


# ---------------------------------------------------------------------------
# TestServerDispatchDisabled
# ---------------------------------------------------------------------------
class TestServerDispatchDisabled:
    """When server_url is not set, no HTTP calls are made."""

    async def test_no_dispatch_without_server_url(self, tmp_path: Path) -> None:
        from amplifier_module_hook_context_intelligence.handlers.logging_handler import (
            LoggingHandler,
        )

        resolver = _FakeResolver(tmp_path, "proj", server_url=None)
        handler = LoggingHandler(resolver)

        with patch("asyncio.create_task") as mock_task:
            result = await handler(
                "session:start",
                {"session_id": "s1", "timestamp": "2026-01-15T10:00:00Z", "working_dir": "/w"},
            )

        assert isinstance(result, HookResult)
        assert result.action == "continue"
        mock_task.assert_not_called()

    async def test_jsonl_still_written_without_server_url(self, tmp_path: Path) -> None:
        from amplifier_module_hook_context_intelligence.handlers.logging_handler import (
            LoggingHandler,
        )

        resolver = _FakeResolver(tmp_path, "proj", server_url=None)
        handler = LoggingHandler(resolver)
        await handler(
            "session:start",
            {"session_id": "s1", "timestamp": "2026-01-15T10:00:00Z", "working_dir": "/w"},
        )

        jsonl = tmp_path / "proj" / "sessions" / "s1" / "context-intelligence" / "events.jsonl"
        assert jsonl.exists()


# ---------------------------------------------------------------------------
# TestServerDispatchEnabled
# ---------------------------------------------------------------------------
class TestServerDispatchEnabled:
    """When server_url is set, asyncio.create_task is called."""

    async def test_dispatch_creates_task(self, tmp_path: Path) -> None:
        from amplifier_module_hook_context_intelligence.handlers.logging_handler import (
            LoggingHandler,
        )

        resolver = _FakeResolver(
            tmp_path, "proj", server_url="http://localhost:8000", workspace="test-ws"
        )
        handler = LoggingHandler(resolver)

        with patch("asyncio.create_task") as mock_task:
            result = await handler(
                "tool:pre",
                {"session_id": "s1", "timestamp": "2026-01-15T10:00:01Z"},
            )

        assert isinstance(result, HookResult)
        assert result.action == "continue"
        mock_task.assert_called_once()

    async def test_jsonl_still_written_with_server_url(self, tmp_path: Path) -> None:
        from amplifier_module_hook_context_intelligence.handlers.logging_handler import (
            LoggingHandler,
        )

        resolver = _FakeResolver(
            tmp_path, "proj", server_url="http://localhost:8000", workspace="ws"
        )
        handler = LoggingHandler(resolver)

        with patch("asyncio.create_task"):
            await handler(
                "tool:pre",
                {"session_id": "s1", "timestamp": "2026-01-15T10:00:01Z"},
            )

        jsonl = tmp_path / "proj" / "sessions" / "s1" / "context-intelligence" / "events.jsonl"
        assert jsonl.exists()


# ---------------------------------------------------------------------------
# TestServerDispatchFailure
# ---------------------------------------------------------------------------
class TestServerDispatchFailure:
    """HTTP failures are logged as warnings; handler still returns continue."""

    async def test_http_failure_logs_warning(self, tmp_path: Path, caplog) -> None:
        from amplifier_module_hook_context_intelligence.handlers.logging_handler import (
            LoggingHandler,
        )

        resolver = _FakeResolver(
            tmp_path, "proj", server_url="http://unreachable:9999", workspace="ws"
        )
        handler = LoggingHandler(resolver)

        # Call the dispatch method directly (not via create_task)
        with caplog.at_level(logging.WARNING):
            await handler._dispatch_to_server("tool:pre", {"session_id": "s1"})

        assert any("server_dispatch_failed" in r.message for r in caplog.records)
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence/modules/hook-context-intelligence
pytest tests/test_logging_handler_server_dispatch.py -v
```

Expected: **FAIL** — `_FakeResolver` has `server_url` attribute but `LoggingHandler` doesn't read it; `_dispatch_to_server` method doesn't exist.

### Step 3: Update LoggingHandler

Edit `modules/hook-context-intelligence/amplifier_module_hook_context_intelligence/handlers/logging_handler.py`.

**3a. Add `asyncio` import** at the top (after the existing imports):

```python
import asyncio
```

**3b. Update `__init__`** to cache server config. Find:

```python
    def __init__(self, resolver: Any) -> None:
        self._resolver = resolver
        self.handled_events = set()
        self._seen_sessions: set[str] = set()
```

Replace with:

```python
    def __init__(self, resolver: Any) -> None:
        self._resolver = resolver
        self.handled_events = set()
        self._seen_sessions: set[str] = set()
        self._server_url: str | None = getattr(resolver, "server_url", None) or None
        self._workspace: str | None = getattr(resolver, "workspace", None) or None
```

**3c. Add server dispatch at the end of `__call__`.** Find the `return HookResult(action="continue")` at the end of `__call__` and add the dispatch right before it. The method currently ends:

```python
            self._append_event(session_dir, event, data)
        except Exception:
            logger.exception("LoggingHandler error processing %s", event)

        return HookResult(action="continue")
```

Replace with:

```python
            self._append_event(session_dir, event, data)
        except Exception:
            logger.exception("LoggingHandler error processing %s", event)

        if self._server_url:
            asyncio.create_task(self._dispatch_to_server(event, data))

        return HookResult(action="continue")
```

**3d. Add the `_dispatch_to_server` method** at the end of the class (after `_append_event`):

```python
    async def _dispatch_to_server(self, event: str, data: dict[str, Any]) -> None:
        """Fire-and-forget POST to the Context Intelligence Server.

        Failures are logged as warnings — JSONL is the durable record.
        """
        try:
            import httpx

            async with httpx.AsyncClient() as client:
                await client.post(
                    f"{self._server_url}/events",
                    json={
                        "event": event,
                        "workspace": self._workspace or "default",
                        "data": data,
                    },
                    timeout=5.0,
                )
        except Exception:
            logger.warning(
                "server_dispatch_failed",
                extra={"event": event, "server_url": self._server_url},
            )
```

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence/modules/hook-context-intelligence
pytest tests/test_logging_handler_server_dispatch.py -v
```

Expected: **ALL PASS** (5 tests)

### Step 5: Run existing LoggingHandler tests to verify no regression

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence/modules/hook-context-intelligence
pytest tests/test_logging_handler.py -v
```

Expected: **ALL PASS** — existing tests use `_FakeResolver` without `server_url`, so `self._server_url` is `None` and dispatch is skipped.

### Step 6: Commit to feature branch

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence
git add modules/hook-context-intelligence/amplifier_module_hook_context_intelligence/handlers/logging_handler.py modules/hook-context-intelligence/tests/test_logging_handler_server_dispatch.py
git commit -m "feat: enhance LoggingHandler with server dispatch"
```

> **Reminder: DO NOT run `git push`.**

---

## Task 7: Update BlobTool to HTTP Client (Bundle)

**Working directory:** `/home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence/`
**Branch:** `feat/server-dispatch`

**Files:**
- Modify: `modules/hook-context-intelligence/amplifier_module_hook_context_intelligence/blob_tool.py`
- Create: `modules/hook-context-intelligence/tests/test_blob_tool_http.py`

### Step 1: Write the failing tests

Create file `modules/hook-context-intelligence/tests/test_blob_tool_http.py`:

```python
"""Tests for BlobTool HTTP client (replaces DiskBlobStore dependency).

Tests mock httpx.AsyncClient to verify correct URLs and response parsing.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# TestBlobListHTTP
# ---------------------------------------------------------------------------
class TestBlobListHTTP:
    """blob_list() calls GET /blobs/{session_id} and parses response."""

    async def test_list_calls_correct_url(self) -> None:
        from amplifier_module_hook_context_intelligence.blob_tool import BlobTool

        tool = BlobTool(server_url="http://localhost:8000")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "blobs": [
                "ci-blob://sess-1/node-abc__messages",
                "ci-blob://sess-1/node-def__tool_output",
            ]
        }
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = await tool.blob_list("sess-1")

        mock_client.get.assert_called_once_with("http://localhost:8000/blobs/sess-1")
        assert len(result) == 2

    async def test_list_parses_field_and_node(self) -> None:
        from amplifier_module_hook_context_intelligence.blob_tool import BlobTool

        tool = BlobTool(server_url="http://localhost:8000")

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "blobs": ["ci-blob://sess-1/node-abc__messages"]
        }
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = await tool.blob_list("sess-1")

        item = result[0]
        assert item["uri"] == "ci-blob://sess-1/node-abc__messages"
        assert item["field"] == "messages"
        assert item["node_id"] == "node-abc"
        assert item["size_bytes"] is None  # not available over HTTP

    async def test_list_empty_session(self) -> None:
        from amplifier_module_hook_context_intelligence.blob_tool import BlobTool

        tool = BlobTool(server_url="http://localhost:8000")

        mock_response = MagicMock()
        mock_response.json.return_value = {"blobs": []}
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = await tool.blob_list("sess-empty")

        assert result == []

    async def test_list_key_without_separator(self) -> None:
        from amplifier_module_hook_context_intelligence.blob_tool import BlobTool

        tool = BlobTool(server_url="http://localhost:8000")

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "blobs": ["ci-blob://sess-1/simple-key"]
        }
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = await tool.blob_list("sess-1")

        item = result[0]
        assert item["field"] == "unknown"
        assert item["node_id"] == "simple-key"


# ---------------------------------------------------------------------------
# TestBlobDumpHTTP
# ---------------------------------------------------------------------------
class TestBlobDumpHTTP:
    """blob_dump() calls GET /blobs/{session_id}/{key} and writes to disk."""

    async def test_dump_calls_correct_url(self, tmp_path: Path) -> None:
        from amplifier_module_hook_context_intelligence.blob_tool import BlobTool

        tool = BlobTool(server_url="http://localhost:8000")
        dest = str(tmp_path / "output.json")

        mock_response = MagicMock()
        mock_response.text = json.dumps({"content": "hello"})
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = await tool.blob_dump("ci-blob://sess-1/node-abc__messages", dest)

        mock_client.get.assert_called_once_with(
            "http://localhost:8000/blobs/sess-1/node-abc__messages"
        )
        assert result == dest
        assert Path(dest).is_file()
        assert json.loads(Path(dest).read_text()) == {"content": "hello"}

    async def test_dump_default_dest_path(self, tmp_path: Path) -> None:
        from amplifier_module_hook_context_intelligence.blob_tool import BlobTool

        tool = BlobTool(server_url="http://localhost:8000")

        mock_response = MagicMock()
        mock_response.text = json.dumps({"data": "value"})
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = await tool.blob_dump("ci-blob://sess-1/my-key__field")

        assert result.endswith("my-key__field.json")
        assert Path(result).is_file()
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence/modules/hook-context-intelligence
pytest tests/test_blob_tool_http.py -v
```

Expected: **FAIL** — `BlobTool.__init__()` expects a `store: DiskBlobStore` parameter, not `server_url`.

### Step 3: Rewrite blob_tool.py

Replace the entire contents of `modules/hook-context-intelligence/amplifier_module_hook_context_intelligence/blob_tool.py` with:

```python
"""BlobTool — agent-facing tool for inspecting and materializing blobs.

Agents never load blob content into the context window directly.
Instead they use blob_list() to discover blob metadata and blob_dump()
to materialize a blob to disk, then read it with file tools.

This version uses HTTP endpoints on the Context Intelligence Server
instead of the local DiskBlobStore.
"""

from __future__ import annotations

import pathlib
import tempfile

_SEP = "__"  # key separator: <node_id>__<field>
_URI_SCHEME = "ci-blob://"


class BlobTool:
    """Agent-facing tool for blob inspection and materialization via HTTP.

    Connects to the Context Intelligence Server's blob endpoints:
    - GET /blobs/{session_id}       — list blob URIs
    - GET /blobs/{session_id}/{key} — retrieve blob content
    """

    def __init__(self, server_url: str) -> None:
        self._server_url = server_url.rstrip("/")

    async def blob_list(self, session_id: str) -> list[dict]:
        """List blob metadata for all blobs in a session.

        Returns a list of dicts, each containing:
            uri         - ci-blob:// URI
            field       - last component after splitting key on '__'
            node_id     - everything before the last '__' in the key
            size_bytes  - None (file stat not available over HTTP)
        """
        import httpx

        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{self._server_url}/blobs/{session_id}")
            resp.raise_for_status()
            data = resp.json()

        result = []
        for uri in data.get("blobs", []):
            # Extract key from ci-blob://session_id/key
            key = uri[len(_URI_SCHEME):]
            if "/" in key:
                key = key.split("/", 1)[1]

            sep_idx = key.rfind(_SEP)
            if sep_idx == -1:
                node_id = key
                field = "unknown"
            else:
                node_id = key[:sep_idx]
                field = key[sep_idx + len(_SEP):]

            result.append({
                "uri": uri,
                "field": field,
                "node_id": node_id,
                "size_bytes": None,
            })
        return result

    async def blob_dump(self, uri: str, dest_path: str | None = None) -> str:
        """Materialize a blob to disk and return the file path.

        Args:
            uri: A ci-blob:// URI identifying the blob.
            dest_path: Optional destination path.
                Defaults to /tmp/ci-blobs/<key>.json.

        Returns:
            Path where the blob file was written.
        """
        import httpx

        # Parse ci-blob://session_id/key
        rest = uri[len(_URI_SCHEME):]
        session_id, key = rest.split("/", 1)

        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self._server_url}/blobs/{session_id}/{key}"
            )
            resp.raise_for_status()

        if dest_path is None:
            dest_path = str(
                pathlib.Path(tempfile.gettempdir()) / "ci-blobs" / f"{key}.json"
            )

        pathlib.Path(dest_path).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(dest_path).write_text(resp.text)
        return dest_path
```

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence/modules/hook-context-intelligence
pytest tests/test_blob_tool_http.py -v
```

Expected: **ALL PASS** (6 tests)

### Step 5: Commit to feature branch

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence
git add modules/hook-context-intelligence/amplifier_module_hook_context_intelligence/blob_tool.py modules/hook-context-intelligence/tests/test_blob_tool_http.py
git commit -m "feat: update BlobTool to use HTTP endpoints"
```

> **Note:** The old `tests/test_blob_tool.py` tests will now fail because `BlobTool` no longer accepts a `DiskBlobStore`. This is expected — those tests test the old interface. They should be removed or updated in a separate cleanup. For this task, the new `test_blob_tool_http.py` covers the new interface. Do NOT delete the old test file yet — leave it for the cleanup phase.

> **Reminder: DO NOT run `git push`.**

---

## Task 8: Wire BlobTool into Bundle Mount Path (Bundle)

**Working directory:** `/home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence/`
**Branch:** `feat/server-dispatch`

**Files:**
- Modify: `modules/hook-context-intelligence/amplifier_module_hook_context_intelligence/config_resolver.py`
- Modify: `modules/hook-context-intelligence/amplifier_module_hook_context_intelligence/__init__.py`
- Modify: `modules/hook-context-intelligence/tests/test_mount_dispatcher.py`

### Step 1: Write the failing test

Append to `modules/hook-context-intelligence/tests/test_mount_dispatcher.py`:

```python
# ---------------------------------------------------------------------------
# TestBlobToolRegistration
# ---------------------------------------------------------------------------
class TestBlobToolRegistration:
    """BlobTool is registered when server_url is configured."""

    async def test_blob_tool_not_registered_without_server_url(self) -> None:
        """Without server_url, no blob tools are registered."""
        coordinator = _make_coordinator()
        config: dict[str, Any] = {}
        from amplifier_module_hook_context_intelligence import mount

        cleanup = await mount(coordinator, config)

        # Check that no tool registration calls include "blob"
        tool_register_calls = [
            call for call in coordinator.hooks.register.call_args_list
        ]
        # BlobTool registers via coordinator.tools, not coordinator.hooks
        tools_attr = getattr(coordinator, "tools", None)
        if tools_attr is not None:
            register_calls = tools_attr.register.call_args_list if hasattr(tools_attr, "register") else []
            blob_calls = [c for c in register_calls if "blob" in str(c).lower()]
            assert len(blob_calls) == 0

        if cleanup:
            cleanup()

    async def test_blob_tool_registered_with_server_url(self) -> None:
        """With server_url in config, BlobTool methods are registered as tools."""
        coordinator = _make_coordinator()
        coordinator.tools = MagicMock()
        coordinator.tools.register = MagicMock()
        config: dict[str, Any] = {"server_url": "http://localhost:8000"}
        from amplifier_module_hook_context_intelligence import mount

        cleanup = await mount(coordinator, config)

        # Verify blob_list and blob_dump were registered
        register_calls = coordinator.tools.register.call_args_list
        registered_names = [call.args[0] if call.args else call.kwargs.get("name", "") for call in register_calls]
        assert "blob_list" in registered_names
        assert "blob_dump" in registered_names

        if cleanup:
            cleanup()
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence/modules/hook-context-intelligence
pytest tests/test_mount_dispatcher.py::TestBlobToolRegistration -v
```

Expected: **FAIL** — `mount()` does not read `server_url` or register BlobTool.

### Step 3: Add `server_url` property to ConfigResolver

Edit `modules/hook-context-intelligence/amplifier_module_hook_context_intelligence/config_resolver.py`.

Add a new property after the existing `log_level` property:

```python
    @property
    def server_url(self) -> str | None:
        """Context Intelligence Server URL, or None if not configured.

        When set, LoggingHandler dispatches events to the server and
        BlobTool uses HTTP endpoints instead of DiskBlobStore.

        Reads from config['server_url']. No coordinator fallback.
        """
        value = self._config.get("server_url")
        return str(value) if value else None

    @property
    def workspace(self) -> str | None:
        """Workspace name for server dispatch.

        Reads from config['workspace'], falling back to forest_name.
        """
        value = self._config.get("workspace")
        if value:
            return str(value)
        return self.forest_name
```

### Step 4: Wire BlobTool into mount()

Edit `modules/hook-context-intelligence/amplifier_module_hook_context_intelligence/__init__.py`.

Add BlobTool registration after the LoggingHandler registration block. Find this section:

```python
    cleanup_fns.append(_logging_cleanup)

    # -- [CONDITIONAL] GraphDataHook ---------------------------------------
```

Insert the BlobTool block between those two sections:

```python
    cleanup_fns.append(_logging_cleanup)

    # -- [CONDITIONAL] BlobTool (HTTP) -------------------------------------
    if resolver.server_url:
        try:
            from .blob_tool import BlobTool

            blob_tool = BlobTool(server_url=resolver.server_url)
            tools = getattr(coordinator, "tools", None)
            if tools is not None and hasattr(tools, "register"):
                tools.register("blob_list", blob_tool.blob_list, description="List blobs for a session")
                tools.register("blob_dump", blob_tool.blob_dump, description="Retrieve a blob by URI")
                logger.info("BlobTool registered with server_url=%s", resolver.server_url)
        except Exception:
            logger.exception("Failed to register BlobTool; continuing without blob tools")

    # -- [CONDITIONAL] GraphDataHook ---------------------------------------
```

### Step 5: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence/modules/hook-context-intelligence
pytest tests/test_mount_dispatcher.py -v
```

Expected: **ALL PASS** — existing tests unaffected (no `server_url` in config), new tests pass.

### Step 6: Commit to feature branch

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence
git add modules/hook-context-intelligence/amplifier_module_hook_context_intelligence/config_resolver.py modules/hook-context-intelligence/amplifier_module_hook_context_intelligence/__init__.py modules/hook-context-intelligence/amplifier_module_hook_context_intelligence/blob_tool.py modules/hook-context-intelligence/tests/test_mount_dispatcher.py
git commit -m "feat: wire BlobTool into mount path with HTTP endpoints"
```

> **Reminder: DO NOT run `git push`.**

---

## Task 9: Docker Compose Final Polish (Server)

**Working directory:** `/home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence/`

**Files:**
- Modify: `docker-compose.yml`

### Step 1: Read the current docker-compose.yml

Read the file to see the current state (from Phase 1). Expected content:

```yaml
services:
  context-intelligence-server:
    build: .
    ports:
      - "8000:8000"
    environment:
      - CI_SERVER_NEO4J_URL=neo4j://neo4j:7687
      - CI_SERVER_BLOB_PATH=/data/blobs
    volumes:
      - blob_data:/data/blobs
    depends_on:
      - neo4j
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/status"]
      interval: 10s
      timeout: 5s
      retries: 3

  neo4j:
    image: neo4j:5
    environment:
      - NEO4J_AUTH=neo4j/password
    ports:
      - "7474:7474"
    volumes:
      - neo4j_data:/data

volumes:
  blob_data:
  neo4j_data:
```

### Step 2: Replace with polished version

Replace the entire `docker-compose.yml` content with:

```yaml
services:
  context-intelligence-server:
    build: .
    ports:
      - "8000:8000"
    environment:
      - CI_SERVER_NEO4J_URL=neo4j://neo4j:7687
      - CI_SERVER_BLOB_PATH=/data/blobs
      - PYTHONUNBUFFERED=1
    volumes:
      - blob_data:/data/blobs
    depends_on:
      neo4j:
        condition: service_healthy
    restart: unless-stopped
    labels:
      com.context-intelligence.component: server
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/status"]
      interval: 10s
      timeout: 5s
      retries: 3
    networks:
      - context-intelligence

  neo4j:
    image: neo4j:5
    environment:
      - NEO4J_AUTH=neo4j/password
    ports:
      - "7474:7474"    # browser UI — debug/diagnostics only
    # bolt 7687 intentionally NOT exposed — all Cypher goes through POST /cypher
    volumes:
      - neo4j_data:/data
    restart: unless-stopped
    labels:
      com.context-intelligence.component: neo4j
    healthcheck:
      test: ["CMD", "wget", "-O", "-", "http://localhost:7474"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 15s
    networks:
      - context-intelligence

networks:
  context-intelligence:
    driver: bridge

volumes:
  blob_data:
  neo4j_data:
```

### Step 3: Validate the compose file

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
docker compose config
```

Expected: Valid YAML output with no errors. The output should show the resolved configuration with both services, the network, both volumes, healthchecks, restart policies, and labels.

> **Note:** Do NOT run `docker compose up`. This is syntax validation only.

### Step 4: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add docker-compose.yml
git commit -m "feat: phase 4 — docker compose polish (healthchecks, network, labels)"
```

---

## Task 10: Final Integration Test and Commit

**Files:** No new files — verification only.

### Step 1: Run full server test suite

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/ -v
```

Expected: **ALL PASS.** If any test fails, fix it before proceeding.

### Step 2: Final server commit (if needed)

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git status
```

If anything is unstaged:

```bash
git add .
git commit -m "feat: phase 4 — dashboard, observability complete"
```

### Step 3: Run bundle test suite (on feature branch)

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence
git branch  # verify: * feat/server-dispatch
```

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence/modules/hook-context-intelligence
pytest tests/test_logging_handler.py tests/test_logging_handler_server_dispatch.py tests/test_blob_tool_http.py tests/test_mount_dispatcher.py tests/test_config_resolver.py -v
```

Expected: **ALL PASS** for the listed test files.

> **Note:** `tests/test_blob_tool.py` (the old DiskBlobStore-based tests) will fail because `BlobTool` no longer accepts a `DiskBlobStore`. This is expected and intentional — those tests cover the old interface that has been replaced. Do not fix them in this phase.

### Step 4: Final bundle commit (if needed)

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence
git status
```

If anything is unstaged:

```bash
git add .
git commit -m "feat: phase 4 — bundle server dispatch integration"
```

### Step 5: Verify git logs for both repos

**Server:**
```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git log --oneline -6
```

Expected: Phase 4 commits with `feat: phase 4 —` prefixes.

**Bundle:**
```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-bundle-context-intelligence
git log --oneline -5
```

Expected: Phase 4 bundle commits on `feat/server-dispatch`.

> **⚠️ FINAL REMINDER: The `feat/server-dispatch` branch is NOT pushed. Push and PR creation is deferred to after end-to-end validation of the full server stack.**

---

## Summary

| Task | What It Builds | Tests Added | Repo |
|------|---------------|-------------|------|
| 1 | Feature branch `feat/server-dispatch` | 0 | Bundle |
| 2 | SessionWorker activity tracking (last_event, events_processed) | 3 | Server |
| 3 | `dashboard.py` — EventRingBuffer + build_status_response | 8 | Server |
| 4 | Ring buffer wired into drain loop | 1 | Server |
| 5 | Enriched `GET /status` + `GET /` HTML dashboard | 3 | Server |
| 6 | LoggingHandler server dispatch (fire-and-forget) | 5 | Bundle |
| 7 | BlobTool → HTTP client | 6 | Bundle |
| 8 | BlobTool wired into mount() + ConfigResolver.server_url | 2 | Bundle |
| 9 | Docker Compose polish (healthchecks, network, labels) | 0 | Server |
| 10 | Final integration verification | 0 | Both |
| **Total** | | **28 tests** | |

---

## Key Constraints Checklist

- [x] Bundle work on `feat/server-dispatch` branch — never on main
- [x] Bundle branch is NOT pushed — push deferred to /finish phase
- [x] `asyncio.create_task` for server dispatch — fire-and-forget, not `await`
- [x] BlobTool drops `DiskBlobStore` dependency — pure HTTP client
- [x] `BlobTool.blob_list()` returns `size_bytes: None` — acceptable for HTTP
- [x] Dashboard is vanilla HTML/JS — no frameworks, no build step
- [x] `GET /status` serves as Docker Compose healthcheck — returns 200 reliably
