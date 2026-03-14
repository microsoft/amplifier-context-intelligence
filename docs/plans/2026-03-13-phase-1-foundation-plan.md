# Context Intelligence Server — Phase 1: Foundation

> **For execution:** Use `/execute-plan` mode or the subagent-driven-development recipe.

**Goal:** Scaffold the Context Intelligence Server with a working FastAPI skeleton, per-session asyncio queue, and Docker Compose stack.

**Architecture:** Single-process FastAPI app with a SessionRegistry holding per-session SessionWorker instances. Events POST to /events, get enqueued to the session's asyncio.Queue, and drained by a background coroutine. Phase 1 drain loop logs only — real processing is Phase 2.

**Tech Stack:** Python 3.11, FastAPI, Pydantic v2, uvicorn, pytest-asyncio, Docker Compose, Neo4j 5.x

---

## Repo Context

This is a **greenfield repo**. There is no existing code — only `README.md` and `docs/plans/`. All files below are created from scratch.

**Root directory:** `/home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence/`

All relative paths in this plan are relative to that root.

---

## Final Directory Structure

After all 10 tasks, the repo looks like this:

```
.
├── pyproject.toml
├── Dockerfile
├── docker-compose.yml
├── context_intelligence_server/
│   ├── __init__.py
│   ├── config.py
│   ├── models.py
│   ├── registry.py
│   └── main.py
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── test_config.py
    ├── test_models.py
    ├── test_registry.py
    └── test_main.py
```

---

## Task 1: Package Scaffold

**Files:**
- Create: `pyproject.toml`
- Create: `context_intelligence_server/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`
- Create: `tests/test_config.py`

### Step 1: Create `pyproject.toml`

Create file `pyproject.toml`:

```python
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.backends"

[project]
name = "context-intelligence-server"
version = "0.1.0"
description = "Context Intelligence Server — graph processing and blob management for Amplifier"
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.115.0",
    "uvicorn[standard]>=0.30.0",
    "pydantic-settings>=2.0.0",
    "httpx>=0.27.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.24.0",
    "httpx>=0.27.0",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

### Step 2: Create `context_intelligence_server/__init__.py`

Create file `context_intelligence_server/__init__.py`:

```python
"""Context Intelligence Server."""

__version__ = "0.1.0"
```

### Step 3: Create `tests/__init__.py`

Create file `tests/__init__.py`:

```python
```

(Empty file — just makes `tests/` a package.)

### Step 4: Create `tests/conftest.py`

Create file `tests/conftest.py`:

```python
"""Shared test fixtures for context_intelligence_server tests."""
```

### Step 5: Write the failing test

Create file `tests/test_config.py`:

```python
"""Tests for package identity and configuration."""


def test_package_version():
    from context_intelligence_server import __version__

    assert __version__ == "0.1.0"
```

### Step 6: Install the package and run the test

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pip install -e ".[dev]"
pytest tests/test_config.py::test_package_version -v
```

Expected: **PASS** — the package installs and the version is correct.

### Step 7: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add pyproject.toml context_intelligence_server/__init__.py tests/__init__.py tests/conftest.py tests/test_config.py
git commit -m "feat: phase 1 — package scaffold with pyproject.toml"
```

---

## Task 2: Configuration via Pydantic Settings

**Files:**
- Create: `context_intelligence_server/config.py`
- Modify: `tests/test_config.py`

### Step 1: Write the failing tests

Append to `tests/test_config.py`:

```python
import os
from unittest.mock import patch


def test_settings_defaults():
    """Settings load with sensible defaults when no env vars are set."""
    from context_intelligence_server.config import Settings

    settings = Settings()
    assert settings.server_host == "0.0.0.0"
    assert settings.server_port == 8000
    assert settings.neo4j_url == "neo4j://neo4j:7687"
    assert settings.neo4j_user == "neo4j"
    assert settings.neo4j_password == "password"
    assert settings.blob_path == "/data/blobs"
    assert settings.log_level == "INFO"


def test_settings_env_override():
    """Settings can be overridden via CI_SERVER_ prefixed env vars."""
    from context_intelligence_server.config import Settings

    with patch.dict(os.environ, {"CI_SERVER_SERVER_PORT": "9999", "CI_SERVER_LOG_LEVEL": "DEBUG"}):
        settings = Settings()
    assert settings.server_port == 9999
    assert settings.log_level == "DEBUG"


def test_get_settings_returns_instance():
    """get_settings() returns a Settings instance (cached)."""
    from context_intelligence_server.config import get_settings

    s = get_settings()
    assert s.server_host == "0.0.0.0"
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_config.py -v -k "not test_package_version"
```

Expected: **FAIL** — `ModuleNotFoundError: No module named 'context_intelligence_server.config'`

### Step 3: Write the implementation

Create file `context_intelligence_server/config.py`:

```python
"""Application configuration via Pydantic Settings."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Server configuration.

    All fields can be overridden via environment variables
    prefixed with ``CI_SERVER_``. For example, ``CI_SERVER_SERVER_PORT=9999``.
    """

    model_config = SettingsConfigDict(env_prefix="CI_SERVER_")

    server_host: str = "0.0.0.0"
    server_port: int = 8000
    neo4j_url: str = "neo4j://neo4j:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"
    blob_path: str = "/data/blobs"
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    """Return the cached application settings singleton."""
    return Settings()
```

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_config.py -v
```

Expected: **ALL PASS** (4 tests: `test_package_version`, `test_settings_defaults`, `test_settings_env_override`, `test_get_settings_returns_instance`)

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/config.py tests/test_config.py
git commit -m "feat: phase 1 — pydantic settings configuration"
```

---

## Task 3: Pydantic Models

**Files:**
- Create: `context_intelligence_server/models.py`
- Create: `tests/test_models.py`

### Step 1: Write the failing tests

Create file `tests/test_models.py`:

```python
"""Tests for Pydantic request/response models."""

import pytest
from pydantic import ValidationError


def test_event_request_valid():
    """EventRequest parses a well-formed payload."""
    from context_intelligence_server.models import EventRequest

    req = EventRequest(
        event="tool:pre",
        workspace="my-feature-branch",
        data={"session_id": "abc-123", "timestamp": "2026-03-13T14:30:00Z"},
    )
    assert req.event == "tool:pre"
    assert req.workspace == "my-feature-branch"
    assert req.data["session_id"] == "abc-123"


def test_event_request_missing_event():
    """EventRequest rejects payload without required 'event' field."""
    from context_intelligence_server.models import EventRequest

    with pytest.raises(ValidationError):
        EventRequest(workspace="ws", data={})


def test_event_request_missing_workspace():
    """EventRequest rejects payload without required 'workspace' field."""
    from context_intelligence_server.models import EventRequest

    with pytest.raises(ValidationError):
        EventRequest(event="tool:pre", data={})


def test_event_request_data_without_session_id():
    """EventRequest accepts data dict that has no session_id — extraction is server-side."""
    from context_intelligence_server.models import EventRequest

    req = EventRequest(event="session:start", workspace="ws", data={"foo": "bar"})
    assert "session_id" not in req.data


def test_event_response_defaults():
    """EventResponse has sensible defaults."""
    from context_intelligence_server.models import EventResponse

    resp = EventResponse(session_id="abc-123")
    assert resp.status == "queued"
    assert resp.session_id == "abc-123"


def test_event_response_null_session():
    """EventResponse allows null session_id."""
    from context_intelligence_server.models import EventResponse

    resp = EventResponse(session_id=None)
    assert resp.session_id is None


def test_status_response():
    """StatusResponse carries all required fields."""
    from context_intelligence_server.models import StatusResponse

    resp = StatusResponse(status="ok", uptime_seconds=42.5, active_sessions=3)
    assert resp.status == "ok"
    assert resp.uptime_seconds == 42.5
    assert resp.active_sessions == 3
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_models.py -v
```

Expected: **FAIL** — `ModuleNotFoundError: No module named 'context_intelligence_server.models'`

### Step 3: Write the implementation

Create file `context_intelligence_server/models.py`:

```python
"""Pydantic request and response models for the Context Intelligence Server API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class EventRequest(BaseModel):
    """Incoming event envelope from the bundle's LoggingHandler."""

    event: str
    workspace: str
    data: dict[str, Any]


class EventResponse(BaseModel):
    """Response to POST /events."""

    status: str = "queued"
    session_id: str | None = None


class StatusResponse(BaseModel):
    """Response to GET /status."""

    status: str
    uptime_seconds: float
    active_sessions: int
```

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_models.py -v
```

Expected: **ALL PASS** (7 tests)

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/models.py tests/test_models.py
git commit -m "feat: phase 1 — pydantic request/response models"
```

---

## Task 4: SessionRegistry and SessionWorker

**Files:**
- Create: `context_intelligence_server/registry.py`
- Create: `tests/test_registry.py`

### Step 1: Write the failing tests

Create file `tests/test_registry.py`:

```python
"""Tests for SessionRegistry and SessionWorker."""

import pytest

from context_intelligence_server.registry import SessionRegistry


@pytest.fixture
def registry() -> SessionRegistry:
    return SessionRegistry()


async def test_get_or_create_new_worker(registry: SessionRegistry):
    """First call for a session_id creates a new worker."""
    worker = registry.get_or_create("sess-1", "my-workspace")
    assert worker.session_id == "sess-1"
    assert worker.workspace == "my-workspace"
    assert worker.queue.empty()
    assert worker.task is None


async def test_get_or_create_returns_same_worker(registry: SessionRegistry):
    """Second call for the same session_id returns the existing worker."""
    w1 = registry.get_or_create("sess-1", "ws")
    w2 = registry.get_or_create("sess-1", "ws")
    assert w1 is w2


async def test_active_count(registry: SessionRegistry):
    """active_count reflects the number of registered workers."""
    assert registry.active_count() == 0
    registry.get_or_create("sess-1", "ws")
    assert registry.active_count() == 1
    registry.get_or_create("sess-2", "ws")
    assert registry.active_count() == 2


async def test_active_sessions(registry: SessionRegistry):
    """active_sessions returns list of registered session IDs."""
    registry.get_or_create("sess-a", "ws")
    registry.get_or_create("sess-b", "ws")
    assert sorted(registry.active_sessions()) == ["sess-a", "sess-b"]


async def test_remove(registry: SessionRegistry):
    """remove deletes a worker from the registry."""
    registry.get_or_create("sess-1", "ws")
    assert registry.active_count() == 1
    registry.remove("sess-1")
    assert registry.active_count() == 0


async def test_remove_nonexistent_is_noop(registry: SessionRegistry):
    """Removing a session_id that doesn't exist does not raise."""
    registry.remove("does-not-exist")  # should not raise
    assert registry.active_count() == 0


async def test_queue_put_get(registry: SessionRegistry):
    """Worker's asyncio.Queue supports put/get round-trip."""
    worker = registry.get_or_create("sess-1", "ws")
    await worker.queue.put(("tool:pre", "ws", {"session_id": "sess-1"}))
    event, workspace, data = await worker.queue.get()
    assert event == "tool:pre"
    assert workspace == "ws"
    assert data["session_id"] == "sess-1"
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_registry.py -v
```

Expected: **FAIL** — `ModuleNotFoundError: No module named 'context_intelligence_server.registry'`

### Step 3: Write the implementation

Create file `context_intelligence_server/registry.py`:

```python
"""Session registry and per-session worker management."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field


@dataclass
class SessionWorker:
    """Holds per-session state: an asyncio queue and a background drain task.

    The ``task`` field is ``None`` until the drain loop is started by the
    application lifespan (wired in ``main.py``).
    """

    session_id: str
    workspace: str
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    task: asyncio.Task | None = None


class SessionRegistry:
    """Registry of active session workers, keyed by session_id."""

    def __init__(self) -> None:
        self._workers: dict[str, SessionWorker] = {}

    def get_or_create(self, session_id: str, workspace: str) -> SessionWorker:
        """Return existing worker or create a new one for *session_id*."""
        if session_id not in self._workers:
            self._workers[session_id] = SessionWorker(
                session_id=session_id,
                workspace=workspace,
            )
        return self._workers[session_id]

    def remove(self, session_id: str) -> None:
        """Remove a worker from the registry. No-op if not found."""
        self._workers.pop(session_id, None)

    def active_count(self) -> int:
        """Return the number of active workers."""
        return len(self._workers)

    def active_sessions(self) -> list[str]:
        """Return a list of active session IDs."""
        return list(self._workers.keys())
```

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_registry.py -v
```

Expected: **ALL PASS** (7 tests)

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/registry.py tests/test_registry.py
git commit -m "feat: phase 1 — session registry and worker dataclass"
```

---

## Task 5: FastAPI App Skeleton — GET /status

**Files:**
- Create: `context_intelligence_server/main.py`
- Create: `tests/test_main.py`

### Step 1: Write the failing tests

Create file `tests/test_main.py`:

```python
"""Tests for the FastAPI application routes."""

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def client():
    from context_intelligence_server.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


async def test_status_returns_200(client: AsyncClient):
    """GET /status returns 200."""
    resp = await client.get("/status")
    assert resp.status_code == 200


async def test_status_body(client: AsyncClient):
    """GET /status returns expected JSON shape."""
    resp = await client.get("/status")
    body = resp.json()
    assert body["status"] == "ok"
    assert body["uptime_seconds"] >= 0
    assert body["active_sessions"] == 0
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_main.py -v
```

Expected: **FAIL** — `ModuleNotFoundError: No module named 'context_intelligence_server.main'`

### Step 3: Write the implementation

Create file `context_intelligence_server/main.py`:

```python
"""FastAPI application for the Context Intelligence Server."""

from __future__ import annotations

import time

from fastapi import FastAPI

from context_intelligence_server.models import StatusResponse
from context_intelligence_server.registry import SessionRegistry

app = FastAPI(title="Context Intelligence Server")

_start_time: float = time.time()
registry = SessionRegistry()


@app.get("/status", response_model=StatusResponse)
async def status() -> StatusResponse:
    """Health check endpoint. Used as Docker Compose healthcheck."""
    return StatusResponse(
        status="ok",
        uptime_seconds=time.time() - _start_time,
        active_sessions=registry.active_count(),
    )
```

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_main.py -v
```

Expected: **ALL PASS** (2 tests)

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/main.py tests/test_main.py
git commit -m "feat: phase 1 — FastAPI app with GET /status"
```

---

## Task 6: POST /events Endpoint

**Files:**
- Modify: `context_intelligence_server/main.py`
- Modify: `tests/test_main.py`

### Step 1: Write the failing tests

Append to `tests/test_main.py`:

```python
async def test_post_events_returns_202(client: AsyncClient):
    """POST /events with valid payload returns 202 Accepted."""
    payload = {
        "event": "tool:pre",
        "workspace": "my-workspace",
        "data": {"session_id": "sess-1", "timestamp": "2026-03-13T14:30:00Z"},
    }
    resp = await client.post("/events", json=payload)
    assert resp.status_code == 202


async def test_post_events_body(client: AsyncClient):
    """POST /events returns queued status and session_id."""
    payload = {
        "event": "tool:pre",
        "workspace": "ws",
        "data": {"session_id": "sess-1"},
    }
    resp = await client.post("/events", json=payload)
    body = resp.json()
    assert body["status"] == "queued"
    assert body["session_id"] == "sess-1"


async def test_post_events_increments_active_sessions(client: AsyncClient):
    """After POST /events, active_sessions reflects the new worker."""
    payload = {
        "event": "session:start",
        "workspace": "ws",
        "data": {"session_id": "sess-new"},
    }
    await client.post("/events", json=payload)
    resp = await client.get("/status")
    assert resp.json()["active_sessions"] >= 1


async def test_post_events_missing_event_returns_422(client: AsyncClient):
    """POST /events without 'event' field returns 422 Unprocessable Entity."""
    payload = {"workspace": "ws", "data": {}}
    resp = await client.post("/events", json=payload)
    assert resp.status_code == 422


async def test_post_events_no_session_id_returns_null(client: AsyncClient):
    """POST /events with data lacking session_id returns session_id=null."""
    payload = {
        "event": "tool:pre",
        "workspace": "ws",
        "data": {"some_field": "value"},
    }
    resp = await client.post("/events", json=payload)
    body = resp.json()
    assert resp.status_code == 202
    assert body["session_id"] is None
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_main.py -v -k "post_events"
```

Expected: **FAIL** — `404 Not Found` (POST /events route does not exist yet)

### Step 3: Add the endpoint to `context_intelligence_server/main.py`

Add these imports at the top of `main.py` (alongside the existing ones):

```python
from context_intelligence_server.models import EventRequest, EventResponse
```

Add this route after the existing `/status` route:

```python
@app.post("/events", status_code=202, response_model=EventResponse)
async def ingest_event(request: EventRequest) -> EventResponse:
    """Ingest an event from the bundle. Enqueues to the session's worker queue."""
    session_id = request.data.get("session_id", "")
    worker = registry.get_or_create(session_id, request.workspace)
    await worker.queue.put((request.event, request.workspace, request.data))
    return EventResponse(status="queued", session_id=session_id or None)
```

The full updated import block at the top of `main.py` should be:

```python
from context_intelligence_server.models import EventRequest, EventResponse, StatusResponse
```

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_main.py -v
```

Expected: **ALL PASS** (7 tests)

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/main.py tests/test_main.py
git commit -m "feat: phase 1 — POST /events endpoint with queue ingestion"
```

---

## Task 7: Background Drain Loop (Placeholder)

**Files:**
- Modify: `context_intelligence_server/main.py`
- Modify: `context_intelligence_server/registry.py`
- Modify: `tests/test_main.py`

### Step 1: Write the failing test

Append to `tests/test_main.py`:

```python
import asyncio


async def test_drain_loop_processes_event(client: AsyncClient):
    """After POST /events, the drain loop eventually processes the queued event."""
    from context_intelligence_server.main import registry

    payload = {
        "event": "tool:pre",
        "workspace": "ws",
        "data": {"session_id": "drain-test"},
    }
    await client.post("/events", json=payload)

    worker = registry.get_or_create("drain-test", "ws")

    # Wait for the queue to drain (the background task consumes it)
    try:
        await asyncio.wait_for(worker.queue.join(), timeout=5.0)
    except asyncio.TimeoutError:
        pytest.fail("Drain loop did not process the event within 5 seconds")

    assert worker.queue.empty()
```

### Step 2: Run tests to verify it fails

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_main.py::test_drain_loop_processes_event -v
```

Expected: **FAIL** — `queue.join()` times out because no drain loop is running.

### Step 3: Add the drain loop to `main.py` and start task in `registry.py`

First, add a `start_drain` method to `context_intelligence_server/registry.py`. Add these imports at the top:

```python
import logging

logger = logging.getLogger("context_intelligence_server")
```

Add this function at module level (after the `SessionWorker` dataclass, before `SessionRegistry`):

```python
async def drain_worker(worker: SessionWorker) -> None:
    """Drain loop: dequeue events and log them.

    Phase 1 placeholder — real handler dispatch is Phase 2.
    """
    while True:
        try:
            event, workspace, data = await worker.queue.get()
            session_id = data.get("session_id", "unknown")
            logger.info(
                "event_received: event=%s session_id=%s workspace=%s",
                event,
                session_id,
                workspace,
            )
        except Exception:
            logger.exception("drain_worker error for session=%s", worker.session_id)
        finally:
            worker.queue.task_done()
```

Add this method to `SessionRegistry`:

```python
    def start_drain(self, worker: SessionWorker) -> None:
        """Start the background drain task for a worker if not already running."""
        if worker.task is None or worker.task.done():
            worker.task = asyncio.create_task(
                drain_worker(worker),
                name=f"drain-{worker.session_id}",
            )
```

Update `get_or_create` to auto-start the drain task. Replace the existing method body:

```python
    def get_or_create(self, session_id: str, workspace: str) -> SessionWorker:
        """Return existing worker or create a new one for *session_id*."""
        if session_id not in self._workers:
            worker = SessionWorker(
                session_id=session_id,
                workspace=workspace,
            )
            self._workers[session_id] = worker
            self.start_drain(worker)
        return self._workers[session_id]
```

The full final `context_intelligence_server/registry.py` after this task:

```python
"""Session registry and per-session worker management."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

logger = logging.getLogger("context_intelligence_server")


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


async def drain_worker(worker: SessionWorker) -> None:
    """Drain loop: dequeue events and log them.

    Phase 1 placeholder — real handler dispatch is Phase 2.
    """
    while True:
        try:
            event, workspace, data = await worker.queue.get()
            session_id = data.get("session_id", "unknown")
            logger.info(
                "event_received: event=%s session_id=%s workspace=%s",
                event,
                session_id,
                workspace,
            )
        except Exception:
            logger.exception("drain_worker error for session=%s", worker.session_id)
        finally:
            worker.queue.task_done()


class SessionRegistry:
    """Registry of active session workers, keyed by session_id."""

    def __init__(self) -> None:
        self._workers: dict[str, SessionWorker] = {}

    def get_or_create(self, session_id: str, workspace: str) -> SessionWorker:
        """Return existing worker or create a new one for *session_id*."""
        if session_id not in self._workers:
            worker = SessionWorker(
                session_id=session_id,
                workspace=workspace,
            )
            self._workers[session_id] = worker
            self.start_drain(worker)
        return self._workers[session_id]

    def start_drain(self, worker: SessionWorker) -> None:
        """Start the background drain task for a worker if not already running."""
        if worker.task is None or worker.task.done():
            worker.task = asyncio.create_task(
                drain_worker(worker),
                name=f"drain-{worker.session_id}",
            )

    def remove(self, session_id: str) -> None:
        """Remove a worker from the registry. Cancel its drain task if running."""
        worker = self._workers.pop(session_id, None)
        if worker and worker.task and not worker.task.done():
            worker.task.cancel()

    def active_count(self) -> int:
        """Return the number of active workers."""
        return len(self._workers)

    def active_sessions(self) -> list[str]:
        """Return a list of active session IDs."""
        return list(self._workers.keys())
```

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_main.py -v
pytest tests/test_registry.py -v
```

Expected: **ALL PASS** for both test files. The drain loop test should complete well within the 5-second timeout.

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/registry.py tests/test_main.py
git commit -m "feat: phase 1 — background drain loop with placeholder logging"
```

---

## Task 8: Structured JSON Logging

**Files:**
- Modify: `context_intelligence_server/main.py`

### Step 1: Add logging configuration to `main.py`

Add this import at the top of `context_intelligence_server/main.py`:

```python
import logging
```

Add this block **before** the `app = FastAPI(...)` line:

```python
from context_intelligence_server.config import get_settings

_settings = get_settings()

logging.basicConfig(
    level=_settings.log_level,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}',
)
logger = logging.getLogger("context_intelligence_server")
```

The full updated `context_intelligence_server/main.py` after this task:

```python
"""FastAPI application for the Context Intelligence Server."""

from __future__ import annotations

import logging
import time

from fastapi import FastAPI

from context_intelligence_server.config import get_settings
from context_intelligence_server.models import EventRequest, EventResponse, StatusResponse
from context_intelligence_server.registry import SessionRegistry

_settings = get_settings()

logging.basicConfig(
    level=_settings.log_level,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}',
)
logger = logging.getLogger("context_intelligence_server")

app = FastAPI(title="Context Intelligence Server")

_start_time: float = time.time()
registry = SessionRegistry()


@app.get("/status", response_model=StatusResponse)
async def status() -> StatusResponse:
    """Health check endpoint. Used as Docker Compose healthcheck."""
    return StatusResponse(
        status="ok",
        uptime_seconds=time.time() - _start_time,
        active_sessions=registry.active_count(),
    )


@app.post("/events", status_code=202, response_model=EventResponse)
async def ingest_event(request: EventRequest) -> EventResponse:
    """Ingest an event from the bundle. Enqueues to the session's worker queue."""
    session_id = request.data.get("session_id", "")
    worker = registry.get_or_create(session_id, request.workspace)
    await worker.queue.put((request.event, request.workspace, request.data))
    logger.info("event_enqueued: event=%s session_id=%s", request.event, session_id)
    return EventResponse(status="queued", session_id=session_id or None)
```

### Step 2: Run existing tests to confirm nothing broke

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/ -v
```

Expected: **ALL PASS** — logging configuration does not break any existing tests.

### Step 3: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/main.py
git commit -m "feat: phase 1 — structured JSON logging"
```

---

## Task 9: Dockerfile and docker-compose.yml

**Files:**
- Create: `Dockerfile`
- Create: `docker-compose.yml`

### Step 1: Create `Dockerfile`

Create file `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install curl for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY context_intelligence_server/ ./context_intelligence_server/

RUN pip install --no-cache-dir .

EXPOSE 8000

CMD ["uvicorn", "context_intelligence_server.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Step 2: Create `docker-compose.yml`

Create file `docker-compose.yml`:

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
      - "7474:7474"    # browser UI — debug/diagnostics only
    # bolt 7687 intentionally NOT exposed — all Cypher goes through POST /cypher
    volumes:
      - neo4j_data:/data

volumes:
  blob_data:
  neo4j_data:
```

### Step 3: Validate the compose file

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
docker compose config
```

Expected: The compose file is valid — outputs the resolved configuration with no errors.

> **Note:** Do NOT run `docker compose up` here. The compose file is validated syntactically only. Building and running the full stack is a manual verification step.

### Step 4: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add Dockerfile docker-compose.yml
git commit -m "feat: phase 1 — Dockerfile and docker-compose.yml"
```

---

## Task 10: Final Test Suite and Commit

**Files:**
- No new files — verification only.

### Step 1: Run the full test suite

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/ -v
```

Expected output — **all tests pass**:

```
tests/test_config.py::test_package_version PASSED
tests/test_config.py::test_settings_defaults PASSED
tests/test_config.py::test_settings_env_override PASSED
tests/test_config.py::test_get_settings_returns_instance PASSED
tests/test_models.py::test_event_request_valid PASSED
tests/test_models.py::test_event_request_missing_event PASSED
tests/test_models.py::test_event_request_missing_workspace PASSED
tests/test_models.py::test_event_request_data_without_session_id PASSED
tests/test_models.py::test_event_response_defaults PASSED
tests/test_models.py::test_event_response_null_session PASSED
tests/test_models.py::test_status_response PASSED
tests/test_registry.py::test_get_or_create_new_worker PASSED
tests/test_registry.py::test_get_or_create_returns_same_worker PASSED
tests/test_registry.py::test_active_count PASSED
tests/test_registry.py::test_active_sessions PASSED
tests/test_registry.py::test_remove PASSED
tests/test_registry.py::test_remove_nonexistent_is_noop PASSED
tests/test_registry.py::test_queue_put_get PASSED
tests/test_main.py::test_status_returns_200 PASSED
tests/test_main.py::test_status_body PASSED
tests/test_main.py::test_post_events_returns_202 PASSED
tests/test_main.py::test_post_events_body PASSED
tests/test_main.py::test_post_events_increments_active_sessions PASSED
tests/test_main.py::test_post_events_missing_event_returns_422 PASSED
tests/test_main.py::test_post_events_no_session_id_returns_null PASSED
tests/test_main.py::test_drain_loop_processes_event PASSED
```

**26 tests total**, all passing.

### Step 2: Check for any unstaged files

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git status
```

If anything is unstaged, stage and commit:

```bash
git add .
git commit -m "feat: phase 1 — context intelligence server foundation (complete)"
```

### Step 3: Verify git log

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git log --oneline -10
```

Expected: 9 commits from this phase (Tasks 1–9), each with a `feat: phase 1 —` prefix.

---

## Summary

| Task | What It Builds | Tests Added |
|------|---------------|-------------|
| 1 | `pyproject.toml`, package `__init__.py`, test scaffold | 1 |
| 2 | `config.py` — Pydantic Settings with `CI_SERVER_` env prefix | 3 |
| 3 | `models.py` — EventRequest, EventResponse, StatusResponse | 7 |
| 4 | `registry.py` — SessionRegistry + SessionWorker dataclass | 7 |
| 5 | `main.py` — FastAPI app + `GET /status` | 2 |
| 6 | `POST /events` endpoint with queue ingestion | 5 |
| 7 | Background drain loop (placeholder logging) | 1 |
| 8 | Structured JSON logging configuration | 0 (existing tests verify no regression) |
| 9 | `Dockerfile` + `docker-compose.yml` | 0 (manual validation) |
| 10 | Final test suite run + clean commit | 0 (verification only) |
| **Total** | | **26 tests** |

---

## What Phase 2 Will Build (Not In Scope Here)

- Event handler dispatch (7 handlers moved from bundle)
- `HookStateService` and `SessionCursors`
- Neo4j writes via `Neo4jGraphStore`
- Blob storage via `DiskBlobStore` with `asyncio.to_thread`
- `POST /cypher` proxy endpoint
- `GET /blobs/{session_id}/{key}` and `GET /blobs/{session_id}` endpoints
- 30-second periodic flush timer
- Terminal event detection and worker cleanup
