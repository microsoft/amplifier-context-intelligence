# Exploration System Phase 1: Infrastructure + WebSocket Bridge

> **Execution:** Use the subagent-driven-development workflow to implement this plan.

**Goal:** Build the intelligence service infrastructure (Docker Compose expansion, Dockerfiles, config) and the thin WebSocket bridge service that will connect the future A2UI frontend to Amplifier sessions.

**Architecture:** The intelligence service is a separate Python package (`intelligence_service/`) providing a FastAPI app with a WebSocket endpoint (`/ws`), health check (`/health`), and bundle reload stub (`/admin/reload-bundle`). It manages WebSocket connections via a session manager (Protocol-based for future swapping), translates messages through an A2UI bridge module, and supports graceful shutdown via a drain manager. The Docker Compose stack expands from 2 services to 4: ingestion server, intelligence service, frontend (nginx placeholder), and Neo4j.

**Tech Stack:** Python 3.11+, FastAPI, Pydantic Settings, pytest + httpx + Starlette TestClient, Docker Compose, nginx:alpine

---

## Working Directory

All file paths in this plan are relative to the `amplifier-context-intelligence` submodule:

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
```

Verify you are on the feature branch:

```bash
git branch --show-current
# Expected: feat/exploration-system
```

## Setup

Before starting, install the existing project's dev dependencies so both packages are importable:

```bash
pip install -e ".[dev]"
```

The `intelligence_service/` package lives at the repo root as a sibling to `context_intelligence_server/`. It is importable directly without separate `pip install` because pytest adds the repo root to `sys.path`. The inner `intelligence_service/pyproject.toml` exists solely for Docker image builds.

---

### Task 1: Package Scaffolding

**Files:**
- Create: `intelligence_service/pyproject.toml`
- Create: `intelligence_service/__init__.py`
- Create: `intelligence_service/__main__.py`
- Create: `tests/intelligence_service/__init__.py`
- Create: `tests/intelligence_service/conftest.py`

No TDD for boilerplate scaffolding.

**Step 1: Create `intelligence_service/pyproject.toml`**

This file is used only by `Dockerfile.intelligence` to build the Docker image. For local development, the package is importable directly from the repo root.

```python
# File: intelligence_service/pyproject.toml
```

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "intelligence-service"
version = "0.1.0"
description = "WebSocket bridge for the Context Intelligence exploration system"
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.115.0",
    "uvicorn[standard]>=0.30.0",
    "pydantic-settings>=2.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.24.0",
    "httpx>=0.27.0",
]

[tool.hatch.build.targets.wheel]
packages = ["intelligence_service"]
```

**Step 2: Create `intelligence_service/__init__.py`**

```python
"""Intelligence Service - WebSocket bridge for the Context Intelligence exploration system."""

__version__ = "0.1.0"
```

**Step 3: Create `intelligence_service/__main__.py`**

```python
"""Entry point for the Intelligence Service."""

import uvicorn

from intelligence_service.config import get_settings


def main() -> None:
    """Start the Intelligence Service."""
    settings = get_settings()
    uvicorn.run(
        "intelligence_service.app:app",
        host=settings.server_host,
        port=settings.server_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
```

**Step 4: Create `tests/intelligence_service/__init__.py`**

Empty file:

```python
```

**Step 5: Create `tests/intelligence_service/conftest.py`**

```python
"""Pytest configuration for intelligence_service tests."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from intelligence_service.app import app


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Async HTTP client for testing Intelligence Service REST endpoints."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as c:
        yield c
```

**Step 6: Verify the package is importable**

Run:

```bash
python -c "import intelligence_service; print(intelligence_service.__version__)"
```

Expected: `0.1.0`

**Step 7: Commit**

```bash
git add intelligence_service/ tests/intelligence_service/ && git commit -m "feat(intelligence-service): scaffold package structure"
```

---

### Task 2: Configuration Module

**Files:**
- Create: `intelligence_service/config.py`
- Create: `tests/intelligence_service/test_config.py`

**Step 1: Write the failing test**

Create `tests/intelligence_service/test_config.py`:

```python
"""Tests for intelligence_service configuration."""

from intelligence_service.config import Settings, get_settings


def test_settings_defaults():
    """Settings should have correct default values."""
    s = Settings()
    assert s.server_host == "0.0.0.0"
    assert s.server_port == 8100
    assert s.ingestion_server_url == "http://context-intelligence-server:8000"
    assert s.bundle_name == "context-intelligence-server"
    assert s.drain_timeout_seconds == 30
    assert s.max_sessions == 50
    assert s.blob_path == "/data/blobs"
    assert s.log_level == "INFO"


def test_settings_env_override(monkeypatch):
    """Environment variables with INTEL_SERVICE_ prefix should override defaults."""
    monkeypatch.setenv("INTEL_SERVICE_SERVER_PORT", "9999")
    monkeypatch.setenv("INTEL_SERVICE_LOG_LEVEL", "DEBUG")

    s = Settings()
    assert s.server_port == 9999
    assert s.log_level == "DEBUG"


def test_get_settings_returns_instance():
    """get_settings() should return a Settings instance."""
    get_settings.cache_clear()

    settings = get_settings()
    assert isinstance(settings, Settings)
    assert settings.server_port == 8100
```

**Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/intelligence_service/test_config.py -v
```

Expected: FAIL (ImportError — `intelligence_service.config` does not exist yet)

**Step 3: Write minimal implementation**

Create `intelligence_service/config.py`:

```python
"""Configuration via pydantic-settings for the Intelligence Service."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_prefix="INTEL_SERVICE_")

    server_host: str = "0.0.0.0"
    server_port: int = 8100
    ingestion_server_url: str = "http://context-intelligence-server:8000"
    bundle_name: str = "context-intelligence-server"
    drain_timeout_seconds: int = 30
    max_sessions: int = 50
    blob_path: str = "/data/blobs"
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    """Return cached Settings instance."""
    return Settings()
```

**Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/intelligence_service/test_config.py -v
```

Expected: 3 passed

**Step 5: Commit**

```bash
git add intelligence_service/config.py tests/intelligence_service/test_config.py && git commit -m "feat(intelligence-service): add configuration module with pydantic settings"
```

---

### Task 3: Session Manager

**Files:**
- Create: `intelligence_service/session_manager.py`
- Create: `tests/intelligence_service/test_session_manager.py`

**Step 1: Write the failing tests**

Create `tests/intelligence_service/test_session_manager.py`:

```python
"""Tests for session manager."""

from intelligence_service.session_manager import StubSessionManager


async def test_create_session_returns_id():
    """create_session should return a non-empty session ID string."""
    sm = StubSessionManager()
    sid = await sm.create_session()
    assert isinstance(sid, str)
    assert len(sid) > 0


async def test_create_session_increments_count():
    """Each create_session should increment active_count."""
    sm = StubSessionManager()
    assert sm.active_count == 0
    await sm.create_session()
    assert sm.active_count == 1
    await sm.create_session()
    assert sm.active_count == 2


async def test_destroy_session_decrements_count():
    """destroy_session should remove the session and decrement active_count."""
    sm = StubSessionManager()
    sid = await sm.create_session()
    assert sm.active_count == 1
    await sm.destroy_session(sid)
    assert sm.active_count == 0


async def test_destroy_nonexistent_session_is_noop():
    """destroy_session on a nonexistent ID should not raise."""
    sm = StubSessionManager()
    await sm.destroy_session("nonexistent")
    assert sm.active_count == 0


async def test_get_session_returns_metadata():
    """get_session should return session metadata dict."""
    sm = StubSessionManager()
    sid = await sm.create_session()
    session = await sm.get_session(sid)
    assert session is not None
    assert session["session_id"] == sid
    assert session["status"] == "active"


async def test_get_session_returns_none_for_unknown():
    """get_session should return None for unknown session ID."""
    sm = StubSessionManager()
    assert await sm.get_session("unknown") is None


async def test_reset_session_returns_new_id():
    """reset_session should destroy old session and return a new one."""
    sm = StubSessionManager()
    old_id = await sm.create_session()
    new_id = await sm.reset_session(old_id)
    assert new_id != old_id
    assert sm.active_count == 1
    assert await sm.get_session(old_id) is None
    assert await sm.get_session(new_id) is not None
```

**Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/intelligence_service/test_session_manager.py -v
```

Expected: FAIL (ImportError — `intelligence_service.session_manager` does not exist yet)

**Step 3: Write minimal implementation**

Create `intelligence_service/session_manager.py`:

```python
"""Session manager for mapping WebSocket connections to Amplifier sessions."""

import uuid
from typing import Protocol


class SessionManager(Protocol):
    """Protocol for session management. Allows swapping implementations."""

    async def create_session(self) -> str: ...
    async def destroy_session(self, session_id: str) -> None: ...
    async def reset_session(self, session_id: str) -> str: ...
    async def get_session(self, session_id: str) -> dict[str, str] | None: ...

    @property
    def active_count(self) -> int: ...


class StubSessionManager:
    """In-memory stub session manager for Phase 1.

    The real AmplifierSessionManager will implement the same SessionManager
    protocol when bundle integration is ready.
    """

    def __init__(self) -> None:
        self._sessions: dict[str, dict[str, str]] = {}

    async def create_session(self) -> str:
        """Create a new session and return its ID."""
        session_id = str(uuid.uuid4())
        self._sessions[session_id] = {"session_id": session_id, "status": "active"}
        return session_id

    async def destroy_session(self, session_id: str) -> None:
        """Destroy a session by ID. No-op if session does not exist."""
        self._sessions.pop(session_id, None)

    async def reset_session(self, session_id: str) -> str:
        """Destroy the current session and create a new one. Returns the new ID."""
        await self.destroy_session(session_id)
        return await self.create_session()

    async def get_session(self, session_id: str) -> dict[str, str] | None:
        """Return session metadata or None if not found."""
        return self._sessions.get(session_id)

    @property
    def active_count(self) -> int:
        """Number of active sessions."""
        return len(self._sessions)
```

**Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/intelligence_service/test_session_manager.py -v
```

Expected: 7 passed

**Step 5: Commit**

```bash
git add intelligence_service/session_manager.py tests/intelligence_service/test_session_manager.py && git commit -m "feat(intelligence-service): add session manager with protocol and stub"
```

---

### Task 4: A2UI Bridge

**Files:**
- Create: `intelligence_service/a2ui_bridge.py`
- Create: `tests/intelligence_service/test_a2ui_bridge.py`

**Step 1: Write the failing tests**

Create `tests/intelligence_service/test_a2ui_bridge.py`:

```python
"""Tests for A2UI bridge message translation."""

from intelligence_service.a2ui_bridge import (
    format_action_ack,
    format_error,
    format_response,
    format_session_created,
    parse_incoming,
)


def test_parse_incoming_extracts_type():
    """parse_incoming should extract the message type."""
    msg = parse_incoming({"type": "message", "text": "hello"})
    assert msg.msg_type == "message"
    assert msg.payload["text"] == "hello"


def test_parse_incoming_defaults_to_unknown():
    """parse_incoming should default to 'unknown' for missing type."""
    msg = parse_incoming({"text": "no type"})
    assert msg.msg_type == "unknown"


def test_parse_incoming_preserves_full_payload():
    """parse_incoming should preserve all fields in payload."""
    raw = {"type": "action", "componentId": "graph-1", "data": {"nodeId": "n42"}}
    msg = parse_incoming(raw)
    assert msg.payload == raw


def test_format_session_created():
    """format_session_created should produce valid session_created message."""
    result = format_session_created("sess-1", "Welcome")
    assert result["type"] == "session_created"
    assert result["session_id"] == "sess-1"
    assert result["message"] == "Welcome"


def test_format_session_created_default_message():
    """format_session_created should use default message when none provided."""
    result = format_session_created("sess-1")
    assert result["type"] == "session_created"
    assert result["message"] == "Session created."


def test_format_response():
    """format_response should produce valid response message."""
    result = format_response("sess-1", "Hello back")
    assert result["type"] == "response"
    assert result["session_id"] == "sess-1"
    assert result["content"] == "Hello back"


def test_format_action_ack():
    """format_action_ack should produce valid action_ack message."""
    result = format_action_ack("sess-1", "graph-1")
    assert result["type"] == "action_ack"
    assert result["session_id"] == "sess-1"
    assert result["component_id"] == "graph-1"


def test_format_error():
    """format_error should produce valid error message."""
    result = format_error("sess-1", "Something went wrong")
    assert result["type"] == "error"
    assert result["session_id"] == "sess-1"
    assert result["message"] == "Something went wrong"
```

**Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/intelligence_service/test_a2ui_bridge.py -v
```

Expected: FAIL (ImportError — `intelligence_service.a2ui_bridge` does not exist yet)

**Step 3: Write minimal implementation**

Create `intelligence_service/a2ui_bridge.py`:

```python
"""A2UI message bridge: translates between WebSocket messages and A2UI protocol.

Phase 1 provides message parsing and formatting helpers. When the Amplifier
bundle integration arrives, this module will also handle translating Amplifier
tool outputs (render_surface, update_viz) into A2UI JSON messages.
"""

from typing import Any


class IncomingMessage:
    """Parsed incoming WebSocket message."""

    def __init__(self, msg_type: str, payload: dict[str, Any]) -> None:
        self.msg_type = msg_type
        self.payload = payload


def parse_incoming(raw: dict[str, Any]) -> IncomingMessage:
    """Parse a raw WebSocket JSON message into an IncomingMessage."""
    msg_type = raw.get("type", "unknown")
    return IncomingMessage(msg_type=msg_type, payload=raw)


def format_session_created(session_id: str, message: str = "") -> dict[str, Any]:
    """Format a session_created outgoing message."""
    return {
        "type": "session_created",
        "session_id": session_id,
        "message": message or "Session created.",
    }


def format_response(session_id: str, content: str) -> dict[str, Any]:
    """Format a stub response message."""
    return {
        "type": "response",
        "session_id": session_id,
        "content": content,
    }


def format_action_ack(session_id: str, component_id: str) -> dict[str, Any]:
    """Format an action acknowledgment."""
    return {
        "type": "action_ack",
        "session_id": session_id,
        "component_id": component_id,
    }


def format_error(session_id: str, message: str) -> dict[str, Any]:
    """Format an error message."""
    return {
        "type": "error",
        "session_id": session_id,
        "message": message,
    }
```

**Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/intelligence_service/test_a2ui_bridge.py -v
```

Expected: 8 passed

**Step 5: Commit**

```bash
git add intelligence_service/a2ui_bridge.py tests/intelligence_service/test_a2ui_bridge.py && git commit -m "feat(intelligence-service): add A2UI bridge message translation"
```

---

### Task 5: Drain Manager

**Files:**
- Create: `intelligence_service/drain.py`
- Create: `tests/intelligence_service/test_drain.py`

**Step 1: Write the failing tests**

Create `tests/intelligence_service/test_drain.py`:

```python
"""Tests for drain manager."""

import asyncio

from intelligence_service.drain import DrainManager


def test_drain_starts_accepting():
    """DrainManager should start in accepting state."""
    dm = DrainManager()
    assert dm.accepting is True


def test_drain_starts_with_zero_active():
    """DrainManager should start with zero active connections."""
    dm = DrainManager()
    assert dm.active_count == 0


def test_register_increments_active():
    """register should increment active_count."""
    dm = DrainManager()
    dm.register("sess-1")
    assert dm.active_count == 1
    dm.register("sess-2")
    assert dm.active_count == 2


def test_unregister_decrements_active():
    """unregister should decrement active_count."""
    dm = DrainManager()
    dm.register("sess-1")
    dm.unregister("sess-1")
    assert dm.active_count == 0


def test_unregister_nonexistent_is_noop():
    """unregister on unknown session should not raise."""
    dm = DrainManager()
    dm.unregister("nonexistent")
    assert dm.active_count == 0


async def test_start_drain_stops_accepting():
    """start_drain should set accepting to False."""
    dm = DrainManager(timeout_seconds=1)
    await dm.start_drain()
    assert dm.accepting is False


async def test_start_drain_returns_true_when_no_sessions():
    """start_drain should return True immediately when no active sessions."""
    dm = DrainManager(timeout_seconds=1)
    result = await dm.start_drain()
    assert result is True


async def test_start_drain_waits_for_sessions_to_unregister():
    """start_drain should wait for sessions to unregister then return True."""
    dm = DrainManager(timeout_seconds=5)
    dm.register("sess-1")

    async def delayed_unregister():
        await asyncio.sleep(0.1)
        dm.unregister("sess-1")

    task = asyncio.create_task(delayed_unregister())
    result = await dm.start_drain()
    assert result is True
    assert dm.active_count == 0
    await task


async def test_start_drain_returns_false_on_timeout():
    """start_drain should return False when timeout is hit with remaining sessions."""
    dm = DrainManager(timeout_seconds=0.1)
    dm.register("sess-stuck")
    result = await dm.start_drain()
    assert result is False
    assert dm.active_count == 1
```

**Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/intelligence_service/test_drain.py -v
```

Expected: FAIL (ImportError — `intelligence_service.drain` does not exist yet)

**Step 3: Write minimal implementation**

Create `intelligence_service/drain.py`:

```python
"""Graceful shutdown drain manager for the Intelligence Service.

On SIGTERM (Docker stop), the lifespan handler calls start_drain() which:
1. Stops accepting new WebSocket connections
2. Waits for active sessions to disconnect (up to drain_timeout)
3. Returns False if timeout hit (caller decides whether to force-close)
"""

import asyncio


class DrainManager:
    """Manages graceful shutdown by draining active WebSocket sessions."""

    def __init__(self, timeout_seconds: int = 30) -> None:
        self._accepting = True
        self._timeout = timeout_seconds
        self._active: set[str] = set()
        self._drained = asyncio.Event()
        self._drained.set()  # Start as "drained" (no active connections)

    @property
    def accepting(self) -> bool:
        """Whether new connections are being accepted."""
        return self._accepting

    @property
    def active_count(self) -> int:
        """Number of active sessions being drained."""
        return len(self._active)

    def register(self, session_id: str) -> None:
        """Register an active session."""
        self._active.add(session_id)
        self._drained.clear()

    def unregister(self, session_id: str) -> None:
        """Unregister a session. Signals drain complete when last session leaves."""
        self._active.discard(session_id)
        if not self._active:
            self._drained.set()

    async def start_drain(self) -> bool:
        """Stop accepting new connections and wait for active sessions to finish.

        Returns True if all sessions drained cleanly, False if timeout was hit.
        """
        self._accepting = False
        if not self._active:
            return True
        try:
            await asyncio.wait_for(self._drained.wait(), timeout=self._timeout)
            return True
        except asyncio.TimeoutError:
            return False
```

**Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/intelligence_service/test_drain.py -v
```

Expected: 9 passed

**Step 5: Commit**

```bash
git add intelligence_service/drain.py tests/intelligence_service/test_drain.py && git commit -m "feat(intelligence-service): add drain manager for graceful shutdown"
```

---

### Task 6: App Skeleton with Health Endpoint

**Files:**
- Create: `intelligence_service/app.py`
- Create: `tests/intelligence_service/test_app.py`

**Step 1: Write the failing tests**

Create `tests/intelligence_service/test_app.py`:

```python
"""Tests for Intelligence Service FastAPI application."""

import httpx

from intelligence_service.app import app


async def test_health_returns_200_with_status_ok():
    """GET /health should return 200 with status ok."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_reload_bundle_returns_stub_response():
    """GET /admin/reload-bundle should return stub response."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get("/admin/reload-bundle")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "reload_not_implemented"
```

**Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/intelligence_service/test_app.py -v
```

Expected: FAIL (ImportError — `intelligence_service.app` does not exist yet)

**Step 3: Write minimal implementation**

Create `intelligence_service/app.py`:

```python
"""FastAPI application for the Intelligence Service WebSocket bridge."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

logger = logging.getLogger("intelligence_service")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application lifespan."""
    logger.info("lifespan_startup: intelligence service starting")
    try:
        yield
    finally:
        logger.info("lifespan_shutdown: intelligence service stopped")


app = FastAPI(title="Intelligence Service", lifespan=lifespan)


@app.get("/health")
async def get_health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/admin/reload-bundle")
async def reload_bundle() -> dict[str, str]:
    """Trigger bundle reload. Stub for Phase 1."""
    return {
        "status": "reload_not_implemented",
        "message": "Bundle reload will be available when agent integration is complete.",
    }
```

**Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/intelligence_service/test_app.py -v
```

Expected: 2 passed

**Step 5: Commit**

```bash
git add intelligence_service/app.py tests/intelligence_service/test_app.py && git commit -m "feat(intelligence-service): add FastAPI app skeleton with health endpoint"
```

---

### Task 7: WebSocket Endpoint

**Files:**
- Modify: `intelligence_service/app.py` (add imports, update lifespan, add `/ws` endpoint)
- Modify: `tests/intelligence_service/test_app.py` (add WebSocket tests)

This task wires together all components: session manager, A2UI bridge, and drain manager.

**Step 1: Write the failing tests**

Append these tests to `tests/intelligence_service/test_app.py`. WebSocket tests use Starlette's synchronous `TestClient` because `httpx.AsyncClient` does not support WebSocket:

```python
from starlette.testclient import TestClient

from intelligence_service.app import app


def test_ws_connect_receives_session_created():
    """WebSocket connect should receive session_created message."""
    client = TestClient(app)
    with client.websocket_connect("/ws") as ws:
        data = ws.receive_json()
        assert data["type"] == "session_created"
        assert "session_id" in data


def test_ws_message_receives_response():
    """Sending a message should receive a response."""
    client = TestClient(app)
    with client.websocket_connect("/ws") as ws:
        ws.receive_json()  # session_created
        ws.send_json({"type": "message", "text": "hello"})
        data = ws.receive_json()
        assert data["type"] == "response"
        assert "hello" in data["content"]


def test_ws_new_session_returns_different_id():
    """Sending new_session should receive session_created with a different ID."""
    client = TestClient(app)
    with client.websocket_connect("/ws") as ws:
        first = ws.receive_json()
        first_id = first["session_id"]
        ws.send_json({"type": "new_session"})
        second = ws.receive_json()
        assert second["type"] == "session_created"
        assert second["session_id"] != first_id


def test_ws_action_receives_ack():
    """Sending an action should receive an action_ack."""
    client = TestClient(app)
    with client.websocket_connect("/ws") as ws:
        ws.receive_json()  # session_created
        ws.send_json({"type": "action", "componentId": "graph-1"})
        data = ws.receive_json()
        assert data["type"] == "action_ack"
        assert data["component_id"] == "graph-1"


def test_ws_unknown_type_receives_error():
    """Sending an unknown type should receive an error."""
    client = TestClient(app)
    with client.websocket_connect("/ws") as ws:
        ws.receive_json()  # session_created
        ws.send_json({"type": "invalid_type"})
        data = ws.receive_json()
        assert data["type"] == "error"
        assert "invalid_type" in data["message"]
```

**Step 2: Run test to verify the new tests fail**

Run:

```bash
pytest tests/intelligence_service/test_app.py -v
```

Expected: 2 passed (existing), 5 failed (new WebSocket tests — no `/ws` endpoint yet)

**Step 3: Update `intelligence_service/app.py` with full implementation**

Replace the entire contents of `intelligence_service/app.py` with:

```python
"""FastAPI application for the Intelligence Service WebSocket bridge."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

import intelligence_service.a2ui_bridge as a2ui_bridge
from intelligence_service.config import get_settings
from intelligence_service.drain import DrainManager
from intelligence_service.session_manager import StubSessionManager

_settings = get_settings()

logger = logging.getLogger("intelligence_service")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application lifespan: initialize drain manager and session manager."""
    logger.info("lifespan_startup: intelligence service starting")
    app.state.drain = DrainManager(timeout_seconds=_settings.drain_timeout_seconds)
    app.state.session_manager = StubSessionManager()
    try:
        yield
    finally:
        logger.info("lifespan_shutdown: draining active sessions")
        await app.state.drain.start_drain()
        logger.info("lifespan_shutdown: intelligence service stopped")


app = FastAPI(title="Intelligence Service", lifespan=lifespan)


@app.get("/health")
async def get_health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/admin/reload-bundle")
async def reload_bundle() -> dict[str, str]:
    """Trigger bundle reload. Stub for Phase 1."""
    return {
        "status": "reload_not_implemented",
        "message": "Bundle reload will be available when agent integration is complete.",
    }


@app.websocket("/ws")
async def ws_connect(websocket: WebSocket) -> None:
    """WebSocket endpoint for A2UI sessions."""
    drain: DrainManager = websocket.app.state.drain
    sm: StubSessionManager = websocket.app.state.session_manager

    if not drain.accepting:
        await websocket.close(code=1013, reason="Service is shutting down")
        return

    await websocket.accept()
    session_id = await sm.create_session()
    drain.register(session_id)

    try:
        await websocket.send_json(
            a2ui_bridge.format_session_created(
                session_id,
                "Intelligence service connected. Agent integration pending.",
            )
        )

        while True:
            data = await websocket.receive_json()
            msg = a2ui_bridge.parse_incoming(data)

            if msg.msg_type == "new_session":
                drain.unregister(session_id)
                await sm.destroy_session(session_id)
                session_id = await sm.create_session()
                drain.register(session_id)
                await websocket.send_json(
                    a2ui_bridge.format_session_created(
                        session_id, "New session created."
                    )
                )

            elif msg.msg_type == "message":
                text = msg.payload.get("text", "")
                await websocket.send_json(
                    a2ui_bridge.format_response(
                        session_id,
                        f"Received: {text}. Agent processing not yet available.",
                    )
                )

            elif msg.msg_type == "action":
                component_id = msg.payload.get("componentId", "unknown")
                await websocket.send_json(
                    a2ui_bridge.format_action_ack(session_id, component_id)
                )

            else:
                await websocket.send_json(
                    a2ui_bridge.format_error(
                        session_id,
                        f"Unknown message type: {msg.msg_type}",
                    )
                )

    except WebSocketDisconnect:
        pass
    finally:
        drain.unregister(session_id)
        await sm.destroy_session(session_id)
```

**Step 4: Run ALL intelligence_service tests to verify everything passes**

Run:

```bash
pytest tests/intelligence_service/ -v
```

Expected: All tests pass (config: 3, session_manager: 7, a2ui_bridge: 8, drain: 9, app: 7 = 34 total)

**Step 5: Commit**

```bash
git add intelligence_service/app.py tests/intelligence_service/test_app.py && git commit -m "feat(intelligence-service): add WebSocket endpoint for A2UI sessions"
```

---

### Task 8: Dockerfile.intelligence + Entrypoint Script

**Files:**
- Create: `Dockerfile.intelligence`
- Create: `entrypoint-intelligence.sh`

No TDD for Dockerfiles.

**Step 1: Create `Dockerfile.intelligence`**

```dockerfile
FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the package metadata first (layer caching for deps)
COPY intelligence_service/pyproject.toml pyproject.toml

# Copy the Python package
COPY intelligence_service/ intelligence_service/

# Install the package and its dependencies
RUN uv pip install --system .

# Copy entrypoint
COPY entrypoint-intelligence.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8100

ENTRYPOINT ["/entrypoint.sh"]
```

**Step 2: Create `entrypoint-intelligence.sh`**

```bash
#!/bin/bash
set -e

# Config overlay: copy mounted config defaults to data dir, preserving user edits.
# /config is mounted read-only from the host config/ directory.
# /data persists across restarts via a Docker volume.
mkdir -p /data
if [ -d /config ]; then
    cp -rn /config/* /data/ 2>/dev/null || true
fi

# Start the WebSocket bridge service
exec uvicorn intelligence_service.app:app --host 0.0.0.0 --port 8100
```

**Step 3: Verify Dockerfile syntax**

Run:

```bash
docker build --check -f Dockerfile.intelligence . 2>&1 || echo "docker build --check not supported, skipping syntax check"
```

If `--check` is not supported, just verify the file exists:

```bash
head -1 Dockerfile.intelligence
```

Expected: `FROM python:3.13-slim`

**Step 4: Commit**

```bash
git add Dockerfile.intelligence entrypoint-intelligence.sh && git commit -m "feat(docker): add Dockerfile.intelligence with entrypoint"
```

---

### Task 9: Dockerfile.frontend + Placeholder Page

**Files:**
- Create: `Dockerfile.frontend`
- Create: `frontend/index.html`

No TDD for Docker/HTML scaffolding.

**Step 1: Create `frontend/index.html`**

```html
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Context Intelligence Explorer</title>
  <style>
    body {
      font-family: system-ui, -apple-system, sans-serif;
      display: flex;
      justify-content: center;
      align-items: center;
      min-height: 100vh;
      margin: 0;
      background: #0f172a;
      color: #e2e8f0;
    }
    .container { text-align: center; max-width: 480px; padding: 2rem; }
    h1 { font-size: 1.5rem; margin-bottom: 0.5rem; }
    p { color: #94a3b8; line-height: 1.6; }
    a { color: #38bdf8; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Context Intelligence Explorer</h1>
    <p>The AI-driven exploration frontend is coming soon (Phase 2).</p>
    <p>In the meantime, visit the <a href="http://localhost:8000">operational dashboard</a> for session monitoring.</p>
  </div>
</body>
</html>
```

**Step 2: Create `Dockerfile.frontend`**

```dockerfile
FROM nginx:alpine

COPY frontend/index.html /usr/share/nginx/html/index.html

EXPOSE 80
```

**Step 3: Verify files exist**

Run:

```bash
cat frontend/index.html | head -3 && echo "---" && cat Dockerfile.frontend
```

Expected: HTML doctype header and Dockerfile content

**Step 4: Commit**

```bash
git add Dockerfile.frontend frontend/ && git commit -m "feat(docker): add Dockerfile.frontend placeholder"
```

---

### Task 10: Config Directory

**Files:**
- Create: `config/settings.yaml`
- Create: `config/secrets.env`

No TDD for configuration files.

**Step 1: Create `config/settings.yaml`**

This is an Amplifier settings file for the Intelligence Service container. It declares all 7 providers and the routing matrix. Consumed by Amplifier when the bundle integration arrives.

```yaml
# Amplifier settings for the Intelligence Service container.
# Mounted at /config/settings.yaml inside the container.
# The entrypoint copies this to /data/settings.yaml (no-overwrite)
# so user edits persist across container restarts.

routing_matrix:
  default_provider: provider-anthropic
  # Routing rules will be configured when agents are ready.
  # Example: route visualization-heavy tasks to provider-gemini.

providers:
  - name: provider-anthropic
    source: "git+https://github.com/amplifier-ai/provider-anthropic"
  - name: provider-openai
    source: "git+https://github.com/amplifier-ai/provider-openai"
  - name: provider-gemini
    source: "git+https://github.com/amplifier-ai/provider-gemini"
  - name: provider-azure-openai
    source: "git+https://github.com/amplifier-ai/provider-azure-openai"
  - name: provider-github-copilot
    source: "git+https://github.com/amplifier-ai/provider-github-copilot"
  - name: provider-ollama
    source: "git+https://github.com/amplifier-ai/provider-ollama"
  - name: provider-vllm
    source: "git+https://github.com/amplifier-ai/provider-vllm"
```

**Step 2: Create `config/secrets.env`**

```env
# API keys for the Intelligence Service container.
# Fill in the keys for providers you want to use.
# Leave empty for providers you don't need.
# These are loaded via env_file in docker-compose.yml.
ANTHROPIC_API_KEY=
OPENAI_API_KEY=
GOOGLE_API_KEY=
AZURE_OPENAI_API_KEY=
AZURE_OPENAI_ENDPOINT=
GITHUB_TOKEN=
OLLAMA_HOST=
VLLM_BASE_URL=
```

**Step 3: Verify files exist**

Run:

```bash
ls -la config/
```

Expected: `settings.yaml` and `secrets.env`

**Step 4: Commit**

```bash
git add config/ && git commit -m "feat(config): add settings.yaml and secrets.env for intelligence service"
```

---

### Task 11: Docker Compose Update

**Files:**
- Modify: `docker-compose.yml` (add `intelligence-service` and `frontend` services)

No TDD for Docker Compose.

**Step 1: Replace `docker-compose.yml` with the full 4-service configuration**

Replace the entire file with:

```yaml
services:
  context-intelligence-server:
    build: .
    ports:
      - "8000:8000"
    environment:
      CI_SERVER_NEO4J_URL: neo4j://neo4j:7687
      CI_SERVER_BLOB_PATH: /data/blobs
      PYTHONUNBUFFERED: "1"
    volumes:
      - blob_data:/data/blobs
      - log_data:/data/logs
    depends_on:
      neo4j:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/status"]
      interval: 10s
      timeout: 5s
      retries: 3
    restart: unless-stopped
    labels:
      com.context-intelligence.component: server
    networks:
      - context-intelligence

  intelligence-service:
    build:
      context: .
      dockerfile: Dockerfile.intelligence
    ports:
      - "8100:8100"
    environment:
      INTEL_SERVICE_INGESTION_SERVER_URL: http://context-intelligence-server:8000
      INTEL_SERVICE_BLOB_PATH: /data/blobs
      PYTHONUNBUFFERED: "1"
    volumes:
      - blob_data:/data/blobs:ro
      - ./config:/config:ro
    env_file:
      - config/secrets.env
    depends_on:
      context-intelligence-server:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8100/health')"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 120s
    restart: unless-stopped
    labels:
      com.context-intelligence.component: intelligence-service
    networks:
      - context-intelligence

  frontend:
    build:
      context: .
      dockerfile: Dockerfile.frontend
    ports:
      - "3000:80"
    depends_on:
      intelligence-service:
        condition: service_healthy
    restart: unless-stopped
    labels:
      com.context-intelligence.component: frontend
    networks:
      - context-intelligence

  neo4j:
    image: neo4j:5.26.22-community
    environment:
      NEO4J_AUTH: none
    ports:
      - "7474:7474"  # browser UI
      - "7687:7687"  # bolt
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

volumes:
  blob_data:
  neo4j_data:
  log_data:

networks:
  context-intelligence:
    driver: bridge
```

**Step 2: Validate compose syntax**

Run:

```bash
docker compose config --quiet 2>&1 && echo "Compose config valid" || echo "Compose config has errors"
```

Expected: `Compose config valid`

**Step 3: Commit**

```bash
git add docker-compose.yml && git commit -m "feat(docker): expand Docker Compose to 4-service stack"
```

---

### Task 12: Landing Page Navigation Card

**Files:**
- Modify: `context_intelligence_server/web/index.html` (add exploration UI nav card)

No TDD for HTML changes. The existing test `test_dashboard_returns_html` already verifies the landing page renders.

**Step 1: Add the exploration UI navigation card**

In `context_intelligence_server/web/index.html`, find the closing `</div>` of the `landing-grid` div (after the Neo4j Browser card) and add the new card before it.

Find this block (the Neo4j Browser card):

```html
      <a class="nav-card" href="http://localhost:7474" target="_blank">
        <div class="nav-card-icon">&#x25C8;</div>
        <div class="nav-card-title">Neo4j Browser</div>
        <div class="nav-card-desc">Query the session graph directly with Cypher. Connect to bolt://localhost:7687, no auth.</div>
        <div class="nav-card-arrow">Open Neo4j browser &rarr;</div>
      </a>
    </div>
```

Replace with:

```html
      <a class="nav-card" href="http://localhost:7474" target="_blank">
        <div class="nav-card-icon">&#x25C8;</div>
        <div class="nav-card-title">Neo4j Browser</div>
        <div class="nav-card-desc">Query the session graph directly with Cypher. Connect to bolt://localhost:7687, no auth.</div>
        <div class="nav-card-arrow">Open Neo4j browser &rarr;</div>
      </a>
      <a class="nav-card" href="http://localhost:3000" target="_blank">
        <div class="nav-card-icon">&#x25CE;</div>
        <div class="nav-card-title">Exploration UI</div>
        <div class="nav-card-desc">AI-driven graph exploration &mdash; ask questions about sessions, delegations, tool calls, and patterns.</div>
        <div class="nav-card-arrow">Open explorer &rarr;</div>
      </a>
    </div>
```

**Step 2: Verify the card is present**

Run:

```bash
grep -c "Exploration UI" context_intelligence_server/web/index.html
```

Expected: `1`

**Step 3: Run existing landing page tests to verify nothing broke**

Run:

```bash
pytest tests/test_main.py::test_dashboard_returns_html -v
```

Expected: PASS

**Step 4: Commit**

```bash
git add context_intelligence_server/web/index.html && git commit -m "feat(landing-page): add exploration UI navigation card"
```

---

### Task 13: Full Test Suite Verification

**Files:** None (verification only)

**Step 1: Run the complete intelligence_service test suite**

Run:

```bash
pytest tests/intelligence_service/ -v
```

Expected: All tests pass (approximately 34 tests across 5 test files)

**Step 2: Run the existing ingestion server test suite**

Run:

```bash
pytest tests/ -v --ignore=tests/intelligence_service
```

Expected: All existing tests still pass (no regressions)

**Step 3: Run the full combined test suite**

Run:

```bash
pytest tests/ -v
```

Expected: All tests pass

**Step 4: Verify Docker Compose config is valid**

Run:

```bash
docker compose config --quiet 2>&1 && echo "OK"
```

Expected: `OK`

**Step 5: Verify all new files exist**

Run:

```bash
echo "=== Intelligence Service ===" && \
ls intelligence_service/*.py && \
echo "=== Tests ===" && \
ls tests/intelligence_service/*.py && \
echo "=== Docker ===" && \
ls Dockerfile.intelligence entrypoint-intelligence.sh Dockerfile.frontend && \
echo "=== Config ===" && \
ls config/ && \
echo "=== Frontend ===" && \
ls frontend/index.html
```

Expected: All files listed without errors

**Step 6: Verify git status is clean**

Run:

```bash
git status
```

Expected: `nothing to commit, working tree clean` (on branch `feat/exploration-system`)

---

## Summary

After completing all 13 tasks, Phase 1 delivers:

| Component | Files | Tests |
|-----------|-------|-------|
| Config module | `intelligence_service/config.py` | 3 tests |
| Session manager | `intelligence_service/session_manager.py` | 7 tests |
| A2UI bridge | `intelligence_service/a2ui_bridge.py` | 8 tests |
| Drain manager | `intelligence_service/drain.py` | 9 tests |
| App + WebSocket | `intelligence_service/app.py` | 7 tests |
| Dockerfile.intelligence | `Dockerfile.intelligence`, `entrypoint-intelligence.sh` | — |
| Dockerfile.frontend | `Dockerfile.frontend`, `frontend/index.html` | — |
| Config overlay | `config/settings.yaml`, `config/secrets.env` | — |
| Docker Compose | `docker-compose.yml` (updated) | — |
| Landing page | `context_intelligence_server/web/index.html` (updated) | — |

**Total: ~34 new tests, 12 new files, 2 modified files, 13 commits on `feat/exploration-system`**

## What Comes Next

- **Phase 2:** Frontend SPA — Vite + TypeScript + Lit project in `frontend/`, A2UI client, 6 custom catalog components
- **Research phase:** Deep dive into bundle analyst agent, session-analyst patterns, A2UI SDK
- **Phase 3+:** Server bundle implementation, self-improvement loop, DOT file documentation, integration testing
