# Phase 2: Handler Migration — Implementation Plan

> **Execution:** Use the subagent-driven-development workflow to implement this plan.

**Goal:** Port all graph-processing logic (handlers, services, Neo4j store, pipeline) from the bundle into the standalone Context Intelligence Server, wire it into the drain loop, and validate end-to-end event processing with mocked Neo4j.

**Architecture:** Seven event handlers are ported verbatim from the bundle (import paths change, `graph_forest_name` → `workspace` everywhere). `HookStateService` loses its coordinator dependency — `workspace` is set directly at construction. A new `pipeline.py` replaces `_wrap_with_session_guarantee` with server-side error isolation. The drain loop in `registry.py` gains a 30-second periodic flush fallback.

**Tech Stack:** Python 3.12+, FastAPI, pytest + pytest-asyncio, neo4j async driver, Pydantic Settings (from Phase 1)

**All commands run from:** `cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence`

**Package name:** `context_intelligence_server`

---

## Preconditions (Phase 1 Complete)

Phase 1 left these files in place:

```
context_intelligence_server/
├── __init__.py
├── main.py       # FastAPI app: GET /status, POST /events (enqueues, returns 202)
├── config.py     # Pydantic Settings
├── models.py     # EventRequest, EventResponse, StatusResponse
└── registry.py   # SessionRegistry, SessionWorker (queue + drain Task)

tests/
├── conftest.py
├── test_main.py
└── test_registry.py

pyproject.toml
Dockerfile
docker-compose.yml
```

---

### Task 1: Port `protocol.py` and `utils.py`

**Files:**
- Create: `context_intelligence_server/protocol.py`
- Create: `context_intelligence_server/utils.py`
- Create: `tests/test_utils.py`

**Step 1: Write the failing tests**

Create `tests/test_utils.py`:

```python
"""Tests for utils: make_node_id, make_edge_id, HandlerLogger, EventLogContext."""

from __future__ import annotations

import logging

import pytest

from context_intelligence_server.utils import (
    EventLogContext,
    HandlerLogger,
    make_edge_id,
    make_node_id,
)


class TestMakeNodeId:
    def test_basic_iso_timestamp(self):
        result = make_node_id("s1", "prompt:submit", "2026-01-01T00:00:00Z")
        assert result == "s1__prompt_submit__1767225600000"

    def test_fractional_seconds(self):
        result = make_node_id("s1", "prompt:submit", "2026-01-01T00:00:00.500Z")
        assert result == "s1__prompt_submit__1767225600500"

    def test_timezone_offset(self):
        result = make_node_id("s1", "session:resume", "2026-01-01T02:00:00+00:00")
        assert result == "s1__session_resume__1767232800000"

    def test_deterministic(self):
        a = make_node_id("s1", "prompt:submit", "2026-01-01T00:00:00Z")
        b = make_node_id("s1", "prompt:submit", "2026-01-01T00:00:00Z")
        assert a == b

    def test_different_events_produce_different_ids(self):
        a = make_node_id("s1", "prompt:submit", "2026-01-01T00:00:00Z")
        b = make_node_id("s1", "session:start", "2026-01-01T00:00:00Z")
        assert a != b

    def test_different_sessions_produce_different_ids(self):
        a = make_node_id("s1", "prompt:submit", "2026-01-01T00:00:00Z")
        b = make_node_id("s2", "prompt:submit", "2026-01-01T00:00:00Z")
        assert a != b

    def test_disambiguator_appended_to_id(self):
        result = make_node_id("s1", "tool:pre", "2026-01-01T00:00:00Z", disambiguator="call_abc")
        assert result == "s1__tool_pre__1767225600000__call_abc"

    def test_disambiguator_none_preserves_old_format(self):
        result = make_node_id("s1", "tool:pre", "2026-01-01T00:00:00Z")
        assert result == "s1__tool_pre__1767225600000"

    def test_same_timestamp_different_disambiguator_produces_different_ids(self):
        a = make_node_id("s1", "tool:pre", "2026-01-01T00:00:00Z", disambiguator="call_001")
        b = make_node_id("s1", "tool:pre", "2026-01-01T00:00:00Z", disambiguator="call_002")
        assert a != b


class TestMakeEdgeId:
    def test_basic_construction(self):
        result = make_edge_id("session-1", "node-2", "HAS_STEP")
        assert result == "session-1==[HAS_STEP]==node-2"

    def test_parseable_back_to_components(self):
        edge_id = make_edge_id("src-node", "tgt-node", "HAS_STEP")
        parts = edge_id.split("==[")
        source = parts[0]
        edge_type, target = parts[1].split("]==")
        assert source == "src-node"
        assert edge_type == "HAS_STEP"
        assert target == "tgt-node"

    def test_deterministic(self):
        a = make_edge_id("src", "tgt", "HAS_STEP")
        b = make_edge_id("src", "tgt", "HAS_STEP")
        assert a == b

    def test_different_edge_types_produce_different_ids(self):
        a = make_edge_id("src", "tgt", "HAS_STEP")
        b = make_edge_id("src", "tgt", "FOLLOWED_BY")
        assert a != b


class TestHandlerLogger:
    def test_with_event_returns_event_log_context(self):
        lg = logging.getLogger("test.handler_logger")
        hl = HandlerLogger(handler_name="SessionHandler", logger=lg)
        ctx = hl.with_event("session:start", {"session_id": "s1"})
        assert isinstance(ctx, EventLogContext)

    def test_with_event_missing_session_id_uses_empty_string(self):
        lg = logging.getLogger("test.handler_logger.missing")
        hl = HandlerLogger(handler_name="SessionHandler", logger=lg)
        ctx = hl.with_event("session:start", {})
        assert isinstance(ctx, EventLogContext)


class TestEventLogContext:
    def test_info_includes_prefix(self, caplog):
        lg = logging.getLogger("test.elc.info")
        ctx = EventLogContext("SessionHandler", "s1", "session:start", lg)
        with caplog.at_level(logging.INFO, logger="test.elc.info"):
            ctx.info("node created")
        assert caplog.records[0].message == "[SessionHandler] [s1] [session:start] node created"

    def test_warning_includes_prefix(self, caplog):
        lg = logging.getLogger("test.elc.warning")
        ctx = EventLogContext("SessionHandler", "s1", "session:start", lg)
        with caplog.at_level(logging.WARNING, logger="test.elc.warning"):
            ctx.warning("something odd")
        assert caplog.records[0].message == "[SessionHandler] [s1] [session:start] something odd"

    def test_error_includes_prefix(self, caplog):
        lg = logging.getLogger("test.elc.error")
        ctx = EventLogContext("SessionHandler", "s1", "session:start", lg)
        with caplog.at_level(logging.ERROR, logger="test.elc.error"):
            ctx.error("something broke")
        assert caplog.records[0].message == "[SessionHandler] [s1] [session:start] something broke"

    def test_info_supports_lazy_formatting_args(self, caplog):
        lg = logging.getLogger("test.elc.info_args")
        ctx = EventLogContext("RunHandler", "s1", "prompt:submit", lg)
        with caplog.at_level(logging.INFO, logger="test.elc.info_args"):
            ctx.info("Created PromptStep node %s", "node-123")
        assert (
            caplog.records[0].message
            == "[RunHandler] [s1] [prompt:submit] Created PromptStep node node-123"
        )
```

Also write a minimal protocol test inline in `tests/test_utils.py` (append to the file):

```python
class TestHookResult:
    def test_default_action_is_continue(self):
        from context_intelligence_server.protocol import HookResult

        result = HookResult()
        assert result.action == "continue"

    def test_custom_action(self):
        from context_intelligence_server.protocol import HookResult

        result = HookResult(action="stop")
        assert result.action == "stop"


class TestEventHandlerProtocol:
    def test_is_runtime_checkable(self):
        from context_intelligence_server.protocol import EventHandler

        assert hasattr(EventHandler, "__protocol_attrs__") or hasattr(
            EventHandler, "_is_runtime_protocol"
        )

    def test_conforming_class_passes_isinstance(self):
        from typing import Any

        from context_intelligence_server.protocol import EventHandler, HookResult

        class FakeHandler:
            handled_events: set[str] = {"test:event"}
            services = None

            async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
                return HookResult()

        assert isinstance(FakeHandler(), EventHandler)

    def test_missing_handled_events_fails_isinstance(self):
        from typing import Any

        from context_intelligence_server.protocol import EventHandler, HookResult

        class BadHandler:
            services = None

            async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
                return HookResult()

        assert not isinstance(BadHandler(), EventHandler)
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/test_utils.py -v
```

Expected: `ModuleNotFoundError: No module named 'context_intelligence_server.utils'`

**Step 3: Write `context_intelligence_server/protocol.py`**

```python
"""EventHandler protocol and HookResult — server-side replacements for amplifier_core types."""

from __future__ import annotations

import dataclasses
from collections.abc import Set as AbstractSet
from typing import Any, Protocol, runtime_checkable


@dataclasses.dataclass
class HookResult:
    """Lightweight replacement for amplifier_core.models.HookResult.

    In the server context, the return value is never inspected by the pipeline —
    it exists solely so handler code can be ported verbatim from the bundle.
    """

    action: str = "continue"


@runtime_checkable
class EventHandler(Protocol):
    """Protocol for all context-intelligence event handlers."""

    handled_events: AbstractSet[str]
    """The set of event names this handler owns (set or frozenset)."""

    services: Any
    """HookStateService instance injected at construction."""

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        """Handle a dispatched event."""
        ...
```

**Step 4: Write `context_intelligence_server/utils.py`**

Port verbatim from the bundle — no changes needed (no `graph_forest_name` references):

```python
"""Shared utilities for context-intelligence handlers."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any


def make_node_id(
    session_id: str,
    event_name: str,
    timestamp: str,
    disambiguator: str | None = None,
) -> str:
    """Generate a deterministic, filesystem-safe node ID from event data.

    Pattern: {session_id}__{safe_event}__{timestamp_ms}
    With disambiguator: {session_id}__{safe_event}__{timestamp_ms}__{disambiguator}
    """
    safe_event = event_name.replace(":", "_")
    dt = datetime.fromisoformat(timestamp)
    epoch_ms = int(dt.astimezone(timezone.utc).timestamp() * 1000)
    node_id = f"{session_id}__{safe_event}__{epoch_ms}"
    if disambiguator is not None:
        node_id = f"{node_id}__{disambiguator}"
    return node_id


def make_edge_id(source_id: str, target_id: str, edge_type: str) -> str:
    """Generate a deterministic edge ID from source, target, and type.

    Pattern: {source_id}==[{edge_type}]=={target_id}
    """
    return f"{source_id}==[{edge_type}]=={target_id}"


class EventLogContext:
    """Log context with handler name, session_id, and event name pre-bound as prefix."""

    def __init__(
        self,
        handler_name: str,
        session_id: str,
        event: str,
        logger: logging.Logger,
    ) -> None:
        self._logger = logger
        self._prefix = f"[{handler_name}] [{session_id}] [{event}]"

    def info(self, message: str, *args: object) -> None:
        self._logger.info("%s " + message, self._prefix, *args)

    def warning(self, message: str, *args: object) -> None:
        self._logger.warning("%s " + message, self._prefix, *args)

    def error(self, message: str, *args: object) -> None:
        self._logger.error("%s " + message, self._prefix, *args)


class HandlerLogger:
    """Structured logging wrapper that binds handler name to every log call."""

    def __init__(self, handler_name: str, logger: logging.Logger) -> None:
        self._handler_name = handler_name
        self._logger = logger

    def with_event(self, event: str, data: dict[str, Any]) -> EventLogContext:
        session_id = data.get("session_id", "")
        return EventLogContext(
            handler_name=self._handler_name,
            session_id=session_id,
            event=event,
            logger=self._logger,
        )
```

**Step 5: Run tests to verify they pass**

```bash
pytest tests/test_utils.py -v
```

Expected: All pass.

**Step 6: Commit**

```bash
git add context_intelligence_server/protocol.py context_intelligence_server/utils.py tests/test_utils.py
git commit -m "feat(phase2): port protocol.py and utils.py from bundle"
```

---

### Task 2: Port `graph_store.py`

**Files:**
- Create: `context_intelligence_server/graph_store.py`
- Create: `tests/test_graph_store.py`

**Step 1: Write the failing test**

Create `tests/test_graph_store.py`:

```python
"""Tests for GraphStore and QueryableStore protocol definitions."""

from __future__ import annotations

from typing import Any


class TestGraphStoreProtocol:
    def test_is_runtime_checkable(self):
        from context_intelligence_server.graph_store import GraphStore

        assert hasattr(GraphStore, "__protocol_attrs__") or hasattr(
            GraphStore, "_is_runtime_protocol"
        )

    def test_conforming_class_passes_isinstance(self):
        from context_intelligence_server.graph_store import GraphStore

        class FakeStore:
            @property
            def workspace(self) -> str:
                return "test"

            async def upsert_node(
                self, node_id: str, labels: set[str], properties: dict[str, Any]
            ) -> None: ...

            async def upsert_edge(
                self, source: str, target: str, edge_type: str, properties: dict[str, Any]
            ) -> None: ...

            async def get_node(self, node_id: str) -> dict[str, Any] | None: ...

            async def get_edge(
                self, source: str, target: str, edge_type: str
            ) -> dict[str, Any] | None: ...

            async def flush(self) -> None: ...

            async def close(self) -> None: ...

        assert isinstance(FakeStore(), GraphStore)

    def test_missing_upsert_node_fails_isinstance(self):
        from context_intelligence_server.graph_store import GraphStore

        class BadStore:
            @property
            def workspace(self) -> str:
                return "test"

            async def upsert_edge(
                self, source: str, target: str, edge_type: str, properties: dict[str, Any]
            ) -> None: ...

            async def get_node(self, node_id: str) -> dict[str, Any] | None: ...
            async def get_edge(
                self, source: str, target: str, edge_type: str
            ) -> dict[str, Any] | None: ...
            async def flush(self) -> None: ...
            async def close(self) -> None: ...

        assert not isinstance(BadStore(), GraphStore)


class TestQueryableStoreProtocol:
    def test_is_runtime_checkable(self):
        from context_intelligence_server.graph_store import QueryableStore

        assert hasattr(QueryableStore, "__protocol_attrs__") or hasattr(
            QueryableStore, "_is_runtime_protocol"
        )
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/test_graph_store.py -v
```

Expected: `ModuleNotFoundError`

**Step 3: Write `context_intelligence_server/graph_store.py`**

Port from bundle with `graph_forest_name` → `workspace` in all protocol properties and docstrings:

```python
"""GraphStore protocol — the async interface for graph storage backends.

Non-negotiable guarantees
-------------------------
1. upsert_node / upsert_edge MUST return immediately (buffer, no I/O).
2. get_node / get_edge MUST reflect buffered state (buffer-first reads).
3. flush() persists buffered writes (called by lifecycle triggers, not handlers).
4. close() MUST call flush() before releasing resources.
5. Flush failure MUST NOT propagate to handlers.

QueryableStore extension
------------------------
6. supported_dialects advertises the set of query languages the backend speaks.
7. execute_query runs a query in the specified (or default) dialect.
8. ValueError is raised when the requested dialect is not in supported_dialects.

Workspace awareness
-------------------
9.  workspace is a read-only property set at construction time.
10. All writes are scoped to the store's workspace.
11. Point lookups by ID are workspace-agnostic (IDs are globally unique).
12. execute_query supports optional workspace for cross-workspace queries.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class GraphStore(Protocol):
    """Async protocol for graph storage backends."""

    @property
    def workspace(self) -> str:
        """The workspace this store writes to.

        Set at construction time and immutable for the lifetime of the store.
        All writes are scoped to this workspace.  Point lookups by ID are
        workspace-agnostic (IDs are globally unique).
        """
        ...

    async def upsert_node(self, node_id: str, labels: set[str], properties: dict[str, Any]) -> None:
        """Insert or update a node. MUST return immediately — buffer only, no I/O."""
        ...

    async def upsert_edge(
        self, source: str, target: str, edge_type: str, properties: dict[str, Any]
    ) -> None:
        """Insert or update an edge. MUST return immediately — buffer only, no I/O."""
        ...

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        """Retrieve a node by ID. MUST reflect buffered state."""
        ...

    async def get_edge(self, source: str, target: str, edge_type: str) -> dict[str, Any] | None:
        """Retrieve an edge by composite key. MUST reflect buffered state."""
        ...

    async def flush(self) -> None:
        """Persist buffered writes. Flush failure MUST NOT propagate to handlers."""
        ...

    async def close(self) -> None:
        """Shut down the store. MUST call flush() before releasing resources."""
        ...


@runtime_checkable
class QueryableStore(GraphStore, Protocol):
    """Extension of GraphStore that supports ad-hoc queries."""

    @property
    def supported_dialects(self) -> frozenset[str]:
        """The set of query dialects this backend can execute."""
        ...

    async def execute_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        dialect: str | None = None,
        workspace: str | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a query in the given dialect.

        Parameters
        ----------
        query: The query string.
        params: Optional bind parameters.
        dialect: Which query language to use. None means the backend's default.
        workspace: Workspace scope for the query. None scopes to the store's own
            workspace. "*" disables workspace filtering (cross-workspace query).
        """
        ...
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/test_graph_store.py -v
```

Expected: All pass.

**Step 5: Commit**

```bash
git add context_intelligence_server/graph_store.py tests/test_graph_store.py
git commit -m "feat(phase2): port graph_store.py protocol (workspace rename)"
```

---

### Task 3: Port `services.py` — `SessionCursors`, `GraphState`, `HookConfig`

**Files:**
- Create: `context_intelligence_server/services.py`
- Create: `tests/test_services.py`

**Step 1: Write the failing tests**

Create `tests/test_services.py`:

```python
"""Tests for SessionCursors, GraphState, and HookConfig."""

from __future__ import annotations

import dataclasses

import pytest


class TestHookConfig:
    def test_construction_with_empty_config(self):
        from context_intelligence_server.services import HookConfig

        config = HookConfig(raw_config={})
        assert config.exclude_events == set()

    def test_construction_with_exclude_events(self):
        from context_intelligence_server.services import HookConfig

        config = HookConfig(
            raw_config={"exclude_events": ["content_block:delta", "thinking:delta"]}
        )
        assert config.exclude_events == {"content_block:delta", "thinking:delta"}

    def test_is_excluded_exact_match(self):
        from context_intelligence_server.services import HookConfig

        config = HookConfig(raw_config={"exclude_events": ["session:start"]})
        assert config.is_excluded("session:start") is True
        assert config.is_excluded("session:end") is False

    def test_is_excluded_wildcard_match(self):
        from context_intelligence_server.services import HookConfig

        config = HookConfig(raw_config={"exclude_events": ["session-naming:*"]})
        assert config.is_excluded("session-naming:foo") is True
        assert config.is_excluded("session:start") is False


class TestSessionCursors:
    def test_defaults(self):
        from context_intelligence_server.services import SessionCursors

        cursors = SessionCursors()
        assert cursors.current_run_id is None
        assert cursors.current_step_id is None
        assert cursors.run_counter == 0
        assert cursors.step_counter == 0
        assert cursors.prompt_preview == ""
        assert cursors.parallel_groups == {}
        assert cursors.tool_call_map == {}

    def test_is_dataclass(self):
        from context_intelligence_server.services import SessionCursors

        assert dataclasses.is_dataclass(SessionCursors)


class TestGraphState:
    def test_construction_default_workspace(self):
        from context_intelligence_server.services import GraphState

        graph = GraphState()
        assert graph.workspace == "default"

    def test_construction_explicit_workspace(self):
        from context_intelligence_server.services import GraphState

        graph = GraphState(workspace="my-project")
        assert graph.workspace == "my-project"

    def test_workspace_is_settable(self):
        from context_intelligence_server.services import GraphState

        graph = GraphState()
        graph.workspace = "new-workspace"
        assert graph.workspace == "new-workspace"

    async def test_upsert_node_creates_node(self):
        from context_intelligence_server.services import GraphState

        graph = GraphState()
        await graph.upsert_node("s1", labels={"Session"}, properties={"started": True})
        node = await graph.get_node("s1")
        assert node is not None
        assert node["labels"] == {"Session"}
        assert node["properties"]["started"] is True

    async def test_upsert_node_merges_labels(self):
        from context_intelligence_server.services import GraphState

        graph = GraphState()
        await graph.upsert_node("s1", labels={"Session", "Root"}, properties={})
        await graph.upsert_node("s1", labels={"Resumed"}, properties={})
        node = await graph.get_node("s1")
        assert node is not None
        assert node["labels"] == {"Session", "Root", "Resumed"}

    async def test_upsert_node_merges_properties(self):
        from context_intelligence_server.services import GraphState

        graph = GraphState()
        await graph.upsert_node("s1", labels={"Session"}, properties={"started": True})
        await graph.upsert_node("s1", labels={"Session"}, properties={"ended": True})
        node = await graph.get_node("s1")
        assert node is not None
        assert node["properties"]["started"] is True
        assert node["properties"]["ended"] is True

    async def test_upsert_edge_creates_edge(self):
        from context_intelligence_server.services import GraphState

        graph = GraphState()
        await graph.upsert_edge("s1", "r1", edge_type="HAS_RUN", properties={})
        edge = await graph.get_edge("s1", "r1", edge_type="HAS_RUN")
        assert edge is not None

    async def test_get_nonexistent_node_returns_none(self):
        from context_intelligence_server.services import GraphState

        graph = GraphState()
        assert await graph.get_node("nonexistent") is None

    async def test_get_nonexistent_edge_returns_none(self):
        from context_intelligence_server.services import GraphState

        graph = GraphState()
        assert await graph.get_edge("a", "b", edge_type="X") is None

    async def test_flush_is_noop(self):
        from context_intelligence_server.services import GraphState

        graph = GraphState()
        await graph.flush()  # should not raise

    async def test_close_is_noop(self):
        from context_intelligence_server.services import GraphState

        graph = GraphState()
        await graph.close()  # should not raise

    def test_no_graph_forest_name_attribute(self):
        """workspace replaces graph_forest_name — the old name must not exist."""
        from context_intelligence_server.services import GraphState

        graph = GraphState()
        assert not hasattr(graph, "graph_forest_name")
        assert not hasattr(graph, "_graph_forest_name")
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/test_services.py -v
```

Expected: `ModuleNotFoundError`

**Step 3: Write `context_intelligence_server/services.py` (partial — GraphState, SessionCursors, HookConfig)**

```python
"""Shared services for all context-intelligence handlers."""

from __future__ import annotations

import dataclasses
import fnmatch
import logging
from typing import Any

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SessionCursors:
    """Per-session cursor state for tracking active run/step/tool positions."""

    current_run_id: str | None = None
    current_step_id: str | None = None
    run_counter: int = 0
    step_counter: int = 0
    prompt_preview: str = ""
    parallel_groups: dict[str, list[str]] = dataclasses.field(default_factory=dict)
    tool_call_map: dict[str, str] = dataclasses.field(default_factory=dict)


class HookConfig:
    """Configuration for the context-intelligence hook."""

    def __init__(self, raw_config: dict[str, Any]) -> None:
        self._raw = raw_config
        self._exclude_patterns: set[str] = set(raw_config.get("exclude_events", []))

    @property
    def exclude_events(self) -> set[str]:
        return self._exclude_patterns

    def is_excluded(self, event: str) -> bool:
        for pattern in self._exclude_patterns:
            if fnmatch.fnmatch(event, pattern):
                return True
        return False


class GraphState:
    """In-memory property graph state conforming to the GraphStore protocol."""

    def __init__(self, workspace: str = "default") -> None:
        self._workspace = workspace
        self._nodes: dict[str, dict[str, Any]] = {}
        self._edges: dict[tuple[str, str, str], dict[str, Any]] = {}

    @property
    def workspace(self) -> str:
        return self._workspace

    @workspace.setter
    def workspace(self, value: str) -> None:
        self._workspace = value

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        return self._nodes.get(node_id)

    async def upsert_node(self, node_id: str, labels: set[str], properties: dict[str, Any]) -> None:
        existing = self._nodes.get(node_id)
        if existing is not None:
            existing["labels"] |= labels
            existing["properties"].update(properties)
            return
        self._nodes[node_id] = {"id": node_id, "labels": set(labels), "properties": dict(properties)}

    async def get_edge(self, source: str, target: str, edge_type: str) -> dict[str, Any] | None:
        return self._edges.get((source, target, edge_type))

    async def upsert_edge(
        self, source: str, target: str, edge_type: str, properties: dict[str, Any]
    ) -> None:
        key = (source, target, edge_type)
        existing = self._edges.get(key)
        if existing is not None:
            existing["properties"].update(properties)
            return
        self._edges[key] = {
            "source": source,
            "target": target,
            "type": edge_type,
            "properties": dict(properties),
        }

    def schedule_flush(self) -> None:
        """No-op for in-memory state."""

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        await self.flush()
```

Note: `HookStateService` is added in Task 4. This file is created now and extended in the next task.

**Step 4: Run tests to verify they pass**

```bash
pytest tests/test_services.py -v
```

Expected: All pass.

**Step 5: Commit**

```bash
git add context_intelligence_server/services.py tests/test_services.py
git commit -m "feat(phase2): port SessionCursors, GraphState, HookConfig (workspace rename)"
```

---

### Task 4: Port `services.py` — `HookStateService`

**Files:**
- Modify: `context_intelligence_server/services.py`
- Modify: `tests/test_services.py`
- Modify: `tests/conftest.py`

**Step 1: Add failing tests to `tests/test_services.py`**

Append to `tests/test_services.py`:

```python
class TestHookStateService:
    def test_construction_sets_workspace_on_graph(self):
        from context_intelligence_server.services import GraphState, HookStateService

        svc = HookStateService(workspace="my-project")
        assert isinstance(svc.graph, GraphState)
        assert svc.graph.workspace == "my-project"

    def test_construction_default_workspace(self):
        from context_intelligence_server.services import HookStateService

        svc = HookStateService()
        assert svc.graph.workspace == "default"

    def test_uses_injected_graph_store(self):
        from context_intelligence_server.services import GraphState, HookStateService

        prebuilt = GraphState(workspace="injected")
        svc = HookStateService(workspace="my-project", graph_store=prebuilt)
        assert svc.graph is prebuilt
        # workspace should be overwritten to match the service's workspace
        assert svc.graph.workspace == "my-project"

    def test_no_coordinator_attribute(self):
        """Server-side HookStateService has no coordinator dependency."""
        from context_intelligence_server.services import HookStateService

        svc = HookStateService()
        assert not hasattr(svc, "coordinator")
        assert not hasattr(svc, "_forest_resolved")

    def test_get_cursors_lazy_creation(self):
        from context_intelligence_server.services import HookStateService, SessionCursors

        svc = HookStateService()
        cursors = svc.get_cursors("sess-1")
        assert isinstance(cursors, SessionCursors)

    def test_get_cursors_same_instance_returned(self):
        from context_intelligence_server.services import HookStateService

        svc = HookStateService()
        a = svc.get_cursors("sess-1")
        b = svc.get_cursors("sess-1")
        assert a is b

    def test_get_cursors_different_sessions_different_instances(self):
        from context_intelligence_server.services import HookStateService

        svc = HookStateService()
        a = svc.get_cursors("sess-1")
        b = svc.get_cursors("sess-2")
        assert a is not b

    def test_remove_cursors(self):
        from context_intelligence_server.services import HookStateService

        svc = HookStateService()
        original = svc.get_cursors("sess-1")
        original.run_counter = 5
        svc.remove_cursors("sess-1")
        fresh = svc.get_cursors("sess-1")
        assert fresh is not original
        assert fresh.run_counter == 0

    def test_remove_nonexistent_is_safe(self):
        from context_intelligence_server.services import HookStateService

        svc = HookStateService()
        svc.remove_cursors("does-not-exist")  # should not raise

    async def test_ensure_session_node_creates_root_session(self):
        from context_intelligence_server.services import HookStateService

        svc = HookStateService(workspace="test-ws")
        await svc.ensure_session_node("s1", {"timestamp": "2026-01-01T00:00:00Z"})
        node = await svc.graph.get_node("s1")
        assert node is not None
        assert node["labels"] == {"Session", "Root"}
        assert node["properties"]["status"] == "running"

    async def test_ensure_session_node_idempotent(self):
        from context_intelligence_server.services import HookStateService

        svc = HookStateService()
        await svc.ensure_session_node("s1", {"timestamp": "2026-01-01T00:00:00Z"})
        await svc.ensure_session_node("s1", {"timestamp": "2026-01-02T00:00:00Z"})
        node = await svc.graph.get_node("s1")
        # Second call is a no-op — timestamp stays from the first call
        assert node["properties"]["started_at"] == "2026-01-01T00:00:00Z"

    async def test_ensure_session_node_subsession(self):
        from context_intelligence_server.services import HookStateService

        svc = HookStateService()
        await svc.ensure_session_node("child", {"timestamp": "2026-01-01T00:00:00Z", "parent_id": "parent"})
        node = await svc.graph.get_node("child")
        assert node is not None
        assert node["labels"] == {"Session", "Subsession"}

    def test_blob_store_default_is_none(self):
        from context_intelligence_server.services import HookStateService

        svc = HookStateService()
        assert svc.blob_store is None
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/test_services.py::TestHookStateService -v
```

Expected: `ImportError` (HookStateService doesn't exist yet)

**Step 3: Add `HookStateService` to `context_intelligence_server/services.py`**

Append to the end of `context_intelligence_server/services.py`:

```python
class HookStateService:
    """Top-level service container shared across all handlers.

    Server-side replacement — no coordinator dependency.
    Workspace is set directly at construction time.
    """

    def __init__(
        self,
        workspace: str = "default",
        graph_store: Any = None,
        *,
        raw_config: dict[str, Any] | None = None,
        blob_store: Any = None,
    ) -> None:
        self.config = HookConfig(raw_config if raw_config is not None else {})
        if graph_store is not None:
            self.graph = graph_store
        else:
            self.graph = GraphState()
        # Set workspace on the graph store — replaces _resolve_forest_name_from_coordinator()
        self.graph.workspace = workspace
        self._cursors: dict[str, SessionCursors] = {}
        self._seen_sessions: set[str] = set()
        self.blob_store = blob_store

    def get_cursors(self, session_id: str) -> SessionCursors:
        """Return the SessionCursors for *session_id*, lazily creating one if needed."""
        if session_id not in self._cursors:
            self._cursors[session_id] = SessionCursors()
        return self._cursors[session_id]

    async def ensure_session_node(self, session_id: str, data: dict[str, Any]) -> None:
        """Ensure a Session node exists in the graph for this session_id.

        Idempotent — repeated calls for the same session_id are no-ops.
        """
        if session_id in self._seen_sessions:
            return
        self._seen_sessions.add(session_id)

        timestamp = data.get("timestamp", "")
        parent_id = (data.get("parent_id") or data.get("parent") or "").strip()
        labels: set[str] = {"Session", "Subsession"} if parent_id else {"Session", "Root"}
        properties: dict[str, Any] = {
            "started_at": timestamp,
            "status": "running",
        }
        await self.graph.upsert_node(session_id, labels, properties)

    def remove_cursors(self, session_id: str) -> None:
        """Remove cursor state for *session_id*. Safe to call for nonexistent sessions."""
        self._cursors.pop(session_id, None)
```

**Step 4: Update `tests/conftest.py`**

Add the `services` fixture that all handler tests will use:

```python
# Add to the existing tests/conftest.py (after any Phase 1 fixtures):

import pytest

from context_intelligence_server.services import HookStateService


@pytest.fixture
def services() -> HookStateService:
    """A fresh HookStateService using default GraphState (no external store)."""
    return HookStateService(workspace="test-workspace")
```

**Step 5: Run tests to verify they pass**

```bash
pytest tests/test_services.py -v
```

Expected: All pass.

**Step 6: Commit**

```bash
git add context_intelligence_server/services.py tests/test_services.py tests/conftest.py
git commit -m "feat(phase2): port HookStateService — workspace replaces coordinator"
```

---

### Task 5: Port `neo4j_store.py` — connection and buffer

**Files:**
- Create: `context_intelligence_server/neo4j_store.py`
- Create: `tests/test_neo4j_store.py`
- Modify: `pyproject.toml` (add `neo4j` dependency)

**Step 1: Add `neo4j` to `pyproject.toml`**

Add `"neo4j>=5.0"` to the `dependencies` list in `pyproject.toml`.

Then install:

```bash
pip install -e ".[dev]"
```

**Step 2: Write the failing tests**

Create `tests/test_neo4j_store.py`:

```python
"""Tests for Neo4jGraphStore — buffer operations and protocol conformance."""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

NEO4J_URI = os.environ.get("NEO4J_URI", "neo4j://localhost:7687")
NEO4J_AUTH = ("neo4j", "password")
NEO4J_DATABASE = "neo4j"


class TestProtocolConformance:
    def test_isinstance_graph_store(self):
        from context_intelligence_server.graph_store import GraphStore
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH, database=NEO4J_DATABASE)
        assert isinstance(store, GraphStore)

    def test_isinstance_queryable_store(self):
        from context_intelligence_server.graph_store import QueryableStore
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH, database=NEO4J_DATABASE)
        assert isinstance(store, QueryableStore)


class TestWorkspaceProperty:
    def test_workspace_default_is_default(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH)
        assert store.workspace == "default"

    def test_workspace_explicit(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH, workspace="my-ws")
        assert store.workspace == "my-ws"

    def test_workspace_is_settable(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH)
        store.workspace = "new-ws"
        assert store.workspace == "new-ws"

    def test_no_graph_forest_name_attribute(self):
        """workspace replaces graph_forest_name — the old name must not exist."""
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH)
        assert not hasattr(store, "graph_forest_name")
        assert not hasattr(store, "_graph_forest_name")


class TestBufferOperations:
    async def test_upsert_node_adds_to_buffer(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH)
        await store.upsert_node("n1", {"Session"}, {"status": "running"})
        assert "n1" in store._node_buffer

    async def test_upsert_node_merges_properties(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH)
        await store.upsert_node("n1", {"Session"}, {"a": 1})
        await store.upsert_node("n1", {"Session"}, {"b": 2})
        assert store._node_buffer["n1"]["properties"]["a"] == 1
        assert store._node_buffer["n1"]["properties"]["b"] == 2

    async def test_upsert_node_merges_labels(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH)
        await store.upsert_node("n1", {"Session"}, {})
        await store.upsert_node("n1", {"Root"}, {})
        assert store._node_buffer["n1"]["labels"] == {"Session", "Root"}

    async def test_upsert_edge_adds_to_buffer(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH)
        await store.upsert_edge("s1", "r1", "HAS_RUN", {"seq": 1})
        assert ("s1", "r1", "HAS_RUN") in store._edge_buffer

    async def test_get_node_buffer_first(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH)
        await store.upsert_node("n1", {"Session"}, {"status": "running"})
        node = await store.get_node("n1")
        assert node is not None
        assert node["properties"]["status"] == "running"

    async def test_get_node_returns_copy(self):
        """get_node returns a copy — mutating the result does not affect the buffer."""
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH)
        await store.upsert_node("n1", {"Session"}, {"status": "running"})
        node = await store.get_node("n1")
        node["properties"]["status"] = "MUTATED"
        original = await store.get_node("n1")
        assert original["properties"]["status"] == "running"

    async def test_get_edge_buffer_first(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH)
        await store.upsert_edge("s1", "r1", "HAS_RUN", {"seq": 1})
        edge = await store.get_edge("s1", "r1", "HAS_RUN")
        assert edge is not None
        assert edge["properties"]["seq"] == 1

    def test_supported_dialects_includes_cypher(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH)
        assert "cypher" in store.supported_dialects
```

**Step 3: Run tests to verify they fail**

```bash
pytest tests/test_neo4j_store.py -v
```

Expected: `ModuleNotFoundError`

**Step 4: Write `context_intelligence_server/neo4j_store.py` (connection + buffer only)**

```python
"""Neo4jGraphStore — buffer-first reads with async Neo4j persistence.

All references to graph_forest_name are replaced with workspace.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any

from neo4j import AsyncGraphDatabase

logger = logging.getLogger(__name__)


class Neo4jGraphStore:
    """Async Neo4j graph store conforming to the QueryableStore protocol."""

    def __init__(
        self,
        uri: str = "neo4j://localhost:7687",
        auth: tuple[str, str] | None = ("neo4j", "password"),
        database: str = "neo4j",
        workspace: str | None = None,
    ) -> None:
        self._driver = AsyncGraphDatabase.driver(uri, auth=auth)
        self._database = database
        self._workspace: str | None = workspace

        # Write buffers
        self._node_buffer: dict[str, dict[str, Any]] = {}
        self._edge_buffer: dict[tuple[str, str, str], dict[str, Any]] = {}

        # Schema tracking
        self._schema_initialized: bool = False
        self._closed: bool = False

        # Background flush tracking
        self._flush_task: asyncio.Task[None] | None = None

    # -- Properties ----------------------------------------------------------

    @property
    def workspace(self) -> str:
        """The workspace this store writes to. Returns 'default' if not yet set."""
        return self._workspace or "default"

    @workspace.setter
    def workspace(self, value: str) -> None:
        self._workspace = value

    @property
    def supported_dialects(self) -> frozenset[str]:
        return frozenset({"cypher"})

    # -- GraphStore methods (buffer operations) --------------------------------

    async def upsert_node(self, node_id: str, labels: set[str], properties: dict[str, Any]) -> None:
        existing = self._node_buffer.get(node_id)
        if existing:
            existing["labels"] |= labels
            existing["properties"].update(properties)
        else:
            self._node_buffer[node_id] = {
                "id": node_id,
                "labels": set(labels),
                "properties": dict(properties),
            }

    async def upsert_edge(
        self, source: str, target: str, edge_type: str, properties: dict[str, Any]
    ) -> None:
        key = (source, target, edge_type)
        existing = self._edge_buffer.get(key)
        if existing:
            existing["properties"].update(properties)
        else:
            self._edge_buffer[key] = {
                "source": source,
                "target": target,
                "type": edge_type,
                "properties": dict(properties),
            }

    # -- Neo4j-compatible primitive types ------------------------------------
    _NEO4J_PRIMITIVES = (str, int, float, bool)

    @staticmethod
    def _sanitize_properties(props: dict[str, Any]) -> dict[str, Any]:
        """Sanitize property values for Neo4j compatibility."""
        result: dict[str, Any] = {}
        for key, value in props.items():
            if value is None:
                continue
            if isinstance(value, Neo4jGraphStore._NEO4J_PRIMITIVES):
                result[key] = value
            elif isinstance(value, list):
                if value and all(
                    isinstance(item, Neo4jGraphStore._NEO4J_PRIMITIVES) for item in value
                ):
                    result[key] = value
                else:
                    result[key] = json.dumps(value, default=str)
            elif isinstance(value, dict):
                result[key] = json.dumps(value, default=str)
            else:
                result[key] = str(value)
        return result

    @staticmethod
    def _convert_timestamps(props: dict[str, Any]) -> dict[str, Any]:
        """Convert *_at ISO-8601 string properties to Python datetime objects."""
        result = {}
        for key, value in props.items():
            if key.endswith("_at") and isinstance(value, str) and value:
                try:
                    result[key] = datetime.fromisoformat(value)
                except ValueError:
                    logger.warning(
                        "Could not parse timestamp for property %r: %r — keeping as string",
                        key,
                        value,
                    )
                    result[key] = value
            else:
                result[key] = value
        return result

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        # Buffer-first
        buffered = self._node_buffer.get(node_id)
        if buffered is not None:
            return {**buffered, "properties": dict(buffered["properties"])}

        # Fallback: query Neo4j
        async with self._driver.session(database=self._database) as session:
            result = await session.run(
                "MATCH (n {node_id: $node_id, workspace: $ws}) RETURN n",
                node_id=node_id,
                ws=self.workspace,
            )
            record = await result.single()
            if record is None:
                return None
            neo4j_node = record["n"]
            props = dict(neo4j_node)
            props.pop("node_id", None)
            props.pop("workspace", None)
            return {
                "id": node_id,
                "labels": set(neo4j_node.labels),
                "properties": props,
            }

    async def get_edge(self, source: str, target: str, edge_type: str) -> dict[str, Any] | None:
        # Buffer-first
        key = (source, target, edge_type)
        buffered = self._edge_buffer.get(key)
        if buffered is not None:
            return {**buffered, "properties": dict(buffered["properties"])}

        # Fallback: query Neo4j
        async with self._driver.session(database=self._database) as session:
            result = await session.run(
                "MATCH (s {node_id: $source, workspace: $ws})"
                "-[r]->"
                "(t {node_id: $target, workspace: $ws}) "
                "WHERE type(r) = $edge_type AND r.workspace = $ws RETURN r",
                source=source,
                target=target,
                edge_type=edge_type,
                ws=self.workspace,
            )
            record = await result.single()
            if record is None:
                return None
            neo4j_rel = record["r"]
            props = dict(neo4j_rel)
            props.pop("workspace", None)
            return {
                "source": source,
                "target": target,
                "type": edge_type,
                "properties": props,
            }

    # Placeholder — flush, close, execute_query, _ensure_schema added in Task 6
    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def execute_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        dialect: str | None = None,
        workspace: str | None = None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError("Added in Task 6")
```

**Step 5: Run tests to verify they pass**

```bash
pytest tests/test_neo4j_store.py -v
```

Expected: All pass.

**Step 6: Commit**

```bash
git add context_intelligence_server/neo4j_store.py tests/test_neo4j_store.py pyproject.toml
git commit -m "feat(phase2): port Neo4jGraphStore connection + buffer (workspace rename)"
```

---

### Task 6: Port `neo4j_store.py` — flush, schema, close, execute_query

**Files:**
- Modify: `context_intelligence_server/neo4j_store.py`
- Modify: `tests/test_neo4j_store.py`

**Step 1: Add failing tests to `tests/test_neo4j_store.py`**

Append to `tests/test_neo4j_store.py`:

```python
class TestFlushWritesWorkspace:
    """Verify flush produces rows with 'workspace' property, NOT 'graph_forest_name'."""

    async def test_flush_node_rows_contain_workspace(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH, workspace="test-ws")
        await store.upsert_node("n1", {"Session"}, {"status": "running"})

        # Capture the rows passed to tx.run during flush
        captured_rows: list[Any] = []

        mock_tx = AsyncMock()

        async def capture_run(query, **kwargs):
            if "rows" in kwargs:
                captured_rows.extend(kwargs["rows"])
            return AsyncMock()

        mock_tx.run = capture_run
        mock_tx.commit = AsyncMock()

        mock_session = AsyncMock()
        mock_session.begin_transaction = AsyncMock(return_value=mock_tx)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        store._schema_initialized = True  # skip schema setup
        with patch.object(store._driver, "session", return_value=mock_session):
            await store.flush()

        assert len(captured_rows) > 0
        for row in captured_rows:
            assert "workspace" in row["props"], f"Row missing 'workspace': {row}"
            assert row["props"]["workspace"] == "test-ws"
            assert "graph_forest_name" not in row["props"], (
                f"Row still contains 'graph_forest_name': {row}"
            )

    async def test_flush_edge_rows_contain_workspace(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH, workspace="test-ws")
        await store.upsert_node("s1", {"Session"}, {})
        await store.upsert_node("r1", {"OrchestratorRun"}, {})
        await store.upsert_edge("s1", "r1", "HAS_RUN", {"seq": 1})

        captured_edge_rows: list[Any] = []

        mock_tx = AsyncMock()

        async def capture_run(query, **kwargs):
            if "rows" in kwargs and "HAS_RUN" in str(query):
                captured_edge_rows.extend(kwargs["rows"])
            return AsyncMock()

        mock_tx.run = capture_run
        mock_tx.commit = AsyncMock()

        mock_session = AsyncMock()
        mock_session.begin_transaction = AsyncMock(return_value=mock_tx)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        store._schema_initialized = True
        with patch.object(store._driver, "session", return_value=mock_session):
            await store.flush()

        assert len(captured_edge_rows) > 0
        for row in captured_edge_rows:
            assert "workspace" in row["props"]
            assert row["props"]["workspace"] == "test-ws"
            assert "graph_forest_name" not in row["props"]

    async def test_flush_empty_buffers_is_noop(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH)
        # Should not raise or contact Neo4j
        await store.flush()

    async def test_flush_clears_buffers_on_success(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH, workspace="test")
        await store.upsert_node("n1", {"Session"}, {"x": 1})

        mock_tx = AsyncMock()
        mock_tx.run = AsyncMock()
        mock_tx.commit = AsyncMock()

        mock_session = AsyncMock()
        mock_session.begin_transaction = AsyncMock(return_value=mock_tx)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        store._schema_initialized = True
        with patch.object(store._driver, "session", return_value=mock_session):
            await store.flush()

        assert store._node_buffer == {}
        assert store._edge_buffer == {}

    async def test_flush_restores_buffers_on_failure(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH, workspace="test")
        await store.upsert_node("n1", {"Session"}, {"x": 1})

        mock_tx = AsyncMock()
        mock_tx.run = AsyncMock(side_effect=RuntimeError("connection lost"))
        mock_tx.rollback = AsyncMock()

        mock_session = AsyncMock()
        mock_session.begin_transaction = AsyncMock(return_value=mock_tx)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        store._schema_initialized = True
        with patch.object(store._driver, "session", return_value=mock_session):
            await store.flush()  # should not raise

        # Buffer should be restored
        assert "n1" in store._node_buffer


class TestSchemaIndexesWorkspace:
    async def test_ensure_schema_creates_workspace_index(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH)

        captured_queries: list[str] = []
        mock_session = AsyncMock()

        async def capture_run(query, **kwargs):
            captured_queries.append(query)
            return AsyncMock()

        mock_session.run = capture_run
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch.object(store._driver, "session", return_value=mock_session):
            await store._ensure_schema()

        # Verify workspace column is used, not graph_forest_name
        workspace_idx = [q for q in captured_queries if "workspace" in q.lower()]
        forest_idx = [q for q in captured_queries if "graph_forest_name" in q.lower()]
        assert len(workspace_idx) > 0, "No index on 'workspace' found"
        assert len(forest_idx) == 0, f"Found graph_forest_name in indexes: {forest_idx}"


class TestExecuteQuery:
    async def test_execute_query_injects_workspace_param(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH, workspace="my-ws")

        mock_result = AsyncMock()
        mock_result.__aiter__ = lambda self: self
        mock_result.__anext__ = AsyncMock(side_effect=StopAsyncIteration)

        mock_session = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch.object(store._driver, "session", return_value=mock_session):
            await store.execute_query("MATCH (n) RETURN n", params={"limit": 10})

        call_args = mock_session.run.call_args
        passed_params = call_args[1] if call_args[1] else call_args[0][1]
        assert "workspace" in passed_params
        assert passed_params["workspace"] == "my-ws"
        assert "graph_forest_name" not in passed_params

    async def test_execute_query_unsupported_dialect_raises(self):
        from context_intelligence_server.neo4j_store import Neo4jGraphStore

        store = Neo4jGraphStore(uri=NEO4J_URI, auth=NEO4J_AUTH)
        with pytest.raises(ValueError, match="Unsupported dialect"):
            await store.execute_query("SELECT 1", dialect="sql")
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/test_neo4j_store.py::TestFlushWritesWorkspace -v
pytest tests/test_neo4j_store.py::TestSchemaIndexesWorkspace -v
pytest tests/test_neo4j_store.py::TestExecuteQuery -v
```

Expected: Failures (flush is a no-op placeholder, execute_query raises NotImplementedError)

**Step 3: Replace the placeholder methods in `context_intelligence_server/neo4j_store.py`**

Replace the `flush`, `close`, `execute_query` placeholders and add `_ensure_schema`, `schedule_flush`, `_background_flush`:

```python
    async def flush(self) -> None:
        """Flush buffered nodes and edges to Neo4j using UNWIND-based batch Cypher."""
        # Phase 1: snapshot and optimistically clear
        node_snapshot = dict(self._node_buffer)
        edge_snapshot = dict(self._edge_buffer)
        self._node_buffer.clear()
        self._edge_buffer.clear()

        # Phase 2: early exit if nothing to flush
        if not node_snapshot and not edge_snapshot:
            return

        # Phase 3: write to Neo4j
        try:
            await self._ensure_schema()

            async with self._driver.session(database=self._database) as session:
                tx = await session.begin_transaction()
                try:
                    ws = self.workspace

                    # -- UNWIND nodes grouped by primary label --
                    if node_snapshot:
                        enrichment_rows: list[dict[str, Any]] = []
                        primary_groups: dict[str, list[dict[str, Any]]] = {}
                        for node_id, entry in node_snapshot.items():
                            labels = entry["labels"]
                            row: dict[str, Any] = {
                                "node_id": node_id,
                                "props": {
                                    **self._convert_timestamps(
                                        self._sanitize_properties(entry["properties"])
                                    ),
                                    "node_id": node_id,
                                    "workspace": ws,
                                },
                                "labels": list(labels),
                            }
                            if not labels:
                                enrichment_rows.append(row)
                            else:
                                primary = sorted(labels)[0]
                                primary_groups.setdefault(primary, []).append(row)

                        if enrichment_rows:
                            await tx.run(
                                "UNWIND $rows AS row "
                                "MATCH (n {node_id: row.node_id}) "
                                "SET n += row.props",
                                rows=enrichment_rows,
                            )

                        for primary_label, rows in primary_groups.items():
                            await tx.run(
                                f"UNWIND $rows AS row "
                                f"MERGE (n:`{primary_label}` {{node_id: row.node_id}}) "
                                f"SET n += row.props",
                                rows=rows,
                            )

                        # -- Apply additional labels in second pass --
                        all_labeled_rows = [r for group in primary_groups.values() for r in group]
                        label_groups: dict[frozenset[str], list[str]] = {}
                        for row in all_labeled_rows:
                            key = frozenset(row["labels"])
                            if len(key) > 1:
                                label_groups.setdefault(key, []).append(row["node_id"])

                        for label_set, node_ids in label_groups.items():
                            label_clause = ":".join(f"`{lbl}`" for lbl in sorted(label_set))
                            await tx.run(
                                f"UNWIND $ids AS nid "
                                f"MATCH (n {{node_id: nid}}) "
                                f"SET n:{label_clause}",
                                ids=node_ids,
                            )

                    # -- UNWIND edges grouped by type --
                    if edge_snapshot:
                        edge_type_groups: dict[str, list[dict[str, Any]]] = {}
                        for (_src, _tgt, etype), entry in edge_snapshot.items():
                            edge_type_groups.setdefault(etype, []).append(
                                {
                                    "source": entry["source"],
                                    "target": entry["target"],
                                    "props": {
                                        **self._convert_timestamps(
                                            self._sanitize_properties(entry["properties"])
                                        ),
                                        "workspace": ws,
                                    },
                                }
                            )

                        for rel_type, edge_rows in edge_type_groups.items():
                            await tx.run(
                                f"UNWIND $rows AS row "
                                f"MATCH (s {{node_id: row.source}}) "
                                f"MATCH (t {{node_id: row.target}}) "
                                f"MERGE (s)-[r:`{rel_type}`]->(t) "
                                f"SET r += row.props",
                                rows=edge_rows,
                            )

                    await tx.commit()
                except Exception:
                    await tx.rollback()
                    raise

        except Exception:
            # Restore buffers on failure
            self._node_buffer.update(node_snapshot)
            self._edge_buffer.update(edge_snapshot)
            logger.warning("flush failed, buffers restored", exc_info=True)

    async def _ensure_schema(self) -> None:
        """Ensure Neo4j schema indexes exist (idempotent, runs once per instance)."""
        if self._schema_initialized:
            return

        try:
            async with self._driver.session(database=self._database) as session:
                await session.run(
                    "CREATE INDEX idx_session_node_id IF NOT EXISTS FOR (n:Session) ON (n.node_id)"
                )
                await session.run(
                    "CREATE INDEX idx_orchestrator_run_node_id IF NOT EXISTS FOR (n:OrchestratorRun) ON (n.node_id)"
                )
                await session.run(
                    "CREATE INDEX idx_step_node_id IF NOT EXISTS FOR (n:Step) ON (n.node_id)"
                )
                await session.run(
                    "CREATE INDEX idx_tool_execution_node_id IF NOT EXISTS FOR (n:ToolExecution) ON (n.node_id)"
                )
                await session.run(
                    "CREATE INDEX idx_event_node_id IF NOT EXISTS FOR (n:Event) ON (n.node_id)"
                )
                # Workspace filtering index on Session
                await session.run(
                    "CREATE INDEX idx_session_workspace IF NOT EXISTS "
                    "FOR (n:Session) ON (n.workspace)"
                )
        except Exception:
            logger.warning("schema initialization failed", exc_info=True)
            raise

        self._schema_initialized = True

    def schedule_flush(self) -> None:
        """Schedule a non-blocking background flush."""
        if self._flush_task is not None and not self._flush_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
            self._flush_task = loop.create_task(self._background_flush())
        except RuntimeError:
            pass

    async def _background_flush(self) -> None:
        try:
            await self.flush()
        except Exception:
            logger.warning("background flush failed", exc_info=True)

    async def close(self) -> None:
        if not self._closed:
            if self._flush_task is not None and not self._flush_task.done():
                try:
                    await self._flush_task
                except Exception:
                    pass
            await self.flush()
            try:
                await self._driver.close()
            except RuntimeError as exc:
                if "attached to a different loop" in str(exc):
                    logger.debug("Neo4j driver close deferred to GC: event loop mismatch.")
                else:
                    raise
            self._closed = True

    # -- QueryableStore methods ----------------------------------------------

    async def execute_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        dialect: str | None = None,
        workspace: str | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a raw Cypher query with dialect validation and workspace param injection."""
        if dialect is not None and dialect not in self.supported_dialects:
            msg = f"Unsupported dialect {dialect!r}. Supported: {self.supported_dialects}"
            raise ValueError(msg)

        resolved_ws = workspace if workspace is not None else self.workspace
        resolved_params: dict[str, Any] = dict(params) if params else {}
        if resolved_ws != "*":
            resolved_params["workspace"] = resolved_ws

        async with self._driver.session(database=self._database) as session:
            result = await session.run(query, resolved_params)
            return [dict(record) async for record in result]
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/test_neo4j_store.py -v
```

Expected: All pass.

**Step 5: Commit**

```bash
git add context_intelligence_server/neo4j_store.py tests/test_neo4j_store.py
git commit -m "feat(phase2): port Neo4jGraphStore flush/schema/close (workspace rename)"
```

---

### Task 7: Update `registry.py` — add `HookStateService` to `SessionWorker`

**Files:**
- Modify: `context_intelligence_server/registry.py`
- Modify: `tests/test_registry.py`

**Step 1: Add failing tests to `tests/test_registry.py`**

Append to `tests/test_registry.py`:

```python
class TestSessionWorkerHasServices:
    def test_worker_has_services_attribute(self):
        from context_intelligence_server.registry import SessionRegistry

        registry = SessionRegistry()
        worker = registry.get_or_create("sess-1", workspace="test-ws")
        assert hasattr(worker, "services")

    def test_worker_services_workspace_matches(self):
        from context_intelligence_server.registry import SessionRegistry

        registry = SessionRegistry()
        worker = registry.get_or_create("sess-1", workspace="my-project")
        assert worker.services.graph.workspace == "my-project"

    def test_worker_has_workspace_attribute(self):
        from context_intelligence_server.registry import SessionRegistry

        registry = SessionRegistry()
        worker = registry.get_or_create("sess-1", workspace="test-ws")
        assert worker.workspace == "test-ws"
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/test_registry.py::TestSessionWorkerHasServices -v
```

Expected: Failure (worker doesn't have `services` yet, `get_or_create` doesn't accept `workspace`)

**Step 3: Update `context_intelligence_server/registry.py`**

Update the `SessionWorker` dataclass and `SessionRegistry.get_or_create` to include `HookStateService`:

In the `SessionWorker` dataclass, add:
```python
workspace: str
services: HookStateService
```

In `SessionRegistry.get_or_create`, update to accept `workspace: str` and instantiate:
```python
from context_intelligence_server.services import HookStateService

# Inside get_or_create:
services = HookStateService(workspace=workspace)
worker = SessionWorker(queue=asyncio.Queue(), workspace=workspace, services=services)
```

The exact edit depends on Phase 1's `registry.py` structure. The key additions are:
1. `SessionWorker` gains `workspace: str` and `services: HookStateService` fields
2. `get_or_create(session_id, workspace)` creates `HookStateService(workspace=workspace)` on first access
3. Import `HookStateService` from `context_intelligence_server.services`

**Step 4: Run tests to verify they pass**

```bash
pytest tests/test_registry.py -v
```

Expected: All pass (existing Phase 1 tests may need `workspace` parameter added to their `get_or_create` calls).

**Step 5: Commit**

```bash
git add context_intelligence_server/registry.py tests/test_registry.py
git commit -m "feat(phase2): add HookStateService to SessionWorker"
```

---

### Task 8: Create `pipeline.py`

**Files:**
- Create: `context_intelligence_server/pipeline.py`
- Create: `tests/test_pipeline.py`

**Step 1: Write the failing tests**

Create `tests/test_pipeline.py`:

```python
"""Tests for the server-side event processing pipeline."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService


def _make_mock_worker(workspace: str = "test-ws") -> MagicMock:
    """Create a mock SessionWorker with real HookStateService."""
    worker = MagicMock()
    worker.services = HookStateService(workspace=workspace)
    worker.workspace = workspace
    return worker


class TestFindHandler:
    def test_finds_matching_handler(self):
        from context_intelligence_server.pipeline import _find_handler, setup_handlers

        svc = HookStateService(workspace="test")
        handlers = setup_handlers(svc)
        handler = _find_handler("session:start", handlers)
        assert handler is not None

    def test_returns_default_for_unclaimed_event(self):
        from context_intelligence_server.pipeline import _find_handler, setup_handlers

        svc = HookStateService(workspace="test")
        handlers = setup_handlers(svc)
        # "my:custom:event" is not claimed by any entity handler
        handler = _find_handler("my:custom:event", handlers)
        assert handler is not None  # DefaultHandler catches it

    def test_wildcard_matching(self):
        """StepHandler claims 'content_block:*' — verify wildcard dispatch."""
        from context_intelligence_server.pipeline import _find_handler, setup_handlers

        svc = HookStateService(workspace="test")
        handlers = setup_handlers(svc)
        handler = _find_handler("content_block:delta", handlers)
        assert handler is not None

    def test_system_events_claimed_not_default(self):
        """SystemEventHandler claims context:compaction — prevent DefaultHandler."""
        from context_intelligence_server.pipeline import _find_handler, setup_handlers
        from context_intelligence_server.handlers.event import SystemEventHandler

        svc = HookStateService(workspace="test")
        handlers = setup_handlers(svc)
        handler = _find_handler("context:compaction", handlers)
        assert handler is not None
        assert isinstance(handler, SystemEventHandler)


class TestProcessEvent:
    async def test_calls_ensure_session_node(self):
        from context_intelligence_server.pipeline import process_event, setup_handlers

        worker = _make_mock_worker()
        handlers = setup_handlers(worker.services)

        data = {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"}
        await process_event(worker, "session:start", data, handlers)

        node = await worker.services.graph.get_node("s1")
        assert node is not None
        assert "Session" in node["labels"]

    async def test_handler_exception_does_not_propagate(self):
        """Pipeline wraps handler dispatch in try/except — errors are logged, not raised."""
        from context_intelligence_server.pipeline import process_event, setup_handlers

        worker = _make_mock_worker()
        handlers = setup_handlers(worker.services)

        # Replace the first handler with one that raises
        broken_handler = AsyncMock(side_effect=RuntimeError("handler exploded"))
        broken_handler.handled_events = frozenset({"session:start"})
        handlers["entity"][0] = broken_handler

        data = {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"}
        # Should not raise
        await process_event(worker, "session:start", data, handlers)

    async def test_missing_session_id_still_dispatches(self):
        """Events without session_id skip ensure_session_node but still dispatch."""
        from context_intelligence_server.pipeline import process_event, setup_handlers

        worker = _make_mock_worker()
        handlers = setup_handlers(worker.services)
        # Should not raise
        await process_event(worker, "session:start", {"timestamp": "2026-01-01T00:00:00Z"}, handlers)


class TestTerminalFlush:
    async def test_session_end_triggers_flush(self):
        from context_intelligence_server.pipeline import process_event, setup_handlers

        worker = _make_mock_worker()
        worker.services.graph.flush = AsyncMock()
        handlers = setup_handlers(worker.services)

        data = {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"}
        await process_event(worker, "session:end", data, handlers)
        worker.services.graph.flush.assert_awaited()

    async def test_orchestrator_complete_triggers_flush(self):
        from context_intelligence_server.pipeline import process_event, setup_handlers

        worker = _make_mock_worker()
        worker.services.graph.flush = AsyncMock()
        handlers = setup_handlers(worker.services)

        # Seed state so orchestrator:complete has a run_id to work with
        worker.services._seen_sessions.add("s1")
        cursors = worker.services.get_cursors("s1")
        cursors.current_run_id = "fake-run-id"

        data = {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z", "status": "success"}
        await process_event(worker, "orchestrator:complete", data, handlers)
        worker.services.graph.flush.assert_awaited()
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/test_pipeline.py -v
```

Expected: `ModuleNotFoundError: No module named 'context_intelligence_server.pipeline'`

**Step 3: Write `context_intelligence_server/pipeline.py`**

```python
"""Server-side event processing pipeline.

Replaces the bundle's _wrap_with_session_guarantee with server-owned error
isolation. Every event is wrapped in try/except — handler errors are logged
with structured JSON and never propagate to the drain loop.
"""

from __future__ import annotations

import fnmatch
import logging
from typing import Any

from context_intelligence_server.handlers.default import DefaultHandler
from context_intelligence_server.handlers.event import SystemEventHandler
from context_intelligence_server.handlers.orchestrator_run import OrchestratorRunHandler
from context_intelligence_server.handlers.recipe import RecipeHandler
from context_intelligence_server.handlers.session import SessionHandler
from context_intelligence_server.handlers.step import StepHandler
from context_intelligence_server.handlers.tool_execution import ToolExecutionHandler
from context_intelligence_server.services import HookStateService

logger = logging.getLogger(__name__)

# Terminal events that trigger an immediate flush
TERMINAL_EVENTS: frozenset[str] = frozenset({
    "session:end",
    "execution:end",
    "orchestrator:complete",
})


def setup_handlers(services: HookStateService) -> dict[str, Any]:
    """Instantiate all 7 handlers and return them in dispatch order.

    Returns a dict with:
      - "entity": list of entity handlers (checked first, order matters)
      - "default": the DefaultHandler (catch-all)
    """
    entity_handlers = [
        SessionHandler(services),
        OrchestratorRunHandler(services),
        StepHandler(services),
        RecipeHandler(services),
        ToolExecutionHandler(services),
        SystemEventHandler(services),
    ]
    default_handler = DefaultHandler(services)
    return {"entity": entity_handlers, "default": default_handler}


def _find_handler(event: str, handlers: dict[str, Any]) -> Any | None:
    """Return the correct handler for *event*.

    First-match-wins against entity handlers' handled_events sets (supports
    fnmatch wildcards like 'content_block:*'). Falls back to DefaultHandler
    for unclaimed events.
    """
    for handler in handlers["entity"]:
        if any(fnmatch.fnmatch(event, pattern) for pattern in handler.handled_events):
            return handler
    return handlers["default"]


async def process_event(
    worker: Any,
    event: str,
    data: dict[str, Any],
    handlers: dict[str, Any],
) -> None:
    """Process a single event through the pipeline.

    Server-side equivalent of _wrap_with_session_guarantee. Critical difference:
    every handler invocation is wrapped in try/except — errors are logged with
    structured context and NEVER propagate. The drain loop must continue.
    """
    session_id = data.get("session_id", "")
    try:
        # Ensure session node exists (idempotent)
        if session_id:
            await worker.services.ensure_session_node(session_id, data)

        # NOTE: blob processing is Phase 3 — data passed as-is for now

        # Dispatch to matching handler
        handler = _find_handler(event, handlers)
        if handler:
            await handler(event, data)

    except Exception:
        logger.exception(
            "event_processing_error",
            extra={"event": event, "session_id": session_id},
        )
        # Never propagate — drain loop must continue
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/test_pipeline.py -v
```

Expected: All pass.

**Step 5: Commit**

```bash
git add context_intelligence_server/pipeline.py tests/test_pipeline.py
git commit -m "feat(phase2): create pipeline.py — server-side event dispatch with error isolation"
```

---

### Task 9: Update drain loop in `registry.py` — 30s periodic flush

**Files:**
- Modify: `context_intelligence_server/registry.py`
- Modify: `tests/test_registry.py`

**Step 1: Add failing tests to `tests/test_registry.py`**

Append to `tests/test_registry.py`:

```python
class TestDrainLoopCallsProcessEvent:
    async def test_queued_event_is_processed(self):
        """When an event is enqueued, the drain loop calls process_event."""
        from unittest.mock import AsyncMock, patch

        from context_intelligence_server.registry import SessionRegistry

        registry = SessionRegistry()
        worker = registry.get_or_create("sess-1", workspace="test")

        with patch("context_intelligence_server.registry.process_event", new_callable=AsyncMock) as mock_proc:
            # Enqueue an event
            await worker.queue.put(("session:start", "test", {"session_id": "sess-1"}))

            # Run drain loop for a brief period
            drain_task = asyncio.create_task(registry.drain_worker(worker))
            await asyncio.sleep(0.1)
            drain_task.cancel()
            try:
                await drain_task
            except asyncio.CancelledError:
                pass

            mock_proc.assert_awaited_once()


class TestPeriodicFlush:
    async def test_timeout_triggers_flush(self):
        """When queue is empty for 30s, flush is called."""
        from unittest.mock import AsyncMock, patch

        from context_intelligence_server.registry import SessionRegistry

        registry = SessionRegistry()
        worker = registry.get_or_create("sess-1", workspace="test")
        worker.services.graph.flush = AsyncMock()

        # Use a very short timeout for testing
        drain_task = asyncio.create_task(
            registry.drain_worker(worker, flush_timeout=0.05)
        )
        await asyncio.sleep(0.15)  # wait for at least one timeout cycle
        drain_task.cancel()
        try:
            await drain_task
        except asyncio.CancelledError:
            pass

        worker.services.graph.flush.assert_awaited()
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/test_registry.py::TestDrainLoopCallsProcessEvent -v
pytest tests/test_registry.py::TestPeriodicFlush -v
```

Expected: Failures (drain loop doesn't call process_event yet, flush_timeout param doesn't exist)

**Step 3: Update `context_intelligence_server/registry.py`**

Update `drain_worker` to use the pipeline and add the 30-second periodic flush. The key changes:

1. Import `process_event` and `setup_handlers` from `pipeline`
2. Initialize handlers once when drain starts
3. Use `asyncio.wait_for` with timeout on `queue.get()`
4. Add `flush_timeout` parameter (default 30.0, overridable for tests)

```python
# In registry.py, update drain_worker:

from context_intelligence_server.pipeline import process_event, setup_handlers

async def drain_worker(self, worker: SessionWorker, flush_timeout: float = 30.0) -> None:
    """Drain loop: dequeue events, dispatch through pipeline, periodic flush."""
    handlers = setup_handlers(worker.services)
    while True:
        try:
            event, workspace, data = await asyncio.wait_for(
                worker.queue.get(), timeout=flush_timeout
            )
            await process_event(worker, event, data, handlers)
            worker.queue.task_done()
        except asyncio.TimeoutError:
            # Periodic fallback flush for sessions that disconnect without
            # a clean terminal event
            try:
                await worker.services.graph.flush()
            except Exception:
                logger.warning("periodic flush failed", exc_info=True)
        except asyncio.CancelledError:
            # Flush on shutdown
            try:
                await worker.services.graph.flush()
            except Exception:
                logger.warning("shutdown flush failed", exc_info=True)
            break
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/test_registry.py -v
```

Expected: All pass.

**Step 5: Commit**

```bash
git add context_intelligence_server/registry.py tests/test_registry.py
git commit -m "feat(phase2): wire pipeline into drain loop with 30s periodic flush"
```

---

### Task 10: Port `handlers/session.py`

**Files:**
- Create: `context_intelligence_server/handlers/__init__.py`
- Create: `context_intelligence_server/handlers/session.py`
- Create: `tests/handlers/__init__.py`
- Create: `tests/handlers/test_session.py`

**Step 1: Write the failing tests**

Create `tests/handlers/__init__.py` (empty file).

Create `tests/handlers/test_session.py`:

```python
"""Tests for SessionHandler — session lifecycle graph mutations."""

from __future__ import annotations

import json

import pytest

from context_intelligence_server.handlers.session import SessionHandler
from context_intelligence_server.services import HookStateService, SessionCursors


class TestSessionIdGuard:
    async def test_missing_session_id_returns_continue(self, services: HookStateService) -> None:
        handler = SessionHandler(services)
        result = await handler("session:start", {"timestamp": "2026-01-01T00:00:00Z"})
        assert result.action == "continue"


class TestSessionStart:
    async def test_root_session(self, services: HookStateService) -> None:
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z", "metadata": {"key": "val"}},
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert node["labels"] == {"Session", "Root"}
        assert node["properties"]["started_at"] == "2026-01-01T00:00:00Z"
        assert node["properties"]["status"] == "running"
        assert node["properties"]["metadata"] == {"key": "val"}

    async def test_subsession(self, services: HookStateService) -> None:
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {"session_id": "child", "parent_id": "parent", "timestamp": "2026-01-01T00:00:00Z"},
        )
        node = await services.graph.get_node("child")
        assert node is not None
        assert node["labels"] == {"Session", "Subsession"}

    async def test_subsession_edge(self, services: HookStateService) -> None:
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {"session_id": "child", "parent_id": "parent", "timestamp": "2026-01-01T00:00:00Z"},
        )
        edge = await services.graph.get_edge("child", "parent", "SUBSESSION_OF")
        assert edge is not None

    async def test_stores_data(self, services: HookStateService) -> None:
        handler = SessionHandler(services)
        await handler("session:start", {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"})
        node = await services.graph.get_node("s1")
        assert "data" in node["properties"]
        stored = json.loads(node["properties"]["data"])
        assert stored["session_id"] == "s1"


class TestSessionFork:
    async def test_fork_labels(self, services: HookStateService) -> None:
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {"session_id": "f1", "parent": "p1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        node = await services.graph.get_node("f1")
        assert node["labels"] == {"Session", "Subsession", "ForkedSession"}

    async def test_fork_missing_parent_degrades_to_root(self, services: HookStateService) -> None:
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {"session_id": "f1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        node = await services.graph.get_node("f1")
        assert node["labels"] == {"Session", "Root", "ForkedSession"}


class TestSessionEnd:
    async def test_end_merges_properties(self, services: HookStateService) -> None:
        handler = SessionHandler(services)
        await handler("session:start", {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"})
        await handler("session:end", {"session_id": "s1", "timestamp": "2026-01-01T01:00:00Z"})
        node = await services.graph.get_node("s1")
        assert node["properties"]["ended_at"] == "2026-01-01T01:00:00Z"
        assert node["properties"]["status"] == "completed"

    async def test_end_removes_cursors(self, services: HookStateService) -> None:
        handler = SessionHandler(services)
        cursors = services.get_cursors("s1")
        cursors.run_counter = 5
        await handler("session:end", {"session_id": "s1", "timestamp": "2026-01-01T01:00:00Z"})
        fresh = services.get_cursors("s1")
        assert fresh.run_counter == 0

    async def test_session_handler_does_not_claim_resume(self) -> None:
        assert "session:resume" not in SessionHandler.handled_events
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/handlers/test_session.py -v
```

Expected: `ModuleNotFoundError`

**Step 3: Create the handler files**

Create `context_intelligence_server/handlers/__init__.py`:

```python
"""Event handlers for the context-intelligence server.

Seven handlers, each conforming to the EventHandler protocol:
- SessionHandler — :Session nodes
- OrchestratorRunHandler — :OrchestratorRun and :Step:PromptStep nodes
- StepHandler — :Step:AssistantStep nodes
- RecipeHandler — recipe orchestration events
- ToolExecutionHandler — :ToolExecution nodes
- SystemEventHandler — no-op sink for system events
- DefaultHandler — :Event:{DerivedLabel} (dynamic labels)
"""
```

Create `context_intelligence_server/handlers/session.py`:

```python
"""SessionHandler — owns :Session node lifecycle events."""

from __future__ import annotations

import json
import logging
from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import EventLogContext, HandlerLogger

logger = logging.getLogger(__name__)


class SessionHandler:
    handled_events: frozenset[str] = frozenset({"session:start", "session:fork", "session:end"})

    def __init__(self, services: HookStateService) -> None:
        self.services = services
        self._log = HandlerLogger("SessionHandler", logger)

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        log = self._log.with_event(event, data)

        session_id = data.get("session_id")
        if not session_id:
            log.error("received event without session_id")
            return HookResult(action="continue")

        timestamp = data.get("timestamp", "")

        if event == "session:start":
            await self._handle_start(session_id, timestamp, data)
        elif event == "session:fork":
            await self._handle_fork(session_id, timestamp, data, log)
        elif event == "session:end":
            await self._handle_end(session_id, timestamp, data)
        return HookResult(action="continue")

    async def _handle_start(self, session_id: str, timestamp: str, data: dict[str, Any]) -> None:
        parent_id = (data.get("parent_id") or "").strip()
        labels: set[str] = {"Session", "Subsession"} if parent_id else {"Session", "Root"}
        properties: dict[str, Any] = {
            "started_at": timestamp,
            "status": "running",
            "metadata": data.get("metadata", {}),
            "data": json.dumps(data),
        }
        await self.services.graph.upsert_node(session_id, labels, properties)
        if parent_id:
            await self.services.graph.upsert_edge(
                session_id, parent_id, "SUBSESSION_OF", {"occurred_at": timestamp}
            )

    async def _handle_fork(
        self, session_id: str, timestamp: str, data: dict[str, Any], log: EventLogContext
    ) -> None:
        parent = data.get("parent")
        if parent:
            labels: set[str] = {"Session", "Subsession", "ForkedSession"}
        else:
            labels = {"Session", "Root", "ForkedSession"}
            log.warning("session:fork for %r has no parent — degrading to Root", session_id)
        properties: dict[str, Any] = {
            "started_at": timestamp,
            "status": "running",
            "metadata": data.get("metadata", {}),
            "data": json.dumps(data),
        }
        await self.services.graph.upsert_node(session_id, labels, properties)
        if parent:
            await self.services.graph.upsert_edge(
                session_id, parent, "SUBSESSION_OF", {"occurred_at": timestamp}
            )

    async def _handle_end(self, session_id: str, timestamp: str, data: dict[str, Any]) -> None:
        properties: dict[str, Any] = {
            "ended_at": timestamp,
            "status": data.get("status", "completed"),
            "data_session_end": json.dumps(data),
        }
        await self.services.graph.upsert_node(session_id, {"Session"}, properties)
        await self.services.graph.flush()
        self.services.remove_cursors(session_id)
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/handlers/test_session.py -v
```

Expected: All pass.

**Step 5: Commit**

```bash
git add context_intelligence_server/handlers/ tests/handlers/
git commit -m "feat(phase2): port SessionHandler"
```

---

### Task 11: Port `handlers/orchestrator_run.py`

**Files:**
- Create: `context_intelligence_server/handlers/orchestrator_run.py`
- Create: `tests/handlers/test_orchestrator_run.py`

**Step 1: Write the failing tests**

Create `tests/handlers/test_orchestrator_run.py`:

```python
"""Tests for OrchestratorRunHandler — full lifecycle events."""

from __future__ import annotations

import json

from context_intelligence_server.handlers.orchestrator_run import (
    PREVIEW_MAX_LEN,
    OrchestratorRunHandler,
    _STATUS_MAP,
)
from context_intelligence_server.handlers.session import SessionHandler
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id

assert _STATUS_MAP == {"success": "complete", "cancelled": "cancelled", "error": "error"}

TIMESTAMP = "2026-03-06T01:00:00Z"
EXPECTED_PROMPT_ID = "s1__prompt_submit__1772758800000"


async def _seed_session(services: HookStateService, session_id: str = "s1") -> None:
    handler = SessionHandler(services)
    await handler("session:start", {"session_id": session_id, "timestamp": "2026-03-06T00:00:00Z"})


class TestPromptSubmit:
    async def test_creates_prompt_step_node(self, services: HookStateService) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        await handler("prompt:submit", {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "Hello"})
        node = await services.graph.get_node(EXPECTED_PROMPT_ID)
        assert node is not None
        assert node["labels"] == {"Step", "PromptStep"}

    async def test_stores_prompt_text(self, services: HookStateService) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        await handler("prompt:submit", {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "Hello"})
        node = await services.graph.get_node(EXPECTED_PROMPT_ID)
        assert node["properties"]["prompt_text"] == "Hello"

    async def test_preview_truncated(self, services: HookStateService) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        await handler("prompt:submit", {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "x" * 300})
        node = await services.graph.get_node(EXPECTED_PROMPT_ID)
        assert len(node["properties"]["prompt_preview"]) == PREVIEW_MAX_LEN

    async def test_missing_session_id(self, services: HookStateService) -> None:
        handler = OrchestratorRunHandler(services)
        result = await handler("prompt:submit", {"timestamp": TIMESTAMP})
        assert result.action == "continue"


class TestExecutionStart:
    async def test_creates_run_node_and_edges(self, services: HookStateService) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        await handler("prompt:submit", {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "Hi"})
        await handler("execution:start", {"session_id": "s1", "timestamp": "2026-03-06T02:00:00Z"})
        run_id = make_node_id("s1", "execution:start", "2026-03-06T02:00:00Z")
        node = await services.graph.get_node(run_id)
        assert node is not None
        assert node["labels"] == {"OrchestratorRun"}
        # HAS_RUN edge
        edge = await services.graph.get_edge("s1", run_id, "HAS_RUN")
        assert edge is not None


class TestOrchestratorComplete:
    async def test_closes_run_with_status(self, services: HookStateService) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        await handler("prompt:submit", {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "Hi"})
        await handler("execution:start", {"session_id": "s1", "timestamp": "2026-03-06T02:00:00Z"})
        run_id = services.get_cursors("s1").current_run_id
        await handler(
            "orchestrator:complete",
            {"session_id": "s1", "timestamp": "2026-03-06T03:00:00Z", "status": "success"},
        )
        node = await services.graph.get_node(run_id)
        assert node["properties"]["status"] == "complete"  # mapped from "success"

    async def test_clears_current_run_id(self, services: HookStateService) -> None:
        await _seed_session(services)
        handler = OrchestratorRunHandler(services)
        await handler("prompt:submit", {"session_id": "s1", "timestamp": TIMESTAMP, "prompt": "Hi"})
        await handler("execution:start", {"session_id": "s1", "timestamp": "2026-03-06T02:00:00Z"})
        await handler(
            "orchestrator:complete",
            {"session_id": "s1", "timestamp": "2026-03-06T03:00:00Z", "status": "success"},
        )
        assert services.get_cursors("s1").current_run_id is None
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/handlers/test_orchestrator_run.py -v
```

Expected: `ModuleNotFoundError`

**Step 3: Create `context_intelligence_server/handlers/orchestrator_run.py`**

Port verbatim from the bundle. Only import paths change:

- `from amplifier_core.models import HookResult` → `from context_intelligence_server.protocol import HookResult`
- `from ..services import HookStateService` → `from context_intelligence_server.services import HookStateService`
- `from ..utils import ...` → `from context_intelligence_server.utils import ...`

The handler body is identical to the bundle source (lines 1–230 of the bundle file) with only those three import lines changed.

**Step 4: Run tests to verify they pass**

```bash
pytest tests/handlers/test_orchestrator_run.py -v
```

Expected: All pass.

**Step 5: Commit**

```bash
git add context_intelligence_server/handlers/orchestrator_run.py tests/handlers/test_orchestrator_run.py
git commit -m "feat(phase2): port OrchestratorRunHandler"
```

---

### Task 12: Port `handlers/step.py` and `handlers/tool_execution.py`

**Files:**
- Create: `context_intelligence_server/handlers/step.py`
- Create: `context_intelligence_server/handlers/tool_execution.py`
- Create: `tests/handlers/test_step.py`
- Create: `tests/handlers/test_tool_execution.py`

**Step 1: Write the failing tests**

Create `tests/handlers/test_step.py`:

```python
"""Tests for StepHandler — AssistantStep lifecycle."""

from __future__ import annotations

from context_intelligence_server.handlers.orchestrator_run import OrchestratorRunHandler
from context_intelligence_server.handlers.session import SessionHandler
from context_intelligence_server.handlers.step import StepHandler
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


async def _seed_run(services: HookStateService) -> str:
    """Seed session + prompt + execution:start, return run_id."""
    sh = SessionHandler(services)
    await sh("session:start", {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"})
    oh = OrchestratorRunHandler(services)
    await oh("prompt:submit", {"session_id": "s1", "timestamp": "2026-01-01T01:00:00Z", "prompt": "Hi"})
    await oh("execution:start", {"session_id": "s1", "timestamp": "2026-01-01T02:00:00Z"})
    return services.get_cursors("s1").current_run_id


class TestProviderRequest:
    async def test_creates_assistant_step_node(self, services: HookStateService) -> None:
        await _seed_run(services)
        handler = StepHandler(services)
        await handler(
            "provider:request",
            {"session_id": "s1", "timestamp": "2026-01-01T03:00:00Z", "provider": "anthropic"},
        )
        step_id = make_node_id("s1", "provider:request", "2026-01-01T03:00:00Z")
        node = await services.graph.get_node(step_id)
        assert node is not None
        assert node["labels"] == {"Step", "AssistantStep"}

    async def test_has_step_edge_from_run(self, services: HookStateService) -> None:
        run_id = await _seed_run(services)
        handler = StepHandler(services)
        await handler(
            "provider:request",
            {"session_id": "s1", "timestamp": "2026-01-01T03:00:00Z"},
        )
        step_id = make_node_id("s1", "provider:request", "2026-01-01T03:00:00Z")
        edge = await services.graph.get_edge(run_id, step_id, "HAS_STEP")
        assert edge is not None

    async def test_next_edge_to_previous_step(self, services: HookStateService) -> None:
        await _seed_run(services)
        handler = StepHandler(services)
        previous_step_id = services.get_cursors("s1").current_step_id
        await handler(
            "provider:request",
            {"session_id": "s1", "timestamp": "2026-01-01T03:00:00Z"},
        )
        step_id = make_node_id("s1", "provider:request", "2026-01-01T03:00:00Z")
        if previous_step_id:
            edge = await services.graph.get_edge(previous_step_id, step_id, "NEXT")
            assert edge is not None


class TestLlmResponse:
    async def test_enriches_with_usage_tokens(self, services: HookStateService) -> None:
        await _seed_run(services)
        handler = StepHandler(services)
        await handler(
            "provider:request",
            {"session_id": "s1", "timestamp": "2026-01-01T03:00:00Z"},
        )
        step_id = services.get_cursors("s1").current_step_id
        await handler(
            "llm:response",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T03:01:00Z",
                "usage": {"input_tokens": 100, "output_tokens": 50},
            },
        )
        node = await services.graph.get_node(step_id)
        assert node["properties"]["input_tokens"] == 100
        assert node["properties"]["output_tokens"] == 50

    async def test_content_block_wildcard_claimed(self) -> None:
        assert any("content_block" in e for e in StepHandler.handled_events)
```

Create `tests/handlers/test_tool_execution.py`:

```python
"""Tests for ToolExecutionHandler — tool lifecycle events."""

from __future__ import annotations

from context_intelligence_server.handlers.orchestrator_run import OrchestratorRunHandler
from context_intelligence_server.handlers.session import SessionHandler
from context_intelligence_server.handlers.step import StepHandler
from context_intelligence_server.handlers.tool_execution import ToolExecutionHandler
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


async def _seed_step(services: HookStateService) -> str:
    """Seed through provider:request, return step_id."""
    sh = SessionHandler(services)
    await sh("session:start", {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"})
    oh = OrchestratorRunHandler(services)
    await oh("prompt:submit", {"session_id": "s1", "timestamp": "2026-01-01T01:00:00Z", "prompt": "Hi"})
    await oh("execution:start", {"session_id": "s1", "timestamp": "2026-01-01T02:00:00Z"})
    step_h = StepHandler(services)
    await step_h("provider:request", {"session_id": "s1", "timestamp": "2026-01-01T03:00:00Z"})
    return services.get_cursors("s1").current_step_id


class TestToolPre:
    async def test_creates_tool_execution_node(self, services: HookStateService) -> None:
        step_id = await _seed_step(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T04:00:00Z",
                "tool_name": "read_file",
                "tool_call_id": "call_001",
            },
        )
        te_id = make_node_id("s1", "tool:pre", "2026-01-01T04:00:00Z", disambiguator="call_001")
        node = await services.graph.get_node(te_id)
        assert node is not None
        assert node["labels"] == {"ToolExecution"}
        assert node["properties"]["tool_name"] == "read_file"

    async def test_triggered_edge_from_step(self, services: HookStateService) -> None:
        step_id = await _seed_step(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T04:00:00Z",
                "tool_name": "read_file",
                "tool_call_id": "call_001",
            },
        )
        te_id = make_node_id("s1", "tool:pre", "2026-01-01T04:00:00Z", disambiguator="call_001")
        edge = await services.graph.get_edge(step_id, te_id, "TRIGGERED")
        assert edge is not None


class TestToolPost:
    async def test_completes_tool_execution(self, services: HookStateService) -> None:
        await _seed_step(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "tool:pre",
            {"session_id": "s1", "timestamp": "2026-01-01T04:00:00Z", "tool_name": "read_file", "tool_call_id": "c1"},
        )
        await handler(
            "tool:post",
            {"session_id": "s1", "timestamp": "2026-01-01T04:01:00Z", "tool_call_id": "c1", "result": "file content"},
        )
        te_id = services.get_cursors("s1").tool_call_map.get("c1")
        node = await services.graph.get_node(te_id)
        assert node["properties"]["status"] == "complete"


class TestDelegation:
    async def test_delegate_agent_spawned_adds_label(self, services: HookStateService) -> None:
        await _seed_step(services)
        handler = ToolExecutionHandler(services)
        await handler(
            "tool:pre",
            {"session_id": "s1", "timestamp": "2026-01-01T04:00:00Z", "tool_name": "delegate", "tool_call_id": "c3"},
        )
        await handler(
            "delegate:agent_spawned",
            {"session_id": "s1", "tool_call_id": "c3", "child_session_id": "child-1", "child_agent": "reviewer"},
        )
        te_id = services.get_cursors("s1").tool_call_map.get("c3")
        node = await services.graph.get_node(te_id)
        assert "Delegation" in node["labels"]
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/handlers/test_step.py tests/handlers/test_tool_execution.py -v
```

Expected: `ModuleNotFoundError`

**Step 3: Create both handler files**

Create `context_intelligence_server/handlers/step.py` — port verbatim from bundle, changing only the three import lines:

- `from amplifier_core.models import HookResult` → `from context_intelligence_server.protocol import HookResult`
- `from ..services import HookStateService` → `from context_intelligence_server.services import HookStateService`
- `from ..utils import EventLogContext, HandlerLogger, make_node_id` → `from context_intelligence_server.utils import EventLogContext, HandlerLogger, make_node_id`

The handler body is identical to the bundle source.

Create `context_intelligence_server/handlers/tool_execution.py` — same pattern: only import lines change, handler body is identical.

**Step 4: Run tests to verify they pass**

```bash
pytest tests/handlers/test_step.py tests/handlers/test_tool_execution.py -v
```

Expected: All pass.

**Step 5: Commit**

```bash
git add context_intelligence_server/handlers/step.py context_intelligence_server/handlers/tool_execution.py \
    tests/handlers/test_step.py tests/handlers/test_tool_execution.py
git commit -m "feat(phase2): port StepHandler and ToolExecutionHandler"
```

---

### Task 13: Port `handlers/recipe.py`, `handlers/event.py`, `handlers/default.py`

**Files:**
- Create: `context_intelligence_server/handlers/recipe.py`
- Create: `context_intelligence_server/handlers/event.py`
- Create: `context_intelligence_server/handlers/default.py`
- Create: `tests/handlers/test_recipe.py`
- Create: `tests/handlers/test_event.py`
- Create: `tests/handlers/test_default.py`

**Step 1: Write the failing tests**

Create `tests/handlers/test_recipe.py`:

```python
"""Tests for RecipeHandler — recipe lifecycle events."""

from __future__ import annotations

from context_intelligence_server.handlers.recipe import RecipeHandler
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


class TestRecipeStart:
    async def test_creates_recipe_start_event_node(self, services: HookStateService) -> None:
        handler = RecipeHandler(services)
        await handler(
            "recipe:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "recipe_name": "deploy",
                "total_steps": 3,
            },
        )
        node_id = make_node_id("s1", "recipe:start", "2026-01-01T00:00:00Z")
        node = await services.graph.get_node(node_id)
        assert node is not None
        assert "Event" in node["labels"]
        assert "RecipeStart" in node["labels"]

    async def test_has_event_edge(self, services: HookStateService) -> None:
        handler = RecipeHandler(services)
        await handler(
            "recipe:start",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z", "recipe_name": "deploy"},
        )
        node_id = make_node_id("s1", "recipe:start", "2026-01-01T00:00:00Z")
        edge = await services.graph.get_edge("s1", node_id, "HAS_EVENT")
        assert edge is not None


class TestRecipeComplete:
    async def test_creates_recipe_complete_node(self, services: HookStateService) -> None:
        handler = RecipeHandler(services)
        await handler(
            "recipe:complete",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T01:00:00Z",
                "success": True,
            },
        )
        node_id = make_node_id("s1", "recipe:complete", "2026-01-01T01:00:00Z")
        node = await services.graph.get_node(node_id)
        assert node is not None
        assert node["properties"]["success"] is True


class TestHandledEvents:
    def test_claims_all_recipe_events(self) -> None:
        expected = {"recipe:start", "recipe:step", "recipe:complete", "recipe:approval",
                    "recipe:loop_iteration", "recipe:loop_complete"}
        assert expected == set(RecipeHandler.handled_events)
```

Create `tests/handlers/test_event.py`:

```python
"""Tests for SystemEventHandler — no-op sink for system events."""

from __future__ import annotations

from context_intelligence_server.handlers.event import SystemEventHandler
from context_intelligence_server.services import HookStateService


class TestSystemEventHandler:
    async def test_is_noop(self, services: HookStateService) -> None:
        handler = SystemEventHandler(services)
        result = await handler("context:compaction", {"session_id": "s1"})
        assert result.action == "continue"

    def test_claims_expected_events(self) -> None:
        expected = {"context:compaction", "cancel:requested", "cancel:completed"}
        assert expected == set(SystemEventHandler.handled_events)
```

Create `tests/handlers/test_default.py`:

```python
"""Tests for DefaultHandler — Event node creation for unclaimed events."""

from __future__ import annotations

import json

from context_intelligence_server.handlers.default import DefaultHandler
from context_intelligence_server.handlers.orchestrator_run import OrchestratorRunHandler
from context_intelligence_server.handlers.session import SessionHandler
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


class TestDefaultHandler:
    async def test_creates_event_node_with_derived_label(self, services: HookStateService) -> None:
        handler = DefaultHandler(services)
        await handler("session:resume", {"session_id": "s1", "timestamp": "2026-01-01T02:00:00Z"})
        event_id = make_node_id("s1", "session:resume", "2026-01-01T02:00:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        assert node["labels"] == {"Event", "SessionResume"}

    async def test_creates_has_event_edge_from_session(self, services: HookStateService) -> None:
        handler = DefaultHandler(services)
        await handler("session:resume", {"session_id": "s1", "timestamp": "2026-01-01T02:00:00Z"})
        event_id = make_node_id("s1", "session:resume", "2026-01-01T02:00:00Z")
        edge = await services.graph.get_edge("s1", event_id, "HAS_EVENT")
        assert edge is not None

    async def test_skips_event_without_session_id(self, services: HookStateService) -> None:
        handler = DefaultHandler(services)
        result = await handler("session:resume", {"timestamp": "2026-01-01T02:00:00Z"})
        assert result.action == "continue"

    async def test_stores_data_property(self, services: HookStateService) -> None:
        handler = DefaultHandler(services)
        await handler("session:resume", {"session_id": "s1", "timestamp": "2026-01-01T02:00:00Z", "extra": "val"})
        event_id = make_node_id("s1", "session:resume", "2026-01-01T02:00:00Z")
        node = await services.graph.get_node(event_id)
        data = json.loads(node["properties"]["data"])
        assert data["extra"] == "val"


class TestDefaultHandlerRunAwareness:
    async def test_event_during_active_run_attaches_to_run(self, services: HookStateService) -> None:
        sh = SessionHandler(services)
        await sh("session:start", {"session_id": "s1", "timestamp": "2026-03-06T00:00:00Z"})
        oh = OrchestratorRunHandler(services)
        await oh("prompt:submit", {"session_id": "s1", "timestamp": "2026-03-06T01:00:00Z", "prompt": "Hello"})
        await oh("execution:start", {"session_id": "s1", "timestamp": "2026-03-06T02:00:00Z"})
        run_id = services.get_cursors("s1").current_run_id

        handler = DefaultHandler(services)
        await handler("artifact:read", {"session_id": "s1", "timestamp": "2026-03-06T02:30:00Z"})
        event_id = make_node_id("s1", "artifact:read", "2026-03-06T02:30:00Z")
        edge = await services.graph.get_edge(run_id, event_id, "HAS_EVENT")
        assert edge is not None

    async def test_event_without_active_run_attaches_to_session(self, services: HookStateService) -> None:
        handler = DefaultHandler(services)
        await handler("session:resume", {"session_id": "s1", "timestamp": "2026-01-01T02:00:00Z"})
        event_id = make_node_id("s1", "session:resume", "2026-01-01T02:00:00Z")
        edge = await services.graph.get_edge("s1", event_id, "HAS_EVENT")
        assert edge is not None


class TestDeriveLabel:
    def test_derive_label(self) -> None:
        assert DefaultHandler.derive_label("context:compaction") == "ContextCompaction"
        assert DefaultHandler.derive_label("session:resume") == "SessionResume"
        assert DefaultHandler.derive_label("custom:my_event") == "CustomMyEvent"
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/handlers/test_recipe.py tests/handlers/test_event.py tests/handlers/test_default.py -v
```

Expected: `ModuleNotFoundError`

**Step 3: Create the three handler files**

Create `context_intelligence_server/handlers/event.py`:

```python
"""SystemEventHandler — owns known system events (compaction, cancellation)."""

from __future__ import annotations

from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService


class SystemEventHandler:
    """No-op sink — claims system events to prevent DefaultHandler from seeing them."""

    handled_events: frozenset[str] = frozenset({
        "context:compaction",
        "cancel:requested",
        "cancel:completed",
    })

    def __init__(self, services: HookStateService) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        return HookResult(action="continue")
```

Create `context_intelligence_server/handlers/default.py`:

```python
"""DefaultHandler — catches all unclaimed, non-excluded events."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id

logger = logging.getLogger(__name__)


class DefaultHandler:
    """Creates :Event:{DerivedLabel} nodes from unclaimed events."""

    handled_events: set[str]

    def __init__(self, services: HookStateService) -> None:
        self.services = services
        self.handled_events = set()

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        session_id = data.get("session_id")
        if not session_id:
            logger.debug("DefaultHandler: no session_id in %s, skipping", event)
            return HookResult(action="continue")

        timestamp = data.get("timestamp", "")
        derived = self.derive_label(event)

        event_node_id = make_node_id(session_id, event, timestamp)
        await self.services.graph.upsert_node(
            event_node_id,
            {"Event", derived},
            {"event_name": event, "occurred_at": timestamp, "data": json.dumps(data)},
        )

        cursors = self.services.get_cursors(session_id)
        parent_id = cursors.current_run_id if cursors.current_run_id else session_id

        await self.services.graph.upsert_edge(
            parent_id, event_node_id, "HAS_EVENT", {"occurred_at": timestamp}
        )

        return HookResult(action="continue")

    @staticmethod
    def derive_label(event_name: str) -> str:
        """Derive PascalCase label. 'context:compaction' -> 'ContextCompaction'."""
        parts = re.split(r"[:_]", event_name)
        return "".join(part.capitalize() for part in parts if part)
```

Create `context_intelligence_server/handlers/recipe.py` — port verbatim from bundle with import path changes:

- `from amplifier_core.models import HookResult` → `from context_intelligence_server.protocol import HookResult`
- `from ..services import HookStateService` → `from context_intelligence_server.services import HookStateService`
- `from ..utils import EventLogContext, HandlerLogger, make_node_id` → `from context_intelligence_server.utils import EventLogContext, HandlerLogger, make_node_id`
- `from .default import DefaultHandler` → `from context_intelligence_server.handlers.default import DefaultHandler`

Handler body is identical to the bundle source.

**Step 4: Run tests to verify they pass**

```bash
pytest tests/handlers/test_recipe.py tests/handlers/test_event.py tests/handlers/test_default.py -v
```

Expected: All pass.

**Step 5: Commit**

```bash
git add context_intelligence_server/handlers/recipe.py context_intelligence_server/handlers/event.py \
    context_intelligence_server/handlers/default.py \
    tests/handlers/test_recipe.py tests/handlers/test_event.py tests/handlers/test_default.py
git commit -m "feat(phase2): port RecipeHandler, SystemEventHandler, DefaultHandler"
```

---

### Task 14: Integration test + full suite

**Files:**
- Create: `tests/integration/__init__.py`
- Create: `tests/integration/test_event_pipeline.py`

**Step 1: Write the integration test**

Create `tests/integration/__init__.py` (empty file).

Create `tests/integration/test_event_pipeline.py`:

```python
"""Integration test — full pipeline processing with mocked Neo4j.

Verifies that events flow through the complete pipeline:
POST data → SessionWorker → pipeline.process_event → handler → GraphState
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from context_intelligence_server.pipeline import process_event, setup_handlers
from context_intelligence_server.services import HookStateService


def _make_worker(workspace: str = "integration-test") -> Any:
    """Create a minimal mock worker with real HookStateService (in-memory graph)."""
    from unittest.mock import MagicMock

    worker = MagicMock()
    worker.services = HookStateService(workspace=workspace)
    worker.workspace = workspace
    return worker


class TestFullEventSequence:
    """Process a realistic session through the pipeline and verify graph state."""

    async def test_session_start_to_orchestrator_complete(self) -> None:
        worker = _make_worker()
        handlers = setup_handlers(worker.services)
        graph = worker.services.graph

        # 1. session:start
        await process_event(worker, "session:start", {
            "session_id": "int-s1",
            "timestamp": "2026-03-13T10:00:00Z",
            "metadata": {"test": True},
        }, handlers)
        session_node = await graph.get_node("int-s1")
        assert session_node is not None
        assert "Session" in session_node["labels"]
        assert "Root" in session_node["labels"]

        # 2. prompt:submit
        await process_event(worker, "prompt:submit", {
            "session_id": "int-s1",
            "timestamp": "2026-03-13T10:01:00Z",
            "prompt": "Refactor the auth module",
        }, handlers)
        cursors = worker.services.get_cursors("int-s1")
        assert cursors.current_step_id is not None

        # 3. execution:start
        await process_event(worker, "execution:start", {
            "session_id": "int-s1",
            "timestamp": "2026-03-13T10:02:00Z",
        }, handlers)
        assert cursors.current_run_id is not None
        run_id = cursors.current_run_id

        # Verify HAS_RUN edge
        has_run = await graph.get_edge("int-s1", run_id, "HAS_RUN")
        assert has_run is not None

        # 4. provider:request (AssistantStep)
        await process_event(worker, "provider:request", {
            "session_id": "int-s1",
            "timestamp": "2026-03-13T10:03:00Z",
            "provider": "anthropic",
        }, handlers)
        step_id = cursors.current_step_id
        step_node = await graph.get_node(step_id)
        assert step_node is not None
        assert "AssistantStep" in step_node["labels"]

        # Verify HAS_STEP edge
        has_step = await graph.get_edge(run_id, step_id, "HAS_STEP")
        assert has_step is not None

        # 5. tool:pre
        await process_event(worker, "tool:pre", {
            "session_id": "int-s1",
            "timestamp": "2026-03-13T10:04:00Z",
            "tool_name": "read_file",
            "tool_call_id": "call_001",
        }, handlers)
        te_id = cursors.tool_call_map.get("call_001")
        assert te_id is not None
        te_node = await graph.get_node(te_id)
        assert te_node is not None
        assert "ToolExecution" in te_node["labels"]

        # Verify TRIGGERED edge
        triggered = await graph.get_edge(step_id, te_id, "TRIGGERED")
        assert triggered is not None

        # 6. tool:post
        await process_event(worker, "tool:post", {
            "session_id": "int-s1",
            "timestamp": "2026-03-13T10:05:00Z",
            "tool_call_id": "call_001",
            "result": "file contents here",
        }, handlers)
        te_node = await graph.get_node(te_id)
        assert te_node["properties"]["status"] == "complete"

        # 7. orchestrator:complete
        await process_event(worker, "orchestrator:complete", {
            "session_id": "int-s1",
            "timestamp": "2026-03-13T10:06:00Z",
            "status": "success",
            "turn_count": 3,
        }, handlers)
        run_node = await graph.get_node(run_id)
        assert run_node["properties"]["status"] == "complete"
        assert cursors.current_run_id is None  # cleared after complete


class TestWorkspaceSetCorrectly:
    async def test_workspace_propagated_to_graph(self) -> None:
        worker = _make_worker(workspace="my-feature-branch")
        assert worker.services.graph.workspace == "my-feature-branch"


class TestHandlerErrorIsolation:
    async def test_bad_timestamp_does_not_crash_pipeline(self) -> None:
        """make_node_id raises on empty timestamp — pipeline must catch and continue."""
        worker = _make_worker()
        handlers = setup_handlers(worker.services)

        # This will fail inside the handler (make_node_id on empty timestamp)
        # but process_event should catch the exception
        await process_event(worker, "prompt:submit", {
            "session_id": "int-s2",
            "timestamp": "",  # will cause ValueError in make_node_id
            "prompt": "test",
        }, handlers)
        # If we get here without raising, the error isolation works


class TestUnclaimedEventsFlowToDefault:
    async def test_custom_event_creates_event_node(self) -> None:
        worker = _make_worker()
        handlers = setup_handlers(worker.services)
        graph = worker.services.graph

        await process_event(worker, "session:resume", {
            "session_id": "int-s3",
            "timestamp": "2026-03-13T10:00:00Z",
        }, handlers)

        from context_intelligence_server.utils import make_node_id

        event_id = make_node_id("int-s3", "session:resume", "2026-03-13T10:00:00Z")
        node = await graph.get_node(event_id)
        assert node is not None
        assert "Event" in node["labels"]
        assert "SessionResume" in node["labels"]


class TestSystemEventsAreNoOp:
    async def test_context_compaction_does_not_create_nodes(self) -> None:
        worker = _make_worker()
        handlers = setup_handlers(worker.services)

        await process_event(worker, "context:compaction", {
            "session_id": "int-s4",
            "timestamp": "2026-03-13T10:00:00Z",
        }, handlers)

        # SystemEventHandler is a no-op — only the session stub from
        # ensure_session_node should exist
        from context_intelligence_server.utils import make_node_id

        compaction_id = make_node_id("int-s4", "context:compaction", "2026-03-13T10:00:00Z")
        node = await worker.services.graph.get_node(compaction_id)
        assert node is None  # SystemEventHandler doesn't create nodes
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/integration/test_event_pipeline.py -v
```

Expected: Should pass if all Tasks 1–13 are complete. If any fail, fix the issue before proceeding.

**Step 3: Run the full test suite**

```bash
pytest tests/ -v
```

Expected: ALL tests pass.

**Step 4: Verify no `graph_forest_name` in server code**

```bash
grep -r "graph_forest_name" context_intelligence_server/
```

Expected: **Zero matches.** If any are found, they are a porting error and must be fixed.

**Step 5: Commit**

```bash
git add tests/integration/
git commit -m "feat(phase2): integration test — full event pipeline end-to-end"
```

**Step 6: Final commit for Phase 2**

```bash
git add -A
git commit -m "feat: phase 2 complete — handler migration to standalone server"
```

---

## Verification Checklist

Run these after all 14 tasks are complete:

| Check | Command | Expected |
|---|---|---|
| All tests pass | `pytest tests/ -v` | 0 failures |
| No `graph_forest_name` in server code | `grep -r "graph_forest_name" context_intelligence_server/` | 0 matches |
| No `amplifier_core` imports | `grep -r "amplifier_core" context_intelligence_server/` | 0 matches |
| No `coordinator` references | `grep -r "coordinator" context_intelligence_server/` | 0 matches |
| No `_resolve_forest` references | `grep -r "_resolve_forest" context_intelligence_server/` | 0 matches |
| Lint passes | `ruff check context_intelligence_server/ tests/` | 0 errors |
| Type check passes | `pyright context_intelligence_server/` | 0 errors |

## File Inventory (Phase 2 Creates)

```
context_intelligence_server/
├── protocol.py          # HookResult + EventHandler protocol
├── graph_store.py       # GraphStore + QueryableStore protocols (workspace)
├── utils.py             # make_node_id, make_edge_id, HandlerLogger, EventLogContext
├── services.py          # SessionCursors, GraphState, HookConfig, HookStateService
├── neo4j_store.py       # Neo4jGraphStore (workspace throughout)
├── pipeline.py          # process_event, setup_handlers, _find_handler
└── handlers/
    ├── __init__.py
    ├── session.py
    ├── orchestrator_run.py
    ├── step.py
    ├── tool_execution.py
    ├── recipe.py
    ├── event.py
    └── default.py

tests/
├── test_utils.py
├── test_graph_store.py
├── test_services.py
├── test_neo4j_store.py
├── test_pipeline.py
├── handlers/
│   ├── __init__.py
│   ├── test_session.py
│   ├── test_orchestrator_run.py
│   ├── test_step.py
│   ├── test_tool_execution.py
│   ├── test_recipe.py
│   ├── test_event.py
│   └── test_default.py
└── integration/
    ├── __init__.py
    └── test_event_pipeline.py
```

## Files Modified (Phase 1 → Phase 2)

- `context_intelligence_server/registry.py` — SessionWorker gains `workspace` + `services`, drain loop uses pipeline
- `tests/conftest.py` — adds `services` fixture
- `tests/test_registry.py` — adds service/pipeline tests
- `pyproject.toml` — adds `neo4j>=5.0` dependency
