"""Tests for SystemEventHandler — system events (compaction, cancellation)."""

from __future__ import annotations

import json

from context_intelligence_server.handlers.event import SystemEventHandler
from context_intelligence_server.handlers.orchestrator_run import OrchestratorRunHandler
from context_intelligence_server.handlers.session import SessionHandler
from context_intelligence_server.handlers.step import StepHandler
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id

# ---------------------------------------------------------------------------
# Seed helper timestamps
# ---------------------------------------------------------------------------

TIMESTAMP_SESSION = "2026-01-01T00:00:00Z"
TIMESTAMP_RUN = "2026-01-01T00:01:00Z"
TIMESTAMP_STEP = "2026-01-01T00:02:00Z"
TIMESTAMP_EVENT = "2026-01-01T00:03:00Z"


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


async def _seed_session(services: HookStateService, session_id: str = "s1") -> None:
    """Create a Session node via SessionHandler."""
    handler = SessionHandler(services)
    await handler(
        "session:start",
        {"session_id": session_id, "timestamp": TIMESTAMP_SESSION},
    )


async def _seed_run(services: HookStateService, session_id: str = "s1") -> str:
    """Create an OrchestratorRun via OrchestratorRunHandler; return run_id."""
    handler = OrchestratorRunHandler(services)
    await handler(
        "execution:start",
        {"session_id": session_id, "timestamp": TIMESTAMP_RUN},
    )
    run_id = services.get_cursors(session_id).current_run_id
    assert run_id is not None
    return run_id


async def _seed_step(services: HookStateService, session_id: str = "s1") -> str:
    """Create an AssistantStep via StepHandler; return step_id."""
    handler = StepHandler(services)
    await handler(
        "provider:request",
        {
            "session_id": session_id,
            "timestamp": TIMESTAMP_STEP,
            "provider": "test",
        },
    )
    step_id = services.get_cursors(session_id).current_step_id
    assert step_id is not None
    return step_id


# ---------------------------------------------------------------------------
# Existing tests (preserved)
# ---------------------------------------------------------------------------


class TestSystemEventHandlerClaims:
    """SystemEventHandler must claim exactly the 3 system events."""

    def test_claims_context_compaction(self) -> None:
        assert "context:compaction" in SystemEventHandler.handled_events

    def test_claims_cancel_requested(self) -> None:
        assert "cancel:requested" in SystemEventHandler.handled_events

    def test_claims_cancel_completed(self) -> None:
        assert "cancel:completed" in SystemEventHandler.handled_events

    def test_claims_exactly_three_events(self) -> None:
        assert len(SystemEventHandler.handled_events) == 3


class TestSystemEventHandlerIsNoOp:
    """SystemEventHandler returns continue for all claimed events without touching session node."""

    async def test_context_compaction_returns_continue(
        self, services: HookStateService
    ) -> None:
        handler = SystemEventHandler(services)
        result = await handler(
            "context:compaction",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        assert result.action == "continue"

    async def test_cancel_requested_returns_continue(
        self, services: HookStateService
    ) -> None:
        handler = SystemEventHandler(services)
        result = await handler(
            "cancel:requested",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        assert result.action == "continue"

    async def test_cancel_completed_returns_continue(
        self, services: HookStateService
    ) -> None:
        handler = SystemEventHandler(services)
        result = await handler(
            "cancel:completed",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        assert result.action == "continue"

    async def test_creates_no_graph_nodes(self, services: HookStateService) -> None:
        """SystemEventHandler must not create the session node itself."""
        handler = SystemEventHandler(services)
        await handler(
            "context:compaction",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        # Only check the session node — event node has a different ID
        node = await services.graph.get_node("s1")
        assert node is None

    async def test_returns_continue_without_session_id(
        self, services: HookStateService
    ) -> None:
        handler = SystemEventHandler(services)
        result = await handler(
            "cancel:requested",
            {"timestamp": "2026-01-01T00:00:00Z"},
        )
        assert result.action == "continue"


# ---------------------------------------------------------------------------
# New tests: TestContextCompactionHappyPath
# ---------------------------------------------------------------------------


class TestContextCompactionHappyPath:
    """context:compaction creates :Event:ContextCompaction node with correct props."""

    async def test_creates_event_node(self, services: HookStateService) -> None:
        """Event node is created in graph after context:compaction fires."""
        await _seed_session(services)
        handler = SystemEventHandler(services)
        await handler(
            "context:compaction",
            {"session_id": "s1", "timestamp": TIMESTAMP_EVENT},
        )
        node_id = make_node_id("s1", "context:compaction", TIMESTAMP_EVENT)
        node = await services.graph.get_node(node_id)
        assert node is not None

    async def test_correct_labels(self, services: HookStateService) -> None:
        """Event node has exactly the labels {Event, ContextCompaction}."""
        await _seed_session(services)
        handler = SystemEventHandler(services)
        await handler(
            "context:compaction",
            {"session_id": "s1", "timestamp": TIMESTAMP_EVENT},
        )
        node_id = make_node_id("s1", "context:compaction", TIMESTAMP_EVENT)
        node = await services.graph.get_node(node_id)
        assert node is not None
        assert set(node["labels"]) == {"Event", "ContextCompaction"}

    async def test_extracts_token_fields(self, services: HookStateService) -> None:
        """Token fields are written to the node when present in payload."""
        await _seed_session(services)
        handler = SystemEventHandler(services)
        await handler(
            "context:compaction",
            {
                "session_id": "s1",
                "timestamp": TIMESTAMP_EVENT,
                "before_tokens": 10000,
                "after_tokens": 5000,
                "tokens_freed": 5000,
            },
        )
        node_id = make_node_id("s1", "context:compaction", TIMESTAMP_EVENT)
        node = await services.graph.get_node(node_id)
        assert node is not None
        assert node["before_tokens"] == 10000
        assert node["after_tokens"] == 5000
        assert node["tokens_freed"] == 5000

    async def test_missing_optional_fields_not_written(
        self, services: HookStateService
    ) -> None:
        """Optional fields absent from payload are not written to node."""
        await _seed_session(services)
        handler = SystemEventHandler(services)
        await handler(
            "context:compaction",
            {"session_id": "s1", "timestamp": TIMESTAMP_EVENT},
        )
        node_id = make_node_id("s1", "context:compaction", TIMESTAMP_EVENT)
        node = await services.graph.get_node(node_id)
        assert node is not None
        for field in (
            "before_tokens",
            "after_tokens",
            "tokens_freed",
            "before_messages",
            "after_messages",
            "messages_removed",
            "strategy_level",
            "budget",
        ):
            assert field not in node, f"optional field {field!r} must not be present"

    async def test_has_event_edge_from_active_step(
        self, services: HookStateService
    ) -> None:
        """HAS_EVENT edge scopes to current_step_id when step is active (Step-first)."""
        await _seed_session(services)
        await _seed_run(services)
        step_id = await _seed_step(services)
        handler = SystemEventHandler(services)
        await handler(
            "context:compaction",
            {"session_id": "s1", "timestamp": TIMESTAMP_EVENT},
        )
        node_id = make_node_id("s1", "context:compaction", TIMESTAMP_EVENT)
        edge = await services.graph.get_edge(step_id, node_id)
        assert edge is not None
        assert edge["type"] == "HAS_EVENT"

    async def test_fallback_to_run_when_no_step(
        self, services: HookStateService
    ) -> None:
        """HAS_EVENT edge scopes to current_run_id when no step is active."""
        await _seed_session(services)
        run_id = await _seed_run(services)
        # No step seeded — current_step_id stays None
        handler = SystemEventHandler(services)
        await handler(
            "context:compaction",
            {"session_id": "s1", "timestamp": TIMESTAMP_EVENT},
        )
        node_id = make_node_id("s1", "context:compaction", TIMESTAMP_EVENT)
        edge = await services.graph.get_edge(run_id, node_id)
        assert edge is not None
        assert edge["type"] == "HAS_EVENT"

    async def test_fallback_to_session_when_no_run(
        self, services: HookStateService
    ) -> None:
        """HAS_EVENT edge scopes to session_id when neither run nor step is active."""
        await _seed_session(services)
        handler = SystemEventHandler(services)
        await handler(
            "context:compaction",
            {"session_id": "s1", "timestamp": TIMESTAMP_EVENT},
        )
        node_id = make_node_id("s1", "context:compaction", TIMESTAMP_EVENT)
        edge = await services.graph.get_edge("s1", node_id)
        assert edge is not None
        assert edge["type"] == "HAS_EVENT"

    async def test_stores_data_blob_as_json(self, services: HookStateService) -> None:
        """Full raw payload is stored as valid JSON in the 'data' field."""
        await _seed_session(services)
        handler = SystemEventHandler(services)
        payload = {
            "session_id": "s1",
            "timestamp": TIMESTAMP_EVENT,
            "before_tokens": 10000,
            "after_tokens": 5000,
        }
        await handler("context:compaction", payload)
        node_id = make_node_id("s1", "context:compaction", TIMESTAMP_EVENT)
        node = await services.graph.get_node(node_id)
        assert node is not None
        stored = json.loads(node["data"])
        assert stored == payload

    async def test_missing_session_id_returns_continue(
        self, services: HookStateService
    ) -> None:
        """Missing session_id returns continue without raising."""
        handler = SystemEventHandler(services)
        result = await handler(
            "context:compaction",
            {"timestamp": TIMESTAMP_EVENT},
        )
        assert result.action == "continue"

    async def test_returns_continue(self, services: HookStateService) -> None:
        """Handler always returns HookResult(action='continue')."""
        await _seed_session(services)
        handler = SystemEventHandler(services)
        result = await handler(
            "context:compaction",
            {"session_id": "s1", "timestamp": TIMESTAMP_EVENT},
        )
        assert result.action == "continue"
