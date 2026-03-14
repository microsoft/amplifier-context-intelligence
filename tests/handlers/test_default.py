"""Tests for DefaultHandler — Event node creation for unclaimed events.

Adapted from bundle's test_default_handler.py for the server-side implementation,
which uses the flat-dict GraphState API (no nested 'properties' key, 2-arg get_edge).
"""

from __future__ import annotations

import json

from context_intelligence_server.handlers.default import DefaultHandler
from context_intelligence_server.handlers.orchestrator_run import OrchestratorRunHandler
from context_intelligence_server.handlers.session import SessionHandler
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


class TestDeriveLabelConversions:
    """DefaultHandler.derive_label converts event names to PascalCase labels."""

    def test_colon_separator(self) -> None:
        assert DefaultHandler.derive_label("context:compaction") == "ContextCompaction"

    def test_single_word(self) -> None:
        assert DefaultHandler.derive_label("session:resume") == "SessionResume"

    def test_underscore_separator(self) -> None:
        assert DefaultHandler.derive_label("my_event") == "MyEvent"

    def test_mixed_separators(self) -> None:
        assert DefaultHandler.derive_label("custom:my_event") == "CustomMyEvent"

    def test_cancel_requested(self) -> None:
        assert DefaultHandler.derive_label("cancel:requested") == "CancelRequested"

    def test_cancel_completed(self) -> None:
        assert DefaultHandler.derive_label("cancel:completed") == "CancelCompleted"


class TestDefaultHandlerCreatesEventNodes:
    """DefaultHandler creates :Event:{DerivedLabel} nodes + HAS_EVENT edges."""

    async def test_creates_event_node_with_derived_label(
        self, services: HookStateService
    ) -> None:
        handler = DefaultHandler(services)
        await handler(
            "session:resume",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
            },
        )
        event_id = make_node_id("s1", "session:resume", "2026-01-01T02:00:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        assert set(node["labels"]) == {"Event", "SessionResume"}
        assert node["occurred_at"] == "2026-01-01T02:00:00Z"
        assert node["event_name"] == "session:resume"

    async def test_creates_has_event_edge(self, services: HookStateService) -> None:
        handler = DefaultHandler(services)
        await handler(
            "session:resume",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
            },
        )
        event_id = make_node_id("s1", "session:resume", "2026-01-01T02:00:00Z")
        edge = await services.graph.get_edge("s1", event_id)
        assert edge is not None
        assert edge["occurred_at"] == "2026-01-01T02:00:00Z"

    async def test_skips_event_without_session_id(
        self, services: HookStateService
    ) -> None:
        handler = DefaultHandler(services)
        result = await handler(
            "session:resume",
            {"timestamp": "2026-01-01T02:00:00Z"},
        )
        assert result.action == "continue"
        # No nodes should have been created
        node = await services.graph.get_node("s1")
        assert node is None

    async def test_works_with_arbitrary_unclaimed_event(
        self, services: HookStateService
    ) -> None:
        """DefaultHandler is generic — works for any event name."""
        handler = DefaultHandler(services)
        await handler(
            "custom:my_event",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T03:00:00Z",
            },
        )
        event_id = make_node_id("s1", "custom:my_event", "2026-01-01T03:00:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        assert set(node["labels"]) == {"Event", "CustomMyEvent"}
        assert node["event_name"] == "custom:my_event"

    async def test_returns_continue(self, services: HookStateService) -> None:
        handler = DefaultHandler(services)
        result = await handler(
            "session:resume",
            {"session_id": "s1", "timestamp": "2026-01-01T02:00:00Z"},
        )
        assert result.action == "continue"


class TestDefaultHandlerDataProperty:
    """DefaultHandler stores full event payload in the 'data' property."""

    async def test_stores_data_property(self, services: HookStateService) -> None:
        """Event node has 'data' property containing the full JSON payload."""
        handler = DefaultHandler(services)
        await handler(
            "session:resume",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
                "custom_info": "extra-value",
            },
        )
        event_id = make_node_id("s1", "session:resume", "2026-01-01T02:00:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        data = json.loads(node["data"])
        assert data["session_id"] == "s1"
        assert data["custom_info"] == "extra-value"


class TestDefaultHandlerRunAwareness:
    """DefaultHandler attaches HAS_EVENT to OrchestratorRun when active,
    falls back to Session when no active run.
    """

    async def _seed_active_run(
        self, services: HookStateService, session_id: str = "s1"
    ) -> str:
        """Create Session + prompt:submit + execution:start so current_run_id is set.

        Returns the run node ID.
        """
        session_handler = SessionHandler(services)
        await session_handler(
            "session:start",
            {"session_id": session_id, "timestamp": "2026-03-06T00:00:00Z"},
        )
        run_handler = OrchestratorRunHandler(services)
        await run_handler(
            "prompt:submit",
            {
                "session_id": session_id,
                "timestamp": "2026-03-06T01:00:00Z",
                "prompt": "Hello",
            },
        )
        await run_handler(
            "execution:start",
            {"session_id": session_id, "timestamp": "2026-03-06T02:00:00Z"},
        )
        cursors = services.get_cursors(session_id)
        assert cursors.current_run_id is not None
        return cursors.current_run_id

    async def test_event_during_active_run_attaches_to_run(
        self, services: HookStateService
    ) -> None:
        """When current_run_id exists, HAS_EVENT goes from OrchestratorRun to Event."""
        run_id = await self._seed_active_run(services)
        handler = DefaultHandler(services)
        await handler(
            "artifact:read",
            {"session_id": "s1", "timestamp": "2026-03-06T02:30:00Z"},
        )
        event_id = make_node_id("s1", "artifact:read", "2026-03-06T02:30:00Z")

        # HAS_EVENT should come from the run, not the session
        edge_from_run = await services.graph.get_edge(run_id, event_id)
        assert edge_from_run is not None, "HAS_EVENT edge from run is missing"

        # HAS_EVENT from session should NOT exist
        edge_from_session = await services.graph.get_edge("s1", event_id)
        assert edge_from_session is None, (
            "HAS_EVENT from session should not exist when run is active"
        )

    async def test_event_without_active_run_attaches_to_session(
        self, services: HookStateService
    ) -> None:
        """When no current_run_id, HAS_EVENT goes from Session (existing behavior)."""
        handler = DefaultHandler(services)
        await handler(
            "session:resume",
            {"session_id": "s1", "timestamp": "2026-01-01T02:00:00Z"},
        )
        event_id = make_node_id("s1", "session:resume", "2026-01-01T02:00:00Z")
        edge = await services.graph.get_edge("s1", event_id)
        assert edge is not None, "HAS_EVENT edge from session is missing"

    async def test_event_after_run_completes_attaches_to_session(
        self, services: HookStateService
    ) -> None:
        """After orchestrator:complete clears current_run_id, events go back to Session."""
        await self._seed_active_run(services)
        run_handler = OrchestratorRunHandler(services)
        await run_handler(
            "orchestrator:complete",
            {
                "session_id": "s1",
                "timestamp": "2026-03-06T03:00:00Z",
                "status": "success",
                "turn_count": 1,
            },
        )

        # current_run_id should be cleared
        cursors = services.get_cursors("s1")
        assert cursors.current_run_id is None

        handler = DefaultHandler(services)
        await handler(
            "prompt:complete",
            {"session_id": "s1", "timestamp": "2026-03-06T03:01:00Z"},
        )
        event_id = make_node_id("s1", "prompt:complete", "2026-03-06T03:01:00Z")

        # Should attach to session, not the (now-closed) run
        edge_from_session = await services.graph.get_edge("s1", event_id)
        assert edge_from_session is not None

    async def test_run_aware_event_node_still_has_correct_labels(
        self, services: HookStateService
    ) -> None:
        """Event node labels and properties are unchanged by run-awareness."""
        await self._seed_active_run(services)
        handler = DefaultHandler(services)
        await handler(
            "artifact:read",
            {"session_id": "s1", "timestamp": "2026-03-06T02:30:00Z"},
        )
        event_id = make_node_id("s1", "artifact:read", "2026-03-06T02:30:00Z")
        node = await services.graph.get_node(event_id)
        assert node is not None
        assert set(node["labels"]) == {"Event", "ArtifactRead"}
        assert node["event_name"] == "artifact:read"
