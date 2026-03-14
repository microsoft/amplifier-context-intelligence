"""Tests for SystemEventHandler — no-op sink for system events."""

from __future__ import annotations

from context_intelligence_server.handlers.event import SystemEventHandler
from context_intelligence_server.services import HookStateService


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
    """SystemEventHandler returns continue for all claimed events without graph mutations."""

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
        """SystemEventHandler must not create any graph nodes."""
        handler = SystemEventHandler(services)
        await handler(
            "context:compaction",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
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
