"""Tests for DefaultHandler — Event node creation for unclaimed events.

3-level label hierarchy: [FullPascal, Category, 'Event']
No run-awareness — events always attach to session node directly.
"""

from __future__ import annotations

import json
import logging

from context_intelligence_server.handlers.default import DefaultHandler
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


class TestDeriveLabelConversions:
    """DefaultHandler.derive_labels returns [FullPascalEvent, CategoryEvent, 'Event']."""

    def test_tool_pre(self) -> None:
        assert DefaultHandler.derive_labels("tool:pre") == [
            "ToolPreEvent",
            "ToolEvent",
            "Event",
        ]

    def test_session_start(self) -> None:
        assert DefaultHandler.derive_labels("session:start") == [
            "SessionStartEvent",
            "SessionEvent",
            "Event",
        ]

    def test_recipe_loop_iteration(self) -> None:
        assert DefaultHandler.derive_labels("recipe:loop_iteration") == [
            "RecipeLoopIterationEvent",
            "RecipeEvent",
            "Event",
        ]

    def test_context_compaction(self) -> None:
        assert DefaultHandler.derive_labels("context:compaction") == [
            "ContextCompactionEvent",
            "ContextEvent",
            "Event",
        ]

    def test_delegate_agent_spawned(self) -> None:
        assert DefaultHandler.derive_labels("delegate:agent_spawned") == [
            "DelegateAgentSpawnedEvent",
            "DelegateEvent",
            "Event",
        ]

    def test_cancel_requested(self) -> None:
        assert DefaultHandler.derive_labels("cancel:requested") == [
            "CancelRequestedEvent",
            "CancelEvent",
            "Event",
        ]

    def test_underscore_only(self) -> None:
        """No colon — FullPascalEvent == CategoryEvent."""
        assert DefaultHandler.derive_labels("my_event") == [
            "MyEventEvent",
            "MyEventEvent",
            "Event",
        ]

    def test_single_word(self) -> None:
        """Single word — FullPascalEvent == CategoryEvent."""
        assert DefaultHandler.derive_labels("ping") == [
            "PingEvent",
            "PingEvent",
            "Event",
        ]

    def test_session_fork(self) -> None:
        """session:fork produces SessionForkEvent — distinct from SubSession/Session entity labels."""
        assert DefaultHandler.derive_labels("session:fork") == [
            "SessionForkEvent",
            "SessionEvent",
            "Event",
        ]


class TestDefaultHandlerCreatesEventNodes:
    """DefaultHandler creates Event nodes with 3-level labels + HAS_EVENT edges."""

    async def test_creates_event_node_with_three_labels(
        self, services: HookStateService
    ) -> None:
        """Event node has 3 labels: FullPascal, Category, and 'Event'."""
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
        labels = set(node["labels"])
        assert "SessionResumeEvent" in labels
        assert "SessionEvent" in labels
        assert "Event" in labels
        assert node["occurred_at"] == "2026-01-01T02:00:00Z"
        assert node["event_name"] == "session:resume"

    async def test_creates_has_event_edge_from_session_no_run_awareness(
        self, services: HookStateService
    ) -> None:
        """HAS_EVENT edge goes from session_id to event node (no run-awareness)."""
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
        """DefaultHandler is generic — custom:my_event → CustomMyEventEvent, CustomEvent, Event."""
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
        labels = set(node["labels"])
        assert "CustomMyEventEvent" in labels
        assert "CustomEvent" in labels
        assert "Event" in labels
        assert node["event_name"] == "custom:my_event"

    async def test_returns_continue(self, services: HookStateService) -> None:
        handler = DefaultHandler(services)
        result = await handler(
            "session:resume",
            {"session_id": "s1", "timestamp": "2026-01-01T02:00:00Z"},
        )
        assert result.action == "continue"


class TestDefaultHandlerTiebreaker:
    """tool_call_id used as disambiguator for tool events."""

    async def test_tool_pre_uses_tool_call_id_as_disambiguator(
        self, services: HookStateService
    ) -> None:
        """tool:pre passes tool_call_id to make_node_id as fourth arg."""
        handler = DefaultHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
                "tool_call_id": "tc-abc",
            },
        )
        event_id = make_node_id("s1", "tool:pre", "2026-01-01T02:00:00Z", "tc-abc")
        node = await services.graph.get_node(event_id)
        assert node is not None

    async def test_non_tool_events_have_no_disambiguator(
        self, services: HookStateService
    ) -> None:
        """Non-tool events do NOT use tool_call_id as disambiguator."""
        handler = DefaultHandler(services)
        await handler(
            "session:resume",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
                "tool_call_id": "tc-abc",  # present but should be ignored
            },
        )
        # Node should exist without the disambiguator
        event_id_no_disambiguator = make_node_id(
            "s1", "session:resume", "2026-01-01T02:00:00Z"
        )
        node = await services.graph.get_node(event_id_no_disambiguator)
        assert node is not None
        # Node with disambiguator should NOT exist
        event_id_with_disambiguator = make_node_id(
            "s1", "session:resume", "2026-01-01T02:00:00Z", "tc-abc"
        )
        node_wrong = await services.graph.get_node(event_id_with_disambiguator)
        assert node_wrong is None

    async def test_parallel_tool_calls_produce_distinct_nodes(
        self, services: HookStateService
    ) -> None:
        """Same timestamp, different tool_call_ids produce distinct nodes."""
        handler = DefaultHandler(services)
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
                "tool_call_id": "tc-001",
            },
        )
        await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T02:00:00Z",
                "tool_call_id": "tc-002",
            },
        )
        event_id_1 = make_node_id("s1", "tool:pre", "2026-01-01T02:00:00Z", "tc-001")
        event_id_2 = make_node_id("s1", "tool:pre", "2026-01-01T02:00:00Z", "tc-002")
        assert event_id_1 != event_id_2
        node1 = await services.graph.get_node(event_id_1)
        node2 = await services.graph.get_node(event_id_2)
        assert node1 is not None
        assert node2 is not None


class TestDefaultHandlerDataProperty:
    """DefaultHandler stores full event payload in 'data' property as JSON string."""

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


class TestDefaultHandlerEdgeType:
    """HAS_EVENT edge has type='HAS_EVENT'."""

    async def test_has_event_edge_type(self, services: HookStateService) -> None:
        """Edge from session → event node must have type='HAS_EVENT'."""
        handler = DefaultHandler(services)
        await handler(
            "session:resume",
            {"session_id": "s1", "timestamp": "2026-01-01T02:00:00Z"},
        )
        event_id = make_node_id("s1", "session:resume", "2026-01-01T02:00:00Z")
        edge = await services.graph.get_edge("s1", event_id)
        assert edge is not None
        assert edge.get("type") == "HAS_EVENT"


class TestDefaultHandlerDroppedEventLogging:
    """D-1: DefaultHandler must log at WARNING when dropping events without session_id."""

    async def test_missing_session_id_logs_warning(self, services, caplog):
        handler = DefaultHandler(services)
        with caplog.at_level(logging.WARNING):
            result = await handler(
                "session:resume", {"timestamp": "2026-01-01T02:00:00Z"}
            )
        assert result.action == "continue"
        assert "DefaultHandler" in caplog.text
        assert "session:resume" in caplog.text
        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warning_records) >= 1
