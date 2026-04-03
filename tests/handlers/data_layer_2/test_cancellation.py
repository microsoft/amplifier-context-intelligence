"""Tests for CancellationHandler — occurrence-only, cancel:completed only.

Verifies:
- handled_events == frozenset({'cancel:completed'}) — cancel:requested and cancel:start excluded
- cancel:completed creates Cancellation:SST_EVENT node keyed as
  '{session_id}::cancellation::{timestamp}' with was_immediate (from data['immediate'])
  and occurred_at
- E11: Session -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> Cancellation (always)
- Guard: missing session_id returns continue with zero graph mutations
"""

from __future__ import annotations

from context_intelligence_server.handlers.data_layer_2.cancellation import (
    CancellationHandler,
)
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


# ---------------------------------------------------------------------------
# 1. TestCancellationHandlerHandledEvents
# ---------------------------------------------------------------------------


class TestCancellationHandlerHandledEvents:
    """handled_events == frozenset({'cancel:completed'}) — cancel:requested and cancel:start excluded."""

    def test_handled_events_is_exact_frozenset(self) -> None:
        """handled_events must be exactly frozenset({'cancel:completed'})."""
        assert CancellationHandler.handled_events == frozenset({"cancel:completed"})

    def test_cancel_requested_not_in_handled_events(self) -> None:
        """cancel:requested must NOT be in handled_events (zero real examples exist)."""
        assert "cancel:requested" not in CancellationHandler.handled_events

    def test_cancel_start_not_in_handled_events(self) -> None:
        """cancel:start must NOT be in handled_events."""
        assert "cancel:start" not in CancellationHandler.handled_events


# ---------------------------------------------------------------------------
# 2. TestCancelCompletedCreatesNode
# ---------------------------------------------------------------------------


class TestCancelCompletedCreatesNode:
    """cancel:completed creates Cancellation:SST_EVENT node with correct key and properties."""

    async def test_node_created_with_correct_compound_key(
        self, services: HookStateService
    ) -> None:
        """cancel:completed must create node at '{session_id}::cancellation::{timestamp}'."""
        handler = CancellationHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "immediate": True,
        }
        await handler("cancel:completed", data)

        node_id = "s1::cancellation::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(node_id)
        assert node is not None, f"cancel:completed must create node at '{node_id}'"

    async def test_node_has_cancellation_and_sst_event_labels(
        self, services: HookStateService
    ) -> None:
        """Cancellation node must have 'Cancellation' and 'SST_EVENT' labels."""
        handler = CancellationHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "immediate": True,
        }
        await handler("cancel:completed", data)

        node = await services.graph.get_node("s1::cancellation::2026-01-01T00:00:00Z")
        assert node is not None
        assert "Cancellation" in node["labels"], (
            f"Cancellation label missing. Got: {node['labels']}"
        )
        assert "SST_EVENT" in node["labels"], (
            f"SST_EVENT label missing. Got: {node['labels']}"
        )

    async def test_node_carries_session_id(self, services: HookStateService) -> None:
        """Cancellation node must carry session_id property."""
        handler = CancellationHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "immediate": False,
        }
        await handler("cancel:completed", data)

        node = await services.graph.get_node("s1::cancellation::2026-01-01T00:00:00Z")
        assert node is not None
        assert node["session_id"] == "s1"

    async def test_node_carries_occurred_at(self, services: HookStateService) -> None:
        """Cancellation node must carry occurred_at property matching the event timestamp."""
        handler = CancellationHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "immediate": False,
        }
        await handler("cancel:completed", data)

        node = await services.graph.get_node("s1::cancellation::2026-01-01T00:00:00Z")
        assert node is not None
        assert node["occurred_at"] == "2026-01-01T00:00:00Z"

    async def test_was_immediate_true_when_immediate_is_true(
        self, services: HookStateService
    ) -> None:
        """was_immediate must be True when data['immediate'] is True."""
        handler = CancellationHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "immediate": True,
        }
        await handler("cancel:completed", data)

        node = await services.graph.get_node("s1::cancellation::2026-01-01T00:00:00Z")
        assert node is not None
        assert node["was_immediate"] is True

    async def test_was_immediate_false_when_immediate_is_false(
        self, services: HookStateService
    ) -> None:
        """was_immediate must be False when data['immediate'] is False."""
        handler = CancellationHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "immediate": False,
        }
        await handler("cancel:completed", data)

        node = await services.graph.get_node("s1::cancellation::2026-01-01T00:00:00Z")
        assert node is not None
        assert node["was_immediate"] is False


# ---------------------------------------------------------------------------
# 3. TestCancellationE11Edge
# ---------------------------------------------------------------------------


class TestCancellationE11Edge:
    """E11: Session -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> Cancellation (always)."""

    async def test_e11_edge_session_to_cancellation_has_part(
        self, services: HookStateService
    ) -> None:
        """E11 edge must be Session -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> Cancellation."""
        handler = CancellationHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "immediate": True,
        }
        await handler("cancel:completed", data)

        cancellation_id = "s1::cancellation::2026-01-01T00:00:00Z"
        edge = await services.graph.get_edge("s1", cancellation_id)
        assert edge is not None, (
            f"E11: HAS_PART edge from 's1' to '{cancellation_id}' must exist"
        )
        assert edge.get("type") == "HAS_PART", (
            f"E11 edge type must be HAS_PART. Got: {edge.get('type')}"
        )
        assert edge.get("sst_semantic") == "CONTAINS", (
            f"E11 edge sst_semantic must be CONTAINS. Got: {edge.get('sst_semantic')}"
        )


# ---------------------------------------------------------------------------
# 4. TestCancellationSessionIdGuard
# ---------------------------------------------------------------------------


class TestCancellationSessionIdGuard:
    """Missing session_id returns continue with zero graph mutations."""

    async def test_missing_session_id_returns_continue(
        self, services: HookStateService
    ) -> None:
        """Missing session_id must return HookResult(action='continue')."""
        handler = CancellationHandler(services)

        result = await handler(
            "cancel:completed",
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "immediate": True,
            },
        )
        assert result.action == "continue"

    async def test_no_graph_mutations_on_missing_session_id(
        self, services: HookStateService
    ) -> None:
        """Missing session_id must result in zero nodes and zero edges created."""
        handler = CancellationHandler(services)

        await handler(
            "cancel:completed",
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "immediate": True,
            },
        )

        assert len(services.graph._nodes) == 0, (
            "No nodes must be created when session_id is missing"
        )
        assert len(services.graph._edges) == 0, (
            "No edges must be created when session_id is missing"
        )


# ---------------------------------------------------------------------------
# 5. TestCancellationSourcedFrom
# ---------------------------------------------------------------------------


class TestCancellationSourcedFrom:
    """SOURCED_FROM: Cancellation -[:SOURCED_FROM]-> data_layer_1 cancel:completed event node."""

    async def test_sourced_from_edge_created_for_cancel_completed(
        self, services: HookStateService
    ) -> None:
        """cancel:completed must create a SOURCED_FROM edge from the Cancellation node to the
        data_layer_1 event node identified by make_node_id(session_id, 'cancel:completed', timestamp).
        """
        handler = CancellationHandler(services)
        await handler(
            "cancel:completed",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "immediate": True,
            },
        )
        cancellation_node_id = "s1::cancellation::2026-01-01T00:00:00Z"
        data_layer_1_node_id = make_node_id(
            "s1", "cancel:completed", "2026-01-01T00:00:00Z"
        )
        edge = await services.graph.get_edge(cancellation_node_id, data_layer_1_node_id)
        assert edge is not None, (
            f"SOURCED_FROM edge must exist from '{cancellation_node_id}' to '{data_layer_1_node_id}'"
        )
        assert edge["type"] == "SOURCED_FROM"
