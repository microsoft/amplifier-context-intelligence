"""Tests for ContextCompactionHandler — ContextCompaction node creation (occurrence-only).

Covers:
- handled_events == frozenset({'context:pre_compact', 'context:post_compact'})
- Both events create ContextCompaction:SST_EVENT node keyed as
  '{session_id}::compaction::{timestamp}' with occurred_at
- E12: Session -[:HAS_COMPACTION {sst_semantic: 'CONTAINS'}]-> ContextCompaction (always)
- Guard: missing session_id returns continue with zero graph mutations
"""

from __future__ import annotations

from context_intelligence_server.handlers.data_layer_2.context_compaction import (
    ContextCompactionHandler,
)
from context_intelligence_server.services import HookStateService


# ---------------------------------------------------------------------------
# 1. TestContextCompactionHandlerHandledEvents
# ---------------------------------------------------------------------------


class TestContextCompactionHandlerHandledEvents:
    """handled_events == frozenset({'context:pre_compact', 'context:post_compact'})."""

    def test_handled_events_is_exact_frozenset(self) -> None:
        """handled_events must be exactly frozenset({'context:pre_compact', 'context:post_compact'})."""
        assert ContextCompactionHandler.handled_events == frozenset(
            {"context:pre_compact", "context:post_compact"}
        )

    def test_context_compact_not_in_handled_events(self) -> None:
        """context:compact must NOT be in handled_events."""
        assert "context:compact" not in ContextCompactionHandler.handled_events

    def test_context_start_not_in_handled_events(self) -> None:
        """context:start must NOT be in handled_events."""
        assert "context:start" not in ContextCompactionHandler.handled_events


# ---------------------------------------------------------------------------
# 2. TestContextPreCompactCreatesNode
# ---------------------------------------------------------------------------


class TestContextPreCompactCreatesNode:
    """context:pre_compact creates ContextCompaction:SST_EVENT node with correct key and properties."""

    async def test_node_created_with_correct_compound_key(
        self, services: HookStateService
    ) -> None:
        """context:pre_compact must create node at '{session_id}::compaction::{timestamp}'."""
        handler = ContextCompactionHandler(services)
        await handler(
            "context:pre_compact",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node_id = "s1::compaction::2026-01-01T00:00:00Z"
        node = await services.graph.get_node(node_id)
        assert node is not None, f"context:pre_compact must create node at '{node_id}'"

    async def test_node_has_context_compaction_and_sst_event_labels(
        self, services: HookStateService
    ) -> None:
        """ContextCompaction node must have 'ContextCompaction' and 'SST_EVENT' labels."""
        handler = ContextCompactionHandler(services)
        await handler(
            "context:pre_compact",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("s1::compaction::2026-01-01T00:00:00Z")
        assert node is not None
        assert "ContextCompaction" in node["labels"], (
            f"ContextCompaction label missing. Got: {node['labels']}"
        )
        assert "SST_EVENT" in node["labels"], (
            f"SST_EVENT label missing. Got: {node['labels']}"
        )

    async def test_node_has_session_id_and_occurred_at(
        self, services: HookStateService
    ) -> None:
        """ContextCompaction node must carry session_id and occurred_at properties."""
        handler = ContextCompactionHandler(services)
        await handler(
            "context:pre_compact",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("s1::compaction::2026-01-01T00:00:00Z")
        assert node is not None
        assert node["session_id"] == "s1"
        assert node["occurred_at"] == "2026-01-01T00:00:00Z"


# ---------------------------------------------------------------------------
# 3. TestContextPostCompactCreatesNode
# ---------------------------------------------------------------------------


class TestContextPostCompactCreatesNode:
    """context:post_compact creates ContextCompaction:SST_EVENT node with correct key and properties."""

    async def test_node_created_with_correct_compound_key(
        self, services: HookStateService
    ) -> None:
        """context:post_compact must create node at '{session_id}::compaction::{timestamp}'."""
        handler = ContextCompactionHandler(services)
        await handler(
            "context:post_compact",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:01Z",
            },
        )
        node_id = "s1::compaction::2026-01-01T00:00:01Z"
        node = await services.graph.get_node(node_id)
        assert node is not None, f"context:post_compact must create node at '{node_id}'"

    async def test_post_compact_has_same_labels(
        self, services: HookStateService
    ) -> None:
        """context:post_compact node must have identical labels as context:pre_compact."""
        handler = ContextCompactionHandler(services)
        await handler(
            "context:post_compact",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:01Z",
            },
        )
        node = await services.graph.get_node("s1::compaction::2026-01-01T00:00:01Z")
        assert node is not None
        assert "ContextCompaction" in node["labels"]
        assert "SST_EVENT" in node["labels"]


# ---------------------------------------------------------------------------
# 4. TestContextCompactionE12Edge
# ---------------------------------------------------------------------------


class TestContextCompactionE12Edge:
    """E12: Session -[:HAS_COMPACTION {sst_semantic: 'CONTAINS'}]-> ContextCompaction."""

    async def test_e12_edge_created_on_pre_compact(
        self, services: HookStateService
    ) -> None:
        """context:pre_compact must create E12 HAS_COMPACTION edge from Session to ContextCompaction."""
        handler = ContextCompactionHandler(services)
        await handler(
            "context:pre_compact",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        edge = await services.graph.get_edge(
            "s1", "s1::compaction::2026-01-01T00:00:00Z"
        )
        assert edge is not None, "E12 edge must exist on context:pre_compact"
        assert edge["type"] == "HAS_COMPACTION"
        assert edge["sst_semantic"] == "CONTAINS"

    async def test_e12_edge_created_on_post_compact(
        self, services: HookStateService
    ) -> None:
        """context:post_compact must also create E12 HAS_COMPACTION edge."""
        handler = ContextCompactionHandler(services)
        await handler(
            "context:post_compact",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:01Z",
            },
        )
        edge = await services.graph.get_edge(
            "s1", "s1::compaction::2026-01-01T00:00:01Z"
        )
        assert edge is not None, "E12 edge must exist on context:post_compact"
        assert edge["type"] == "HAS_COMPACTION"
        assert edge["sst_semantic"] == "CONTAINS"


# ---------------------------------------------------------------------------
# 5. TestContextCompactionSessionIdGuard
# ---------------------------------------------------------------------------


class TestContextCompactionSessionIdGuard:
    """Missing session_id must short-circuit without any graph mutations."""

    async def test_missing_session_id_creates_no_nodes(
        self, services: HookStateService
    ) -> None:
        """context:pre_compact with no session_id must create zero nodes."""
        handler = ContextCompactionHandler(services)
        await handler(
            "context:pre_compact",
            {"timestamp": "2026-01-01T00:00:00Z"},
        )
        assert len(services.graph._nodes) == 0

    async def test_missing_session_id_creates_no_edges(
        self, services: HookStateService
    ) -> None:
        """context:post_compact with no session_id must create zero edges."""
        handler = ContextCompactionHandler(services)
        await handler(
            "context:post_compact",
            {"timestamp": "2026-01-01T00:00:00Z"},
        )
        assert len(services.graph._edges) == 0

    async def test_missing_session_id_returns_continue(
        self, services: HookStateService
    ) -> None:
        """Missing session_id must return HookResult(action='continue')."""
        handler = ContextCompactionHandler(services)
        result = await handler(
            "context:pre_compact",
            {"timestamp": "2026-01-01T00:00:00Z"},
        )
        assert result.action == "continue"
