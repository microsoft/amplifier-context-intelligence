"""Tests for the ownership edge integrity checker (ownership.py).

Tests single-parent semantics on HAS_RUN, HAS_STEP, and TRIGGERED edges.
"""

from __future__ import annotations

import logging

import pytest

from context_intelligence_server.ownership import (
    OWNERSHIP_EDGE_TYPES,
    _find_owner_in_buffer,
    check_ownership,
)
from context_intelligence_server.services import GraphState


class TestCheckOwnership:
    async def test_no_existing_edge_returns_true(self) -> None:
        """If no edge of edge_type pointing to dst_id exists, return True."""
        graph = GraphState()
        result = await check_ownership(graph, "dst_node", "HAS_RUN", "src_node")
        assert result is True

    async def test_same_source_is_idempotent(self) -> None:
        """If same src already owns dst via edge_type, return True and leave edge untouched."""
        graph = GraphState()
        await graph.upsert_edge(
            "src_a", "dst_node", {"type": "HAS_RUN", "occurred_at": "t1"}
        )

        result = await check_ownership(graph, "dst_node", "HAS_RUN", "src_a")
        assert result is True

        # Edge should still be intact
        edge = await graph.get_edge("src_a", "dst_node")
        assert edge is not None
        assert edge["type"] == "HAS_RUN"

    async def test_different_source_removes_old_edge(self) -> None:
        """If a different src already owns dst, remove old edge and return True."""
        graph = GraphState()
        await graph.upsert_edge(
            "old_src", "dst_node", {"type": "HAS_RUN", "occurred_at": "t1"}
        )

        result = await check_ownership(graph, "dst_node", "HAS_RUN", "new_src")
        assert result is True

        # Old edge must be removed
        edge = await graph.get_edge("old_src", "dst_node")
        assert edge is None

    async def test_different_source_logs_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Ownership mutation from different source logs a WARNING."""
        graph = GraphState()
        await graph.upsert_edge(
            "old_src", "dst_node", {"type": "HAS_STEP", "occurred_at": "t1"}
        )

        with caplog.at_level(
            logging.WARNING, logger="context_intelligence_server.ownership"
        ):
            await check_ownership(graph, "dst_node", "HAS_STEP", "new_src")

        assert len(caplog.records) > 0
        warning_messages = [r.message for r in caplog.records]
        assert any("ownership" in msg.lower() for msg in warning_messages)

    async def test_non_matching_edge_type_ignored(self) -> None:
        """Non-ownership edge types are not enforced; edge untouched, returns True."""
        graph = GraphState()
        # NEXT is not an ownership edge type
        await graph.upsert_edge(
            "old_src", "dst_node", {"type": "NEXT", "occurred_at": "t1"}
        )

        result = await check_ownership(graph, "dst_node", "NEXT", "new_src")
        assert result is True

        # Edge must still exist — NEXT is not an ownership edge
        edge = await graph.get_edge("old_src", "dst_node")
        assert edge is not None


class TestOwnershipEdgeTypes:
    def test_frozenset_contains_has_run(self) -> None:
        assert "HAS_RUN" in OWNERSHIP_EDGE_TYPES

    def test_frozenset_contains_has_step(self) -> None:
        assert "HAS_STEP" in OWNERSHIP_EDGE_TYPES

    def test_frozenset_contains_triggered(self) -> None:
        assert "TRIGGERED" in OWNERSHIP_EDGE_TYPES

    def test_is_frozenset(self) -> None:
        assert isinstance(OWNERSHIP_EDGE_TYPES, frozenset)


class TestFindOwnerInBuffer:
    def test_returns_src_when_edge_found(self) -> None:
        """_find_owner_in_buffer returns the src_id for a matching edge."""
        graph = GraphState()
        graph._edges[("src_x", "dst_y")] = {"type": "HAS_RUN", "occurred_at": "t1"}

        result = _find_owner_in_buffer(graph, "dst_y", "HAS_RUN")
        assert result == "src_x"

    def test_returns_none_when_no_edge(self) -> None:
        """_find_owner_in_buffer returns None when no matching edge exists."""
        graph = GraphState()
        result = _find_owner_in_buffer(graph, "dst_y", "HAS_RUN")
        assert result is None

    def test_returns_none_for_wrong_type(self) -> None:
        """_find_owner_in_buffer returns None when edge type doesn't match."""
        graph = GraphState()
        graph._edges[("src_x", "dst_y")] = {"type": "NEXT", "occurred_at": "t1"}

        result = _find_owner_in_buffer(graph, "dst_y", "HAS_RUN")
        assert result is None

    def test_returns_none_for_wrong_dst(self) -> None:
        """_find_owner_in_buffer returns None when dst_id doesn't match."""
        graph = GraphState()
        graph._edges[("src_x", "dst_y")] = {"type": "HAS_RUN", "occurred_at": "t1"}

        result = _find_owner_in_buffer(graph, "other_dst", "HAS_RUN")
        assert result is None
