"""Tests for ContentBlockHandler — content block assembly and tool_call block ID cache.

Covers:
- handled_events == frozenset({'content_block:start', 'content_block:end'})
- content_block:start creates ContentBlock:SST_EVENT node keyed as
  '{session_id}::block::{iteration_n}::{block_index}' with session_id, block_index,
  started_at; iteration_n extracted from active_iteration_id cursor (split('::')[-1])
- E07: Iteration -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> ContentBlock
  created when active_iteration_id is set; NOT created when no active iteration (zero edges)
- content_block:end enriches ContentBlock with block_type (from block.type), ended_at
- content_block:end caches block.id in pending_tool_block_ids for tool_call blocks
  ONLY when block_type == 'tool_call' AND block.id is present; text blocks NOT cached;
  tool_call blocks without id NOT cached; missing block dict does not crash
- Guard: missing session_id returns continue with zero graph mutations
"""

from __future__ import annotations

from context_intelligence_server.handlers.data_layer_2.content_block import (
    ContentBlockHandler,
)
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


# ---------------------------------------------------------------------------
# 1. TestContentBlockHandlerHandledEvents
# ---------------------------------------------------------------------------


class TestContentBlockHandlerHandledEvents:
    """handled_events == frozenset({'content_block:start', 'content_block:end'})."""

    def test_handled_events_is_exact_frozenset(self) -> None:
        """handled_events must be exactly frozenset({'content_block:start', 'content_block:end'})."""
        assert ContentBlockHandler.handled_events == frozenset(
            {"content_block:start", "content_block:end"}
        )

    def test_other_events_not_in_handled_events(self) -> None:
        """Events like 'content_block:delta' must NOT be in handled_events."""
        assert "content_block:delta" not in ContentBlockHandler.handled_events


# ---------------------------------------------------------------------------
# 2. TestContentBlockStartCreatesNode
# ---------------------------------------------------------------------------


class TestContentBlockStartCreatesNode:
    """content_block:start creates ContentBlock:SST_EVENT node with correct key and properties."""

    async def test_node_created_with_correct_compound_key(
        self, services: HookStateService
    ) -> None:
        """content_block:start must create node at '{session_id}::block::{iteration_n}::{block_index}'."""
        services.data_layer_2.active_iteration_id = "s1::iteration::1"
        handler = ContentBlockHandler(services)
        await handler(
            "content_block:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "block_index": 0,
            },
        )
        node_id = "s1::block::1::0"
        node = await services.graph.get_node(node_id)
        assert node is not None, f"content_block:start must create node at '{node_id}'"

    async def test_node_has_content_block_and_sst_event_labels(
        self, services: HookStateService
    ) -> None:
        """ContentBlock node must have 'ContentBlock' and 'SST_EVENT' labels."""
        services.data_layer_2.active_iteration_id = "s1::iteration::1"
        handler = ContentBlockHandler(services)
        await handler(
            "content_block:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "block_index": 0,
            },
        )
        node = await services.graph.get_node("s1::block::1::0")
        assert node is not None
        assert "ContentBlock" in node["labels"], (
            f"ContentBlock label missing. Got: {node['labels']}"
        )
        assert "SST_EVENT" in node["labels"], (
            f"SST_EVENT label missing. Got: {node['labels']}"
        )

    async def test_node_has_session_id_property(
        self, services: HookStateService
    ) -> None:
        """ContentBlock node must have session_id property."""
        services.data_layer_2.active_iteration_id = "s1::iteration::1"
        handler = ContentBlockHandler(services)
        await handler(
            "content_block:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "block_index": 0,
            },
        )
        node = await services.graph.get_node("s1::block::1::0")
        assert node is not None
        assert node.get("session_id") == "s1", (
            f"session_id property missing or wrong. Got: {node!r}"
        )

    async def test_node_has_block_index_and_started_at_properties(
        self, services: HookStateService
    ) -> None:
        """ContentBlock node must have block_index and started_at properties."""
        services.data_layer_2.active_iteration_id = "s1::iteration::1"
        handler = ContentBlockHandler(services)
        await handler(
            "content_block:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "block_index": 2,
            },
        )
        node = await services.graph.get_node("s1::block::1::2")
        assert node is not None
        assert node.get("block_index") == 2, (
            f"block_index property missing or wrong. Got: {node!r}"
        )
        assert node.get("started_at") == "2026-01-01T00:00:00Z", (
            f"started_at property missing or wrong. Got: {node!r}"
        )


# ---------------------------------------------------------------------------
# 3. TestE07HasPartEdge
# ---------------------------------------------------------------------------


class TestE07HasPartEdge:
    """E07: Iteration -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> ContentBlock."""

    async def test_e07_edge_created_when_active_iteration_id_is_set(
        self, services: HookStateService
    ) -> None:
        """E07 edge must be created when active_iteration_id is set before content_block:start."""
        services.data_layer_2.active_iteration_id = "s1::iteration::1"
        handler = ContentBlockHandler(services)
        await handler(
            "content_block:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "block_index": 0,
            },
        )
        iteration_id = "s1::iteration::1"
        block_id = "s1::block::1::0"
        edge = await services.graph.get_edge(iteration_id, block_id)
        assert edge is not None, (
            f"E07 HAS_PART edge from '{iteration_id}' to '{block_id}' must exist "
            "when active_iteration_id cursor is set"
        )
        assert edge.get("type") == "HAS_PART", (
            f"E07 edge must have type='HAS_PART'. Got: {edge.get('type')}"
        )
        assert edge.get("sst_semantic") == "CONTAINS", (
            f"E07 edge must have sst_semantic='CONTAINS'. Got: {edge.get('sst_semantic')}"
        )

    async def test_e07_not_created_when_no_active_iteration(
        self, services: HookStateService
    ) -> None:
        """E07 must NOT be created when active_iteration_id is None; only SOURCED_FROM edge is created."""
        handler = ContentBlockHandler(services)
        # active_iteration_id is None by default (no prior provider:request event)
        assert services.data_layer_2.active_iteration_id is None

        await handler(
            "content_block:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "block_index": 0,
            },
        )
        # No E07 edge should be created, but SOURCED_FROM edge is always created
        assert len(services.graph._edges) == 1, (
            f"No E07 edge should exist when active_iteration_id is None. "
            f"Got {len(services.graph._edges)} edges: {list(services.graph._edges.keys())}"
        )


# ---------------------------------------------------------------------------
# 4. TestContentBlockEndUpsertsProperties
# ---------------------------------------------------------------------------


class TestContentBlockEndUpsertsProperties:
    """content_block:end enriches ContentBlock with block_type and ended_at."""

    async def test_content_block_end_sets_block_type(
        self, services: HookStateService
    ) -> None:
        """content_block:end must set block_type from block.type on the ContentBlock node."""
        services.data_layer_2.active_iteration_id = "s1::iteration::1"
        handler = ContentBlockHandler(services)

        # First fire content_block:start to create the node
        await handler(
            "content_block:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "block_index": 0,
            },
        )
        # Then fire content_block:end to enrich it
        await handler(
            "content_block:end",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:01:00Z",
                "block_index": 0,
                "block": {"type": "text"},
            },
        )
        node = await services.graph.get_node("s1::block::1::0")
        assert node is not None
        assert node.get("block_type") == "text", (
            f"content_block:end must set block_type from block.type. Got: {node!r}"
        )

    async def test_content_block_end_sets_ended_at(
        self, services: HookStateService
    ) -> None:
        """content_block:end must set ended_at on the ContentBlock node."""
        services.data_layer_2.active_iteration_id = "s1::iteration::1"
        handler = ContentBlockHandler(services)

        await handler(
            "content_block:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "block_index": 0,
            },
        )
        await handler(
            "content_block:end",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:01:00Z",
                "block_index": 0,
                "block": {"type": "text"},
            },
        )
        node = await services.graph.get_node("s1::block::1::0")
        assert node is not None
        assert node.get("ended_at") == "2026-01-01T00:01:00Z", (
            f"content_block:end must set ended_at. Got: {node!r}"
        )


# ---------------------------------------------------------------------------
# 5. TestContentBlockToolCallCache
# ---------------------------------------------------------------------------


class TestContentBlockToolCallCache:
    """content_block:end caches block.id in pending_tool_block_ids for tool_call blocks only."""

    async def test_tool_call_block_with_id_is_cached(
        self, services: HookStateService
    ) -> None:
        """block.id must be cached in pending_tool_block_ids when block_type=='tool_call' and block.id present."""
        services.data_layer_2.active_iteration_id = "s1::iteration::1"
        handler = ContentBlockHandler(services)

        await handler(
            "content_block:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "block_index": 0,
            },
        )
        await handler(
            "content_block:end",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:01:00Z",
                "block_index": 0,
                "block": {"type": "tool_call", "id": "tool-block-abc"},
            },
        )
        assert "tool-block-abc" in services.data_layer_2.pending_tool_block_ids, (
            "block.id must be cached in pending_tool_block_ids for tool_call blocks"
        )
        assert (
            services.data_layer_2.pending_tool_block_ids["tool-block-abc"]
            == "s1::block::1::0"
        ), (
            "pending_tool_block_ids['tool-block-abc'] must map to the block node id 's1::block::1::0'"
        )

    async def test_text_block_not_cached(self, services: HookStateService) -> None:
        """text blocks must NOT be cached in pending_tool_block_ids."""
        services.data_layer_2.active_iteration_id = "s1::iteration::1"
        handler = ContentBlockHandler(services)

        await handler(
            "content_block:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "block_index": 0,
            },
        )
        await handler(
            "content_block:end",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:01:00Z",
                "block_index": 0,
                "block": {"type": "text", "id": "text-block-xyz"},
            },
        )
        assert len(services.data_layer_2.pending_tool_block_ids) == 0, (
            "text blocks must NOT be cached in pending_tool_block_ids"
        )

    async def test_tool_call_block_without_id_not_cached(
        self, services: HookStateService
    ) -> None:
        """tool_call blocks without block.id must NOT be cached in pending_tool_block_ids."""
        services.data_layer_2.active_iteration_id = "s1::iteration::1"
        handler = ContentBlockHandler(services)

        await handler(
            "content_block:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "block_index": 0,
            },
        )
        await handler(
            "content_block:end",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:01:00Z",
                "block_index": 0,
                "block": {"type": "tool_call"},  # no id field
            },
        )
        assert len(services.data_layer_2.pending_tool_block_ids) == 0, (
            "tool_call blocks without id must NOT be cached in pending_tool_block_ids"
        )

    async def test_missing_block_dict_does_not_crash(
        self, services: HookStateService
    ) -> None:
        """content_block:end with missing block dict must not crash and must return continue."""
        services.data_layer_2.active_iteration_id = "s1::iteration::1"
        handler = ContentBlockHandler(services)

        await handler(
            "content_block:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "block_index": 0,
            },
        )
        result = await handler(
            "content_block:end",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:01:00Z",
                "block_index": 0,
                # block key intentionally omitted
            },
        )
        assert result.action == "continue", (
            f"content_block:end with missing block dict must return action='continue'. "
            f"Got: {result.action!r}"
        )


# ---------------------------------------------------------------------------
# 6. TestContentBlockHandlerGuards
# ---------------------------------------------------------------------------


class TestContentBlockHandlerGuards:
    """Missing session_id must short-circuit before any graph mutation."""

    async def test_missing_session_id_returns_continue_with_zero_graph_mutations(
        self, services: HookStateService
    ) -> None:
        """Missing session_id must return HookResult(action='continue') with no graph mutations."""
        handler = ContentBlockHandler(services)
        result = await handler(
            "content_block:start",
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "block_index": 0,
                # session_id intentionally omitted
            },
        )
        assert result.action == "continue", (
            f"Missing session_id must return action='continue'. Got: {result.action!r}"
        )
        assert len(services.graph._nodes) == 0, (
            f"No graph mutations must occur when session_id is missing. "
            f"Got {len(services.graph._nodes)} nodes."
        )
        assert len(services.graph._edges) == 0, (
            f"No graph mutations must occur when session_id is missing. "
            f"Got {len(services.graph._edges)} edges."
        )


# ---------------------------------------------------------------------------
# 7. TestContentBlockSourcedFrom
# ---------------------------------------------------------------------------


class TestContentBlockSourcedFrom:
    """SOURCED_FROM bridge edges from ContentBlock to data_layer_1 event nodes."""

    async def test_content_block_start_creates_sourced_from_edge(
        self, services: HookStateService
    ) -> None:
        """content_block:start must create SOURCED_FROM edge from block node to data_layer_1 event node."""
        services.data_layer_2.active_iteration_id = "s1::iteration::1"
        handler = ContentBlockHandler(services)
        timestamp = "2026-01-01T00:00:00Z"
        await handler(
            "content_block:start",
            {
                "session_id": "s1",
                "timestamp": timestamp,
                "block_index": 0,
            },
        )
        block_node_id = "s1::block::1::0"
        data_layer_1_node_id = make_node_id("s1", "content_block:start", timestamp)
        edge = await services.graph.get_edge(block_node_id, data_layer_1_node_id)
        assert edge is not None, (
            f"SOURCED_FROM edge from '{block_node_id}' to '{data_layer_1_node_id}' must exist"
        )
        assert edge.get("type") == "SOURCED_FROM", (
            f"Edge must have type='SOURCED_FROM'. Got: {edge.get('type')}"
        )

    async def test_content_block_end_creates_sourced_from_edge(
        self, services: HookStateService
    ) -> None:
        """content_block:end must create SOURCED_FROM edge from block node to data_layer_1 event node."""
        services.data_layer_2.active_iteration_id = "s1::iteration::1"
        handler = ContentBlockHandler(services)
        start_timestamp = "2026-01-01T00:00:00Z"
        end_timestamp = "2026-01-01T00:01:00Z"
        # Must call content_block:start first
        await handler(
            "content_block:start",
            {
                "session_id": "s1",
                "timestamp": start_timestamp,
                "block_index": 0,
            },
        )
        await handler(
            "content_block:end",
            {
                "session_id": "s1",
                "timestamp": end_timestamp,
                "block_index": 0,
                "block": {"type": "text"},
            },
        )
        block_node_id = "s1::block::1::0"
        data_layer_1_node_id = make_node_id("s1", "content_block:end", end_timestamp)
        edge = await services.graph.get_edge(block_node_id, data_layer_1_node_id)
        assert edge is not None, (
            f"SOURCED_FROM edge from '{block_node_id}' to '{data_layer_1_node_id}' must exist"
        )
        assert edge.get("type") == "SOURCED_FROM", (
            f"Edge must have type='SOURCED_FROM'. Got: {edge.get('type')}"
        )
