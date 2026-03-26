"""Tests for ToolCallHandler — tool lifecycle enricher.

TDD RED phase: tests 2–5 fail because the stub ToolCallHandler returns
'continue' without creating ToolCall nodes or edges. Tests 1 and 6 pass
against the stub (handled_events already correct; guards return continue).
"""

from __future__ import annotations

from context_intelligence_server.handlers.default import DefaultHandler
from context_intelligence_server.handlers.tool_call import ToolCallHandler
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id


# ---------------------------------------------------------------------------
# 1. TestToolCallHandlerHandledEvents
# ---------------------------------------------------------------------------


class TestToolCallHandlerHandledEvents:
    """handled_events == frozenset({'tool:pre', 'tool:post', 'tool:error'})."""

    def test_handled_events_is_exact_frozenset(self) -> None:
        """handled_events must be exactly frozenset({'tool:pre', 'tool:post', 'tool:error'})."""
        assert ToolCallHandler.handled_events == frozenset(
            {"tool:pre", "tool:post", "tool:error"}
        )


# ---------------------------------------------------------------------------
# 2. TestToolPreCreatesToolCallNode
# ---------------------------------------------------------------------------


class TestToolPreCreatesToolCallNode:
    """tool:pre creates a ToolCall node with correct ID, labels, and properties."""

    async def test_tool_pre_creates_tool_call_node(
        self, services: HookStateService
    ) -> None:
        """tool:pre must create a ToolCall node at '{session_id}__tool_call__{tool_call_id}'."""
        default_handler = DefaultHandler(services)
        tool_call_handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
            "parallel_group_id": "pg-1",
        }
        # DefaultHandler must run first to create the Event node
        await default_handler("tool:pre", data)
        await tool_call_handler("tool:pre", data)

        node = await services.graph.get_node("s1__tool_call__tc-abc")
        assert node is not None

    async def test_tool_call_node_has_tool_call_label(
        self, services: HookStateService
    ) -> None:
        """ToolCall node must include 'ToolCall' in its labels."""
        default_handler = DefaultHandler(services)
        tool_call_handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await default_handler("tool:pre", data)
        await tool_call_handler("tool:pre", data)

        node = await services.graph.get_node("s1__tool_call__tc-abc")
        assert node is not None
        assert "ToolCall" in node["labels"]

    async def test_tool_call_node_has_tool_name_property(
        self, services: HookStateService
    ) -> None:
        """ToolCall node must have tool_name property."""
        default_handler = DefaultHandler(services)
        tool_call_handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await default_handler("tool:pre", data)
        await tool_call_handler("tool:pre", data)

        node = await services.graph.get_node("s1__tool_call__tc-abc")
        assert node is not None
        assert node["tool_name"] == "bash"

    async def test_tool_call_node_has_tool_call_id_property(
        self, services: HookStateService
    ) -> None:
        """ToolCall node must have tool_call_id property."""
        default_handler = DefaultHandler(services)
        tool_call_handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await default_handler("tool:pre", data)
        await tool_call_handler("tool:pre", data)

        node = await services.graph.get_node("s1__tool_call__tc-abc")
        assert node is not None
        assert node["tool_call_id"] == "tc-abc"

    async def test_tool_call_node_has_parallel_group_id_property(
        self, services: HookStateService
    ) -> None:
        """ToolCall node must have parallel_group_id property when provided."""
        default_handler = DefaultHandler(services)
        tool_call_handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
            "parallel_group_id": "pg-1",
        }
        await default_handler("tool:pre", data)
        await tool_call_handler("tool:pre", data)

        node = await services.graph.get_node("s1__tool_call__tc-abc")
        assert node is not None
        assert node["parallel_group_id"] == "pg-1"

    async def test_tool_call_node_has_session_id_property(
        self, services: HookStateService
    ) -> None:
        """ToolCall node must have session_id property."""
        default_handler = DefaultHandler(services)
        tool_call_handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await default_handler("tool:pre", data)
        await tool_call_handler("tool:pre", data)

        node = await services.graph.get_node("s1__tool_call__tc-abc")
        assert node is not None
        assert node["session_id"] == "s1"

    async def test_tool_pre_creates_has_tool_call_edge_from_session(
        self, services: HookStateService
    ) -> None:
        """HAS_TOOL_CALL edge must be created from session -> ToolCall with started_at."""
        default_handler = DefaultHandler(services)
        tool_call_handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await default_handler("tool:pre", data)
        await tool_call_handler("tool:pre", data)

        edge = await services.graph.get_edge("s1", "s1__tool_call__tc-abc")
        assert edge is not None
        assert edge["started_at"] == "2026-01-01T00:00:00Z"

    async def test_tool_pre_creates_has_event_edge_from_tool_call_to_event(
        self, services: HookStateService
    ) -> None:
        """HAS_EVENT edge must be created from ToolCall -> pre Event node."""
        default_handler = DefaultHandler(services)
        tool_call_handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        # DefaultHandler runs first to create the Event node
        await default_handler("tool:pre", data)
        await tool_call_handler("tool:pre", data)

        tool_call_node_id = "s1__tool_call__tc-abc"
        event_node_id = make_node_id("s1", "tool:pre", "2026-01-01T00:00:00Z", "tc-abc")
        edge = await services.graph.get_edge(tool_call_node_id, event_node_id)
        assert edge is not None


# ---------------------------------------------------------------------------
# 3. TestToolPostEnrichesToolCall
# ---------------------------------------------------------------------------


class TestToolPostEnrichesToolCall:
    """tool:post enriches existing ToolCall: sets ended_at, creates HAS_EVENT edge."""

    async def test_tool_post_sets_ended_at(
        self, services: HookStateService
    ) -> None:
        """tool:post must set ended_at on the ToolCall node."""
        default_handler = DefaultHandler(services)
        tool_call_handler = ToolCallHandler(services)

        pre_data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        post_data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:01:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }

        # Full lifecycle: default + handler for pre, then default + handler for post
        await default_handler("tool:pre", pre_data)
        await tool_call_handler("tool:pre", pre_data)
        await default_handler("tool:post", post_data)
        await tool_call_handler("tool:post", post_data)

        node = await services.graph.get_node("s1__tool_call__tc-abc")
        assert node is not None
        assert node["ended_at"] == "2026-01-01T00:01:00Z"

    async def test_tool_post_creates_has_event_edge_for_post_event(
        self, services: HookStateService
    ) -> None:
        """tool:post must create HAS_EVENT edge from ToolCall -> post Event node."""
        default_handler = DefaultHandler(services)
        tool_call_handler = ToolCallHandler(services)

        pre_data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        post_data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:01:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }

        await default_handler("tool:pre", pre_data)
        await tool_call_handler("tool:pre", pre_data)
        await default_handler("tool:post", post_data)
        await tool_call_handler("tool:post", post_data)

        tool_call_node_id = "s1__tool_call__tc-abc"
        post_event_id = make_node_id("s1", "tool:post", "2026-01-01T00:01:00Z", "tc-abc")
        edge = await services.graph.get_edge(tool_call_node_id, post_event_id)
        assert edge is not None


# ---------------------------------------------------------------------------
# 4. TestToolErrorEnrichesToolCall
# ---------------------------------------------------------------------------


class TestToolErrorEnrichesToolCall:
    """tool:error enriches existing ToolCall: sets ended_at."""

    async def test_tool_error_sets_ended_at(
        self, services: HookStateService
    ) -> None:
        """tool:error must set ended_at on the ToolCall node."""
        default_handler = DefaultHandler(services)
        tool_call_handler = ToolCallHandler(services)

        pre_data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        error_data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:01:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
            "error": "timeout",
        }

        await default_handler("tool:pre", pre_data)
        await tool_call_handler("tool:pre", pre_data)
        await default_handler("tool:error", error_data)
        await tool_call_handler("tool:error", error_data)

        node = await services.graph.get_node("s1__tool_call__tc-abc")
        assert node is not None
        assert node["ended_at"] == "2026-01-01T00:01:00Z"


# ---------------------------------------------------------------------------
# 5. TestParallelToolCalls
# ---------------------------------------------------------------------------


class TestParallelToolCalls:
    """Parallel calls with same timestamp but different tool_call_ids → distinct nodes."""

    async def test_parallel_calls_produce_distinct_tool_call_nodes(
        self, services: HookStateService
    ) -> None:
        """Same timestamp, different tool_call_ids → distinct ToolCall nodes."""
        default_handler = DefaultHandler(services)
        tool_call_handler = ToolCallHandler(services)

        data1 = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-001",
            "tool_name": "bash",
        }
        data2 = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-002",
            "tool_name": "read_file",
        }

        await default_handler("tool:pre", data1)
        await tool_call_handler("tool:pre", data1)
        await default_handler("tool:pre", data2)
        await tool_call_handler("tool:pre", data2)

        node1 = await services.graph.get_node("s1__tool_call__tc-001")
        node2 = await services.graph.get_node("s1__tool_call__tc-002")
        assert node1 is not None
        assert node2 is not None

    async def test_parallel_nodes_have_correct_tool_names(
        self, services: HookStateService
    ) -> None:
        """Each distinct ToolCall node must carry its own tool_name."""
        default_handler = DefaultHandler(services)
        tool_call_handler = ToolCallHandler(services)

        data1 = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-001",
            "tool_name": "bash",
        }
        data2 = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-002",
            "tool_name": "read_file",
        }

        await default_handler("tool:pre", data1)
        await tool_call_handler("tool:pre", data1)
        await default_handler("tool:pre", data2)
        await tool_call_handler("tool:pre", data2)

        node1 = await services.graph.get_node("s1__tool_call__tc-001")
        node2 = await services.graph.get_node("s1__tool_call__tc-002")
        assert node1 is not None
        assert node2 is not None
        assert node1["tool_name"] == "bash"
        assert node2["tool_name"] == "read_file"


# ---------------------------------------------------------------------------
# 6. TestToolCallHandlerGuards
# ---------------------------------------------------------------------------


class TestToolCallHandlerGuards:
    """Missing session_id or tool_call_id must short-circuit without mutations."""

    async def test_missing_session_id_returns_continue(
        self, services: HookStateService
    ) -> None:
        """Missing session_id must return HookResult(action='continue')."""
        handler = ToolCallHandler(services)
        result = await handler(
            "tool:pre",
            {"timestamp": "2026-01-01T00:00:00Z", "tool_call_id": "tc-abc", "tool_name": "bash"},
        )
        assert result.action == "continue"

    async def test_missing_tool_call_id_returns_continue(
        self, services: HookStateService
    ) -> None:
        """Missing tool_call_id must return HookResult(action='continue')."""
        handler = ToolCallHandler(services)
        result = await handler(
            "tool:pre",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z", "tool_name": "bash"},
        )
        assert result.action == "continue"
