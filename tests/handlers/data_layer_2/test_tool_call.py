"""Tests for ToolCallHandler — new schema with direct tool_call_id as node key.

TDD RED phase: tests assert the new schema where:
- handled_events == frozenset({'tool:pre', 'tool:post'}) — tool:error is removed
- ToolCall node ID is the tool_call_id directly (e.g. 'tc-abc'), not the old
  compound key 's1__tool_call__tc-abc'
- No edges are created by tool:pre or tool:post (no HAS_TOOL_CALL, no HAS_EVENT)
- SST_EVENT label is present on the ToolCall node
- tool_input property is stored from the event data
- started_at property is stored from the event timestamp
- result_success, result_output, result_error properties from tool:post data

All tests in classes 1–5 FAIL with the current implementation because:
- handled_events still includes tool:error
- node ID still uses the compound key
- edges are still created
- SST_EVENT label is missing
- result_* properties are missing

Guards in class 6 pass because the guard logic is unchanged.
"""

from __future__ import annotations

from context_intelligence_server.handlers.data_layer_2.tool_call import ToolCallHandler
from context_intelligence_server.services import HookStateService


# ---------------------------------------------------------------------------
# 1. TestToolCallHandlerHandledEvents
# ---------------------------------------------------------------------------


class TestToolCallHandlerHandledEvents:
    """handled_events == frozenset({'tool:pre', 'tool:post'}) — tool:error excluded."""

    def test_handled_events_is_exact_frozenset(self) -> None:
        """handled_events must be exactly frozenset({'tool:pre', 'tool:post'})."""
        assert ToolCallHandler.handled_events == frozenset({"tool:pre", "tool:post"})

    def test_tool_error_not_in_handled_events(self) -> None:
        """tool:error must NOT be in handled_events."""
        assert "tool:error" not in ToolCallHandler.handled_events


# ---------------------------------------------------------------------------
# 2. TestToolPreCreatesToolCallNode
# ---------------------------------------------------------------------------


class TestToolPreCreatesToolCallNode:
    """tool:pre creates a ToolCall node keyed by tool_call_id directly."""

    async def test_tool_pre_creates_tool_call_node_with_correct_id(
        self, services: HookStateService
    ) -> None:
        """tool:pre must create a ToolCall node at the tool_call_id directly ('tc-abc')."""
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await handler("tool:pre", data)

        node = await services.graph.get_node("tc-abc")
        assert node is not None

    async def test_old_compound_node_id_not_created(
        self, services: HookStateService
    ) -> None:
        """The old compound ID 's1__tool_call__tc-abc' must NOT be created."""
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await handler("tool:pre", data)

        node = await services.graph.get_node("s1__tool_call__tc-abc")
        assert node is None

    async def test_tool_call_node_has_tool_call_label(
        self, services: HookStateService
    ) -> None:
        """ToolCall node must include 'ToolCall' in its labels."""
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await handler("tool:pre", data)

        node = await services.graph.get_node("tc-abc")
        assert node is not None
        assert "ToolCall" in node["labels"]

    async def test_tool_call_node_has_sst_event_label(
        self, services: HookStateService
    ) -> None:
        """ToolCall node must include 'SST_EVENT' in its labels."""
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await handler("tool:pre", data)

        node = await services.graph.get_node("tc-abc")
        assert node is not None
        assert "SST_EVENT" in node["labels"]

    async def test_tool_call_node_has_tool_name_property(
        self, services: HookStateService
    ) -> None:
        """ToolCall node must have tool_name property."""
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await handler("tool:pre", data)

        node = await services.graph.get_node("tc-abc")
        assert node is not None
        assert node["tool_name"] == "bash"

    async def test_tool_call_node_has_tool_call_id_property(
        self, services: HookStateService
    ) -> None:
        """ToolCall node must have tool_call_id property."""
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await handler("tool:pre", data)

        node = await services.graph.get_node("tc-abc")
        assert node is not None
        assert node["tool_call_id"] == "tc-abc"

    async def test_tool_call_node_has_session_id_property(
        self, services: HookStateService
    ) -> None:
        """ToolCall node must have session_id property."""
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await handler("tool:pre", data)

        node = await services.graph.get_node("tc-abc")
        assert node is not None
        assert node["session_id"] == "s1"

    async def test_tool_call_node_has_tool_input_property(
        self, services: HookStateService
    ) -> None:
        """ToolCall node must have tool_input property with {'command': 'ls -la'}."""
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
            "tool_input": {"command": "ls -la"},
        }
        await handler("tool:pre", data)

        node = await services.graph.get_node("tc-abc")
        assert node is not None
        assert node["tool_input"] == {"command": "ls -la"}

    async def test_tool_call_node_has_started_at_property(
        self, services: HookStateService
    ) -> None:
        """ToolCall node must have started_at property matching the event timestamp."""
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await handler("tool:pre", data)

        node = await services.graph.get_node("tc-abc")
        assert node is not None
        assert node["started_at"] == "2026-01-01T00:00:00Z"

    async def test_tool_call_node_has_parallel_group_id_when_provided(
        self, services: HookStateService
    ) -> None:
        """ToolCall node must have parallel_group_id property when provided."""
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
            "parallel_group_id": "pg-1",
        }
        await handler("tool:pre", data)

        node = await services.graph.get_node("tc-abc")
        assert node is not None
        assert node["parallel_group_id"] == "pg-1"


# ---------------------------------------------------------------------------
# 3. TestToolPreNoViolationEdges
# ---------------------------------------------------------------------------


class TestToolPreNoViolationEdges:
    """tool:pre must not create any edges — no HAS_TOOL_CALL, no HAS_EVENT."""

    async def test_no_session_to_tool_call_edge(
        self, services: HookStateService
    ) -> None:
        """No edge from session to ToolCall node ('s1' → 'tc-abc') must exist."""
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await handler("tool:pre", data)

        edge = await services.graph.get_edge("s1", "tc-abc")
        assert edge is None

    async def test_no_has_event_edges(self, services: HookStateService) -> None:
        """No edges at all must be created by tool:pre."""
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await handler("tool:pre", data)

        assert len(services.graph._edges) == 0


# ---------------------------------------------------------------------------
# 4. TestToolPostEnrichesToolCall
# ---------------------------------------------------------------------------


class TestToolPostEnrichesToolCall:
    """tool:post enriches existing ToolCall with result properties; no edges created."""

    async def test_tool_post_sets_ended_at(self, services: HookStateService) -> None:
        """tool:post must set ended_at on the ToolCall node."""
        handler = ToolCallHandler(services)

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

        await handler("tool:pre", pre_data)
        await handler("tool:post", post_data)

        node = await services.graph.get_node("tc-abc")
        assert node is not None
        assert node["ended_at"] == "2026-01-01T00:01:00Z"

    async def test_tool_post_sets_result_success(
        self, services: HookStateService
    ) -> None:
        """tool:post must set result_success=True on the ToolCall node."""
        handler = ToolCallHandler(services)

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
            "result_success": True,
        }

        await handler("tool:pre", pre_data)
        await handler("tool:post", post_data)

        node = await services.graph.get_node("tc-abc")
        assert node is not None
        assert node["result_success"] is True

    async def test_tool_post_sets_result_output(
        self, services: HookStateService
    ) -> None:
        """tool:post must set result_output='file.txt' on the ToolCall node."""
        handler = ToolCallHandler(services)

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
            "result_output": "file.txt",
        }

        await handler("tool:pre", pre_data)
        await handler("tool:post", post_data)

        node = await services.graph.get_node("tc-abc")
        assert node is not None
        assert node["result_output"] == "file.txt"

    async def test_tool_post_sets_result_error(
        self, services: HookStateService
    ) -> None:
        """tool:post must set result_error='timeout' on the ToolCall node."""
        handler = ToolCallHandler(services)

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
            "result_error": "timeout",
        }

        await handler("tool:pre", pre_data)
        await handler("tool:post", post_data)

        node = await services.graph.get_node("tc-abc")
        assert node is not None
        assert node["result_error"] == "timeout"

    async def test_tool_post_no_has_event_edge(
        self, services: HookStateService
    ) -> None:
        """Neither tool:pre nor tool:post must create any edges."""
        handler = ToolCallHandler(services)

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

        await handler("tool:pre", pre_data)
        await handler("tool:post", post_data)

        assert len(services.graph._edges) == 0


# ---------------------------------------------------------------------------
# 5. TestParallelToolCalls
# ---------------------------------------------------------------------------


class TestParallelToolCalls:
    """Parallel calls with same timestamp but different tool_call_ids → distinct nodes."""

    async def test_parallel_calls_produce_distinct_tool_call_nodes(
        self, services: HookStateService
    ) -> None:
        """Same timestamp, different tool_call_ids → distinct ToolCall nodes at 'tc-001' and 'tc-002'."""
        handler = ToolCallHandler(services)

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

        await handler("tool:pre", data1)
        await handler("tool:pre", data2)

        node1 = await services.graph.get_node("tc-001")
        node2 = await services.graph.get_node("tc-002")
        assert node1 is not None
        assert node2 is not None

    async def test_parallel_nodes_have_correct_tool_names(
        self, services: HookStateService
    ) -> None:
        """Each distinct ToolCall node must carry its own tool_name."""
        handler = ToolCallHandler(services)

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

        await handler("tool:pre", data1)
        await handler("tool:pre", data2)

        node1 = await services.graph.get_node("tc-001")
        node2 = await services.graph.get_node("tc-002")
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
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "tool_call_id": "tc-abc",
                "tool_name": "bash",
            },
        )
        assert result.action == "continue"

    async def test_missing_tool_call_id_returns_continue(
        self, services: HookStateService
    ) -> None:
        """Missing tool_call_id must return HookResult(action='continue')."""
        handler = ToolCallHandler(services)
        result = await handler(
            "tool:pre",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "tool_name": "bash",
            },
        )
        assert result.action == "continue"
