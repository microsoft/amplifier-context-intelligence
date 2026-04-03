"""Tests for ToolCallHandler — direct tool_call_id node key, SST_EVENT label, no edges.

Verifies:
- handled_events == frozenset({'tool:pre', 'tool:post'}) — tool:error excluded
- ToolCall node ID is the tool_call_id directly ('tc-abc'), not a compound key
- No edges created by tool:pre or tool:post
- SST_EVENT label on all ToolCall nodes
- result_success / result_output / result_error captured from tool:post
- Guard: missing session_id or tool_call_id short-circuits without mutation
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
# 3. TestToolPreNoEdgesWithoutCursors
# ---------------------------------------------------------------------------


class TestToolPreNoEdgesWithoutCursors:
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
            "result": {"error": None},
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
            "result": {"output": "file.txt"},
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
            "result": {"error": "timeout"},
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


# ---------------------------------------------------------------------------
# 7. TestE08HasToolCallEdge
# ---------------------------------------------------------------------------


class TestE08HasToolCallEdge:
    """E08: Iteration -[:HAS_TOOL_CALL {sst_semantic: 'CONTAINS'}]-> ToolCall."""

    async def test_e08_edge_created_when_active_iteration_id_set(
        self, services: HookStateService
    ) -> None:
        """E08 edge must be created when active_iteration_id is set."""
        services.data_layer_2.active_iteration_id = "s1::iteration::1"
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await handler("tool:pre", data)

        edge = await services.graph.get_edge("s1::iteration::1", "tc-abc")
        assert edge is not None
        assert edge.get("sst_semantic") == "CONTAINS"

    async def test_e08_edge_type_is_has_tool_call(
        self, services: HookStateService
    ) -> None:
        """E08 edge must have type HAS_TOOL_CALL."""
        services.data_layer_2.active_iteration_id = "s1::iteration::1"
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await handler("tool:pre", data)

        edge = await services.graph.get_edge("s1::iteration::1", "tc-abc")
        assert edge is not None
        assert edge.get("type") == "HAS_TOOL_CALL"

    async def test_e08_no_edge_when_no_active_iteration(
        self, services: HookStateService
    ) -> None:
        """E08 edge must NOT be created when active_iteration_id is None (zero edges)."""
        # active_iteration_id is None by default
        assert services.data_layer_2.active_iteration_id is None
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
# 8. TestE09CausedEdge
# ---------------------------------------------------------------------------


class TestE09CausedEdge:
    """E09: ContentBlock -[:CAUSED {sst_semantic: 'LEADS_TO'}]-> ToolCall."""

    async def test_e09_edge_created_when_pending_match_exists(
        self, services: HookStateService
    ) -> None:
        """E09 edge must be created when pending_tool_block_ids has matching entry."""
        services.data_layer_2.pending_tool_block_ids["tc-abc"] = "s1::block::1::0"
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await handler("tool:pre", data)

        edge = await services.graph.get_edge("s1::block::1::0", "tc-abc")
        assert edge is not None
        assert edge.get("sst_semantic") == "LEADS_TO"

    async def test_e09_edge_type_is_caused(self, services: HookStateService) -> None:
        """E09 edge must have type CAUSED."""
        services.data_layer_2.pending_tool_block_ids["tc-abc"] = "s1::block::1::0"
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await handler("tool:pre", data)

        edge = await services.graph.get_edge("s1::block::1::0", "tc-abc")
        assert edge is not None
        assert edge.get("type") == "CAUSED"

    async def test_e09_pending_entry_consumed_after_edge_creation(
        self, services: HookStateService
    ) -> None:
        """E09 must consume (pop) the pending entry after creating the edge."""
        services.data_layer_2.pending_tool_block_ids["tc-abc"] = "s1::block::1::0"
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await handler("tool:pre", data)

        assert "tc-abc" not in services.data_layer_2.pending_tool_block_ids

    async def test_e09_no_edge_when_no_pending_match(
        self, services: HookStateService
    ) -> None:
        """E09 edge must NOT be created when no pending match exists."""
        # pending_tool_block_ids is empty by default
        assert len(services.data_layer_2.pending_tool_block_ids) == 0
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await handler("tool:pre", data)

        caused_edges = [
            edge
            for (src, dst), edge in services.graph._edges.items()
            if edge.get("type") == "CAUSED"
        ]
        assert len(caused_edges) == 0


# ---------------------------------------------------------------------------
# 9. TestE10ParallelExecutionEdge
# ---------------------------------------------------------------------------


class TestE10ParallelExecutionEdge:
    """E10: ToolCall -[:PARALLEL_EXECUTION {sst_semantic: 'NEAR'}]- ToolCall."""

    async def test_e10_edge_created_between_parallel_group_tools(
        self, services: HookStateService
    ) -> None:
        """E10 edge must be created between tools in same parallel_group_id."""
        handler = ToolCallHandler(services)

        data_a = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-a",
            "tool_name": "bash",
            "parallel_group_id": "pg-1",
        }
        data_b = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-b",
            "tool_name": "read_file",
            "parallel_group_id": "pg-1",
        }
        await handler("tool:pre", data_a)
        await handler("tool:pre", data_b)

        # Edge between A and B must exist (in either direction)
        edge_ab = await services.graph.get_edge("tc-a", "tc-b")
        edge_ba = await services.graph.get_edge("tc-b", "tc-a")
        edge = edge_ab if edge_ab is not None else edge_ba
        assert edge is not None
        assert edge.get("sst_semantic") == "NEAR"

    async def test_e10_three_tools_produce_three_edges(
        self, services: HookStateService
    ) -> None:
        """Three parallel tools must produce three PARALLEL_EXECUTION edges.

        A-B when B arrives; C-A and C-B when C arrives.
        """
        handler = ToolCallHandler(services)

        for tool_id, tool_name in [
            ("tc-a", "bash"),
            ("tc-b", "read_file"),
            ("tc-c", "write_file"),
        ]:
            await handler(
                "tool:pre",
                {
                    "session_id": "s1",
                    "timestamp": "2026-01-01T00:00:00Z",
                    "tool_call_id": tool_id,
                    "tool_name": tool_name,
                    "parallel_group_id": "pg-1",
                },
            )

        parallel_edges = [
            edge
            for (src, dst), edge in services.graph._edges.items()
            if edge.get("type") == "PARALLEL_EXECUTION"
        ]
        assert len(parallel_edges) == 3

    async def test_e10_no_edge_when_no_parallel_group_id(
        self, services: HookStateService
    ) -> None:
        """E10 edge must NOT be created when parallel_group_id is absent."""
        handler = ToolCallHandler(services)

        data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-abc",
            "tool_name": "bash",
        }
        await handler("tool:pre", data)

        assert len(services.graph._edges) == 0

    async def test_e10_different_groups_do_not_link(
        self, services: HookStateService
    ) -> None:
        """Tools in different parallel groups must NOT be linked to each other."""
        handler = ToolCallHandler(services)

        data_a = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-a",
            "tool_name": "bash",
            "parallel_group_id": "pg-1",
        }
        data_b = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "tool_call_id": "tc-b",
            "tool_name": "read_file",
            "parallel_group_id": "pg-2",
        }
        await handler("tool:pre", data_a)
        await handler("tool:pre", data_b)

        # No cross-group PARALLEL_EXECUTION edges
        edge_ab = await services.graph.get_edge("tc-a", "tc-b")
        edge_ba = await services.graph.get_edge("tc-b", "tc-a")
        assert edge_ab is None
        assert edge_ba is None


# ---------------------------------------------------------------------------
# 10. TestToolCallHandlerHasParallelGroups
# ---------------------------------------------------------------------------


class TestToolCallHandlerHasParallelGroups:
    """Handler must have _parallel_groups as dict instance variable."""

    def test_handler_has_parallel_groups_attribute(
        self, services: HookStateService
    ) -> None:
        """ToolCallHandler must have _parallel_groups as a dict instance variable."""
        handler = ToolCallHandler(services)
        assert hasattr(handler, "_parallel_groups")
        assert isinstance(handler._parallel_groups, dict)
