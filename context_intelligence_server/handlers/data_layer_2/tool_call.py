"""ToolCallHandler — correlates tool:pre/post events into ToolCall nodes.

Each tool invocation lifecycle produces up to two events:
  tool:pre   — tool invocation started
  tool:post  — tool invocation completed successfully

This handler creates a single ToolCall node per invocation (keyed by
tool_call_id directly) with the SST_EVENT label, and enriches it with
result properties on tool:post. Phase B cursor edges (E08, E09, E10) are
created by _handle_pre when the corresponding cursors are set.
"""

from __future__ import annotations

import logging
from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService

logger = logging.getLogger(__name__)


class ToolCallHandler:
    """Enricher handler for tool call lifecycle events.

    Correlates tool:pre and tool:post events into a single ToolCall node in
    the graph. The ToolCall node ID is the tool_call_id directly (not a
    compound key). Phase B cursor edges (E08, E09, E10) are created by
    _handle_pre when the corresponding cursors are set.
    """

    handled_events: frozenset[str] = frozenset({"tool:pre", "tool:post"})

    def __init__(self, services: HookStateService) -> None:
        self.services = services
        self._parallel_groups: dict[str, list[str]] = {}

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        """Handle a tool lifecycle event.

        Extracts session_id and tool_call_id; returns continue without
        mutations if either is missing.
        """
        session_id: str | None = data.get("session_id")
        tool_call_id: str | None = data.get("tool_call_id")

        if not session_id or not tool_call_id:
            return HookResult(action="continue")

        timestamp: str = data.get("timestamp", "")

        if event == "tool:pre":
            await self._handle_pre(session_id, tool_call_id, timestamp, data)
        else:
            # tool:post — enrich the existing node
            await self._handle_post(tool_call_id, timestamp, data)

        return HookResult(action="continue")

    async def _handle_pre(
        self,
        session_id: str,
        tool_call_id: str,
        timestamp: str,
        data: dict[str, Any],
    ) -> None:
        """Create ToolCall node and cursor-dependent edges (E08, E09, E10)."""
        node_data: dict[str, Any] = {
            "labels": ["ToolCall", "SST_EVENT"],
            "tool_name": data.get("tool_name"),
            "tool_call_id": tool_call_id,
            "session_id": session_id,
            "tool_input": data.get("tool_input"),
            "started_at": timestamp,
        }
        parallel_group_id = data.get("parallel_group_id")
        if parallel_group_id is not None:
            node_data["parallel_group_id"] = parallel_group_id

        await self.services.graph.upsert_node(tool_call_id, node_data)

        # E08: Iteration -[:HAS_TOOL_CALL {sst_semantic: 'CONTAINS'}]-> ToolCall
        iteration_id = self.services.data_layer_2.active_iteration_id
        if iteration_id is not None:
            await self.services.graph.upsert_edge(
                iteration_id,
                tool_call_id,
                {"type": "HAS_TOOL_CALL", "sst_semantic": "CONTAINS"},
            )

        # E09: ContentBlock -[:CAUSED {sst_semantic: 'LEADS_TO'}]-> ToolCall
        block_node_id = self.services.data_layer_2.pending_tool_block_ids.pop(
            tool_call_id, None
        )
        if block_node_id is not None:
            await self.services.graph.upsert_edge(
                block_node_id,
                tool_call_id,
                {"type": "CAUSED", "sst_semantic": "LEADS_TO"},
            )

        # E10: ToolCall -[:PARALLEL_EXECUTION {sst_semantic: 'NEAR'}]- ToolCall
        if parallel_group_id is not None:
            group = self._parallel_groups.setdefault(parallel_group_id, [])
            for prior_id in group:
                await self.services.graph.upsert_edge(
                    tool_call_id,
                    prior_id,
                    {"type": "PARALLEL_EXECUTION", "sst_semantic": "NEAR"},
                )
            group.append(tool_call_id)

    async def _handle_post(
        self,
        tool_call_id: str,
        timestamp: str,
        data: dict[str, Any],
    ) -> None:
        """Enrich existing ToolCall node with completion properties."""
        result: dict[str, Any] = data.get("result", {})
        node_data: dict[str, Any] = {
            "labels": ["ToolCall", "SST_EVENT"],
            "ended_at": timestamp,
            "result_success": result.get("error") is None,
            "result_output": result.get("output"),
            "result_error": result.get("error"),
        }
        await self.services.graph.upsert_node(tool_call_id, node_data)
