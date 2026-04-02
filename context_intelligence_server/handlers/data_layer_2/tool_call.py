"""ToolCallHandler — correlates tool:pre/post events into ToolCall nodes.

Each tool invocation lifecycle produces up to two events:
  tool:pre   — tool invocation started
  tool:post  — tool invocation completed successfully

This handler creates a single ToolCall node per invocation (keyed by
tool_call_id directly) with the SST_EVENT label, and enriches it with
result properties on tool:post. No edges are created by this handler.
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
    compound key). No edges are created.
    """

    handled_events: frozenset[str] = frozenset({"tool:pre", "tool:post"})

    def __init__(self, services: HookStateService) -> None:
        self.services = services

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
        """Create ToolCall node keyed by tool_call_id directly.

        No edges are created — edge creation is a data_layer_2 violation.
        """
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

    async def _handle_post(
        self,
        tool_call_id: str,
        timestamp: str,
        data: dict[str, Any],
    ) -> None:
        """Enrich existing ToolCall node with completion properties.

        No edges are created — edge creation is a data_layer_2 violation.
        """
        result: dict[str, Any] = data.get("result", {})
        node_data: dict[str, Any] = {
            "labels": ["ToolCall", "SST_EVENT"],
            "ended_at": timestamp,
            "result_success": result.get("error") is None,
            "result_output": result.get("output"),
            "result_error": result.get("error"),
        }
        await self.services.graph.upsert_node(tool_call_id, node_data)
