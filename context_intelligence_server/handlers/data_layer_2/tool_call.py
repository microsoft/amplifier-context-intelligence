"""ToolCallHandler — correlates tool:pre/post/error events into ToolCall nodes.

Each tool invocation lifecycle produces up to three events:
  tool:pre   — tool invocation started
  tool:post  — tool invocation completed successfully
  tool:error — tool invocation failed

This handler creates a single ToolCall node per invocation (keyed by
session_id + tool_call_id) and attaches the individual Event nodes to it
via HAS_EVENT edges, giving the graph a lifecycle view of every tool call.
"""

from __future__ import annotations

import logging
from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id

logger = logging.getLogger(__name__)


class ToolCallHandler:
    """Enricher handler for tool call lifecycle events.

    Correlates tool:pre, tool:post, and tool:error events into a single
    ToolCall node in the graph.  The ToolCall node ID is deterministic
    (no timestamp component) so all three events reference the same node.
    """

    handled_events: frozenset[str] = frozenset({"tool:pre", "tool:post", "tool:error"})

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

        # Deterministic ToolCall node ID — no timestamp, so pre/post/error
        # all reference the same node.
        tc_node_id = f"{session_id}__tool_call__{tool_call_id}"

        # Event node ID matches what DefaultHandler creates for this event.
        event_node_id = make_node_id(session_id, event, timestamp, tool_call_id)

        if event == "tool:pre":
            await self._handle_pre(
                session_id, tc_node_id, event_node_id, timestamp, data
            )
        else:
            # tool:post or tool:error — close the lifecycle
            await self._handle_close(tc_node_id, event_node_id, timestamp)

        return HookResult(action="continue")

    async def _handle_pre(
        self,
        session_id: str,
        tc_node_id: str,
        event_node_id: str,
        timestamp: str,
        data: dict[str, Any],
    ) -> None:
        """Create ToolCall node and link it to the session and the pre-event."""
        node_data: dict[str, Any] = {
            "labels": ["ToolCall"],
            "tool_name": data.get("tool_name"),
            "tool_call_id": data.get("tool_call_id"),
            "session_id": session_id,
        }
        parallel_group_id = data.get("parallel_group_id")
        if parallel_group_id is not None:
            node_data["parallel_group_id"] = parallel_group_id

        await self.services.graph.upsert_node(tc_node_id, node_data)

        # Session → ToolCall
        await self.services.graph.upsert_edge(
            session_id,
            tc_node_id,
            {"type": "HAS_TOOL_CALL", "started_at": timestamp},
        )

        # ToolCall → pre Event
        await self.services.graph.upsert_edge(
            tc_node_id,
            event_node_id,
            {"type": "HAS_EVENT", "occurred_at": timestamp},
        )

    async def _handle_close(
        self,
        tc_node_id: str,
        event_node_id: str,
        timestamp: str,
    ) -> None:
        """Update ToolCall node with ended_at and link to the closing event."""
        await self.services.graph.upsert_node(
            tc_node_id,
            {"labels": ["ToolCall"], "ended_at": timestamp},
        )

        # ToolCall → post/error Event
        await self.services.graph.upsert_edge(
            tc_node_id,
            event_node_id,
            {"type": "HAS_EVENT", "occurred_at": timestamp},
        )
