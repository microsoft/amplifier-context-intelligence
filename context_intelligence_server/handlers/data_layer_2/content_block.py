"""ContentBlockHandler — content block assembly with E07 edge and tool_call block ID cache.

Assembles ContentBlock:SST_EVENT nodes from content_block:start / content_block:end
event pairs, and wires the semantic edge E07.

Edges created here:
  E07 — Iteration -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> ContentBlock
"""

from __future__ import annotations

import logging
from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService

logger = logging.getLogger(__name__)


class ContentBlockHandler:
    """Handles content_block:start / content_block:end events to assemble ContentBlock nodes.

    Claimed events: content_block:start, content_block:end.
    """

    handled_events: frozenset[str] = frozenset(
        {
            "content_block:start",
            "content_block:end",
        }
    )

    def __init__(self, services: HookStateService) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        """Dispatch to the appropriate sub-handler.

        Reconstructs block_node_id from data fields + cursor.
        Returns HookResult(action='continue') immediately when session_id is
        absent — no graph mutations are performed.
        """
        session_id: str | None = data.get("session_id")
        if not session_id:
            return HookResult(action="continue")

        block_index = data.get("block_index")
        iteration_id = self.services.data_layer_2.active_iteration_id
        iteration_n = iteration_id.split("::")[-1] if iteration_id else "0"
        block_node_id = f"{session_id}::block::{iteration_n}::{block_index}"

        if event == "content_block:start":
            await self._handle_start(session_id, block_node_id, block_index, data)
        elif event == "content_block:end":
            await self._handle_end(block_node_id, data)

        return HookResult(action="continue")

    # ------------------------------------------------------------------
    # Sub-handlers
    # ------------------------------------------------------------------

    async def _handle_start(
        self,
        session_id: str,
        block_node_id: str,
        block_index: Any,
        data: dict[str, Any],
    ) -> None:
        """Create ContentBlock node and conditionally create E07 edge.

        - Creates ContentBlock:SST_EVENT node with session_id, block_index, started_at
        - Conditionally creates E07: Iteration -[:HAS_PART {sst_semantic: 'CONTAINS'}]->
          ContentBlock when active_iteration_id is not None
        """
        timestamp: str = data.get("timestamp", "")

        await self.services.graph.upsert_node(
            block_node_id,
            {
                "labels": ["ContentBlock", "SST_EVENT"],
                "session_id": session_id,
                "block_index": block_index,
                "started_at": timestamp,
            },
        )

        # E07 (conditional): Iteration -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> ContentBlock
        active_iteration_id = self.services.data_layer_2.active_iteration_id
        if active_iteration_id is not None:
            await self.services.graph.upsert_edge(
                active_iteration_id,
                block_node_id,
                {
                    "type": "HAS_PART",
                    "sst_semantic": "CONTAINS",
                },
            )

    async def _handle_end(
        self,
        block_node_id: str,
        data: dict[str, Any],
    ) -> None:
        """Enrich ContentBlock with block_type and ended_at; cache block.id for tool_call blocks.

        - Extracts block dict from data.get('block') or {}
        - Upserts ContentBlock with block_type (block.type), ended_at
        - Caches block.id ONLY for tool_call type blocks when block.id is present
        """
        timestamp: str = data.get("timestamp", "")
        block: dict[str, Any] = data.get("block") or {}
        block_type: str | None = block.get("type")

        await self.services.graph.upsert_node(
            block_node_id,
            {
                "labels": ["ContentBlock", "SST_EVENT"],
                "block_type": block_type,
                "ended_at": timestamp,
            },
        )

        # Cache block.id for tool_call blocks only
        if block_type == "tool_call":
            block_id: str | None = block.get("id")
            if block_id:
                self.services.data_layer_2.pending_tool_block_ids[block_id] = (
                    block_node_id
                )
