"""ContentBlockHandler — content block assembly with E07 edge and tool_call block ID cache.

Assembles ContentBlock:SST_EVENT nodes from content_block:start / content_block:end
event pairs, and wires the semantic edge E07.

Edges created here:
  E07         — Iteration -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> ContentBlock
  SOURCED_FROM — ContentBlock ->[:SOURCED_FROM]-> data_layer_1 content_block:start event
  SOURCED_FROM — ContentBlock ->[:SOURCED_FROM]-> data_layer_1 content_block:end event
"""

from __future__ import annotations

import logging
from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id

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
        # Key the block off the FULL active_iteration_id so a block inherits the
        # iteration's run scope (including the run tiebreaker). Using only the
        # trailing iteration number would let two runs that share an iteration
        # number collide on the same block_node_id and MERGE-overwrite.
        # Resolve the iteration id the same seq-aware way the Iteration handler
        # does, so after a worker rebuild a block inherits the run's real
        # iteration scope rather than the run-less fallback.
        iteration_id = self.services.data_layer_2.resolve_active_iteration_id(
            session_id
        )
        iteration_key = iteration_id if iteration_id else f"{session_id}::iteration::0"
        block_node_id = f"{iteration_key}::block::{block_index}"

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

        # SOURCED_FROM bridge: ContentBlock -> data_layer_1 content_block:start event
        data_layer_1_node_id = make_node_id(
            session_id, "content_block:start", timestamp
        )
        await self.services.graph.upsert_edge(
            block_node_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
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

        # SOURCED_FROM bridge: ContentBlock -> data_layer_1 content_block:end event
        session_id: str = data.get("session_id", "")
        data_layer_1_node_id = make_node_id(session_id, "content_block:end", timestamp)
        await self.services.graph.upsert_edge(
            block_node_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )
