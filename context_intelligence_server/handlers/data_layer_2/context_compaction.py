"""ContextCompactionHandler — ContextCompaction node creation with E12 edge.

Creates ContextCompaction:SST_EVENT nodes from context:pre_compact and
context:post_compact events and wires the semantic edge E12.

Edge created here:
  E12 — Session -[:HAS_COMPACTION {sst_semantic: 'CONTAINS'}]-> ContextCompaction

Occurrence-only recorder — zero real examples exist, no property extraction
beyond occurred_at.
"""

from __future__ import annotations

from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService


class ContextCompactionHandler:
    """Handles context:pre_compact and context:post_compact events.

    Claimed events: context:pre_compact, context:post_compact.

    Creates a ContextCompaction:SST_EVENT node and wires E12
    (Session -> ContextCompaction).
    Occurrence-only recorder — no property extraction beyond occurred_at.
    """

    handled_events: frozenset[str] = frozenset(
        {"context:pre_compact", "context:post_compact"}
    )

    def __init__(self, services: HookStateService) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        """Handle a context:pre_compact or context:post_compact event.

        Returns HookResult(action='continue') immediately when session_id is
        absent — no graph mutations are performed.
        """
        session_id: str | None = data.get("session_id")
        if not session_id:
            return HookResult(action="continue")

        timestamp: str = data.get("timestamp", "")

        # Compute the compound key for the ContextCompaction node
        compaction_node_id = f"{session_id}::compaction::{timestamp}"

        # Create ContextCompaction:SST_EVENT node
        await self.services.graph.upsert_node(
            compaction_node_id,
            {
                "labels": ["ContextCompaction", "SST_EVENT"],
                "session_id": session_id,
                "occurred_at": timestamp,
            },
        )

        # E12: Session -[:HAS_COMPACTION {sst_semantic: 'CONTAINS'}]-> ContextCompaction (always)
        await self.services.graph.upsert_edge(
            session_id,
            compaction_node_id,
            {
                "type": "HAS_COMPACTION",
                "sst_semantic": "CONTAINS",
            },
        )

        return HookResult(action="continue")
