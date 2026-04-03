"""CancellationHandler — Cancellation node creation with E11 edge.

Creates Cancellation:SST_EVENT nodes from cancel:completed events and wires
the semantic edge E11.

Edge created here:
  E11 — Session -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> Cancellation

cancel:requested is intentionally NOT claimed — zero real examples exist in
observed data, so it is left for the DefaultHandler.
"""

from __future__ import annotations

from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService


class CancellationHandler:
    """Handles cancel:completed events.

    Claimed events: cancel:completed.
    cancel:requested is intentionally NOT claimed — zero real examples exist.

    Creates a Cancellation:SST_EVENT node and wires E11 (Session -> Cancellation).
    Occurrence-only recorder — no cursor management.
    """

    handled_events: frozenset[str] = frozenset({"cancel:completed"})

    def __init__(self, services: HookStateService) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        """Handle a cancel:completed event.

        Returns HookResult(action='continue') immediately when session_id is
        absent — no graph mutations are performed.
        """
        session_id: str | None = data.get("session_id")
        if not session_id:
            return HookResult(action="continue")

        timestamp: str = data.get("timestamp", "")
        was_immediate = data.get("immediate")

        # Compute the compound key for the Cancellation node
        cancellation_node_id = f"{session_id}::cancellation::{timestamp}"

        # Create Cancellation:SST_EVENT node
        await self.services.graph.upsert_node(
            cancellation_node_id,
            {
                "labels": ["Cancellation", "SST_EVENT"],
                "session_id": session_id,
                "was_immediate": was_immediate,
                "occurred_at": timestamp,
            },
        )

        # E11: Session -[:HAS_PART {sst_semantic: 'CONTAINS'}]-> Cancellation (always)
        await self.services.graph.upsert_edge(
            session_id,
            cancellation_node_id,
            {
                "type": "HAS_PART",
                "sst_semantic": "CONTAINS",
            },
        )

        return HookResult(action="continue")
