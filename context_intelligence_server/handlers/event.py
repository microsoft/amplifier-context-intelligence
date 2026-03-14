"""SystemEventHandler — owns known system events (compaction, cancellation)."""

from __future__ import annotations

from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService


class SystemEventHandler:
    """Labels preserve full event scope: :Event:ContextCompaction, :Event:CancelRequested, etc.

    This handler is a no-op sink: it claims these events to prevent DefaultHandler from
    creating spurious Event nodes for them, but does not persist anything to the graph.
    """

    handled_events: frozenset[str] = frozenset(
        {
            "context:compaction",
            "cancel:requested",
            "cancel:completed",
        }
    )

    def __init__(self, services: HookStateService) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        return HookResult(action="continue")
