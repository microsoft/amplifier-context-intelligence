"""DefaultHandler — catches all unclaimed, non-excluded events.

Stub implementation: full port from bundle task-13 will replace this.
"""

from __future__ import annotations

from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService


class DefaultHandler:
    """Creates :Event:{DerivedLabel} nodes from unclaimed events."""

    handled_events: frozenset[str] = frozenset()

    def __init__(self, services: HookStateService) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        return HookResult(action="continue")
