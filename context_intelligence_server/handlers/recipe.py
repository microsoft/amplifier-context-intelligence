"""RecipeHandler — recipe orchestration events.

Stub implementation: full port from bundle task-13 will replace this.
"""

from __future__ import annotations

from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService


class RecipeHandler:
    """Handles recipe lifecycle and loop events."""

    handled_events: frozenset[str] = frozenset(
        {
            "recipe:start",
            "recipe:step",
            "recipe:complete",
            "recipe:approval",
            "recipe:loop_iteration",
            "recipe:loop_complete",
        }
    )

    def __init__(self, services: HookStateService) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        return HookResult(action="continue")
