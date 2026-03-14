"""ToolExecutionHandler — owns :ToolExecution lifecycle events.

Stub implementation: full port from bundle task-12 will replace this.
"""

from __future__ import annotations

from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService


class ToolExecutionHandler:
    """Handles tool execution and delegation events."""

    handled_events: frozenset[str] = frozenset(
        {
            "tool:pre",
            "tool:post",
            "tool:error",
            "delegate:agent_spawned",
            "delegate:agent_completed",
            "delegate:context_inherited",
            "delegate:session_resumed",
        }
    )

    def __init__(self, services: HookStateService) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        return HookResult(action="continue")
