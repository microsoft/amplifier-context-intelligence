"""StepHandler — owns :Step:AssistantStep lifecycle events.

Stub implementation: full port from bundle task-12 will replace this.
"""

from __future__ import annotations

from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService


class StepHandler:
    """Handles LLM step lifecycle events, including wildcard content_block:* patterns."""

    handled_events: frozenset[str] = frozenset(
        {
            "provider:request",
            "llm:request",
            "llm:response",
            "content_block:*",
        }
    )

    def __init__(self, services: HookStateService) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        return HookResult(action="continue")
