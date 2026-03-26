"""ToolCallHandler — Phase 2 placeholder for tool event lifecycle.

This module provides a minimal stub so that pipeline.setup_handlers can import
ToolCallHandler without error.  The real implementation (graph writes for
tool:pre, tool:post, tool:error events) is deferred to Phase 2.
"""

from __future__ import annotations

from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService


class ToolCallHandler:
    """Phase 2 placeholder handler for tool call lifecycle events.

    Claimed events: tool:pre, tool:post, tool:error.
    Returns HookResult(action='continue') for all events — no graph mutations
    until Phase 2 implements the full ToolCall node lifecycle.
    """

    handled_events: frozenset[str] = frozenset({"tool:pre", "tool:post", "tool:error"})

    def __init__(self, services: HookStateService) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        """Handle a tool lifecycle event — no-op stub returning continue."""
        return HookResult(action="continue")
