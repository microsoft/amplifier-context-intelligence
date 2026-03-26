"""ToolExecutionHandler stub — full implementation pending task-03."""
from __future__ import annotations
from typing import Any


class ToolExecutionHandler:
    """Stub for ToolExecutionHandler."""

    handled_events: frozenset[str] = frozenset({
        "tool_call:start",
        "tool_call:end",
    })

    def __init__(self, services: Any) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> Any:
        pass
