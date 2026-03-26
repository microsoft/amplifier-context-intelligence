"""StepHandler stub — full implementation pending task-03."""
from __future__ import annotations
from typing import Any


class StepHandler:
    """Stub for StepHandler."""

    handled_events: frozenset[str] = frozenset({
        "step:start",
        "step:end",
    })

    def __init__(self, services: Any) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> Any:
        pass
