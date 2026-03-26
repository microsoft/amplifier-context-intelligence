"""RecipeHandler stub — full implementation pending task-03."""
from __future__ import annotations
from typing import Any


class RecipeHandler:
    """Stub for RecipeHandler."""

    handled_events: frozenset[str] = frozenset({
        "recipe:start",
        "recipe:step",
        "recipe:end",
    })

    def __init__(self, services: Any) -> None:
        self.services = services

    async def __call__(self, event: str, data: dict[str, Any]) -> Any:
        pass
