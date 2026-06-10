"""RecipeLifter — lifts recipe orchestration fields from recipe:* events."""

from __future__ import annotations

from typing import Any

from context_intelligence_server.handlers.data_layer_1.field_lifters.base import (
    FieldLifter,
)


class RecipeLifter(FieldLifter):
    """Lifts recipe_name, current_step, description, status, step_id, total_steps, parent_session_id."""

    event_pattern = "recipe:*"

    def extract(self, event: str, data: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002
        result: dict[str, Any] = {}
        for key in (
            "recipe_name",
            "current_step",
            "description",
            "status",
            "step_id",
            "total_steps",
            "parent_session_id",  # NEW — lifted from recipe:start for sub-recipe identification
        ):
            value = data.get(key)
            if value is not None:
                result[key] = value
        return result
