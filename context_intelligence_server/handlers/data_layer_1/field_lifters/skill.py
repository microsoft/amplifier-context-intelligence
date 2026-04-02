"""SkillLifter — lifts skill_directory and skill_name from skill:* events."""

from __future__ import annotations

from typing import Any

from context_intelligence_server.handlers.field_lifters.base import FieldLifter


class SkillLifter(FieldLifter):
    """Lifts skill_directory and skill_name from skill:* events."""

    event_pattern = "skill:*"

    def extract(self, event: str, data: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002
        result: dict[str, Any] = {}
        for key in ("skill_directory", "skill_name"):
            value = data.get(key)
            if value is not None:
                result[key] = value
        return result
