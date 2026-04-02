"""UniversalLifter — lifts session_id and parent_id from every event."""

from __future__ import annotations

from typing import Any

from context_intelligence_server.handlers.data_layer_1.field_lifters.base import FieldLifter


class UniversalLifter(FieldLifter):
    """Lifts session_id and parent_id from every event's data dict.

    Fires on ALL events ("*" pattern). Extracts only the two fields
    needed for session hierarchy navigation that are present across
    all event types.

    tool_call_id and parallel_group_id are intentionally NOT lifted here —
    they are tool-correlation fields owned by ToolLifter (tool:*) and
    DelegateLifter (delegate:*).
    """

    event_pattern = "*"

    _NAVIGATION_KEYS: tuple[str, ...] = ("session_id", "parent_id")

    def extract(self, event: str, data: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002
        """Lift session_id and parent_id for session hierarchy navigation."""
        result: dict[str, Any] = {}
        for key in self._NAVIGATION_KEYS:
            value = data.get(key)
            if value is not None:
                result[key] = value
        return result
