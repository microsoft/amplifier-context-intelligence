"""UniversalLifter — extracts navigation fields from every event."""

from __future__ import annotations

from typing import Any

from context_intelligence_server.handlers.field_lifters.base import FieldLifter

_NAVIGATION_KEYS: tuple[str, ...] = (
    "session_id",
    "parent_id",
    "tool_call_id",
    "parallel_group_id",
)


class UniversalLifter(FieldLifter):
    """Lifts the 4 universal navigation fields from every event.

    Fires on all events (event_pattern = "*") and extracts:
    - session_id: current session identifier
    - parent_id: parent session identifier for hierarchy traversal
    - tool_call_id: tool call identifier for correlating pre/post/error events
    - parallel_group_id: group identifier for parallel tool executions

    None values and missing keys are silently skipped.
    """

    event_pattern = "*"

    def extract(self, event: str, data: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002
        """Extract navigation fields from event data, skipping None values."""
        result: dict[str, Any] = {}
        for key in _NAVIGATION_KEYS:
            value = data.get(key)
            if value is not None:
                result[key] = value
        return result
