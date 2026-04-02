"""ToolLifter — extracts tool_name, tool_input, tool_call_id, parallel_group_id from tool:* events."""

from __future__ import annotations

from typing import Any

from context_intelligence_server.handlers.field_lifters.base import FieldLifter

_TOOL_KEYS: tuple[str, ...] = (
    "tool_name",
    "tool_input",
    "tool_call_id",
    "parallel_group_id",
)


class ToolLifter(FieldLifter):
    """Lifts tool_name and tool_input from tool:* events.

    Fires on tool:pre, tool:post, tool:error.

    Extracts:
    - tool_name: the name of the tool being called
    - tool_input: the input dict, blob reference, or any value — lifted as-is
    - tool_call_id: correlates tool-call events
    - parallel_group_id: groups parallel tool calls

    None values and missing keys are silently skipped.
    tool_input may be a dict, a blob reference ({"$blob_ref": "ci-blob://..."}),
    or absent — all lifted as-is when present.
    """

    event_pattern = "tool:*"

    def extract(self, event: str, data: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002
        """Extract tool_name and tool_input from tool event data."""
        result: dict[str, Any] = {}
        for key in _TOOL_KEYS:
            value = data.get(key)
            if value is not None:
                result[key] = value
        return result
