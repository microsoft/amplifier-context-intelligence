"""SessionLifter — extracts parent and metadata fields from session:* events."""

from __future__ import annotations

from typing import Any

from context_intelligence_server.handlers.data_layer_1.field_lifters.base import FieldLifter

_METADATA_KEYS: tuple[str, ...] = (
    "agent_name",
    "tool_call_id",
    "parallel_group_id",
    "recipe_name",
    "recipe_step",
    "recipe_step_index",
)


class SessionLifter(FieldLifter):
    """Lifts parent reference and metadata sub-fields from session:* events.

    Fires on session:start, session:fork, session:end, session:resume.

    Extracts:
    - parent: parent session reference used by session:fork (not parent_id)
    - agent_name, tool_call_id, parallel_group_id, recipe_name, recipe_step,
      recipe_step_index: lifted as flat top-level properties from metadata dict

    None values and missing keys are silently skipped.
    Non-dict metadata is ignored entirely.
    """

    event_pattern = "session:*"

    def extract(self, event: str, data: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002
        """Extract parent and metadata fields from session event data."""
        result: dict[str, Any] = {}

        parent = data.get("parent")
        if parent is not None:
            result["parent"] = parent

        metadata = data.get("metadata")
        if isinstance(metadata, dict):
            for key in _METADATA_KEYS:
                value = metadata.get(key)
                if value is not None:
                    result[key] = value

        return result
