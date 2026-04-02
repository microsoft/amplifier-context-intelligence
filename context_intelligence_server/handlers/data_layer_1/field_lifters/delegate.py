"""DelegateLifter — lifts delegation and tool-correlation fields from delegate:* events."""

from __future__ import annotations

from typing import Any

from context_intelligence_server.handlers.data_layer_1.field_lifters.base import FieldLifter


class DelegateLifter(FieldLifter):
    """Lifts agent, sub_session_id, parent_session_id, tool_call_id, parallel_group_id.

    Fires on delegate:agent_spawned, delegate:agent_completed, delegate:error.

    Extracts:
    - agent: the agent identifier being delegated to
    - sub_session_id: the session ID of the spawned sub-agent
    - parent_session_id: the session ID of the parent that spawned the delegate
    - tool_call_id: tool call identifier for correlating delegate events
    - parallel_group_id: group identifier for parallel delegate executions

    None values and missing keys are silently skipped.
    """

    event_pattern = "delegate:*"

    _DELEGATE_KEYS: tuple[str, ...] = (
        "agent",
        "sub_session_id",
        "parent_session_id",
        "tool_call_id",
        "parallel_group_id",
    )

    def extract(self, event: str, data: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002
        """Lift delegation provenance and tool-correlation fields."""
        result: dict[str, Any] = {}
        for key in self._DELEGATE_KEYS:
            value = data.get(key)
            if value is not None:
                result[key] = value
        return result
