"""DelegateLifter — extracts agent, sub_session_id, parent_session_id from delegate:* events."""

from __future__ import annotations

from typing import Any

from context_intelligence_server.handlers.field_lifters.base import FieldLifter

_DELEGATE_KEYS: tuple[str, ...] = (
    "agent",
    "sub_session_id",
    "parent_session_id",
)


class DelegateLifter(FieldLifter):
    """Lifts agent, sub_session_id, and parent_session_id from delegate:* events.

    Fires on delegate:agent_spawned, delegate:agent_completed, delegate:error.

    Extracts:
    - agent: the agent identifier being delegated to
    - sub_session_id: the session ID of the spawned sub-agent
    - parent_session_id: the session ID of the parent that spawned the delegate

    None values and missing keys are silently skipped.
    """

    event_pattern = "delegate:*"

    def extract(self, event: str, data: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002
        """Extract agent, sub_session_id, and parent_session_id from delegate event data."""
        result: dict[str, Any] = {}
        for key in _DELEGATE_KEYS:
            value = data.get(key)
            if value is not None:
                result[key] = value
        return result
