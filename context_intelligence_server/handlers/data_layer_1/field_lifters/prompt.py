"""PromptLifter — extracts prompt and response_preview from prompt:* events."""

from __future__ import annotations

from typing import Any

from context_intelligence_server.handlers.data_layer_1.field_lifters.base import FieldLifter

_PROMPT_KEYS: tuple[str, ...] = (
    "prompt",
    "response_preview",
)


class PromptLifter(FieldLifter):
    """Lifts prompt and response_preview from prompt:* events.

    Fires on prompt:submit, prompt:complete.

    Extracts:
    - prompt: the user prompt text (present on submit and complete)
    - response_preview: preview of the response (present on complete only)

    None values and missing keys are silently skipped.
    """

    event_pattern = "prompt:*"

    def extract(self, event: str, data: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002
        """Extract prompt and response_preview from prompt event data."""
        result: dict[str, Any] = {}
        for key in _PROMPT_KEYS:
            value = data.get(key)
            if value is not None:
                result[key] = value
        return result
