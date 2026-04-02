"""LlmLifter — extracts model and provider from llm:* events."""

from __future__ import annotations

from typing import Any

from context_intelligence_server.handlers.data_layer_1.field_lifters.base import FieldLifter

_LLM_KEYS: tuple[str, ...] = (
    "model",
    "provider",
)


class LlmLifter(FieldLifter):
    """Lifts model and provider from llm:* events.

    Fires on llm:request, llm:response.

    Extracts:
    - model: the model identifier used for the LLM call
    - provider: the provider name (e.g. anthropic, openai)

    None values and missing keys are silently skipped.
    """

    event_pattern = "llm:*"

    def extract(self, event: str, data: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002
        """Extract model and provider from llm event data."""
        result: dict[str, Any] = {}
        for key in _LLM_KEYS:
            value = data.get(key)
            if value is not None:
                result[key] = value
        return result
