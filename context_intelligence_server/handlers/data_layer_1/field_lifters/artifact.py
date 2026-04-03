"""ArtifactLifter — lifts bytes and path from artifact:* events."""

from __future__ import annotations

from typing import Any

from context_intelligence_server.handlers.data_layer_1.field_lifters.base import (
    FieldLifter,
)


class ArtifactLifter(FieldLifter):
    """Lifts bytes and path from artifact:read and artifact:write events."""

    event_pattern = "artifact:*"

    def extract(self, event: str, data: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002
        result: dict[str, Any] = {}
        for key in ("bytes", "path"):
            value = data.get(key)
            if value is not None:
                result[key] = value
        return result
