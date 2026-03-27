"""FieldLifter ABC and safe_prop utility for DefaultHandler event field extraction."""

from __future__ import annotations

import fnmatch
from typing import Any

RESERVED_PROPS: frozenset[str] = frozenset(
    {"node_id", "occurred_at", "event_name", "data", "labels"}
)


def safe_prop(key: str) -> str:
    """Return ``data_{key}`` if key collides with RESERVED_PROPS, else key unchanged."""
    if key in RESERVED_PROPS:
        return f"data_{key}"
    return key


class FieldLifter:
    """Base class for event-specific field extraction.

    Subclasses declare ``event_pattern`` (an fnmatch pattern) and implement
    ``extract()`` to pull structured fields out of the raw event data dict.

    Rules:
    - None values and missing keys are silently skipped (not written).
    - Collisions with RESERVED_PROPS are prefixed with "data_".
    - Subclasses implement extract().
    """

    event_pattern: str = ""

    def matches(self, event: str) -> bool:
        """Return True if event matches this lifter's event_pattern."""
        return fnmatch.fnmatch(event, self.event_pattern)

    def extract(self, event: str, data: dict[str, Any]) -> dict[str, Any]:
        """Extract fields from data for the given event name.

        Raises:
            NotImplementedError: Subclasses must override this method.
        """
        raise NotImplementedError
