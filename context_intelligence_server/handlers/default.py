"""DefaultHandler — catches all unclaimed, non-excluded events."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import make_node_id

logger = logging.getLogger(__name__)

_EVENT_PARTS_RE = re.compile(r"[:_]")


class DefaultHandler:
    """Creates :Event:{DerivedLabel} nodes from unclaimed events.

    For every event that no entity handler claims, the DefaultHandler:
    1. Derives a 3-level label hierarchy from the event name.
    2. Creates an Event node with labels [FullPascalEvent, CategoryEvent, 'Event'].
    3. Attaches it to the Session node via a HAS_EVENT edge.

    This covers app-level events (e.g. session:resume) that don't need
    special entity-node mutations — they are simply recorded as Event
    nodes in the graph.
    """

    handled_events: set[str]

    def __init__(self, services: HookStateService) -> None:
        self.services = services
        self.handled_events = set()

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        session_id = data.get("session_id")
        if not session_id:
            logger.warning("DefaultHandler: dropping event %s — no session_id", event)
            return HookResult(action="continue")

        timestamp = data.get("timestamp", "")
        labels = self.derive_labels(event)

        # tool:* events use tool_call_id as tiebreaker to distinguish
        # parallel tool calls with the same timestamp.
        disambiguator = data.get("tool_call_id") if event.startswith("tool:") else None

        # Create Event node
        event_node_id = make_node_id(session_id, event, timestamp, disambiguator)
        await self.services.graph.upsert_node(
            event_node_id,
            {
                "labels": labels,
                "event_name": event,
                "occurred_at": timestamp,
                "data": json.dumps(data),
            },
        )

        # Attach to the session node
        await self.services.graph.upsert_edge(
            session_id,
            event_node_id,
            {"type": "HAS_EVENT", "occurred_at": timestamp},
        )

        return HookResult(action="continue")

    @staticmethod
    def derive_labels(event_name: str) -> list[str]:
        """Derive 3-level label hierarchy from event name.

        Returns [FullPascalEvent, CategoryEvent, 'Event'] where:
        - FullPascalEvent: all parts (split on : and _) capitalized and joined, with 'Event' suffix
        - CategoryEvent: the prefix before the last colon (same PascalCase transform), with 'Event' suffix
          If no colon, CategoryEvent == FullPascalEvent.

        The 'Event' suffix prevents label clashes with entity node types (e.g.
        session:start would otherwise produce the label 'Session', clashing with
        actual Session nodes).

        Examples:
          'tool:pre'           → ['ToolPreEvent', 'ToolEvent', 'Event']
          'recipe:loop_iter'   → ['RecipeLoopIterEvent', 'RecipeEvent', 'Event']
          'my_event'           → ['MyEventEvent', 'MyEventEvent', 'Event']
          'ping'               → ['PingEvent', 'PingEvent', 'Event']
        """
        parts = _EVENT_PARTS_RE.split(event_name)
        full_pascal = "".join(part.capitalize() for part in parts if part)

        if ":" in event_name:
            last_colon = event_name.rfind(":")
            category_raw = event_name[:last_colon]
            category_parts = _EVENT_PARTS_RE.split(category_raw)
            category = "".join(p.capitalize() for p in category_parts if p)
        else:
            category = full_pascal

        return [f"{full_pascal}Event", f"{category}Event", "Event"]
