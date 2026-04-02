"""DefaultHandler — catches all unclaimed, non-excluded events."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from context_intelligence_server.handlers.field_lifters import (
    ArtifactLifter,
    DelegateLifter,
    FieldLifter,
    LlmLifter,
    PromptLifter,
    RecipeLifter,
    SessionLifter,
    SkillLifter,
    ToolLifter,
    UniversalLifter,
)
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
    3. Applies ALL matching FieldLifters to expose structured fields as top-level
       node properties.
    4. Attaches it to the Session node via a HAS_EVENT edge.

    This covers app-level events (e.g. session:resume) that don't need
    special entity-node mutations — they are simply recorded as Event
    nodes in the graph.
    """

    handled_events: set[str]

    # Stage 3: ALL matching lifters fire (not first-match-wins).
    # UniversalLifter must be FIRST so event-specific lifters can override.
    # All others sorted alphabetically by event family for maintainability.
    _LIFTERS: list[FieldLifter] = [
        UniversalLifter(),
        ArtifactLifter(),  # artifact:*
        DelegateLifter(),  # delegate:*
        LlmLifter(),  # llm:*
        PromptLifter(),  # prompt:*
        RecipeLifter(),  # recipe:*
        SessionLifter(),  # session:*
        SkillLifter(),  # skill:*
        ToolLifter(),  # tool:*
    ]

    def __init__(self, services: HookStateService) -> None:
        self.services = services
        self.handled_events = set()

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        # Stage 1: Guard — drop events without session_id
        session_id = data.get("session_id")
        if not session_id:
            logger.warning("DefaultHandler: dropping event %s — no session_id", event)
            return HookResult(action="continue")

        # Stage 1: Label derivation — [FullPascalEvent, CategoryEvent, 'Event']
        timestamp = data.get("timestamp", "")
        labels = self.derive_labels(event)

        # Stage 2: node_id — session_id + event + timestamp + tool_call_id tiebreaker
        # tool_call_id is used for ALL events (not just tool:*) — events like
        # delegate:agent_spawned also carry tool_call_id for parallel-call reasons.
        disambiguator = data.get("tool_call_id")

        event_node_id = make_node_id(session_id, event, timestamp, disambiguator)

        # Stage 3: Field lifting — ALL matching FieldLifters fire and contribute properties
        lifted: dict[str, Any] = {}
        for lifter in self._LIFTERS:
            if lifter.matches(event):
                lifted.update(lifter.extract(event, data))

        # Stage 4: Node construction — base props + lifted fields + full data blob
        node_props: dict[str, Any] = {
            "labels": labels,
            "event_name": event,
            "occurred_at": timestamp,
            **lifted,
            "data": json.dumps(data),
        }
        await self.services.graph.upsert_node(event_node_id, node_props)

        # Stage 5: HAS_EVENT edge — (Session)-[:HAS_EVENT {occurred_at}]->(Event)
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
