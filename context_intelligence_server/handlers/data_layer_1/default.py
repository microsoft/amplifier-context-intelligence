"""DefaultHandler — catches all unclaimed, non-excluded events."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from context_intelligence_server.handlers.data_layer_1.field_lifters import (
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

# Split an event name into label parts on ANY run of characters that cannot
# appear in a Neo4j identifier -- not just ``:`` and ``_``.
#
# INCIDENT 2026-09-09: this was ``[:_]``, so a hyphen (or dot, space, slash...)
# inside a part survived ``.capitalize()`` straight into the derived label.
# ``routing_matrix-loaded`` produced ``RoutingMatrix-loadedEvent``, which
# ``neo4j_store._validate_identifier`` rejects at FLUSH time -- long after the
# event was accepted. One such event failed its whole chunk
# ("flush_chunk_failed ... nodes=192 edges=479"), the batch burned its
# max_delivery_attempts, and _handle_exhausted_batch then dead-lettered the
# offender AND committed the offset past it -- destroying healthy events that
# merely shared the batch.
#
# Labels are derived SERVER-SIDE from client-supplied event names, so this
# function is a trust boundary: it must emit an identifier the writer will
# accept, for every possible input, or the failure surfaces far from its cause.
_EVENT_PARTS_RE = re.compile(r"[^A-Za-z0-9]+")

# Mirrors neo4j_store._SAFE_IDENTIFIER_RE -- what the write path will accept.
_LABEL_SAFE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Fallback stem when an event name contains NO alphanumeric character at all
# (e.g. "::", "---"). Without it the derived label would collapse to the bare
# "Event" base label and silently merge unrelated events.
_UNNAMED_EVENT_STEM = "Unnamed"


def _pascal_parts(raw: str) -> str:
    """Join *raw*'s alphanumeric parts as PascalCase, safe as a label stem.

    Guarantees the result is either empty or matches ``[A-Za-z][A-Za-z0-9]*``:
    a leading digit is prefixed (``2fa`` -> ``E2fa``) because Neo4j identifiers
    may not start with one.
    """
    stem = "".join(part.capitalize() for part in _EVENT_PARTS_RE.split(raw) if part)
    if stem and stem[0].isdigit():
        stem = f"E{stem}"
    return stem


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

        Every returned label is guaranteed to satisfy Neo4j's identifier rules
        (``[A-Za-z_][A-Za-z0-9_]*``) for ANY input: separators other than
        ``:``/``_`` are split on rather than passed through, a leading digit is
        prefixed, and a name with no alphanumeric content falls back to
        ``Unnamed``. See _EVENT_PARTS_RE for the incident that required this.

        Examples:
          'tool:pre'              → ['ToolPreEvent', 'ToolEvent', 'Event']
          'recipe:loop_iter'      → ['RecipeLoopIterEvent', 'RecipeEvent', 'Event']
          'my_event'              → ['MyEventEvent', 'MyEventEvent', 'Event']
          'ping'                  → ['PingEvent', 'PingEvent', 'Event']
          'routing_matrix-loaded' → ['RoutingMatrixLoadedEvent', ...]  (was 'RoutingMatrix-loadedEvent')
          '2fa:verify'            → ['E2faVerifyEvent', 'E2faEvent', 'Event']
        """
        full_pascal = _pascal_parts(event_name) or _UNNAMED_EVENT_STEM

        if ":" in event_name:
            last_colon = event_name.rfind(":")
            category = _pascal_parts(event_name[:last_colon]) or _UNNAMED_EVENT_STEM
        else:
            category = full_pascal

        labels = [f"{full_pascal}Event", f"{category}Event", "Event"]
        # Belt to the braces above: this function is the trust boundary between
        # a client-supplied event name and a Cypher identifier, so it asserts
        # its own postcondition rather than letting a miss surface as a
        # dead-lettered batch at flush time.
        for label in labels:
            if not _LABEL_SAFE_RE.match(label):  # pragma: no cover - defensive
                raise ValueError(
                    f"derive_labels produced an invalid Neo4j label {label!r} "
                    f"from event name {event_name!r}"
                )
        return labels
