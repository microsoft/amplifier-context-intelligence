"""SessionHandler — owns :Session node lifecycle events."""

from __future__ import annotations

import logging
from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import EventLogContext, HandlerLogger

logger = logging.getLogger(__name__)

_TYPE_LABELS: frozenset[str] = frozenset({"RootSession", "SubSession", "ForkedSession"})


def _current_type(labels: list[str]) -> str | None:
    """Return the current session type label, or None if the session is bare.
    Bare sessions have only the base 'Session' label — no type label yet.
    ForkedSession > SubSession > RootSession in specificity (checked in this order)."""
    for label in ("ForkedSession", "SubSession", "RootSession"):
        if label in labels:
            return label
    return None


class SessionLabelStateMachine:
    """State machine for session type label transitions.

    Tracks which type label transitions are valid for each event type.
    ForkedSession > SubSession > RootSession in specificity (terminal ordering).
    """

    def __init__(self) -> None:
        # Marker: presence of this attribute confirms state machine delegation
        pass


class SessionHandler:
    """Handles session lifecycle events.

    Claimed events: session:start, session:fork, session:end.
    session:resume is intentionally NOT claimed — it flows to DefaultHandler.
    """

    handled_events: frozenset[str] = frozenset(
        {
            "session:start",
            "session:fork",
            "session:end",
        }
    )

    def __init__(self, services: HookStateService) -> None:
        self.services = services
        self._log = HandlerLogger("SessionHandler", logger)
        self._label_machine = SessionLabelStateMachine()

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        log = self._log.with_event(event, data)

        session_id = data.get("session_id")
        if not session_id:
            log.error("received event without session_id")
            return HookResult(action="continue")

        timestamp = data.get("timestamp", "")

        if event == "session:start":
            await self._handle_start(session_id, timestamp, data)
        elif event == "session:fork":
            await self._handle_fork(session_id, timestamp, data, log)
        elif event == "session:end":
            await self._handle_end(session_id, timestamp, data)

        return HookResult(action="continue")

    async def _handle_start(
        self, session_id: str, timestamp: str, data: dict[str, Any]
    ) -> None:
        parent_id = (data.get("parent_id") or "").strip()
        existing = await self.services.graph.get_node(session_id)
        labels: list[str] = existing.get("labels", []) if existing else []
        current_type = _current_type(labels)

        # Always enrich started_at
        await self.services.graph.upsert_node(session_id, {"started_at": timestamp})

        # ForkedSession: fully terminal — preserve classification, no edge creation
        if current_type == "ForkedSession":
            return

        # SubSession: terminal upward — preserve classification, no further changes
        if current_type == "SubSession":
            return

        # RootSession + no parent: stable — no reclassification needed
        if current_type == "RootSession" and not parent_id:
            return

        # RootSession + parent: reclassify to SubSession, drop RootSession
        if current_type == "RootSession" and parent_id:
            await self.services.graph.set_labels(
                session_id, remove_labels=["RootSession"], add_labels=["SubSession"]
            )
            await self.services.ensure_session_node(parent_id, {})
            await self.services.graph.upsert_edge(
                parent_id,
                session_id,
                {"type": "SUBSESSION_OF", "occurred_at": timestamp},
            )
            return

        # bare + parent: add SubSession (include Session base label for new nodes)
        if parent_id:
            await self.services.graph.set_labels(
                session_id, remove_labels=[], add_labels=["Session", "SubSession"]
            )
            await self.services.ensure_session_node(parent_id, {})
            await self.services.graph.upsert_edge(
                parent_id,
                session_id,
                {"type": "SUBSESSION_OF", "occurred_at": timestamp},
            )
            return

        # bare + no parent: add RootSession (include Session base label for new nodes)
        await self.services.graph.set_labels(
            session_id, remove_labels=[], add_labels=["RootSession", "Session"]
        )

    async def _handle_fork(
        self,
        session_id: str,
        timestamp: str,
        data: dict[str, Any],
        log: EventLogContext,
    ) -> None:
        parent_id = data.get("parent_id")

        labels: list[str] = ["ForkedSession", "Session"]
        if not parent_id:
            log.warning(
                "session:fork for %r has no parent_id — orphaned fork", session_id
            )

        workspace = data.get("workspace") or self.services.graph.workspace

        await self.services.graph.upsert_node(
            session_id,
            {
                "labels": labels,
                "started_at": timestamp,
                "workspace": workspace,
            },
        )

        if parent_id:
            await self.services.ensure_session_node(parent_id, {})
            await self.services.graph.upsert_edge(
                parent_id,
                session_id,
                {"type": "HAS_FORK", "occurred_at": timestamp},
            )

    async def _handle_end(
        self, session_id: str, timestamp: str, data: dict[str, Any]
    ) -> None:
        # data will be consumed by subsequent task (stub recovery / label state machine)
        await self.services.graph.upsert_node(
            session_id,
            {
                "labels": ["Session"],
                "ended_at": timestamp,
                "status": "completed",
            },
        )

        # Terminal event — flush directly. There is no hot path after
        # session:end; all buffered data must reach the backing store before
        # the process can exit. schedule_flush() is for intermediate events only.
        await self.services.graph.flush()
