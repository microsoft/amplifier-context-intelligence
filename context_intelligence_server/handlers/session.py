"""SessionHandler — owns :Session node lifecycle events."""

from __future__ import annotations

import json
import logging
from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import EventLogContext, HandlerLogger

logger = logging.getLogger(__name__)


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

        if parent_id:
            labels: list[str] = ["Session", "Subsession"]
        else:
            labels = ["Session", "Root"]

        await self.services.graph.upsert_node(
            session_id,
            {
                "labels": labels,
                "started_at": timestamp,
                "status": "running",
                "metadata": data.get("metadata", {}),
                "data": json.dumps(data),
            },
        )

        cursors = self.services.get_cursors(session_id)
        metadata = data.get("metadata") or {}
        if metadata.get("recipe_name"):
            cursors.is_recipe_session = True

        if parent_id:
            await self.services.ensure_session_node(parent_id, {})
            await self.services.graph.upsert_edge(
                session_id,
                parent_id,
                {"type": "SUBSESSION_OF", "occurred_at": timestamp},
            )

    async def _handle_fork(
        self,
        session_id: str,
        timestamp: str,
        data: dict[str, Any],
        log: EventLogContext,
    ) -> None:
        parent = data.get("parent")

        if parent:
            labels: list[str] = ["Session", "Subsession", "ForkedSession"]
        else:
            labels = ["Session", "Root", "ForkedSession"]
            log.warning(
                "session:fork for %r has no parent — degrading to Root", session_id
            )

        await self.services.graph.upsert_node(
            session_id,
            {
                "labels": labels,
                "started_at": timestamp,
                "status": "running",
                "metadata": data.get("metadata", {}),
                "data": json.dumps(data),
            },
        )

        cursors = self.services.get_cursors(session_id)
        metadata = data.get("metadata") or {}
        if metadata.get("recipe_name"):
            cursors.is_recipe_session = True

        if parent:
            await self.services.ensure_session_node(parent, {})
            await self.services.graph.upsert_edge(
                session_id,
                parent,
                {"type": "SUBSESSION_OF", "occurred_at": timestamp},
            )

    async def _handle_end(
        self, session_id: str, timestamp: str, data: dict[str, Any]
    ) -> None:
        await self.services.graph.upsert_node(
            session_id,
            {
                "labels": ["Session"],
                "ended_at": timestamp,
                "status": data.get("status", "completed"),
                "data_session_end": json.dumps(data),
            },
        )

        # Terminal event — flush directly. There is no hot path after
        # session:end; all buffered data must reach the backing store before
        # the process can exit. schedule_flush() is for intermediate events only.
        await self.services.graph.flush()

        self.services.remove_cursors(session_id)
