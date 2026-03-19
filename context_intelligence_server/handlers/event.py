"""SystemEventHandler — owns known system events (compaction, cancellation)."""

from __future__ import annotations

import json
import logging
from typing import Any

from context_intelligence_server.handlers.default import DefaultHandler
from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import HandlerLogger, make_node_id

logger = logging.getLogger(__name__)


class SystemEventHandler:
    """Creates :Event:{DerivedLabel} nodes for known system events.

    Labels preserve full event scope:
      :Event:ContextCompaction, :Event:CancelRequested, :Event:CancelCompleted

    Owned events: context:compaction, cancel:requested, cancel:completed.
    Each handler upserts an event node and a HAS_EVENT edge from the
    appropriate scope (Step → Run → Session fallback for compaction;
    Run → Session for cancel events).
    """

    handled_events: frozenset[str] = frozenset(
        {
            "context:compaction",
            "cancel:requested",
            "cancel:completed",
        }
    )

    def __init__(self, services: HookStateService) -> None:
        self.services = services
        self._log = HandlerLogger("SystemEventHandler", logger)

    async def __call__(self, event: str, data: dict[str, Any]) -> HookResult:
        if event == "context:compaction":
            return await self._handle_compaction(data)
        if event == "cancel:requested":
            return await self._handle_cancel_requested(data)
        if event == "cancel:completed":
            return await self._handle_cancel_completed(data)
        return HookResult(action="continue")

    # ------------------------------------------------------------------
    # context:compaction
    # ------------------------------------------------------------------

    async def _handle_compaction(self, data: dict[str, Any]) -> HookResult:
        """Persist a ContextCompaction event node scoped to Step → Run → Session."""
        session_id = data.get("session_id")
        if not session_id:
            return HookResult(action="continue")

        timestamp = data.get("timestamp", "")
        cursors = self.services.get_cursors(session_id)

        # Step-first scope fallback
        scope_id = cursors.current_step_id or cursors.current_run_id or session_id

        node_id = make_node_id(session_id, "context:compaction", timestamp)
        derived = DefaultHandler.derive_label("context:compaction")

        props: dict[str, Any] = {
            "labels": ["Event", derived],
            "event_name": "context:compaction",
            "occurred_at": timestamp,
            "data": json.dumps(data),
        }

        # 8 optional fields — only write when non-None
        for field in (
            "before_tokens",
            "after_tokens",
            "tokens_freed",
            "before_messages",
            "after_messages",
            "messages_removed",
            "strategy_level",
            "budget",
        ):
            value = data.get(field)
            if value is not None:
                props[field] = value

        await self.services.graph.upsert_node(node_id, props)
        await self.services.graph.upsert_edge(
            scope_id,
            node_id,
            {"type": "HAS_EVENT", "occurred_at": timestamp},
        )

        log = self._log.with_event("context:compaction", data)
        log.info(
            "Created ContextCompaction event node %s (scope: %s)", node_id, scope_id
        )
        return HookResult(action="continue")

    # ------------------------------------------------------------------
    # cancel:requested
    # ------------------------------------------------------------------

    async def _handle_cancel_requested(self, data: dict[str, Any]) -> HookResult:
        """Persist a CancelRequested event node scoped to Run → Session."""
        session_id = data.get("session_id")
        if not session_id:
            return HookResult(action="continue")

        timestamp = data.get("timestamp", "")
        cursors = self.services.get_cursors(session_id)
        scope_id = cursors.current_run_id or session_id

        node_id = make_node_id(session_id, "cancel:requested", timestamp)
        derived = DefaultHandler.derive_label("cancel:requested")

        props: dict[str, Any] = {
            "labels": ["Event", derived],
            "event_name": "cancel:requested",
            "occurred_at": timestamp,
            "data": json.dumps(data),
            "is_immediate": bool(data.get("is_immediate", False)),
        }

        # running_tools: guard None, stringify entries, only write if non-empty
        running_tools = data.get("running_tools")
        if running_tools is not None:
            stringified = [str(t) for t in running_tools]
            if stringified:
                props["running_tools"] = stringified

        await self.services.graph.upsert_node(node_id, props)
        await self.services.graph.upsert_edge(
            scope_id,
            node_id,
            {"type": "HAS_EVENT", "occurred_at": timestamp},
        )

        log = self._log.with_event("cancel:requested", data)
        log.info("Created CancelRequested event node %s (scope: %s)", node_id, scope_id)
        return HookResult(action="continue")

    # ------------------------------------------------------------------
    # cancel:completed
    # ------------------------------------------------------------------

    async def _handle_cancel_completed(self, data: dict[str, Any]) -> HookResult:
        """Persist a CancelCompleted event node scoped to Run → Session."""
        session_id = data.get("session_id")
        if not session_id:
            return HookResult(action="continue")

        timestamp = data.get("timestamp", "")
        cursors = self.services.get_cursors(session_id)
        scope_id = cursors.current_run_id or session_id

        node_id = make_node_id(session_id, "cancel:completed", timestamp)
        derived = DefaultHandler.derive_label("cancel:completed")

        props: dict[str, Any] = {
            "labels": ["Event", derived],
            "event_name": "cancel:completed",
            "occurred_at": timestamp,
            "data": json.dumps(data),
        }

        # error: coerce to str, only write if not None
        error = data.get("error")
        if error is not None:
            props["error"] = str(error)

        await self.services.graph.upsert_node(node_id, props)
        await self.services.graph.upsert_edge(
            scope_id,
            node_id,
            {"type": "HAS_EVENT", "occurred_at": timestamp},
        )

        log = self._log.with_event("cancel:completed", data)
        log.info("Created CancelCompleted event node %s (scope: %s)", node_id, scope_id)
        return HookResult(action="continue")
