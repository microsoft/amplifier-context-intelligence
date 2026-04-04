"""SessionHandler — owns :Session node lifecycle events."""

from __future__ import annotations

import logging
from typing import Any

from context_intelligence_server.protocol import HookResult
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import (
    EventLogContext,
    HandlerLogger,
    make_node_id,
)

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
        # Phase B: state machine logic will move here from SessionHandler
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

        # Always enrich started_at and session identity properties.
        # IMPORTANT: "Session" MUST be included here so neo4j_store routes this
        # write to MERGE (n:Session {node_id, workspace}) — the same bucket used
        # by ensure_session_node.  Without it, the write goes to the label-free
        # MERGE (n {node_id, workspace}) bucket, which is a DIFFERENT Neo4j
        # operation.  Under concurrent flushes, both MERGE variants see "no node"
        # and each creates a separate Neo4j node — two nodes for the same
        # session_id — which then breaks the uniqueness constraint on flush.
        await self.services.graph.upsert_node(
            session_id,
            {
                "labels": ["Session"],
                "started_at": timestamp,
                "session_id": session_id,
                "parent_id": parent_id if parent_id else None,
            },
        )

        # SOURCED_FROM bridge: Session -> data_layer_1 session:start event
        data_layer_1_node_id = make_node_id(session_id, "session:start", timestamp)
        await self.services.graph.upsert_edge(
            session_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

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
                session_id,
                remove_labels=["RootSession"],
                add_labels=["SubSession", "SST_EVENT"],
            )
            await self.services.ensure_session_node(parent_id, {})
            await self.services.graph.upsert_edge(
                parent_id,
                session_id,
                {
                    "type": "HAS_SUBSESSION",
                    "sst_semantic": "LEADS_TO",
                    "occurred_at": timestamp,
                },
            )
            return

        # bare + parent: add SubSession (include Session base label for new nodes)
        if parent_id:
            await self.services.graph.set_labels(
                session_id,
                remove_labels=[],
                add_labels=["Session", "SubSession", "SST_EVENT"],
            )
            await self.services.ensure_session_node(parent_id, {})
            await self.services.graph.upsert_edge(
                parent_id,
                session_id,
                {
                    "type": "HAS_SUBSESSION",
                    "sst_semantic": "LEADS_TO",
                    "occurred_at": timestamp,
                },
            )
            return

        # bare + no parent: add RootSession (include Session base label for new nodes)
        await self.services.graph.set_labels(
            session_id,
            remove_labels=[],
            add_labels=["RootSession", "Session", "SST_EVENT"],
        )

    async def _handle_fork(
        self,
        session_id: str,
        timestamp: str,
        data: dict[str, Any],
        log: EventLogContext,
    ) -> None:
        parent_id = (data.get("parent_id") or "").strip()
        workspace = data.get("workspace") or self.services.graph.workspace

        if not parent_id:
            log.warning(
                "session:fork for %r has no parent_id — orphaned fork", session_id
            )

        # Get existing node to determine current type
        existing = await self.services.graph.get_node(session_id)
        labels: list[str] = existing.get("labels", []) if existing else []
        current_type = _current_type(labels)

        # Always enrich started_at, workspace, and session identity properties.
        # IMPORTANT: "Session" MUST be included here for the same reason as
        # _handle_start — ensures neo4j_store routes this write to
        # MERGE (n:Session {node_id, workspace}), not the label-free bucket.
        # See _handle_start comment for full explanation.
        await self.services.graph.upsert_node(
            session_id,
            {
                "labels": ["Session"],
                "started_at": timestamp,
                "workspace": workspace,
                "session_id": session_id,
                "parent_id": parent_id if parent_id else None,
            },
        )

        # SOURCED_FROM bridge: Session -> data_layer_1 session:fork event
        data_layer_1_node_id = make_node_id(session_id, "session:fork", timestamp)
        await self.services.graph.upsert_edge(
            session_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

        # ForkedSession: fully terminal — preserve classification, return immediately
        if current_type == "ForkedSession":
            await self._create_mount_plan(session_id, data_layer_1_node_id)
            return

        # RootSession or SubSession: reclassify to ForkedSession, rectify edge
        if current_type in ("RootSession", "SubSession"):
            await self.services.graph.set_labels(
                session_id,
                remove_labels=[current_type],
                add_labels=["ForkedSession", "SST_EVENT"],
            )
            if parent_id:
                self.services.graph.remove_edge(parent_id, session_id)
                await self.services.ensure_session_node(parent_id, {})
                await self.services.graph.upsert_edge(
                    parent_id,
                    session_id,
                    {
                        "type": "FORKED",
                        "sst_semantic": "LEADS_TO",
                        "occurred_at": timestamp,
                    },
                )
            await self._create_mount_plan(session_id, data_layer_1_node_id)
            return

        # bare: add Session + ForkedSession labels (include Session base label for new nodes)
        await self.services.graph.set_labels(
            session_id,
            remove_labels=[],
            add_labels=["Session", "ForkedSession", "SST_EVENT"],
        )
        if parent_id:
            await self.services.ensure_session_node(parent_id, {})
            await self.services.graph.upsert_edge(
                parent_id,
                session_id,
                {
                    "type": "FORKED",
                    "sst_semantic": "LEADS_TO",
                    "occurred_at": timestamp,
                },
            )
        await self._create_mount_plan(session_id, data_layer_1_node_id)

    async def _handle_end(
        self, session_id: str, timestamp: str, data: dict[str, Any]
    ) -> None:
        await self.services.graph.upsert_node(
            session_id,
            {
                "labels": ["Session", "SST_EVENT"],
                "ended_at": timestamp,
                "status": "completed",
                "session_id": session_id,
            },
        )

        # SOURCED_FROM bridge: Session -> data_layer_1 session:end event
        data_layer_1_node_id = make_node_id(session_id, "session:end", timestamp)
        await self.services.graph.upsert_edge(
            session_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

        # Stub recovery: if session:start was permanently missed (bare Session),
        # classify the session now using parent_id from the end event data.
        existing = await self.services.graph.get_node(session_id)
        labels: list[str] = existing.get("labels", []) if existing else []
        if _current_type(labels) is None:
            parent_id = (data.get("parent_id") or "").strip()
            fallback = "SubSession" if parent_id else "RootSession"
            await self.services.graph.set_labels(
                session_id, remove_labels=[], add_labels=[fallback, "SST_EVENT"]
            )

        # Terminal event — flush directly. There is no hot path after
        # session:end; all buffered data must reach the backing store before
        # the process can exit. schedule_flush() is for intermediate events only.
        await self.services.graph.flush()

    async def _create_mount_plan(
        self, session_id: str, data_layer_1_fork_node_id: str
    ) -> None:
        """E04: Session → MountPlan (record existence, no blob dereferencing).
        SOURCED_FROM: MountPlan → session:fork data_layer_1 event (blob source).
        """
        mount_plan_id = f"{session_id}::mount_plan"
        await self.services.graph.upsert_node(
            mount_plan_id, {"labels": ["MountPlan", "SST_THING"]}
        )
        await self.services.graph.upsert_edge(
            session_id,
            mount_plan_id,
            {"type": "HAS_PART", "sst_semantic": "CONTAINS"},
        )
        # SOURCED_FROM bridge: MountPlan -> data_layer_1 session:fork event
        # The session:fork event contains data.raw (blob) with the full mount plan config
        await self.services.graph.upsert_edge(
            mount_plan_id,
            data_layer_1_fork_node_id,
            {"type": "SOURCED_FROM"},
        )
