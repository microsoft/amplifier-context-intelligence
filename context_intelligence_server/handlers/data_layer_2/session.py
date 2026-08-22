"""SessionHandler — owns :Session node lifecycle events."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
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


def _warn_if_dual_terminal(labels: list[str], session_id: str) -> None:
    """Log a WARNING when a node presents more than one terminal label.

    This is a logging-only guard — it mutates nothing.  A node with two
    terminal labels is an eradicated state; if one ever reappears, this
    surfaces it rather than silently converging.
    """
    terminals = [label for label in labels if label in _TYPE_LABELS]
    if len(terminals) > 1:
        logger.warning(
            "session %s presents multiple terminal labels %s; expected at most one (RootSession|SubSession|ForkedSession)",
            session_id,
            terminals,
        )


def _parent_of(data: dict[str, Any]) -> str:
    """Canonical parent session id from an event payload.

    amplifier-core emits session:fork with key "parent" (_session_init.py),
    while event enrichment also carries "parent_id". Accept either so the
    server never depends on which key a client sends. Empty string = no parent.
    """
    return (data.get("parent_id") or data.get("parent") or "").strip()


@dataclass(frozen=True)
class LabelTransition:
    add: list[str] = field(default_factory=list)
    remove: list[str] = field(default_factory=list)


class SessionLabelStateMachine:
    """State machine for session type label transitions.

    Tracks which type label transitions are valid for each event type.
    ForkedSession > SubSession > RootSession in specificity (terminal ordering).
    """

    def classify(
        self, current_type: str | None, event: str, has_parent: bool
    ) -> LabelTransition:
        # StubSession is a plain observability marker, not part of the
        # terminal lattice; every branch assigning a real terminal label
        # also clears it (removing an absent label is a no-op).
        if event == "start":
            if current_type in ("ForkedSession", "SubSession"):
                return LabelTransition()
            if current_type == "RootSession":
                if has_parent:
                    return LabelTransition(
                        add=["SubSession", "SST_EVENT"],
                        remove=["RootSession", "StubSession"],
                    )
                return LabelTransition()
            # bare session (current_type is None)
            if has_parent:
                return LabelTransition(
                    add=["Session", "SubSession", "SST_EVENT"],
                    remove=["StubSession"],
                )
            return LabelTransition(
                add=["RootSession", "Session", "SST_EVENT"],
                remove=["StubSession"],
            )

        if event == "fork":
            if current_type == "ForkedSession":
                return LabelTransition()
            if current_type in ("RootSession", "SubSession"):
                return LabelTransition(
                    add=["ForkedSession", "SST_EVENT"],
                    remove=[current_type, "StubSession"],
                )
            # bare session (current_type is None)
            return LabelTransition(
                add=["Session", "ForkedSession", "SST_EVENT"],
                remove=["StubSession"],
            )

        if event == "end":
            if current_type is not None:
                return LabelTransition()
            # Bare session: start/fork was permanently lost. Mark
            # IncompleteSession rather than fabricating a real terminal.
            return LabelTransition(
                add=["IncompleteSession", "SST_EVENT"], remove=["StubSession"]
            )

        raise ValueError(f"classify() received unknown event: {event!r}")


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
        parent_id = _parent_of(data)
        existing = await self.services.graph.get_node(session_id)
        labels: list[str] = existing.get("labels", []) if existing else []
        _warn_if_dual_terminal(labels, session_id)
        current_type = _current_type(labels)

        # "Session" MUST be in labels so neo4j_store routes this to MERGE,
        # the same bucket as ensure_session_node.
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

        transition = self._label_machine.classify(
            current_type, "start", bool(parent_id)
        )
        if transition.add or transition.remove:
            await self.services.graph.set_labels(
                session_id,
                remove_labels=transition.remove,
                add_labels=transition.add,
            )

        # A session becoming a SubSession under a parent gets a
        # HAS_SUBSESSION edge; Root (no parent) creates no edge.
        if "SubSession" in transition.add and parent_id:
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

    async def _handle_fork(
        self,
        session_id: str,
        timestamp: str,
        data: dict[str, Any],
        log: EventLogContext,
    ) -> None:
        parent_id = _parent_of(data)
        workspace = data.get("workspace") or self.services.graph.workspace

        if not parent_id:
            log.warning(
                "session:fork for %r has no parent_id, orphaned fork", session_id
            )

        existing = await self.services.graph.get_node(session_id)
        labels: list[str] = existing.get("labels", []) if existing else []
        _warn_if_dual_terminal(labels, session_id)
        current_type = _current_type(labels)

        # "Session" MUST be in labels for the same MERGE-bucket reason as
        # _handle_start.
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

        transition = self._label_machine.classify(current_type, "fork", bool(parent_id))
        if transition.add or transition.remove:
            await self.services.graph.set_labels(
                session_id,
                remove_labels=transition.remove,
                add_labels=transition.add,
            )

        # A session becoming a ForkedSession gets a FORKED edge; a
        # reclassified Root/Sub node drops its stale parent edge first.
        if "ForkedSession" in transition.add and parent_id:
            if current_type in ("RootSession", "SubSession"):
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

        # MountPlan companion ALWAYS created on fork, including the no-op case.
        await self._create_mount_plan(session_id, data_layer_1_node_id)

    async def _handle_end(
        self, session_id: str, timestamp: str, data: dict[str, Any]
    ) -> None:
        # Read labels BEFORE the end-event upsert -- upserting first would
        # shadow the persisted type label and spuriously trigger stub-recovery.
        existing = await self.services.graph.get_node(session_id)
        labels: list[str] = existing.get("labels", []) if existing else []
        _warn_if_dual_terminal(labels, session_id)
        parent_id = _parent_of(data)

        end_node_data: dict[str, Any] = {
            # Seed with labels just read so this entry can't shed a
            # persisted terminal type and trigger spurious stub-recovery.
            "labels": ["Session", "SST_EVENT", *labels],
            "ended_at": timestamp,
            "status": "completed",
            "session_id": session_id,
        }
        # Only write parent_id when present; never fabricate a value.
        if parent_id:
            end_node_data["parent_id"] = parent_id

        await self.services.graph.upsert_node(session_id, end_node_data)

        # SOURCED_FROM bridge: Session -> data_layer_1 session:end event
        data_layer_1_node_id = make_node_id(session_id, "session:end", timestamp)
        await self.services.graph.upsert_edge(
            session_id, data_layer_1_node_id, {"type": "SOURCED_FROM"}
        )

        # Stub recovery: if start was permanently missed, mark
        # IncompleteSession rather than fabricating a real terminal label.
        transition = self._label_machine.classify(
            _current_type(labels), "end", bool(parent_id)
        )
        if "IncompleteSession" in transition.add:
            logger.debug(
                "session %s reached end with no start/fork event; "
                "marked IncompleteSession (recovered)",
                session_id,
            )
        if transition.add or transition.remove:
            await self.services.graph.set_labels(
                session_id,
                remove_labels=transition.remove,
                add_labels=transition.add,
            )

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
        await self.services.graph.upsert_edge(
            mount_plan_id,
            data_layer_1_fork_node_id,
            {"type": "SOURCED_FROM"},
        )
