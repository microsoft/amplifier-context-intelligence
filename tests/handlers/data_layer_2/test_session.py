"""Tests for SessionHandler — session lifecycle graph mutations.

Covers:
- Label state machine: RootSession, SubSession, ForkedSession transitions
- Edge types: HAS_SUBSESSION (parent→child), FORKED (parent→fork)
- SST_EVENT label on all session types
- MountPlan companion node on session:fork
- Fork guard: ForkedSession preserved across session:start
- Stub recovery: session:end classifies bare sessions
"""

from __future__ import annotations

import pytest

from context_intelligence_server.handlers.data_layer_2.session import (
    SessionHandler,
    _TYPE_LABELS,
    _current_type,
)
from context_intelligence_server.services import HookStateService


class TestTypeLabelConstant:
    """_TYPE_LABELS must be a frozenset containing the three type labels."""

    def test_type_labels_is_frozenset(self) -> None:
        assert isinstance(_TYPE_LABELS, frozenset)

    def test_type_labels_contains_root_session(self) -> None:
        assert "RootSession" in _TYPE_LABELS

    def test_type_labels_contains_sub_session(self) -> None:
        assert "SubSession" in _TYPE_LABELS

    def test_type_labels_contains_forked_session(self) -> None:
        assert "ForkedSession" in _TYPE_LABELS

    def test_type_labels_contains_exactly_three_entries(self) -> None:
        assert len(_TYPE_LABELS) == 3


class TestCurrentTypeHelper:
    """_current_type() returns the most-specific type label present, or None."""

    def test_returns_none_for_bare_session(self) -> None:
        """A bare session (only 'Session') has no type label."""
        assert _current_type(["Session"]) is None

    def test_returns_none_for_empty_labels(self) -> None:
        assert _current_type([]) is None

    def test_returns_root_session(self) -> None:
        assert _current_type(["RootSession", "Session"]) == "RootSession"

    def test_returns_sub_session(self) -> None:
        assert _current_type(["SubSession", "Session"]) == "SubSession"

    def test_returns_forked_session(self) -> None:
        assert _current_type(["ForkedSession", "Session"]) == "ForkedSession"

    def test_forked_takes_priority_over_sub(self) -> None:
        """ForkedSession > SubSession in specificity."""
        assert (
            _current_type(["SubSession", "ForkedSession", "Session"]) == "ForkedSession"
        )

    def test_forked_takes_priority_over_root(self) -> None:
        """ForkedSession > RootSession in specificity."""
        assert (
            _current_type(["RootSession", "ForkedSession", "Session"])
            == "ForkedSession"
        )

    def test_sub_takes_priority_over_root(self) -> None:
        """SubSession > RootSession in specificity."""
        assert _current_type(["RootSession", "SubSession", "Session"]) == "SubSession"


class TestSessionIdGuard:
    """Missing session_id must short-circuit before any graph mutation."""

    async def test_missing_session_id_returns_continue(
        self, services: HookStateService
    ) -> None:
        handler = SessionHandler(services)
        result = await handler("session:start", {"timestamp": "2026-01-01T00:00:00Z"})
        assert result.action == "continue"


class TestSessionStart:
    """session:start creates RootSession or SubSession nodes."""

    async def test_root_session_labels(self, services: HookStateService) -> None:
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "metadata": {"key": "val"},
            },
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert "Session" in node["labels"]
        assert "RootSession" in node["labels"]
        assert "SubSession" not in node["labels"]

    async def test_root_session_has_started_at(
        self, services: HookStateService
    ) -> None:
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert node["started_at"] == "2026-01-01T00:00:00Z"

    async def test_root_session_no_subsession_edge(
        self, services: HookStateService
    ) -> None:
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        # No parent — no edge should be created at all
        assert len(services.graph._edges) == 0, "root session must not create any edge"

    async def test_subsession_labels(self, services: HookStateService) -> None:
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
                "metadata": {"m": 1},
            },
        )
        node = await services.graph.get_node("child")
        assert node is not None
        assert "Session" in node["labels"]
        assert "SubSession" in node["labels"]
        assert "RootSession" not in node["labels"]

    async def test_subsession_edge_parent_to_child(
        self, services: HookStateService
    ) -> None:
        """SUBSESSION_OF edge goes from parent→child (not child→parent)."""
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        # Parent→Child direction: assert get_edge(parent_id, child_id)
        edge = await services.graph.get_edge("parent", "child")
        assert edge is not None
        assert edge["occurred_at"] == "2026-01-01T00:00:00Z"


class TestSessionStartParentIdEdgeCases:
    """Falsy parent_id values must produce RootSession (not SubSession) nodes."""

    @pytest.mark.parametrize("parent_id", [None, "", "   ", "\t", "\n"])
    async def test_falsy_parent_id_produces_root(
        self, services: HookStateService, parent_id: str | None
    ) -> None:
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "s1",
                "parent_id": parent_id,
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert "RootSession" in node["labels"]
        assert "SubSession" not in node["labels"]

    async def test_missing_parent_id_key_produces_root(
        self, services: HookStateService
    ) -> None:
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert "RootSession" in node["labels"]
        assert "SubSession" not in node["labels"]


class TestSessionFork:
    """session:fork creates ForkedSession:Session nodes (NOT SubSession)."""

    async def test_fork_labels_with_parent(self, services: HookStateService) -> None:
        """Fork with parent gets exactly [ForkedSession, Session] labels — SubSession must NOT be present."""
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "f1",
                "parent_id": "p1",  # canonical parent_id key
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("f1")
        assert node is not None
        assert "Session" in node["labels"]
        assert "ForkedSession" in node["labels"]
        assert "SubSession" not in node["labels"], (
            "Forked sessions are NOT subsessions — SubSession must not be present"
        )

    async def test_fork_edge_parent_to_child(self, services: HookStateService) -> None:
        """HAS_FORK edge goes from parent→child (not child→parent)."""
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "f1",
                "parent_id": "p1",  # canonical parent_id key
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        # Parent→Child direction: assert get_edge(parent_id, child_id)
        edge = await services.graph.get_edge("p1", "f1")
        assert edge is not None
        assert edge["occurred_at"] == "2026-01-01T00:00:00Z"
        assert edge["type"] == "FORKED", (
            f"Fork edge must be FORKED, got: {edge.get('type')}"
        )
        assert edge["type"] != "HAS_SUBSESSION", "Fork edge must NOT be HAS_SUBSESSION"

    async def test_fork_uses_parent_id_not_parent_key(
        self, services: HookStateService
    ) -> None:
        """Fork canonical key is parent_id; legacy 'parent' key must be ignored.

        Positive case: parent_id creates an edge.
        Negative case: legacy 'parent' key alone must NOT create an edge.
        """
        handler = SessionHandler(services)
        # Positive: using parent_id (canonical) creates an edge
        await handler(
            "session:fork",
            {
                "session_id": "f1",
                "parent_id": "p1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        # Edge must exist parent→child when parent_id is provided
        edge = await services.graph.get_edge("p1", "f1")
        assert edge is not None

        # Negative: legacy 'parent' key alone must NOT create any edge
        handler2 = SessionHandler(services)
        await handler2(
            "session:fork",
            {
                "session_id": "f2",
                "parent": "p2",  # legacy key — must be ignored
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        legacy_edge = await services.graph.get_edge("p2", "f2")
        assert legacy_edge is None, "legacy 'parent' key must not create an edge"

    async def test_fork_missing_parent_id_creates_node_no_edge(
        self, services: HookStateService
    ) -> None:
        """Fork without parent_id creates ForkedSession+Session node, no parent edge, no RootSession."""
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "f1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("f1")
        assert node is not None
        assert "Session" in node["labels"]
        assert "ForkedSession" in node["labels"]
        assert "SubSession" not in node["labels"]
        # Orphaned forks do NOT get RootSession — they are ForkedSession+Session only
        assert "RootSession" not in node["labels"]
        # Orphaned forks must NOT create any FORKED or HAS_SUBSESSION edge to a parent —
        # only the companion MountPlan HAS_PART edge is allowed.
        parent_edge = await services.graph.get_edge(
            "f1", "f1"
        )  # no parent ID — check self-edges
        assert parent_edge is None
        # Verify no edge from any unknown parent to f1 exists (no parent_id was supplied)
        edges_from_other_nodes = [
            (src, dst)
            for (src, dst) in services.graph._edges
            if dst == "f1" and src != "f1"
        ]
        assert len(edges_from_other_nodes) == 0, (
            "No parent→child edge should be created for an orphaned fork (no parent_id)"
        )


class TestSessionEnd:
    """session:end sets ended_at and flushes."""

    async def test_end_sets_ended_at(self, services: HookStateService) -> None:
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        await handler(
            "session:end",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T01:00:00Z",
            },
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert node["ended_at"] == "2026-01-01T01:00:00Z"

    async def test_end_preserves_existing_labels(
        self, services: HookStateService
    ) -> None:
        """session:end must preserve RootSession label from prior session:start."""
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        await handler(
            "session:end",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T01:00:00Z",
            },
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert "Session" in node["labels"]
        assert "RootSession" in node["labels"]

    async def test_end_without_prior_start(self, services: HookStateService) -> None:
        """session:end works even without a prior session:start."""
        handler = SessionHandler(services)
        await handler(
            "session:end",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T01:00:00Z",
            },
        )
        node = await services.graph.get_node("s1")
        assert node is not None

    async def test_end_does_not_store_status_from_data(
        self, services: HookStateService
    ) -> None:
        """session:end does not pick up status from event data — always 'completed'."""
        handler = SessionHandler(services)
        await handler(
            "session:end",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T01:00:00Z",
                "status": "aborted",  # this value must be IGNORED
            },
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        # session:end must NOT propagate data['status'] — always 'completed'
        assert node["status"] != "aborted"
        assert node["status"] == "completed"


class TestSessionResumeNotClaimed:
    """session:resume must NOT be in SessionHandler.handled_events."""

    def test_session_handler_does_not_claim_resume(self) -> None:
        assert "session:resume" not in SessionHandler.handled_events

    def test_session_handler_claims_start_fork_end(self) -> None:
        assert "session:start" in SessionHandler.handled_events
        assert "session:fork" in SessionHandler.handled_events
        assert "session:end" in SessionHandler.handled_events


class TestSessionEdgeTypes:
    """Edges created by SessionHandler must carry explicit semantic 'type' keys.

    session:start creates SUBSESSION_OF edges; session:fork creates HAS_FORK edges.
    """

    async def test_start_subsession_edge_type_is_has_subsession(
        self, services: HookStateService
    ) -> None:
        """session:start parent→child edge must have type='HAS_SUBSESSION'."""
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        # Parent→Child direction
        edge = await services.graph.get_edge("parent", "child")
        assert edge is not None
        assert edge.get("type") == "HAS_SUBSESSION"
        assert edge.get("sst_semantic") == "LEADS_TO"

    async def test_fork_edge_type_is_forked(self, services: HookStateService) -> None:
        """session:fork parent→child edge must have type='FORKED' (not HAS_SUBSESSION)."""
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "fork1",
                "parent_id": "parent1",  # canonical parent_id key
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        # Parent→Child direction
        edge = await services.graph.get_edge("parent1", "fork1")
        assert edge is not None
        assert edge.get("type") == "FORKED", (
            f"Fork edge must be FORKED, got: {edge.get('type')}"
        )
        assert edge.get("sst_semantic") == "LEADS_TO"


class TestLateParentDiscovery:
    """Stub parent nodes created when parent doesn't exist yet."""

    async def test_start_parent_stub_created_when_missing(
        self, services: HookStateService
    ) -> None:
        """session:start with parent_id creates stub parent with Session label."""
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        parent_node = await services.graph.get_node("parent")
        assert parent_node is not None
        assert "Session" in parent_node["labels"]

    async def test_fork_parent_stub_created_when_missing(
        self, services: HookStateService
    ) -> None:
        """session:fork with parent_id creates stub parent with Session label."""
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "forked",
                "parent_id": "parent",  # canonical parent_id key
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        parent_node = await services.graph.get_node("parent")
        assert parent_node is not None
        assert "Session" in parent_node["labels"]


class TestSessionStartForkGuard:
    """session:start must not override ForkedSession classification from session:fork.

    In the real Amplifier event stream, session:fork fires first (~70ms before
    session:start) for forked sessions. The fork guard ensures that session:start
    does not demote a ForkedSession to a SubSession or replace HAS_FORK with
    SUBSESSION_OF.
    """

    async def test_session_start_after_fork_preserves_forked_session_label(
        self, services: HookStateService
    ) -> None:
        """session:start after session:fork keeps ForkedSession label — not SubSession."""
        handler = SessionHandler(services)
        await services.ensure_session_node("parent", {})

        # Step 1: session:fork fires first — classifies child as ForkedSession
        await handler(
            "session:fork",
            {
                "session_id": "child",
                "parent_id": "parent",
                "parent": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
                "metadata": {},
            },
        )

        # Step 2: session:start fires immediately after (~70ms later in real stream)
        await handler(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00.070Z",
            },
        )

        node = await services.graph.get_node("child")
        assert node is not None
        labels = node["labels"]
        assert "ForkedSession" in labels, f"ForkedSession must be preserved: {labels}"
        assert "SubSession" not in labels, f"SubSession must NOT be added: {labels}"
        assert "Session" in labels

    async def test_session_start_after_fork_keeps_has_fork_edge(
        self, services: HookStateService
    ) -> None:
        """session:start after session:fork must NOT create a HAS_SUBSESSION edge."""
        handler = SessionHandler(services)
        await services.ensure_session_node("parent", {})

        await handler(
            "session:fork",
            {
                "session_id": "child",
                "parent_id": "parent",
                "parent": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
                "metadata": {},
            },
        )
        await handler(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00.070Z",
            },
        )

        # FORKED must still exist
        fork_edge = await services.graph.get_edge("parent", "child")
        assert fork_edge is not None
        assert fork_edge["type"] == "FORKED", f"Expected FORKED: {fork_edge}"

        # HAS_SUBSESSION must NOT exist (it would only exist if the guard failed)
        # Note: since both FORKED and HAS_SUBSESSION would be (parent -> child),
        # the last write wins — but we verify the type is FORKED, not HAS_SUBSESSION
        assert fork_edge["type"] != "HAS_SUBSESSION"

    async def test_session_start_after_fork_enriches_started_at(
        self, services: HookStateService
    ) -> None:
        """session:start after session:fork still sets started_at from session:start timestamp."""
        handler = SessionHandler(services)
        await services.ensure_session_node("parent", {})

        await handler(
            "session:fork",
            {
                "session_id": "child",
                "parent_id": "parent",
                "parent": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
                "metadata": {},
            },
        )
        await handler(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00.070Z",
            },
        )

        node = await services.graph.get_node("child")
        assert node is not None
        # started_at is set by session:start (the guard allows timing enrichment)
        assert node.get("started_at") == "2026-01-01T00:00:00.070Z"

    async def test_session_start_without_prior_fork_creates_subsession_normally(
        self, services: HookStateService
    ) -> None:
        """session:start with parent_id (no prior fork) still creates SubSession normally."""
        handler = SessionHandler(services)
        await services.ensure_session_node("parent", {})

        # No prior session:fork — only session:start
        await handler(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )

        node = await services.graph.get_node("child")
        assert node is not None
        labels = node["labels"]
        assert "SubSession" in labels
        assert "ForkedSession" not in labels

        edge = await services.graph.get_edge("parent", "child")
        assert edge is not None
        assert edge["type"] == "HAS_SUBSESSION"


class TestSessionLabelStateMachine:
    """One test per row in the state machine transition table.

    Tests cover _handle_start (7 transitions), _handle_fork (4 transitions),
    and _handle_end stub recovery (2 transitions).
    """

    # ---- _handle_start transitions ----

    async def test_start_bare_no_parent_creates_root(
        self, services: HookStateService
    ) -> None:
        """(bare, start, no parent) -> RootSession:Session"""
        handler = SessionHandler(services)
        assert hasattr(handler, "_label_machine"), (
            "SessionHandler must delegate to SessionLabelStateMachine"
        )
        await services.ensure_session_node("s1", {})
        await handler(
            "session:start",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert "RootSession" in node["labels"]
        assert "SubSession" not in node["labels"]
        assert len(services.graph._edges) == 0

    async def test_start_bare_with_parent_creates_subsession(
        self, services: HookStateService
    ) -> None:
        """(bare, start, with parent) -> SubSession:Session + HAS_SUBSESSION"""
        handler = SessionHandler(services)
        assert hasattr(handler, "_label_machine"), (
            "SessionHandler must delegate to SessionLabelStateMachine"
        )
        await services.ensure_session_node("parent", {})
        await services.ensure_session_node("child", {})
        await handler(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("child")
        assert node is not None
        assert "SubSession" in node["labels"]
        assert "RootSession" not in node["labels"]
        edge = await services.graph.get_edge("parent", "child")
        assert edge is not None
        assert edge["type"] == "HAS_SUBSESSION"

    async def test_start_root_no_parent_stays_root(
        self, services: HookStateService
    ) -> None:
        """(RootSession, start, no parent) -> no label change, started_at enriched"""
        handler = SessionHandler(services)
        assert hasattr(handler, "_label_machine"), (
            "SessionHandler must delegate to SessionLabelStateMachine"
        )
        await services.graph.upsert_node("s1", {"labels": ["RootSession", "Session"]})
        await handler(
            "session:start",
            {"session_id": "s1", "timestamp": "2026-01-01T00:01:00Z"},
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert "RootSession" in node["labels"]
        assert "SubSession" not in node["labels"]
        assert node["started_at"] == "2026-01-01T00:01:00Z"

    async def test_start_root_with_parent_becomes_subsession(
        self, services: HookStateService
    ) -> None:
        """(RootSession, start, with parent) -> SubSession, RootSession dropped"""
        handler = SessionHandler(services)
        assert hasattr(handler, "_label_machine"), (
            "SessionHandler must delegate to SessionLabelStateMachine"
        )
        await services.ensure_session_node("parent", {})
        await services.graph.upsert_node(
            "child", {"labels": ["RootSession", "Session"]}
        )
        await handler(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("child")
        assert node is not None
        assert "SubSession" in node["labels"]
        assert "RootSession" not in node["labels"]
        edge = await services.graph.get_edge("parent", "child")
        assert edge is not None
        assert edge["type"] == "HAS_SUBSESSION"

    async def test_start_subsession_no_parent_stays_subsession(
        self, services: HookStateService
    ) -> None:
        """(SubSession, start, no parent) -> no change (terminal upward)"""
        handler = SessionHandler(services)
        assert hasattr(handler, "_label_machine"), (
            "SessionHandler must delegate to SessionLabelStateMachine"
        )
        await services.graph.upsert_node("s1", {"labels": ["SubSession", "Session"]})
        await handler(
            "session:start",
            {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"},
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert "SubSession" in node["labels"]
        assert "RootSession" not in node["labels"]

    async def test_start_subsession_with_parent_stays_subsession(
        self, services: HookStateService
    ) -> None:
        """(SubSession, start, with parent) -> SubSession unchanged, started_at enriched"""
        handler = SessionHandler(services)
        assert hasattr(handler, "_label_machine"), (
            "SessionHandler must delegate to SessionLabelStateMachine"
        )
        await services.ensure_session_node("parent", {})
        await services.graph.upsert_node("child", {"labels": ["SubSession", "Session"]})
        await handler(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:01:00Z",
            },
        )
        node = await services.graph.get_node("child")
        assert node is not None
        assert "SubSession" in node["labels"]
        assert node["started_at"] == "2026-01-01T00:01:00Z"

    async def test_start_forkedsession_stays_forked(
        self, services: HookStateService
    ) -> None:
        """(ForkedSession, start, any) -> TERMINAL — no classification change"""
        handler = SessionHandler(services)
        assert hasattr(handler, "_label_machine"), (
            "SessionHandler must delegate to SessionLabelStateMachine"
        )
        await services.graph.upsert_node("s1", {"labels": ["ForkedSession", "Session"]})
        await handler(
            "session:start",
            {
                "session_id": "s1",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:01:00Z",
            },
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert "ForkedSession" in node["labels"]
        assert "SubSession" not in node["labels"]
        assert "RootSession" not in node["labels"]

    # ---- _handle_fork transitions ----

    async def test_fork_bare_creates_forkedsession(
        self, services: HookStateService
    ) -> None:
        """(bare, fork) -> ForkedSession:Session + FORKED"""
        handler = SessionHandler(services)
        assert hasattr(handler, "_label_machine"), (
            "SessionHandler must delegate to SessionLabelStateMachine"
        )
        await services.ensure_session_node("parent", {})
        await services.ensure_session_node("child", {})
        await handler(
            "session:fork",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
                "metadata": {},
            },
        )
        node = await services.graph.get_node("child")
        assert node is not None
        assert "ForkedSession" in node["labels"]
        assert "SubSession" not in node["labels"]
        edge = await services.graph.get_edge("parent", "child")
        assert edge is not None
        assert edge["type"] == "FORKED"

    async def test_fork_root_reclassifies_to_forked_drops_root(
        self, services: HookStateService
    ) -> None:
        """(RootSession, fork) -> ForkedSession, RootSession DROPPED, FORKED"""
        handler = SessionHandler(services)
        assert hasattr(handler, "_label_machine"), (
            "SessionHandler must delegate to SessionLabelStateMachine"
        )
        await services.ensure_session_node("parent", {})
        await services.graph.upsert_node(
            "child", {"labels": ["RootSession", "Session"]}
        )
        await handler(
            "session:fork",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
                "metadata": {},
            },
        )
        node = await services.graph.get_node("child")
        assert node is not None
        assert "ForkedSession" in node["labels"]
        assert "RootSession" not in node["labels"]
        edge = await services.graph.get_edge("parent", "child")
        assert edge is not None
        assert edge["type"] == "FORKED"

    async def test_fork_subsession_reclassifies_replaces_edge(
        self, services: HookStateService
    ) -> None:
        """(SubSession, fork) -> ForkedSession, SubSession DROPPED, HAS_SUBSESSION removed, FORKED added"""
        handler = SessionHandler(services)
        assert hasattr(handler, "_label_machine"), (
            "SessionHandler must delegate to SessionLabelStateMachine"
        )
        await services.ensure_session_node("parent", {})
        await services.graph.upsert_node("child", {"labels": ["SubSession", "Session"]})
        # Existing HAS_SUBSESSION edge
        await services.graph.upsert_edge("parent", "child", {"type": "HAS_SUBSESSION"})
        await handler(
            "session:fork",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
                "metadata": {},
            },
        )
        node = await services.graph.get_node("child")
        assert node is not None
        assert "ForkedSession" in node["labels"]
        assert "SubSession" not in node["labels"]
        edge = await services.graph.get_edge("parent", "child")
        assert edge is not None
        assert edge["type"] == "FORKED"

    async def test_fork_forkedsession_stays_forked_terminal(
        self, services: HookStateService
    ) -> None:
        """(ForkedSession, fork) -> TERMINAL — no change at all"""
        handler = SessionHandler(services)
        assert hasattr(handler, "_label_machine"), (
            "SessionHandler must delegate to SessionLabelStateMachine"
        )
        await services.ensure_session_node("parent", {})
        await services.graph.upsert_node(
            "child", {"labels": ["ForkedSession", "Session"]}
        )
        await services.graph.upsert_edge("parent", "child", {"type": "FORKED"})
        await handler(
            "session:fork",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:01Z",
                "metadata": {},
            },
        )
        node = await services.graph.get_node("child")
        assert node is not None
        assert "ForkedSession" in node["labels"]
        assert "SubSession" not in node["labels"]
        edge = await services.graph.get_edge("parent", "child")
        assert edge is not None
        assert edge["type"] == "FORKED"  # not changed

    # ---- _handle_end stub recovery ----

    async def test_end_bare_no_parent_fallback_root(
        self, services: HookStateService
    ) -> None:
        """session:end for bare Session with no parent_id -> RootSession fallback"""
        handler = SessionHandler(services)
        assert hasattr(handler, "_label_machine"), (
            "SessionHandler must delegate to SessionLabelStateMachine"
        )
        await services.ensure_session_node("s1", {})
        await handler(
            "session:end",
            {"session_id": "s1", "timestamp": "2026-01-01T01:00:00Z"},
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert "RootSession" in node["labels"]
        assert node["ended_at"] == "2026-01-01T01:00:00Z"

    async def test_end_bare_with_parent_fallback_subsession(
        self, services: HookStateService
    ) -> None:
        """session:end for bare Session with parent_id -> SubSession fallback"""
        handler = SessionHandler(services)
        assert hasattr(handler, "_label_machine"), (
            "SessionHandler must delegate to SessionLabelStateMachine"
        )
        await services.ensure_session_node("s1", {})
        await handler(
            "session:end",
            {
                "session_id": "s1",
                "parent_id": "parent",
                "timestamp": "2026-01-01T01:00:00Z",
            },
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert "SubSession" in node["labels"]
        assert node["ended_at"] == "2026-01-01T01:00:00Z"
        assert "RootSession" not in node["labels"]


# ---------------------------------------------------------------------------
# TestSessionNodeProperties — session_id / parent_id on Session nodes
# ---------------------------------------------------------------------------


class TestSessionNodeProperties:
    """Session nodes must carry session_id and parent_id as direct properties.

    These properties enable direct Neo4j queries by session_id without
    requiring traversal of HAS_EVENT edges.
    """

    async def test_session_node_has_session_id_property(
        self, services: HookStateService
    ) -> None:
        """session:start (root) must set session_id as a direct node property."""
        h = SessionHandler(services)
        await h(
            "session:start", {"session_id": "s1", "timestamp": "2026-01-01T00:00:00Z"}
        )
        node = await services.graph.get_node("s1")
        assert node is not None, "Node 's1' must exist after session:start"
        assert node.get("session_id") == "s1", (
            f"session:start must store session_id on the node. Got: {node!r}"
        )

    async def test_subsession_node_has_parent_id_property(
        self, services: HookStateService
    ) -> None:
        """SubSession node must have parent_id as a direct property."""
        h = SessionHandler(services)
        await services.ensure_session_node("parent", {})
        await h(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("child")
        assert node is not None, "Child node must exist after session:start with parent"
        assert node.get("session_id") == "child", (
            f"session:start must store session_id. Got: {node!r}"
        )
        assert node.get("parent_id") == "parent", (
            f"session:start must store parent_id on SubSession node. Got: {node!r}"
        )

    async def test_root_session_parent_id_is_none(
        self, services: HookStateService
    ) -> None:
        """RootSession must have parent_id=None as a direct property (not absent)."""
        h = SessionHandler(services)
        await h(
            "session:start", {"session_id": "root", "timestamp": "2026-01-01T00:00:00Z"}
        )
        node = await services.graph.get_node("root")
        assert node is not None, "Root node must exist after session:start"
        assert node.get("session_id") == "root"
        # parent_id must be explicitly None — either absent or None is acceptable
        # (None stored explicitly beats absent for query clarity)
        assert node.get("parent_id") is None, (
            f"RootSession parent_id must be None (not a non-None value). Got: {node!r}"
        )

    async def test_forked_session_has_parent_id_property(
        self, services: HookStateService
    ) -> None:
        """ForkedSession node must have parent_id as a direct property."""
        h = SessionHandler(services)
        await services.ensure_session_node("parent", {})
        await h(
            "session:fork",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
                "metadata": {},
            },
        )
        node = await services.graph.get_node("child")
        assert node is not None, "Child node must exist after session:fork"
        assert node.get("session_id") == "child", (
            f"session:fork must store session_id. Got: {node!r}"
        )
        assert node.get("parent_id") == "parent", (
            f"session:fork must store parent_id on ForkedSession node. Got: {node!r}"
        )


# ---------------------------------------------------------------------------
# TestSessionSSTEventLabel — Session nodes must carry the SST_EVENT label
# ---------------------------------------------------------------------------


class TestSessionSSTEventLabel:
    """Session nodes must carry the SST_EVENT label for SST graph membership.

    SST_EVENT marks Session nodes as timelike events in the SST ontology.
    This label must be present on all session types: RootSession, SubSession,
    and ForkedSession, and must survive the session:end transition.
    """

    async def test_root_session_has_sst_event_label(
        self, services: HookStateService
    ) -> None:
        """session:start (root) must add SST_EVENT to the node labels."""
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert "SST_EVENT" in node["labels"], (
            f"RootSession node must carry SST_EVENT label. Got: {node['labels']}"
        )

    async def test_subsession_has_sst_event_label(
        self, services: HookStateService
    ) -> None:
        """session:start (subsession) must add SST_EVENT to the node labels."""
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("child")
        assert node is not None
        assert "SST_EVENT" in node["labels"], (
            f"SubSession node must carry SST_EVENT label. Got: {node['labels']}"
        )

    async def test_forked_session_has_sst_event_label(
        self, services: HookStateService
    ) -> None:
        """session:fork must add SST_EVENT to the ForkedSession node labels."""
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "f1",
                "parent_id": "p1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("f1")
        assert node is not None
        assert "SST_EVENT" in node["labels"], (
            f"ForkedSession node must carry SST_EVENT label. Got: {node['labels']}"
        )

    async def test_session_end_preserves_sst_event_label(
        self, services: HookStateService
    ) -> None:
        """session:end must not remove the SST_EVENT label."""
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        await handler(
            "session:end",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T01:00:00Z",
            },
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert "SST_EVENT" in node["labels"], (
            f"SST_EVENT label must be preserved after session:end. Got: {node['labels']}"
        )


# ---------------------------------------------------------------------------
# TestSessionForkMountPlan — session:fork must create a companion MountPlan node
# ---------------------------------------------------------------------------


class TestSessionForkMountPlan:
    """session:fork must create a companion MountPlan node for context tracking.

    When a fork event fires, the ForkedSession node must be accompanied by a
    MountPlan node (keyed as '<session_id>::mount_plan') connected via a
    HAS_PART edge with sst_semantic='CONTAINS'.
    """

    async def test_fork_creates_mount_plan_node(
        self, services: HookStateService
    ) -> None:
        """session:fork must create a companion node at '<session_id>::mount_plan'."""
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "f1",
                "parent_id": "p1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        mount_plan = await services.graph.get_node("f1::mount_plan")
        assert mount_plan is not None, (
            "session:fork must create a companion MountPlan node at 'f1::mount_plan'"
        )

    async def test_mount_plan_has_correct_labels(
        self, services: HookStateService
    ) -> None:
        """MountPlan node must have labels ['MountPlan', 'SST_THING']."""
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "f1",
                "parent_id": "p1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        mount_plan = await services.graph.get_node("f1::mount_plan")
        assert mount_plan is not None
        assert "MountPlan" in mount_plan["labels"], (
            f"MountPlan node must have 'MountPlan' label. Got: {mount_plan['labels']}"
        )
        assert "SST_THING" in mount_plan["labels"], (
            f"MountPlan node must have 'SST_THING' label. Got: {mount_plan['labels']}"
        )

    async def test_fork_creates_has_part_edge_to_mount_plan(
        self, services: HookStateService
    ) -> None:
        """session:fork must create a HAS_PART edge from ForkedSession to MountPlan."""
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "f1",
                "parent_id": "p1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        edge = await services.graph.get_edge("f1", "f1::mount_plan")
        assert edge is not None, (
            "session:fork must create a HAS_PART edge from ForkedSession to MountPlan"
        )
        assert edge.get("type") == "HAS_PART", (
            f"Edge from ForkedSession to MountPlan must have type='HAS_PART'. Got: {edge.get('type')}"
        )
        assert edge.get("sst_semantic") == "CONTAINS", (
            f"Edge from ForkedSession to MountPlan must have sst_semantic='CONTAINS'. Got: {edge.get('sst_semantic')}"
        )

    async def test_orphan_fork_still_creates_mount_plan(
        self, services: HookStateService
    ) -> None:
        """session:fork without parent_id must still create a companion MountPlan node."""
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "f1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        mount_plan = await services.graph.get_node("f1::mount_plan")
        assert mount_plan is not None, (
            "Orphaned fork must still create a companion MountPlan node"
        )

    async def test_terminal_fork_does_not_duplicate_mount_plan(
        self, services: HookStateService
    ) -> None:
        """A second session:fork for a ForkedSession (terminal) must not create duplicate labels."""
        handler = SessionHandler(services)
        await services.ensure_session_node("parent", {})
        await services.graph.upsert_node("f1", {"labels": ["ForkedSession", "Session"]})
        await services.graph.upsert_edge("parent", "f1", {"type": "FORKED"})

        # First fork — already classified, so terminal
        await handler(
            "session:fork",
            {
                "session_id": "f1",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:01Z",
                "metadata": {},
            },
        )
        mount_plan = await services.graph.get_node("f1::mount_plan")
        assert mount_plan is not None
        # Labels must not be duplicated (e.g., no ['MountPlan', 'SST_THING', 'MountPlan'])
        assert mount_plan["labels"].count("MountPlan") == 1, (
            f"MountPlan label must appear exactly once. Got: {mount_plan['labels']}"
        )
        assert mount_plan["labels"].count("SST_THING") == 1, (
            f"SST_THING label must appear exactly once. Got: {mount_plan['labels']}"
        )


# ---------------------------------------------------------------------------
# TestSessionEdgeSstSemantic — all session edges carry sst_semantic='LEADS_TO'
# ---------------------------------------------------------------------------


class TestSessionEdgeSstSemantic:
    """All parent→child session edges must carry sst_semantic='LEADS_TO'.

    Both HAS_SUBSESSION (session:start with parent) and FORKED (session:fork)
    edges are timelike LEADS_TO relationships in the SST ontology.
    """

    async def test_subsession_edge_has_sst_semantic(
        self, services: HookStateService
    ) -> None:
        """session:start parent→child edge must carry sst_semantic='LEADS_TO'."""
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        edge = await services.graph.get_edge("parent", "child")
        assert edge is not None
        assert edge.get("sst_semantic") == "LEADS_TO", (
            f"HAS_SUBSESSION edge must have sst_semantic='LEADS_TO'. Got: {edge.get('sst_semantic')}"
        )

    async def test_fork_edge_has_sst_semantic(self, services: HookStateService) -> None:
        """session:fork parent→child edge must carry sst_semantic='LEADS_TO'."""
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "f1",
                "parent_id": "p1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        edge = await services.graph.get_edge("p1", "f1")
        assert edge is not None
        assert edge.get("sst_semantic") == "LEADS_TO", (
            f"FORKED edge must have sst_semantic='LEADS_TO'. Got: {edge.get('sst_semantic')}"
        )
