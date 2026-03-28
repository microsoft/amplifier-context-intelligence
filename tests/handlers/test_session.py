"""Tests for SessionHandler — session lifecycle graph mutations.

Rewrites the test file to assert:
- New labels: RootSession/SubSession/ForkedSession
- Parent→child edge direction for SUBSESSION_OF
- Fork uses data['parent_id'] (canonical), not legacy 'parent'
- No cursor tests at all
- session:end does not store status from data (always 'completed')
"""

from __future__ import annotations

import pytest

from context_intelligence_server.handlers.session import SessionHandler
from context_intelligence_server.services import HookStateService


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
        assert edge["type"] == "HAS_FORK", (
            f"Fork edge must be HAS_FORK, got: {edge.get('type')}"
        )
        assert edge["type"] != "SUBSESSION_OF", "Fork edge must NOT be SUBSESSION_OF"

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
        """Fork without parent_id creates ForkedSession+Session node, no edge, no RootSession."""
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
        # Orphaned forks must NOT create any SUBSESSION_OF edge — the GraphState._edges
        # dict is checked directly because no parent node ID exists to query against.
        assert len(services.graph._edges) == 0, (
            "No edge should be created for an orphaned fork (no parent_id)"
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

    async def test_start_subsession_edge_type_is_subsession_of(
        self, services: HookStateService
    ) -> None:
        """session:start parent→child edge must have type='SUBSESSION_OF'."""
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
        assert edge.get("type") == "SUBSESSION_OF"

    async def test_fork_edge_type_is_has_fork(self, services: HookStateService) -> None:
        """session:fork parent→child edge must have type='HAS_FORK' (not SUBSESSION_OF)."""
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
        assert edge.get("type") == "HAS_FORK", (
            f"Fork edge must be HAS_FORK, got: {edge.get('type')}"
        )
        assert edge.get("type") != "SUBSESSION_OF", (
            "Fork edge must NOT be SUBSESSION_OF — forks are not subsessions"
        )


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
        """session:start after session:fork must NOT create a SUBSESSION_OF edge."""
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

        # HAS_FORK must still exist
        fork_edge = await services.graph.get_edge("parent", "child")
        assert fork_edge is not None
        assert fork_edge["type"] == "HAS_FORK", f"Expected HAS_FORK: {fork_edge}"

        # SUBSESSION_OF must NOT exist (it would only exist if the guard failed)
        # Note: since both HAS_FORK and SUBSESSION_OF would be (parent -> child),
        # the last write wins — but we verify the type is HAS_FORK, not SUBSESSION_OF
        assert fork_edge["type"] != "SUBSESSION_OF"

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
        assert edge["type"] == "SUBSESSION_OF"


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
        h = SessionHandler(services)
        await services.ensure_session_node("s1", {})
        await h(
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
        """(bare, start, with parent) -> SubSession:Session + SUBSESSION_OF"""
        h = SessionHandler(services)
        await services.ensure_session_node("parent", {})
        await services.ensure_session_node("child", {})
        await h(
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
        assert edge["type"] == "SUBSESSION_OF"

    async def test_start_root_no_parent_stays_root(
        self, services: HookStateService
    ) -> None:
        """(RootSession, start, no parent) -> no label change, started_at enriched"""
        h = SessionHandler(services)
        await services.graph.upsert_node("s1", {"labels": ["RootSession", "Session"]})
        await h(
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
        h = SessionHandler(services)
        await services.ensure_session_node("parent", {})
        await services.graph.upsert_node(
            "child", {"labels": ["RootSession", "Session"]}
        )
        await h(
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
        assert edge["type"] == "SUBSESSION_OF"

    async def test_start_subsession_no_parent_stays_subsession(
        self, services: HookStateService
    ) -> None:
        """(SubSession, start, no parent) -> no change (terminal upward)"""
        h = SessionHandler(services)
        await services.graph.upsert_node("s1", {"labels": ["SubSession", "Session"]})
        await h(
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
        h = SessionHandler(services)
        await services.ensure_session_node("parent", {})
        await services.graph.upsert_node("child", {"labels": ["SubSession", "Session"]})
        await h(
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
        h = SessionHandler(services)
        await services.graph.upsert_node("s1", {"labels": ["ForkedSession", "Session"]})
        await h(
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
        """(bare, fork) -> ForkedSession:Session + HAS_FORK"""
        h = SessionHandler(services)
        await services.ensure_session_node("parent", {})
        await services.ensure_session_node("child", {})
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
        assert node is not None
        assert "ForkedSession" in node["labels"]
        assert "SubSession" not in node["labels"]
        edge = await services.graph.get_edge("parent", "child")
        assert edge is not None
        assert edge["type"] == "HAS_FORK"

    async def test_fork_root_reclassifies_to_forked_drops_root(
        self, services: HookStateService
    ) -> None:
        """(RootSession, fork) -> ForkedSession, RootSession DROPPED, HAS_FORK"""
        h = SessionHandler(services)
        await services.ensure_session_node("parent", {})
        await services.graph.upsert_node(
            "child", {"labels": ["RootSession", "Session"]}
        )
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
        assert node is not None
        assert "ForkedSession" in node["labels"]
        assert "RootSession" not in node["labels"]
        edge = await services.graph.get_edge("parent", "child")
        assert edge is not None
        assert edge["type"] == "HAS_FORK"

    async def test_fork_subsession_reclassifies_replaces_edge(
        self, services: HookStateService
    ) -> None:
        """(SubSession, fork) -> ForkedSession, SubSession DROPPED, SUBSESSION_OF removed, HAS_FORK added"""
        h = SessionHandler(services)
        await services.ensure_session_node("parent", {})
        await services.graph.upsert_node("child", {"labels": ["SubSession", "Session"]})
        # Existing SUBSESSION_OF edge
        await services.graph.upsert_edge("parent", "child", {"type": "SUBSESSION_OF"})
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
        assert node is not None
        assert "ForkedSession" in node["labels"]
        assert "SubSession" not in node["labels"]
        edge = await services.graph.get_edge("parent", "child")
        assert edge is not None
        assert edge["type"] == "HAS_FORK"

    async def test_fork_forkedsession_stays_forked_terminal(
        self, services: HookStateService
    ) -> None:
        """(ForkedSession, fork) -> TERMINAL — no change at all"""
        h = SessionHandler(services)
        await services.ensure_session_node("parent", {})
        await services.graph.upsert_node(
            "child", {"labels": ["ForkedSession", "Session"]}
        )
        await services.graph.upsert_edge("parent", "child", {"type": "HAS_FORK"})
        await h(
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
        assert edge["type"] == "HAS_FORK"  # not changed

    # ---- _handle_end stub recovery ----

    async def test_end_bare_no_parent_fallback_root(
        self, services: HookStateService
    ) -> None:
        """session:end for bare Session with no parent_id -> RootSession fallback"""
        h = SessionHandler(services)
        await services.ensure_session_node("s1", {})
        await h(
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
        h = SessionHandler(services)
        await services.ensure_session_node("s1", {})
        await h(
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
        assert "RootSession" not in node["labels"]
