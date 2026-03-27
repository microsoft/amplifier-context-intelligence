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
