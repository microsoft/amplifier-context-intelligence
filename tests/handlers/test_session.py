"""Tests for SessionHandler — session lifecycle graph mutations.

Ports and adapts the bundle's test_session_handler.py tests for the
server-side implementation, which uses the flat-dict GraphState API
(no nested 'properties' key).
"""

from __future__ import annotations

import json

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
    """session:start creates Root or Subsession nodes."""

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

    async def test_root_session_properties(self, services: HookStateService) -> None:
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
        assert node["started_at"] == "2026-01-01T00:00:00Z"
        assert node["status"] == "running"
        assert node["metadata"] == {"key": "val"}

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
        # No parent — no edge should be created
        edge = await services.graph.get_edge("s1", "")
        assert edge is None

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

    async def test_subsession_properties(self, services: HookStateService) -> None:
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
        assert node["started_at"] == "2026-01-01T00:00:00Z"
        assert node["status"] == "running"
        assert node["metadata"] == {"m": 1}

    async def test_subsession_edge_created(self, services: HookStateService) -> None:
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        edge = await services.graph.get_edge("child", "parent")
        assert edge is not None
        assert edge["occurred_at"] == "2026-01-01T00:00:00Z"

    async def test_missing_metadata_defaults_to_empty_dict(
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
        assert node["metadata"] == {}

    async def test_session_start_stores_data_as_json(
        self, services: HookStateService
    ) -> None:
        handler = SessionHandler(services)
        event_data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T00:00:00Z",
            "metadata": {"key": "val"},
        }
        await handler("session:start", event_data)
        node = await services.graph.get_node("s1")
        assert node is not None
        assert "data" in node
        stored = json.loads(node["data"])
        assert stored["session_id"] == "s1"


class TestSessionStartParentIdEdgeCases:
    """Falsy parent_id values must produce Root (not Subsession) nodes."""

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
    """session:fork creates ForkedSession nodes."""

    async def test_fork_labels_with_parent(self, services: HookStateService) -> None:
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "f1",
                "parent": "p1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("f1")
        assert node is not None
        assert "Session" in node["labels"]
        assert "SubSession" in node["labels"]
        assert "ForkedSession" in node["labels"]
        assert node["status"] == "running"

    async def test_fork_edge_created(self, services: HookStateService) -> None:
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "f1",
                "parent": "p1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        edge = await services.graph.get_edge("f1", "p1")
        assert edge is not None
        assert edge["occurred_at"] == "2026-01-01T00:00:00Z"

    async def test_fork_missing_parent_degrades_to_root(
        self, services: HookStateService
    ) -> None:
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
        assert "RootSession" in node["labels"]
        assert "ForkedSession" in node["labels"]
        assert "SubSession" not in node["labels"]

    async def test_session_fork_stores_data_as_json(
        self, services: HookStateService
    ) -> None:
        handler = SessionHandler(services)
        event_data = {
            "session_id": "f1",
            "parent": "p1",
            "timestamp": "2026-01-01T00:00:00Z",
        }
        await handler("session:fork", event_data)
        node = await services.graph.get_node("f1")
        assert node is not None
        assert "data" in node
        stored = json.loads(node["data"])
        assert stored["parent"] == "p1"


class TestSessionEnd:
    """session:end merges ended_at/status and removes cursors."""

    async def test_end_merges_properties(self, services: HookStateService) -> None:
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
        assert node["status"] == "completed"

    async def test_end_preserves_existing_labels(
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

    async def test_end_stores_data_session_end_json(
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
        end_data = {
            "session_id": "s1",
            "timestamp": "2026-01-01T01:00:00Z",
            "status": "completed",
        }
        await handler("session:end", end_data)
        node = await services.graph.get_node("s1")
        assert node is not None
        assert "data_session_end" in node
        stored = json.loads(node["data_session_end"])
        assert stored["status"] == "completed"

    async def test_session_end_no_cursor_attribute(
        self, services: HookStateService
    ) -> None:
        """session:end no longer removes cursors (SessionCursors removed)."""
        handler = SessionHandler(services)
        # Verify HookStateService has no cursor methods
        assert not hasattr(services, "get_cursors")
        assert not hasattr(services, "remove_cursors")
        # session:end should still work without cursors
        await handler(
            "session:end",
            {"session_id": "s1", "timestamp": "2026-01-01T01:00:00Z"},
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert node["status"] == "completed"

    async def test_session_end_status_from_data(
        self, services: HookStateService
    ) -> None:
        handler = SessionHandler(services)
        await handler(
            "session:end",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T01:00:00Z",
                "status": "aborted",
            },
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert node["status"] == "aborted"


class TestSessionResumeNotClaimed:
    """session:resume must NOT be in SessionHandler.handled_events."""

    def test_session_handler_does_not_claim_resume(self) -> None:
        assert "session:resume" not in SessionHandler.handled_events

    def test_session_handler_claims_start_fork_end(self) -> None:
        assert "session:start" in SessionHandler.handled_events
        assert "session:fork" in SessionHandler.handled_events
        assert "session:end" in SessionHandler.handled_events


class TestSessionEdgeTypes:
    """Edges created by SessionHandler must carry explicit semantic 'type' keys."""

    async def test_start_subsession_edge_type_is_subsession_of(
        self, services: HookStateService
    ) -> None:
        """session:start child→parent edge must have type='SUBSESSION_OF'."""
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        edge = await services.graph.get_edge("child", "parent")
        assert edge is not None
        assert edge.get("type") == "SUBSESSION_OF"

    async def test_fork_edge_type_is_subsession_of(
        self, services: HookStateService
    ) -> None:
        """session:fork child→parent edge must have type='SUBSESSION_OF'."""
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "fork1",
                "parent": "parent1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        edge = await services.graph.get_edge("fork1", "parent1")
        assert edge is not None
        assert edge.get("type") == "SUBSESSION_OF"


class TestLateParentDiscovery:
    """Late parent discovery creates stub parent nodes when parent doesn't exist yet."""

    async def test_parent_stub_created_when_missing(
        self, services: HookStateService
    ) -> None:
        """session:start with parent_id creates stub parent with Session+Root labels."""
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
        assert "RootSession" in parent_node["labels"]

    async def test_parent_already_exists_no_duplicate(
        self, services: HookStateService
    ) -> None:
        """Existing parent's metadata is preserved (not overwritten)."""
        # Pre-create parent node with metadata
        await services.graph.upsert_node(
            "parent",
            {
                "labels": ["Session", "RootSession"],
                "status": "running",
                "metadata": {"original": "data"},
            },
        )
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
        assert parent_node["metadata"] == {"original": "data"}

    async def test_child_label_flipped_to_subsession(
        self, services: HookStateService
    ) -> None:
        """Child gets Subsession label (not Root) when parent_id present."""
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        child_node = await services.graph.get_node("child")
        assert child_node is not None
        assert "SubSession" in child_node["labels"]
        assert "RootSession" not in child_node["labels"]

    async def test_fork_parent_stub_created(self, services: HookStateService) -> None:
        """session:fork with parent creates stub parent node."""
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "forked",
                "parent": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        parent_node = await services.graph.get_node("parent")
        assert parent_node is not None
        assert "Session" in parent_node["labels"]
        assert "RootSession" in parent_node["labels"]

    async def test_subsession_of_edge_created(self, services: HookStateService) -> None:
        """SUBSESSION_OF edge exists from child to parent with correct type."""
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "child",
                "parent_id": "parent",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        edge = await services.graph.get_edge("child", "parent")
        assert edge is not None
        assert edge.get("type") == "SUBSESSION_OF"


class TestRecipeSessionFlag:
    """Recipe session behavior — is_recipe_session cursor flag removed (SessionCursors removed).
    
    Tests verify session:start/fork still creates session nodes correctly with metadata
    but no longer track recipe_session state via cursors.
    """

    async def test_start_with_recipe_name_creates_node(
        self, services: HookStateService
    ) -> None:
        """session:start with metadata.recipe_name still creates session node."""
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "metadata": {"recipe_name": "my-recipe"},
            },
        )
        node = await services.graph.get_node("s1")
        assert node is not None
        assert node["metadata"] == {"recipe_name": "my-recipe"}

    async def test_start_without_metadata_creates_node(
        self, services: HookStateService
    ) -> None:
        """session:start without metadata still creates session node."""
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

    async def test_start_with_metadata_no_recipe_name_creates_node(
        self, services: HookStateService
    ) -> None:
        """session:start with metadata but no recipe_name still creates session node."""
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "metadata": {"other_key": "value"},
            },
        )
        node = await services.graph.get_node("s1")
        assert node is not None

    async def test_start_with_empty_recipe_name_creates_node(
        self, services: HookStateService
    ) -> None:
        """session:start with empty recipe_name still creates session node."""
        handler = SessionHandler(services)
        await handler(
            "session:start",
            {
                "session_id": "s1",
                "timestamp": "2026-01-01T00:00:00Z",
                "metadata": {"recipe_name": ""},
            },
        )
        node = await services.graph.get_node("s1")
        assert node is not None

    async def test_fork_with_recipe_name_creates_node(
        self, services: HookStateService
    ) -> None:
        """session:fork with metadata.recipe_name still creates session node."""
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "f1",
                "parent": "p1",
                "timestamp": "2026-01-01T00:00:00Z",
                "metadata": {"recipe_name": "my-recipe"},
            },
        )
        node = await services.graph.get_node("f1")
        assert node is not None

    async def test_fork_without_recipe_name_creates_node(
        self, services: HookStateService
    ) -> None:
        """session:fork without recipe_name still creates session node."""
        handler = SessionHandler(services)
        await handler(
            "session:fork",
            {
                "session_id": "f1",
                "parent": "p1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        node = await services.graph.get_node("f1")
        assert node is not None
