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
        assert "Root" in node["labels"]
        assert "Subsession" not in node["labels"]

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
        assert "Subsession" in node["labels"]
        assert "Root" not in node["labels"]

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
        assert "Root" in node["labels"]
        assert "Subsession" not in node["labels"]

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
        assert "Root" in node["labels"]
        assert "Subsession" not in node["labels"]


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
        assert "Subsession" in node["labels"]
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
        assert "Root" in node["labels"]
        assert "ForkedSession" in node["labels"]
        assert "Subsession" not in node["labels"]

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
        assert "Root" in node["labels"]

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

    async def test_session_end_removes_cursors(
        self, services: HookStateService
    ) -> None:
        handler = SessionHandler(services)
        # Prime cursors and mutate to prove they existed
        cursors = services.get_cursors("s1")
        cursors.prompt_preview = "some preview"

        await handler(
            "session:end",
            {"session_id": "s1", "timestamp": "2026-01-01T01:00:00Z"},
        )

        # After session:end, cursors should have been removed;
        # a fresh get_cursors returns default values
        fresh = services.get_cursors("s1")
        assert fresh.prompt_preview == ""

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
