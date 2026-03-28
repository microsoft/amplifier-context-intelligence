"""Tests for HookConfig, GraphState, and HookStateService in services.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from context_intelligence_server.services import (
    GraphState,
    HookConfig,
    HookStateService,
)


# ---------------------------------------------------------------------------
# HookConfig tests
# ---------------------------------------------------------------------------


def test_hook_config_empty():
    """HookConfig with empty config has empty exclude_events set."""
    cfg = HookConfig({})
    assert cfg.exclude_events == set()


def test_hook_config_with_exclude():
    """HookConfig with exclude list returns the correct set of patterns."""
    cfg = HookConfig({"exclude_events": ["session-naming:*", "tool-call:read_file"]})
    assert cfg.exclude_events == {"session-naming:*", "tool-call:read_file"}


def test_hook_config_is_excluded_exact():
    """is_excluded returns True for exact event name matches."""
    cfg = HookConfig({"exclude_events": ["tool-call:read_file"]})
    assert cfg.is_excluded("tool-call:read_file") is True
    assert cfg.is_excluded("tool-call:write_file") is False


def test_hook_config_is_excluded_wildcard():
    """is_excluded uses fnmatch so 'session-naming:*' matches 'session-naming:foo'."""
    cfg = HookConfig({"exclude_events": ["session-naming:*"]})
    assert cfg.is_excluded("session-naming:foo") is True
    assert cfg.is_excluded("session-naming:bar") is True
    assert cfg.is_excluded("tool-call:something") is False


# ---------------------------------------------------------------------------
# GraphState tests
# ---------------------------------------------------------------------------


def test_graph_state_default_workspace():
    """GraphState() uses 'default' as the workspace."""
    state = GraphState()
    assert state.workspace == "default"


def test_graph_state_explicit_workspace():
    """GraphState accepts an explicit workspace at construction."""
    state = GraphState(workspace="my-project")
    assert state.workspace == "my-project"


def test_graph_state_workspace_settable():
    """GraphState.workspace can be updated after construction."""
    state = GraphState()
    state.workspace = "new-workspace"
    assert state.workspace == "new-workspace"


async def test_graph_state_upsert_node_creates():
    """upsert_node creates a node retrievable via get_node."""
    state = GraphState()
    await state.upsert_node("n1", {"name": "Alice", "labels": ["Person"]})
    node = await state.get_node("n1")
    assert node is not None
    assert node["name"] == "Alice"
    assert "Person" in node["labels"]


async def test_graph_state_upsert_node_merges():
    """upsert_node merges labels (union) and merges properties on subsequent calls."""
    state = GraphState()
    await state.upsert_node("n1", {"labels": ["Person"], "name": "Alice"})
    await state.upsert_node("n1", {"labels": ["User"], "age": 30})
    node = await state.get_node("n1")
    assert node is not None
    # Labels should be the union of both calls
    assert "Person" in node["labels"]
    assert "User" in node["labels"]
    # Properties should be merged — both name and age present
    assert node["name"] == "Alice"
    assert node["age"] == 30


async def test_graph_state_upsert_edge_creates():
    """upsert_edge creates an edge retrievable via get_edge."""
    state = GraphState()
    await state.upsert_edge("n1", "n2", {"type": "KNOWS", "since": 2020})
    edge = await state.get_edge("n1", "n2")
    assert edge is not None
    assert edge["type"] == "KNOWS"
    assert edge["since"] == 2020


async def test_graph_state_get_nonexistent_returns_none():
    """get_node and get_edge return None when the node/edge does not exist."""
    state = GraphState()
    assert await state.get_node("missing") is None
    assert await state.get_edge("a", "b") is None


async def test_graph_state_flush_close_noop():
    """flush and close are awaitable no-ops — no exception, no state change."""
    state = GraphState()
    await state.upsert_node("n1", {"name": "Alice"})
    await state.flush()  # must not raise
    await state.close()  # must not raise; internally calls flush
    # Node should still be accessible after flush/close (in-memory, no teardown)
    node = await state.get_node("n1")
    assert node is not None
    assert node["name"] == "Alice"


def test_graph_state_no_graph_forest_name():
    """GraphState must not expose graph_forest_name or _graph_forest_name."""
    state = GraphState()
    assert not hasattr(state, "graph_forest_name")
    assert not hasattr(state, "_graph_forest_name")


async def test_graph_state_get_node_returns_copy():
    """get_node returns a copy — assigning a key must not corrupt the internal buffer."""
    state = GraphState()
    await state.upsert_node("n1", {"name": "Alice", "labels": ["Person"]})
    node = await state.get_node("n1")
    assert node is not None
    # Mutate the returned dict at the top level
    node["name"] = "MUTATED"
    node["injected"] = "should-not-appear"
    # The buffer must be unaffected by top-level key reassignment
    stored = await state.get_node("n1")
    assert stored is not None
    assert stored["name"] == "Alice"
    assert "injected" not in stored


async def test_graph_state_get_edge_returns_copy():
    """get_edge returns a copy — mutating it must not corrupt the internal buffer."""
    state = GraphState()
    await state.upsert_edge("n1", "n2", {"type": "KNOWS", "weight": 1})
    edge = await state.get_edge("n1", "n2")
    assert edge is not None
    # Mutate the returned dict
    edge["type"] = "MUTATED"
    edge["extra"] = "injected"
    # The buffer must be unaffected
    stored = await state.get_edge("n1", "n2")
    assert stored is not None
    assert stored["type"] == "KNOWS"
    assert "extra" not in stored


# ---------------------------------------------------------------------------
# HookStateService tests
# ---------------------------------------------------------------------------


class TestHookStateService:
    """Tests for the server-side HookStateService (no coordinator dependency)."""

    def test_construction_default_workspace(self):
        """HookStateService defaults workspace to 'default' on the internal graph."""
        svc = HookStateService()
        assert svc.graph.workspace == "default"

    def test_construction_sets_workspace_on_graph(self):
        """HookStateService sets the provided workspace on the internal graph."""
        svc = HookStateService(workspace="test-workspace")
        assert svc.graph.workspace == "test-workspace"

    def test_injected_graph_store_workspace_overwrite(self):
        """When a graph_store is injected, its workspace is overwritten by the given workspace."""
        injected = GraphState(workspace="old-workspace")
        svc = HookStateService(workspace="new-workspace", graph_store=injected)
        assert svc.graph is injected
        assert svc.graph.workspace == "new-workspace"

    def test_no_coordinator_attribute(self):
        """HookStateService must not have coordinator or _forest_resolved attributes."""
        svc = HookStateService()
        assert not hasattr(svc, "coordinator")
        assert not hasattr(svc, "_forest_resolved")

    def test_blob_store_default_is_none(self):
        """blob_store defaults to None when not provided."""
        svc = HookStateService()
        assert svc.blob_store is None

    def test_blob_store_can_be_injected(self):
        """blob_store can be provided as a keyword argument."""
        sentinel = object()
        svc = HookStateService(blob_store=sentinel)
        assert svc.blob_store is sentinel

    def test_no_cursor_methods(self):
        """HookStateService must not have get_cursors, set_cursors, or remove_cursors methods."""
        svc = HookStateService()
        assert not hasattr(svc, "get_cursors")
        assert not hasattr(svc, "set_cursors")
        assert not hasattr(svc, "remove_cursors")

    def test_no_cursors_dict(self):
        """HookStateService must not have a _cursors attribute."""
        svc = HookStateService()
        assert not hasattr(svc, "_cursors")

    async def test_ensure_session_node_creates_root(self):
        """ensure_session_node creates a bare Session node (no RootSession) when no parent field is present.

        ensure_session_node is a safety net that creates a minimal session node.
        SessionHandler is the sole authority on session type labels (RootSession, SubSession, ForkedSession).
        """
        svc = HookStateService()
        await svc.ensure_session_node(
            "session-1", {"started_at": "2024-01-01T00:00:00"}
        )
        node = await svc.graph.get_node("session-1")
        assert node is not None
        assert "Session" in node["labels"]
        assert "RootSession" not in node["labels"]
        assert node["status"] == "running"

    async def test_ensure_session_node_is_idempotent(self):
        """ensure_session_node is a no-op when session_id was already processed."""
        svc = HookStateService()
        await svc.ensure_session_node(
            "session-1", {"started_at": "2024-01-01T00:00:00"}
        )
        # Manually modify the node after the first call
        await svc.graph.upsert_node("session-1", {"status": "modified"})
        # Second call must not overwrite the modified status
        await svc.ensure_session_node(
            "session-1", {"started_at": "2024-01-02T00:00:00"}
        )
        node = await svc.graph.get_node("session-1")
        assert node is not None
        assert node["status"] == "modified"

    async def test_ensure_session_node_no_subsession_label_when_parent_id_present(self):
        """ensure_session_node creates a bare Session node (no SubSession) even when parent_id is present.

        ensure_session_node is a safety net that creates a minimal session node.
        SessionHandler is the sole authority on session type labels (RootSession, SubSession, ForkedSession).
        """
        svc = HookStateService()
        await svc.ensure_session_node(
            "session-2",
            {"parent_id": "session-1", "started_at": "2024-01-01T00:00:00"},
        )
        node = await svc.graph.get_node("session-2")
        assert node is not None
        assert "Session" in node["labels"]
        assert "SubSession" not in node["labels"]
        assert node["status"] == "running"

    async def test_ensure_session_node_no_subsession_label_when_parent_field_present(self):
        """ensure_session_node creates a bare Session node even when 'parent' field is present.

        The safety-net node carries only ['Session']; type labels are added by SessionHandler.
        """
        svc = HookStateService()
        await svc.ensure_session_node(
            "session-3",
            {"parent": "session-1", "started_at": "2024-01-01T00:00:00"},
        )
        node = await svc.graph.get_node("session-3")
        assert node is not None
        assert "Session" in node["labels"]
        assert "SubSession" not in node["labels"]

    async def test_ensure_session_node_graph_backed_repopulates_cache(self):
        """When session node already exists in graph but not in _seen_sessions cache,
        the cache is repopulated and the original node data (started_at) is NOT overwritten."""
        svc = HookStateService()
        # Pre-populate graph with a session node (simulating a restart / replay scenario)
        await svc.graph.upsert_node(
            "session-replay",
            {
                "labels": ["Session", "RootSession"],
                "status": "running",
                "started_at": "2024-01-01T00:00:00",
            },
        )
        # Confirm _seen_sessions cache is empty before the call
        assert "session-replay" not in svc._seen_sessions

        # Call ensure_session_node — should detect existing node and skip creation
        await svc.ensure_session_node(
            "session-replay", {"started_at": "2024-01-02T00:00:00"}
        )

        # Cache must now contain the session id
        assert "session-replay" in svc._seen_sessions

        # Original started_at must NOT be overwritten
        node = await svc.graph.get_node("session-replay")
        assert node is not None
        assert node["started_at"] == "2024-01-01T00:00:00"

    async def test_ensure_session_node_graph_backed_creates_when_absent(self):
        """ensure_session_node creates a bare Session node when the session is absent from both
        _seen_sessions cache and the graph.

        ensure_session_node is a safety net that creates a minimal session node;
        it does NOT assign type labels (RootSession, SubSession, ForkedSession).
        """
        svc = HookStateService()
        # Both graph and cache are empty — node should be created
        await svc.ensure_session_node(
            "session-new", {"started_at": "2024-01-01T00:00:00"}
        )

        node = await svc.graph.get_node("session-new")
        assert node is not None
        assert "Session" in node["labels"]
        assert "RootSession" not in node["labels"]
        assert "session-new" in svc._seen_sessions

    async def test_ensure_session_node_no_label_change_on_existing(self):
        """When a node already exists in the graph, ensure_session_node must NOT overwrite it.

        Labels on existing nodes are never changed — ensure_session_node is a no-op when the
        node already exists in the graph (the graph-query tier fires before any write).
        """
        svc = HookStateService()
        # Pre-create a bare Session node (e.g. already stored from a previous run)
        await svc.graph.upsert_node(
            "session-sub",
            {
                "labels": ["Session"],
                "status": "running",
                "started_at": "2024-01-01T00:00:00",
            },
        )

        # Call without a parent field — must be a no-op because the node already exists
        await svc.ensure_session_node("session-sub", {})

        node = await svc.graph.get_node("session-sub")
        assert node is not None
        # Node must be unchanged — only Session label is present
        assert "Session" in node["labels"]
        assert node["status"] == "running"


# ---------------------------------------------------------------------------
# TestEnsureSessionNodeWriteFailure tests
# ---------------------------------------------------------------------------


class TestEnsureSessionNodeWriteFailure:
    """Tests that ensure_session_node does not cache a session ID when upsert_node fails."""

    async def test_failed_upsert_does_not_cache_session(self):
        """If upsert_node raises, session_id must NOT be added to _seen_sessions so
        a subsequent call can retry and succeed."""
        svc = HookStateService()

        # Replace with a failing mock using patch.object — restores automatically on exit
        with patch.object(
            svc.graph, "upsert_node", AsyncMock(side_effect=OSError("write failed"))
        ):
            try:
                await svc.ensure_session_node(
                    "session-fail", {"started_at": "2024-01-01T00:00:00"}
                )
            except OSError:
                pass

        # The session id must NOT have been cached because the write failed
        assert "session-fail" not in svc._seen_sessions

        # Retry — should succeed now that the original upsert_node is restored
        await svc.ensure_session_node(
            "session-fail", {"started_at": "2024-01-01T00:00:00"}
        )

        # Node must exist and cache must be populated
        node = await svc.graph.get_node("session-fail")
        assert node is not None
        assert "session-fail" in svc._seen_sessions


# ---------------------------------------------------------------------------
# GraphState.remove_edge tests
# ---------------------------------------------------------------------------


class TestGraphState:
    """Tests for GraphState.remove_edge."""

    async def test_remove_edge_existing(self):
        """remove_edge removes an existing edge; get_edge returns None afterwards."""
        state = GraphState()
        await state.upsert_edge("n1", "n2", {"type": "KNOWS"})
        state.remove_edge("n1", "n2")
        assert await state.get_edge("n1", "n2") is None

    async def test_remove_edge_nonexistent_is_noop(self):
        """remove_edge on a nonexistent edge must not raise an error."""
        state = GraphState()
        state.remove_edge("does-not-exist", "also-missing")  # must not raise

    async def test_remove_edge_does_not_affect_other_edges(self):
        """Removing one edge must not affect other edges in the graph."""
        state = GraphState()
        await state.upsert_edge("n1", "n2", {"type": "KNOWS"})
        await state.upsert_edge("n2", "n3", {"type": "LIKES"})
        state.remove_edge("n1", "n2")
        # The other edge must still be present
        remaining = await state.get_edge("n2", "n3")
        assert remaining is not None
        assert remaining["type"] == "LIKES"


# ---------------------------------------------------------------------------
# GraphState.set_labels tests
# ---------------------------------------------------------------------------


class TestGraphStateSetLabels:
    """Tests for GraphState.set_labels — atomic label add/remove on graph nodes."""

    async def _make_node(self, labels: list[str]) -> tuple["GraphState", str]:  # type: ignore[name-defined]
        """Helper: create a GraphState with workspace='test' and a node 's1'."""
        state = GraphState(workspace="test")
        await state.upsert_node("s1", {"labels": labels, "status": "running"})
        return state, "s1"

    async def test_add_only(self):
        """Adding labels to a bare Session node works without removing anything."""
        state, node_id = await self._make_node(["Session"])
        await state.set_labels(node_id, remove_labels=[], add_labels=["RootSession"])
        node = await state.get_node(node_id)
        assert node is not None
        assert "RootSession" in node["labels"]
        assert "Session" in node["labels"]

    async def test_remove_only(self):
        """Removing a label leaves other labels intact."""
        state, node_id = await self._make_node(["RootSession", "Session"])
        await state.set_labels(node_id, remove_labels=["RootSession"], add_labels=[])
        node = await state.get_node(node_id)
        assert node is not None
        assert "RootSession" not in node["labels"]
        assert "Session" in node["labels"]

    async def test_remove_and_add(self):
        """Removes old type label and adds new type label atomically."""
        state, node_id = await self._make_node(["RootSession", "Session"])
        await state.set_labels(
            node_id,
            remove_labels=["RootSession"],
            add_labels=["ForkedSession"],
        )
        node = await state.get_node(node_id)
        assert node is not None
        assert "RootSession" not in node["labels"]
        assert "ForkedSession" in node["labels"]
        assert "Session" in node["labels"]

    async def test_nonexistent_node_created_with_add_labels(self):
        """If node does not exist, it is created with add_labels."""
        state = GraphState(workspace="test")
        await state.set_labels(
            "new",
            remove_labels=[],
            add_labels=["ForkedSession", "Session"],
        )
        node = await state.get_node("new")
        assert node is not None
        assert "ForkedSession" in node["labels"]
        assert "Session" in node["labels"]

    async def test_empty_remove_is_noop(self):
        """Empty remove_labels does not affect existing labels."""
        state, node_id = await self._make_node(["Session"])
        await state.set_labels(node_id, remove_labels=[], add_labels=["RootSession"])
        node = await state.get_node(node_id)
        assert node is not None
        assert "Session" in node["labels"]

    async def test_remove_absent_label_is_noop(self):
        """Removing a label not present on the node silently succeeds."""
        state, node_id = await self._make_node(["Session"])
        await state.set_labels(node_id, remove_labels=["RootSession"], add_labels=[])
        node = await state.get_node(node_id)
        assert node is not None
        assert node["labels"] == ["Session"]
