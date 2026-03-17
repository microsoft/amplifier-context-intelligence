"""Tests for SessionCursors, HookConfig, and GraphState in services.py."""

from __future__ import annotations

import dataclasses

from context_intelligence_server.services import (
    GraphState,
    HookConfig,
    HookStateService,
    SessionCursors,
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
# SessionCursors tests
# ---------------------------------------------------------------------------


def test_session_cursors_defaults():
    """SessionCursors initialises with correct default values for remaining fields."""
    sc = SessionCursors()
    assert sc.current_run_id is None
    assert sc.current_step_id is None
    assert sc.prompt_preview == ""
    assert sc.parallel_groups == {}
    assert sc.tool_call_map == {}


def test_session_cursors_is_dataclass():
    """SessionCursors must be a proper dataclass with exactly the 5 pointer fields."""
    assert dataclasses.is_dataclass(SessionCursors)
    # Verify all expected fields are present via dataclasses.fields
    field_names = {f.name for f in dataclasses.fields(SessionCursors)}
    assert "current_run_id" in field_names
    assert "current_step_id" in field_names
    assert "prompt_preview" in field_names
    assert "parallel_groups" in field_names
    assert "tool_call_map" in field_names
    # Counters must NOT be present — they are ephemeral accumulators
    assert "run_counter" not in field_names
    assert "step_counter" not in field_names


def test_session_cursors_no_counter_fields():
    """SessionCursors must not expose run_counter or step_counter attributes."""
    sc = SessionCursors()
    assert not hasattr(sc, "run_counter")
    assert not hasattr(sc, "step_counter")


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

    def test_get_cursors_lazy_creation(self):
        """get_cursors creates a SessionCursors on first access."""
        svc = HookStateService()
        cursors = svc.get_cursors("session-1")
        assert isinstance(cursors, SessionCursors)

    def test_get_cursors_same_instance(self):
        """get_cursors returns the same SessionCursors instance for the same session_id."""
        svc = HookStateService()
        c1 = svc.get_cursors("session-1")
        c2 = svc.get_cursors("session-1")
        assert c1 is c2

    def test_get_cursors_different_sessions(self):
        """get_cursors returns distinct SessionCursors for distinct session ids."""
        svc = HookStateService()
        c1 = svc.get_cursors("session-1")
        c2 = svc.get_cursors("session-2")
        assert c1 is not c2

    def test_remove_cursors_resets(self):
        """remove_cursors causes get_cursors to create a fresh instance on the next call."""
        svc = HookStateService()
        c1 = svc.get_cursors("session-1")
        svc.remove_cursors("session-1")
        c2 = svc.get_cursors("session-1")
        assert c1 is not c2

    def test_remove_cursors_safe_for_nonexistent(self):
        """remove_cursors does not raise when session_id has no cursors entry."""
        svc = HookStateService()
        svc.remove_cursors("nonexistent-session")  # must not raise

    async def test_ensure_session_node_creates_root(self):
        """ensure_session_node creates a Session+Root node when no parent field is present."""
        svc = HookStateService()
        await svc.ensure_session_node(
            "session-1", {"started_at": "2024-01-01T00:00:00"}
        )
        node = await svc.graph.get_node("session-1")
        assert node is not None
        assert "Session" in node["labels"]
        assert "Root" in node["labels"]
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

    async def test_ensure_session_node_creates_subsession(self):
        """ensure_session_node creates a Session+Subsession node when parent_id is present."""
        svc = HookStateService()
        await svc.ensure_session_node(
            "session-2",
            {"parent_id": "session-1", "started_at": "2024-01-01T00:00:00"},
        )
        node = await svc.graph.get_node("session-2")
        assert node is not None
        assert "Session" in node["labels"]
        assert "Subsession" in node["labels"]
        assert node["status"] == "running"

    async def test_ensure_session_node_parent_field_creates_subsession(self):
        """ensure_session_node treats 'parent' field as a parent indicator (no parent_id needed)."""
        svc = HookStateService()
        await svc.ensure_session_node(
            "session-3",
            {"parent": "session-1", "started_at": "2024-01-01T00:00:00"},
        )
        node = await svc.graph.get_node("session-3")
        assert node is not None
        assert "Session" in node["labels"]
        assert "Subsession" in node["labels"]

    async def test_ensure_session_node_graph_backed_repopulates_cache(self):
        """When session node already exists in graph but not in _seen_sessions cache,
        the cache is repopulated and the original node data (started_at) is NOT overwritten."""
        svc = HookStateService()
        # Pre-populate graph with a session node (simulating a restart / replay scenario)
        await svc.graph.upsert_node(
            "session-replay",
            {
                "labels": ["Session", "Root"],
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
        """ensure_session_node creates a Root node when the session is absent from both
        _seen_sessions cache and the graph."""
        svc = HookStateService()
        # Both graph and cache are empty — node should be created
        await svc.ensure_session_node(
            "session-new", {"started_at": "2024-01-01T00:00:00"}
        )

        node = await svc.graph.get_node("session-new")
        assert node is not None
        assert "Root" in node["labels"]
        assert "session-new" in svc._seen_sessions

    async def test_ensure_session_node_no_label_change_on_existing(self):
        """When a Subsession node already exists in the graph, ensure_session_node must NOT
        add a Root label to it — labels on existing nodes are never changed."""
        svc = HookStateService()
        # Pre-create a Subsession node (e.g. already stored from a previous run)
        await svc.graph.upsert_node(
            "session-sub",
            {
                "labels": ["Session", "Subsession"],
                "status": "running",
                "started_at": "2024-01-01T00:00:00",
            },
        )

        # Call without a parent field — a naive implementation would add a Root label
        await svc.ensure_session_node("session-sub", {})

        node = await svc.graph.get_node("session-sub")
        assert node is not None
        # Root must NOT have been added to an existing Subsession node
        assert "Root" not in node["labels"]
        # Subsession label must still be present
        assert "Subsession" in node["labels"]


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
