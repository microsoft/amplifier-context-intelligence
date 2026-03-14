"""Tests for SessionCursors, HookConfig, and GraphState in services.py."""

from __future__ import annotations

import dataclasses

from context_intelligence_server.services import GraphState, HookConfig, SessionCursors


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
    """SessionCursors initialises with correct default values."""
    sc = SessionCursors()
    assert sc.current_run_id is None
    assert sc.current_step_id is None
    assert sc.run_counter == 0
    assert sc.step_counter == 0
    assert sc.prompt_preview == ""
    assert sc.parallel_groups == {}
    assert sc.tool_call_map == {}


def test_session_cursors_is_dataclass():
    """SessionCursors must be a proper dataclass."""
    assert dataclasses.is_dataclass(SessionCursors)
    # Verify all expected fields are present via dataclasses.fields
    field_names = {f.name for f in dataclasses.fields(SessionCursors)}
    assert "current_run_id" in field_names
    assert "parallel_groups" in field_names
    assert "tool_call_map" in field_names


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


def test_graph_state_no_graph_forest_name():
    """GraphState must not expose graph_forest_name or _graph_forest_name."""
    state = GraphState()
    assert not hasattr(state, "graph_forest_name")
    assert not hasattr(state, "_graph_forest_name")
