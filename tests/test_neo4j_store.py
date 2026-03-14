"""Tests for Neo4jGraphStore — connection setup and buffer operations.

Covers:
- Protocol conformance (GraphStore and QueryableStore isinstance checks)
- workspace property: default 'default', explicit value, settable, no graph_forest_name
- supported_dialects: includes 'cypher', returns frozenset
- Buffer operations: upsert_node (add, merge props, merge labels, no duplicate labels),
  upsert_edge (add/merge), get_node (buffer-first, returns copy), get_edge (buffer-first)
- Static helpers: _sanitize_properties, _convert_timestamps
"""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from context_intelligence_server.graph_store import GraphStore, QueryableStore
from context_intelligence_server.neo4j_store import Neo4jGraphStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_store(workspace: str | None = "test-workspace") -> Neo4jGraphStore:
    """Create a Neo4jGraphStore with a mocked Neo4j driver."""
    with patch(
        "context_intelligence_server.neo4j_store.AsyncGraphDatabase"
    ) as mock_adb:
        mock_driver = AsyncMock()
        mock_adb.driver.return_value = mock_driver
        store = Neo4jGraphStore(
            uri="bolt://localhost:7687",
            auth=("neo4j", "password"),
            workspace=workspace,
        )
    return store


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_isinstance_graphstore():
    """Neo4jGraphStore must satisfy GraphStore protocol isinstance check."""
    store = _make_store()
    assert isinstance(store, GraphStore)


def test_isinstance_queryable_store():
    """Neo4jGraphStore must satisfy QueryableStore protocol isinstance check."""
    store = _make_store()
    assert isinstance(store, QueryableStore)


# ---------------------------------------------------------------------------
# Workspace property
# ---------------------------------------------------------------------------


def test_workspace_defaults_to_default_when_none():
    """When workspace=None, workspace property returns 'default'."""
    store = _make_store(workspace=None)
    assert store.workspace == "default"


def test_workspace_explicit_value():
    """Explicit workspace value is returned by the property."""
    store = _make_store(workspace="my-ws")
    assert store.workspace == "my-ws"


def test_workspace_settable():
    """workspace property has a setter that updates the value."""
    store = _make_store(workspace="initial")
    store.workspace = "updated"
    assert store.workspace == "updated"


def test_workspace_setter_to_none_resolves_to_default():
    """Setting workspace to None via setter makes property return 'default'."""
    store = _make_store(workspace="initial")
    store.workspace = None  # type: ignore[assignment]
    assert store.workspace == "default"


def test_no_graph_forest_name_in_source():
    """neo4j_store.py must not contain any graph_forest_name references."""
    import inspect

    import context_intelligence_server.neo4j_store as m

    source = inspect.getsource(m)
    assert "graph_forest_name" not in source


# ---------------------------------------------------------------------------
# supported_dialects
# ---------------------------------------------------------------------------


def test_supported_dialects_contains_cypher():
    """supported_dialects must contain 'cypher'."""
    store = _make_store()
    assert "cypher" in store.supported_dialects


def test_supported_dialects_returns_frozenset():
    """supported_dialects must return a frozenset."""
    store = _make_store()
    assert isinstance(store.supported_dialects, frozenset)


# ---------------------------------------------------------------------------
# Buffer operations — upsert_node
# ---------------------------------------------------------------------------


async def test_upsert_node_adds_node_to_buffer():
    """upsert_node adds a new node; get_node returns it from the buffer."""
    store = _make_store()
    await store.upsert_node("n1", {"name": "Alice"})
    result = await store.get_node("n1")
    assert result is not None
    assert result["name"] == "Alice"


async def test_upsert_node_merges_properties():
    """Second upsert_node merges (updates) existing properties."""
    store = _make_store()
    await store.upsert_node("n1", {"a": 1})
    await store.upsert_node("n1", {"b": 2})
    result = await store.get_node("n1")
    assert result is not None
    assert result["a"] == 1
    assert result["b"] == 2


async def test_upsert_node_overwrites_conflicting_property():
    """Second upsert_node overwrites a property with the same key."""
    store = _make_store()
    await store.upsert_node("n1", {"x": "old"})
    await store.upsert_node("n1", {"x": "new"})
    result = await store.get_node("n1")
    assert result is not None
    assert result["x"] == "new"


async def test_upsert_node_merges_labels_union():
    """Labels from successive upsert_node calls are unioned (no duplicates)."""
    store = _make_store()
    await store.upsert_node("n1", {"labels": ["TypeA"]})
    await store.upsert_node("n1", {"labels": ["TypeB"]})
    result = await store.get_node("n1")
    assert result is not None
    labels = result["labels"]
    assert "TypeA" in labels
    assert "TypeB" in labels


async def test_upsert_node_no_duplicate_labels():
    """Upserting the same label twice does not create duplicates."""
    store = _make_store()
    await store.upsert_node("n1", {"labels": ["TypeA"]})
    await store.upsert_node("n1", {"labels": ["TypeA"]})
    result = await store.get_node("n1")
    assert result is not None
    labels = result["labels"]
    assert labels.count("TypeA") == 1


async def test_upsert_node_adds_labels_when_existing_has_none():
    """If existing buffered node has no labels, labels from data are stored as-is."""
    store = _make_store()
    await store.upsert_node("n1", {"name": "no-labels"})
    await store.upsert_node("n1", {"labels": ["New"]})
    result = await store.get_node("n1")
    assert result is not None
    assert "New" in result["labels"]


# ---------------------------------------------------------------------------
# Buffer operations — upsert_edge
# ---------------------------------------------------------------------------


async def test_upsert_edge_adds_edge_to_buffer():
    """upsert_edge adds an edge; get_edge returns it from the buffer."""
    store = _make_store()
    await store.upsert_edge("src", "dst", {"type": "KNOWS"})
    result = await store.get_edge("src", "dst")
    assert result is not None
    assert result["type"] == "KNOWS"


async def test_upsert_edge_merges_properties():
    """Second upsert_edge call merges edge properties."""
    store = _make_store()
    await store.upsert_edge("src", "dst", {"weight": 1})
    await store.upsert_edge("src", "dst", {"label": "friend"})
    result = await store.get_edge("src", "dst")
    assert result is not None
    assert result["weight"] == 1
    assert result["label"] == "friend"


# ---------------------------------------------------------------------------
# Buffer operations — get_node
# ---------------------------------------------------------------------------


async def test_get_node_returns_copy_not_reference():
    """get_node returns a copy; mutations to the copy do not affect the buffer."""
    store = _make_store()
    await store.upsert_node("n1", {"val": 42})
    copy = await store.get_node("n1")
    assert copy is not None
    copy["val"] = 999  # Mutate the returned copy
    fresh = await store.get_node("n1")
    assert fresh is not None
    assert fresh["val"] == 42  # Buffer must not be mutated


async def test_get_node_buffer_first_returns_buffered_data():
    """get_node returns buffered node data without querying Neo4j."""
    store = _make_store()
    await store.upsert_node("n1", {"data": "buffered"})
    result = await store.get_node("n1")
    assert result == {"data": "buffered"}


async def test_get_node_returns_none_for_missing_node():
    """get_node returns None when node is absent from buffer and Neo4j has no data."""
    store = _make_store()
    # Configure the fallback driver to return no records
    mock_result = MagicMock()
    mock_result.records = []
    store._driver.execute_query = AsyncMock(return_value=mock_result)  # type: ignore[attr-defined]
    result = await store.get_node("nonexistent")
    assert result is None


# ---------------------------------------------------------------------------
# Buffer operations — get_edge
# ---------------------------------------------------------------------------


async def test_get_edge_buffer_first_returns_buffered_data():
    """get_edge returns buffered edge data without querying Neo4j."""
    store = _make_store()
    await store.upsert_edge("a", "b", {"weight": 5})
    result = await store.get_edge("a", "b")
    assert result is not None
    assert result["weight"] == 5


async def test_get_edge_returns_copy_not_reference():
    """get_edge returns a copy; mutations to the copy do not affect the buffer."""
    store = _make_store()
    await store.upsert_edge("a", "b", {"weight": 5})
    copy = await store.get_edge("a", "b")
    assert copy is not None
    copy["weight"] = 999  # Mutate the returned copy
    fresh = await store.get_edge("a", "b")
    assert fresh is not None
    assert fresh["weight"] == 5  # Buffer must not be mutated


async def test_get_edge_returns_none_for_missing_edge():
    """get_edge returns None when edge is absent from buffer and Neo4j has no data."""
    store = _make_store()
    mock_result = MagicMock()
    mock_result.records = []
    store._driver.execute_query = AsyncMock(return_value=mock_result)  # type: ignore[attr-defined]
    result = await store.get_edge("no-src", "no-dst")
    assert result is None


# ---------------------------------------------------------------------------
# Placeholder methods
# ---------------------------------------------------------------------------


async def test_flush_is_noop():
    """flush() must be a no-op placeholder (does not raise)."""
    store = _make_store()
    await store.flush()  # Should not raise


async def test_close_is_noop():
    """close() must be a no-op placeholder (does not raise)."""
    store = _make_store()
    await store.close()  # Should not raise


async def test_execute_query_raises_not_implemented():
    """execute_query raises NotImplementedError (placeholder)."""
    store = _make_store()
    with pytest.raises(NotImplementedError):
        await store.execute_query("MATCH (n) RETURN n")


# ---------------------------------------------------------------------------
# Static helper: _sanitize_properties
# ---------------------------------------------------------------------------


def test_sanitize_properties_skips_none():
    """_sanitize_properties omits keys with None values."""
    result = Neo4jGraphStore._sanitize_properties({"a": 1, "b": None})
    assert "b" not in result
    assert result["a"] == 1


def test_sanitize_properties_keeps_primitives():
    """_sanitize_properties passes through str, int, float, bool as-is."""
    props = {"s": "hello", "i": 42, "f": 3.14, "b": True}
    result = Neo4jGraphStore._sanitize_properties(props)
    assert result == props


def test_sanitize_properties_serializes_dicts():
    """_sanitize_properties JSON-serializes dict values."""
    result = Neo4jGraphStore._sanitize_properties({"meta": {"nested": True}})
    assert result["meta"] == json.dumps({"nested": True})


def test_sanitize_properties_serializes_complex_lists():
    """_sanitize_properties JSON-serializes lists that contain non-primitives."""
    result = Neo4jGraphStore._sanitize_properties({"items": [{"a": 1}]})
    assert result["items"] == json.dumps([{"a": 1}])


def test_sanitize_properties_keeps_primitive_lists():
    """_sanitize_properties keeps lists of primitives as-is."""
    props = {"tags": ["a", "b", "c"]}
    result = Neo4jGraphStore._sanitize_properties(props)
    assert result["tags"] == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# Static helper: _convert_timestamps
# ---------------------------------------------------------------------------


def test_convert_timestamps_converts_at_fields():
    """_convert_timestamps converts *_at ISO strings to datetime objects."""
    props = {"created_at": "2024-01-15T10:30:00", "name": "node"}
    result = Neo4jGraphStore._convert_timestamps(props)
    assert isinstance(result["created_at"], datetime)
    assert result["name"] == "node"  # Non-timestamp unchanged


def test_convert_timestamps_ignores_non_at_fields():
    """_convert_timestamps does not touch fields not ending in _at."""
    props = {"category": "2024-01-15T10:30:00", "updated": "2024-02-01"}
    result = Neo4jGraphStore._convert_timestamps(props)
    assert result["category"] == "2024-01-15T10:30:00"  # Not converted


def test_convert_timestamps_skips_invalid_iso():
    """_convert_timestamps leaves *_at fields unchanged if they are not valid ISO."""
    props = {"created_at": "not-a-date"}
    result = Neo4jGraphStore._convert_timestamps(props)
    assert result["created_at"] == "not-a-date"  # Unchanged


def test_convert_timestamps_does_not_mutate_input():
    """_convert_timestamps returns a new dict without mutating the input."""
    props = {"created_at": "2024-01-15T10:30:00"}
    original = dict(props)
    Neo4jGraphStore._convert_timestamps(props)
    assert props == original  # Input not mutated
