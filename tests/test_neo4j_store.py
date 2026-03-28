"""Tests for Neo4jGraphStore — connection setup, buffer operations, flush, schema, and query.

Covers:
- Protocol conformance (GraphStore and QueryableStore isinstance checks)
- workspace property: default 'default', explicit value, settable, no graph_forest_name
- supported_dialects: includes 'cypher', returns frozenset
- Buffer operations: upsert_node (add, merge props, merge labels, no duplicate labels),
  upsert_edge (add/merge), get_node (buffer-first, returns copy), get_edge (buffer-first)
- Static helpers: _sanitize_properties, _convert_timestamps
- Flush: workspace in rows, empty is no-op, clears buffers on success, restores on failure
- Schema: idempotent, indexes use workspace not graph_forest_name
- execute_query: injects workspace, wildcard skips injection, unsupported dialect raises ValueError
"""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from context_intelligence_server.graph_store import GraphStore, QueryableStore
from context_intelligence_server.neo4j_store import (
    Neo4jGraphStore,
    _validate_identifier,
)


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
# Init — buffer initialization
# ---------------------------------------------------------------------------


def test_label_patches_initialized_as_empty_list():
    """Neo4jGraphStore.__init__ must initialize _label_patches as an empty list."""
    store = _make_store()
    assert hasattr(store, "_label_patches"), "_label_patches attribute must be set in __init__"
    assert store._label_patches == [], "_label_patches must be initialized as []"
    assert isinstance(store._label_patches, list), "_label_patches must be a list"


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
# Placeholder / basic method smoke tests
# ---------------------------------------------------------------------------


async def test_flush_empty_is_noop_no_driver_calls():
    """flush() on empty buffers must not invoke the driver at all."""
    store = _make_store()
    # Driver session should never be touched
    store._driver.session = MagicMock(
        side_effect=AssertionError("should not call driver")
    )
    await store.flush()  # Should not raise or call driver


async def test_close_does_not_raise():
    """close() must not raise when there are no pending tasks."""
    store = _make_store()
    await store.close()  # Should not raise


# ---------------------------------------------------------------------------
# TestFlushWritesWorkspace
# ---------------------------------------------------------------------------


def _make_flush_mocks():
    """Return (mock_tx, mock_session) configured for flush testing.

    ``mock_session.__aenter__`` is explicitly configured to return ``mock_session``
    so that ``async with driver.session(...) as session:`` yields the same object
    whose attributes (``begin_transaction``, ``run``) we can assert on.
    """
    mock_tx = AsyncMock()
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.begin_transaction = AsyncMock(return_value=mock_tx)
    return mock_tx, mock_session


class TestFlushWritesWorkspace:
    """flush() writes workspace property on node and edge rows (not graph_forest_name)."""

    async def test_flush_empty_is_noop(self):
        """flush() with empty buffers makes no driver calls."""
        store = _make_store()
        store._driver.session = MagicMock(
            side_effect=AssertionError("driver must not be called")
        )
        await store.flush()  # No exception expected

    async def test_flush_node_rows_contain_workspace(self):
        """flush() passes workspace in node row props, not graph_forest_name."""
        store = _make_store(workspace="my-workspace")
        await store.upsert_node("n1", {"labels": ["Session"], "name": "test"})

        mock_tx, mock_session = _make_flush_mocks()
        store._driver.session = MagicMock(return_value=mock_session)
        store._schema_initialized = True  # skip schema for this test

        await store.flush()

        assert mock_tx.run.called, "Expected flush to call tx.run for node writes"
        for call in mock_tx.run.call_args_list:
            if "rows" in call.kwargs:
                for row in call.kwargs["rows"]:
                    props = row.get("props", {})
                    assert "workspace" in props, (
                        f"Expected 'workspace' in props, got: {props}"
                    )
                    assert props["workspace"] == "my-workspace"
                    assert "graph_forest_name" not in props

    async def test_flush_edge_rows_contain_workspace(self):
        """flush() passes workspace in edge row props, not graph_forest_name."""
        store = _make_store(workspace="edge-workspace")
        await store.upsert_edge("src", "dst", {"type": "KNOWS", "weight": 1})

        mock_tx, mock_session = _make_flush_mocks()
        store._driver.session = MagicMock(return_value=mock_session)
        store._schema_initialized = True

        await store.flush()

        assert mock_tx.run.called, "Expected flush to call tx.run for edge writes"
        for call in mock_tx.run.call_args_list:
            if "rows" in call.kwargs:
                for row in call.kwargs["rows"]:
                    props = row.get("props", {})
                    assert "workspace" in props, (
                        f"Expected 'workspace' in edge props, got: {props}"
                    )
                    assert props["workspace"] == "edge-workspace"
                    assert "graph_forest_name" not in props

    async def test_flush_clears_buffers_on_success(self):
        """flush() clears node and edge buffers after a successful commit."""
        store = _make_store()
        await store.upsert_node("n1", {"labels": ["Step"], "name": "test"})
        await store.upsert_edge("n1", "n2", {"type": "NEXT"})

        mock_tx, mock_session = _make_flush_mocks()
        store._driver.session = MagicMock(return_value=mock_session)
        store._schema_initialized = True

        await store.flush()

        assert store._node_buffer == {}, (
            "Node buffer should be cleared after successful flush"
        )
        assert store._edge_buffer == {}, (
            "Edge buffer should be cleared after successful flush"
        )

    async def test_flush_restores_buffers_on_failure(self):
        """flush() restores node and edge buffers when the transaction fails."""
        store = _make_store()
        await store.upsert_node("n1", {"labels": ["Session"], "name": "test"})
        await store.upsert_edge("a", "b", {"type": "KNOWS"})

        mock_tx, mock_session = _make_flush_mocks()
        mock_tx.run = AsyncMock(side_effect=RuntimeError("DB write error"))
        store._driver.session = MagicMock(return_value=mock_session)
        store._schema_initialized = True

        with pytest.raises(RuntimeError, match="DB write error"):
            await store.flush()

        # Buffers must be restored
        assert "n1" in store._node_buffer, (
            "Node buffer should be restored after failed flush"
        )
        assert ("a", "b") in store._edge_buffer, (
            "Edge buffer should be restored after failed flush"
        )

    async def test_flush_node_without_labels_uses_enrichment_path(self):
        """flush() routes unlabeled nodes to the enrichment (no-label) MERGE path."""
        store = _make_store(workspace="test-ws")
        await store.upsert_node("bare-node", {"kind": "misc"})  # no labels

        mock_tx, mock_session = _make_flush_mocks()
        store._driver.session = MagicMock(return_value=mock_session)
        store._schema_initialized = True

        await store.flush()

        assert mock_tx.run.called, "Expected flush to write unlabeled node"
        # The enrichment path has no label in MERGE; check workspace is in rows
        for call in mock_tx.run.call_args_list:
            if "rows" in call.kwargs:
                for row in call.kwargs["rows"]:
                    assert "workspace" in row.get("props", {})


# ---------------------------------------------------------------------------
# TestSchemaIndexesWorkspace
# ---------------------------------------------------------------------------


class TestSchemaIndexesWorkspace:
    """_ensure_schema() creates indexes using workspace, not graph_forest_name."""

    def _make_schema_session(self):
        """Return a mock session configured for _ensure_schema testing."""
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        return mock_session

    async def test_schema_creates_workspace_index(self):
        """_ensure_schema() creates idx_session_workspace on Session.workspace."""
        store = _make_store()

        mock_session = self._make_schema_session()
        store._driver.session = MagicMock(return_value=mock_session)

        await store._ensure_schema()

        assert mock_session.run.called, "Expected _ensure_schema to call session.run"
        all_queries = [
            call.args[0] for call in mock_session.run.call_args_list if call.args
        ]
        combined = " ".join(all_queries)
        assert "workspace" in combined, (
            "Expected 'workspace' to appear in schema queries"
        )
        assert "graph_forest_name" not in combined, (
            "graph_forest_name must not appear in schema"
        )

    async def test_schema_creates_node_id_indexes(self):
        """_ensure_schema() creates node_id indexes for all required labels."""
        store = _make_store()

        mock_session = self._make_schema_session()
        store._driver.session = MagicMock(return_value=mock_session)

        await store._ensure_schema()

        all_queries = [
            call.args[0] for call in mock_session.run.call_args_list if call.args
        ]
        combined = " ".join(all_queries)
        for label in ("Session", "OrchestratorRun", "Step", "ToolExecution", "Event"):
            assert label in combined, (
                f"Expected index for label {label!r} in schema queries"
            )

    async def test_schema_sets_initialized_flag(self):
        """_ensure_schema() sets _schema_initialized = True after running."""
        store = _make_store()
        assert store._schema_initialized is False

        mock_session = self._make_schema_session()
        store._driver.session = MagicMock(return_value=mock_session)

        await store._ensure_schema()

        assert store._schema_initialized is True

    async def test_schema_skips_when_already_initialized(self):
        """_ensure_schema() is a no-op when _schema_initialized is already True."""
        store = _make_store()
        store._schema_initialized = True
        # If already initialized, driver must not be touched
        store._driver.session = MagicMock(
            side_effect=AssertionError("must not call driver")
        )
        await store._ensure_schema()  # Should not raise

    async def test_schema_is_idempotent(self):
        """Every CREATE INDEX query in _ensure_schema() must use IF NOT EXISTS.

        The Docker Compose stack uses a persistent neo4j_data volume.  When the
        container restarts the Python process starts fresh (_schema_initialized
        resets to False) but the indexes already exist in Neo4j.  Without
        IF NOT EXISTS every restart raises
        Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists, which
        propagates out of flush() and causes the "Final flush failed during
        close" data-loss path.
        """
        store = _make_store()
        mock_session = self._make_schema_session()
        store._driver.session = MagicMock(return_value=mock_session)

        await store._ensure_schema()

        all_queries = [
            call.args[0] for call in mock_session.run.call_args_list if call.args
        ]
        assert all_queries, "_ensure_schema must issue at least one query"
        for query in all_queries:
            assert "IF NOT EXISTS" in query.upper(), (
                f"Every schema CREATE INDEX must use IF NOT EXISTS so that "
                f"container restarts on a persistent volume do not raise "
                f"EquivalentSchemaRuleAlreadyExists.  Offending query: {query!r}"
            )


# ---------------------------------------------------------------------------
# TestExecuteQuery
# ---------------------------------------------------------------------------


def _make_execute_mocks(records=None):
    """Return (mock_result, mock_session) configured for execute_query testing.

    ``mock_session.__aenter__`` is explicitly configured to return ``mock_session``
    so that ``async with driver.session(...) as session:`` yields the same object
    whose ``run`` attribute we can assert on.
    """
    mock_result = AsyncMock()
    mock_result.data = AsyncMock(return_value=records or [])
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.run = AsyncMock(return_value=mock_result)
    return mock_result, mock_session


class TestExecuteQuery:
    """execute_query injects workspace param, validates dialect, returns list of dicts."""

    async def test_execute_query_injects_workspace_param(self):
        """execute_query adds workspace to the query params (unless workspace='*')."""
        store = _make_store(workspace="injected-ws")
        _mock_result, mock_session = _make_execute_mocks()
        store._driver.session = MagicMock(return_value=mock_session)

        await store.execute_query("MATCH (n) RETURN n")

        call = mock_session.run.call_args
        # params is second positional arg
        params = call.args[1] if len(call.args) > 1 else {}
        assert "workspace" in params, (
            f"workspace should be injected into params, got: {params}"
        )
        assert params["workspace"] == "injected-ws"
        assert "graph_forest_name" not in params

    async def test_execute_query_wildcard_workspace_not_injected(self):
        """execute_query with workspace='*' does NOT inject workspace into params."""
        store = _make_store(workspace="some-ws")
        _mock_result, mock_session = _make_execute_mocks()
        store._driver.session = MagicMock(return_value=mock_session)

        await store.execute_query("MATCH (n) RETURN n", workspace="*")

        call = mock_session.run.call_args
        params = call.args[1] if len(call.args) > 1 else {}
        assert "workspace" not in params, (
            "workspace must NOT be injected when workspace='*'"
        )

    async def test_execute_query_unsupported_dialect_raises_value_error(self):
        """execute_query raises ValueError for unsupported dialects."""
        store = _make_store()
        with pytest.raises(ValueError, match="sql"):
            await store.execute_query("SELECT 1", dialect="sql")

    async def test_execute_query_cypher_is_supported(self):
        """execute_query does not raise for the 'cypher' dialect."""
        store = _make_store()
        _mock_result, mock_session = _make_execute_mocks()
        store._driver.session = MagicMock(return_value=mock_session)
        # Should not raise
        await store.execute_query("MATCH (n) RETURN n", dialect="cypher")

    async def test_execute_query_returns_list_of_dicts(self):
        """execute_query returns a list of dict records."""
        store = _make_store()
        _mock_result, mock_session = _make_execute_mocks(
            records=[{"n": "Alice"}, {"n": "Bob"}]
        )
        store._driver.session = MagicMock(return_value=mock_session)

        result = await store.execute_query("MATCH (n) RETURN n")

        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(r, dict) for r in result)

    async def test_execute_query_uses_store_workspace_when_none(self):
        """execute_query with workspace=None uses the store's own workspace."""
        store = _make_store(workspace="store-ws")
        _mock_result, mock_session = _make_execute_mocks()
        store._driver.session = MagicMock(return_value=mock_session)

        await store.execute_query("MATCH (n) RETURN n", workspace=None)

        call = mock_session.run.call_args
        params = call.args[1] if len(call.args) > 1 else {}
        assert params.get("workspace") == "store-ws"


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


# ---------------------------------------------------------------------------
# _validate_identifier (Cypher injection guard)
# ---------------------------------------------------------------------------


def test_validate_identifier_accepts_valid_label():
    """_validate_identifier passes for valid Neo4j-style identifiers."""
    _validate_identifier("Session", "label")  # should not raise
    _validate_identifier("OrchestratorRun", "label")  # should not raise
    _validate_identifier("MY_TYPE", "edge_type")  # should not raise


def test_validate_identifier_rejects_special_characters():
    """_validate_identifier raises ValueError for identifiers with special chars."""
    with pytest.raises(ValueError, match="Invalid Neo4j label identifier"):
        _validate_identifier("Session) DETACH DELETE (n) //", "label")


def test_validate_identifier_rejects_hyphens():
    """_validate_identifier raises ValueError for identifiers with hyphens."""
    with pytest.raises(ValueError, match="Invalid Neo4j label identifier"):
        _validate_identifier("bad-label", "label")


def test_validate_identifier_rejects_digit_start():
    """_validate_identifier raises ValueError for identifiers starting with a digit."""
    with pytest.raises(ValueError, match="Invalid Neo4j edge_type identifier"):
        _validate_identifier("1invalid", "edge_type")


def test_validate_identifier_rejects_empty_string():
    """_validate_identifier raises ValueError for empty string."""
    with pytest.raises(ValueError, match="Invalid Neo4j label identifier"):
        _validate_identifier("", "label")


class TestFlushIdentifierValidation:
    """flush() raises ValueError before writing when labels/edge types are invalid."""

    async def test_flush_raises_for_invalid_label(self):
        """flush() raises ValueError when a node label fails identifier validation."""
        store = _make_store(workspace="test-ws")
        await store.upsert_node("n1", {"labels": ["Bad-Label!"]})

        mock_tx, mock_session = _make_flush_mocks()
        store._driver.session = MagicMock(return_value=mock_session)
        store._schema_initialized = True

        with pytest.raises(ValueError, match="Invalid Neo4j label identifier"):
            await store.flush()

    async def test_flush_raises_for_invalid_edge_type(self):
        """flush() raises ValueError when an edge type fails identifier validation."""
        store = _make_store(workspace="test-ws")
        await store.upsert_edge("a", "b", {"type": "INVALID-TYPE!"})

        mock_tx, mock_session = _make_flush_mocks()
        store._driver.session = MagicMock(return_value=mock_session)
        store._schema_initialized = True

        with pytest.raises(ValueError, match="Invalid Neo4j edge_type identifier"):
            await store.flush()

    async def test_flush_accepts_valid_label(self):
        """flush() succeeds when node label passes identifier validation."""
        store = _make_store(workspace="test-ws")
        await store.upsert_node("n1", {"labels": ["ValidLabel"]})

        mock_tx, mock_session = _make_flush_mocks()
        store._driver.session = MagicMock(return_value=mock_session)
        store._schema_initialized = True

        await store.flush()  # should not raise

    async def test_flush_accepts_valid_edge_type(self):
        """flush() succeeds when edge type passes identifier validation."""
        store = _make_store(workspace="test-ws")
        await store.upsert_edge("a", "b", {"type": "KNOWS"})

        mock_tx, mock_session = _make_flush_mocks()
        store._driver.session = MagicMock(return_value=mock_session)
        store._schema_initialized = True

        await store.flush()  # should not raise


# ---------------------------------------------------------------------------
# close() logs final flush exception (data-loss guard)
# ---------------------------------------------------------------------------


async def test_close_logs_final_flush_exception():
    """close() logs (not silently swallows) when the final flush raises."""
    store = _make_store()
    # Make flush raise on the final call inside close()
    store.flush = AsyncMock(side_effect=RuntimeError("flush failed"))

    with patch("context_intelligence_server.neo4j_store._LOG") as mock_log:
        await store.close()  # must not raise

    mock_log.exception.assert_called_once()
    logged_msg = mock_log.exception.call_args.args[0]
    assert "flush" in logged_msg.lower() or "buffered" in logged_msg.lower()


# ---------------------------------------------------------------------------
# TestRemoveEdge
# ---------------------------------------------------------------------------


class TestRemoveEdge:
    """Tests for Neo4jGraphStore.remove_edge."""

    async def test_remove_edge_from_buffer(self):
        """remove_edge removes a buffered edge from _edge_buffer."""
        store = _make_store()
        await store.upsert_edge("src", "dst", {"type": "KNOWS"})
        assert ("src", "dst") in store._edge_buffer
        store.remove_edge("src", "dst")
        assert ("src", "dst") not in store._edge_buffer

    async def test_remove_edge_nonexistent_is_noop(self):
        """remove_edge on a nonexistent edge must not raise an error."""
        store = _make_store()
        store.remove_edge("does-not-exist", "also-missing")  # must not raise


# ---------------------------------------------------------------------------
# Bug 1 — get_node fallback must query by n.node_id (not n.id)
# ---------------------------------------------------------------------------


async def test_get_node_fallback_queries_by_node_id_property():
    """Post-flush fallback must issue a query using n.node_id, not n.id.

    Nodes are persisted in Neo4j via flush() with MERGE {node_id: row.node_id, ...}.
    The property stored on the node is therefore ``node_id``, NOT ``id``.
    A fallback query filtering on ``n.id`` will never match any node and will
    silently return None, causing production misses after every flush.

    This test:
    1. Clears the buffer to simulate post-flush state.
    2. Captures the Cypher query sent to the driver.
    3. Asserts the query uses ``n.node_id``, not ``n.id``.

    FAILS before fix  → query string contains "n.id" (wrong property name)
    PASSES after fix  → query string contains "n.node_id"
    """
    store = _make_store()
    store._node_buffer = {}  # simulate post-flush: buffer is empty

    mock_result = MagicMock()
    mock_result.records = []
    mock_execute = AsyncMock(return_value=mock_result)
    store._driver.execute_query = mock_execute  # type: ignore[attr-defined]

    await store.get_node("session-123")

    assert mock_execute.called, (
        "Expected driver.execute_query to be called for Neo4j fallback"
    )
    query: str = mock_execute.call_args[0][0]
    assert "n.node_id" in query, (
        f"Fallback query must filter on 'n.node_id' (the property flush stores on nodes), "
        f"but the issued query was: {query!r}"
    )


# ---------------------------------------------------------------------------
# Bug 2 — flush() must store src_id/dst_id on edge props so get_edge fallback works
# ---------------------------------------------------------------------------


async def test_flush_edge_rows_contain_src_dst_ids():
    """flush() must store src_id and dst_id in edge relationship props.

    The get_edge() Neo4j fallback query filters on:
        WHERE r.src_id = $src_id AND r.dst_id = $dst_id

    For that query to match any rows, the values must actually be stored as
    properties on the relationship during flush().  Currently flush() builds
    row.props from sanitized edge data + workspace only — src_id/dst_id are
    used only to match the endpoint nodes and are never SET on the relationship
    itself.  As a result, every post-flush get_edge() call returns None.

    FAILS before fix  → props dict lacks 'src_id' and 'dst_id'
    PASSES after fix  → props dict contains both, matching the fallback query
    """
    store = _make_store(workspace="test-ws")
    await store.upsert_edge("src-node", "dst-node", {"type": "KNOWS", "weight": 1})

    mock_tx, mock_session = _make_flush_mocks()
    store._driver.session = MagicMock(return_value=mock_session)
    store._schema_initialized = True

    await store.flush()

    assert mock_tx.run.called, "Expected flush to call tx.run for edge writes"
    found_edge_rows = False
    for call in mock_tx.run.call_args_list:
        if "rows" in call.kwargs:
            for row in call.kwargs["rows"]:
                props = row.get("props", {})
                found_edge_rows = True
                assert "src_id" in props, (
                    f"Edge props must contain 'src_id' so the get_edge fallback query "
                    f"(WHERE r.src_id = ...) can find the relationship. Got: {props}"
                )
                assert "dst_id" in props, (
                    f"Edge props must contain 'dst_id' so the get_edge fallback query "
                    f"(WHERE r.dst_id = ...) can find the relationship. Got: {props}"
                )
                assert props["src_id"] == "src-node", (
                    f"Expected src_id='src-node', got {props['src_id']!r}"
                )
                assert props["dst_id"] == "dst-node", (
                    f"Expected dst_id='dst-node', got {props['dst_id']!r}"
                )
    assert found_edge_rows, "Expected at least one edge row to be written during flush"


# ---------------------------------------------------------------------------
# Bug D-05 regression — get_edge() must scope Neo4j fallback by workspace
# ---------------------------------------------------------------------------


async def test_get_edge_neo4j_fallback_includes_workspace_filter():
    """get_edge Neo4j fallback query must scope edges by workspace (bug D-05).

    Previously the query had no workspace filter, allowing cross-workspace edge
    leakage when two sessions in different workspaces happened to share the same
    src_id / dst_id pair.
    """
    store = _make_store(workspace="my-workspace")
    mock_result = MagicMock()
    mock_result.records = []
    store._driver.execute_query = AsyncMock(return_value=mock_result)  # type: ignore[attr-defined]

    await store.get_edge("src-1", "dst-1")

    call_args = store._driver.execute_query.call_args
    query: str = call_args[0][0]
    params: dict = call_args[0][1]

    assert "workspace" in query.lower(), (
        "get_edge fallback query must filter by workspace"
    )
    assert params.get("workspace") == "my-workspace", (
        f"workspace param must match store workspace, got {params!r}"
    )


async def test_get_edge_neo4j_fallback_uses_store_workspace_value():
    """get_edge fallback passes the *current* store workspace, not a hardcoded string."""
    store = _make_store(workspace="workspace-alpha")
    mock_result = MagicMock()
    mock_result.records = []
    store._driver.execute_query = AsyncMock(return_value=mock_result)  # type: ignore[attr-defined]

    await store.get_edge("a", "b")

    params = store._driver.execute_query.call_args[0][1]
    assert params.get("workspace") == "workspace-alpha"


# ---------------------------------------------------------------------------
# TestNeo4jGraphStoreSetLabels
# ---------------------------------------------------------------------------


class TestNeo4jGraphStoreSetLabels:
    """set_labels() buffers label patches and flush() applies or restores them."""

    async def test_set_labels_buffers_patch(self):
        """set_labels appends a patch dict to _label_patches with no immediate I/O."""
        store = _make_store()
        store._label_patches = []

        await store.set_labels("s1", remove_labels=["RootSession"], add_labels=["ForkedSession"])

        assert len(store._label_patches) == 1
        patch = store._label_patches[0]
        assert patch["node_id"] == "s1"
        assert patch["remove"] == ["RootSession"]
        assert patch["add"] == ["ForkedSession"]

    async def test_flush_clears_label_patches(self):
        """After a successful flush, _label_patches is empty."""
        store = _make_store()
        store._label_patches = []

        await store.set_labels("s1", remove_labels=["RootSession"], add_labels=["ForkedSession"])

        mock_tx, mock_session = _make_flush_mocks()
        store._driver.session = MagicMock(return_value=mock_session)
        store._schema_initialized = True

        await store.flush()

        assert store._label_patches == []

    async def test_flush_restores_patches_on_failure(self):
        """If flush fails, _label_patches is restored for retry."""
        store = _make_store()
        store._label_patches = []

        await store.set_labels("s1", remove_labels=["RootSession"], add_labels=["ForkedSession"])

        mock_tx, mock_session = _make_flush_mocks()
        mock_tx.run = AsyncMock(side_effect=RuntimeError("neo4j down"))
        store._driver.session = MagicMock(return_value=mock_session)
        store._schema_initialized = True

        with pytest.raises(RuntimeError):
            await store.flush()

        assert len(store._label_patches) == 1
        assert store._label_patches[0]["node_id"] == "s1"
