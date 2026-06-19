"""Tests for Neo4jGraphStore — connection setup, buffer operations, flush, schema, and query.

Covers:
- Protocol conformance (GraphStore and QueryableStore isinstance checks)
- workspace property: default 'default', explicit value, settable, no graph_forest_name
- supported_dialects: includes 'cypher', returns frozenset
- Buffer operations: upsert_node (add, merge props, merge labels, no duplicate labels),
  upsert_edge (add/merge), get_node (buffer-first, returns copy), get_edge (buffer-first)
- Static helpers: _sanitize_properties
- Flush: workspace in rows, empty is no-op, clears buffers on success, restores on failure
- Schema: idempotent, indexes use workspace not graph_forest_name
- execute_query: injects workspace, wildcard skips injection, unsupported dialect raises ValueError
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from neo4j.exceptions import Neo4jError

from context_intelligence_server.graph_store import GraphStore, QueryableStore
from context_intelligence_server.neo4j_store import (
    Neo4jGraphStore,
    _convert_temporal_props,
    _normalize_temporal,
    _validate_identifier,
    ensure_neo4j_schema,
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
    assert hasattr(store, "_label_patches"), (
        "_label_patches attribute must be set in __init__"
    )
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
    so that ``async with driver.session(...) as session:`` yields the same object.

    The store now persists batches through the driver-managed transaction API
    (``await db_session.execute_write(_write_batch, *args)``), so ``execute_write``
    is wired to drive the supplied write function against ``mock_tx`` exactly as
    the real driver drives it against a managed transaction:
    ``await fn(mock_tx, *args, **kwargs)``.  This keeps the ``mock_tx.run(...)``
    calls captured for assertions and preserves failure injection (set a
    ``side_effect`` on ``mock_tx.run`` and it propagates out of ``flush()``).
    """
    mock_tx = AsyncMock()

    async def _execute_write(fn, *args, **kwargs):
        return await fn(mock_tx, *args, **kwargs)

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.execute_write = AsyncMock(side_effect=_execute_write)
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
        """Every CREATE INDEX/CONSTRAINT query in _ensure_schema() must use IF NOT EXISTS.

        The Docker Compose stack uses a persistent neo4j_data volume.  When the
        container restarts the Python process starts fresh (_schema_initialized
        resets to False) but the indexes already exist in Neo4j.  Without
        IF NOT EXISTS every restart raises
        Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists, which
        propagates out of flush() and causes the "Final flush failed during
        close" data-loss path.

        Non-CREATE queries (e.g. the deduplication MATCH ... DETACH DELETE that
        runs before constraint creation) are excluded from this check — they are
        idempotent by nature (no-op when no duplicates exist) but do not use the
        IF NOT EXISTS syntax.
        """
        store = _make_store()
        mock_session = self._make_schema_session()
        store._driver.session = MagicMock(return_value=mock_session)

        await store._ensure_schema()

        all_queries = [
            call.args[0] for call in mock_session.run.call_args_list if call.args
        ]
        assert all_queries, "_ensure_schema must issue at least one query"
        # Only CREATE statements must include IF NOT EXISTS; MATCH/data queries are exempt.
        create_queries = [
            q for q in all_queries if q.upper().lstrip().startswith("CREATE")
        ]
        assert create_queries, "_ensure_schema must issue at least one CREATE query"
        for query in create_queries:
            assert "IF NOT EXISTS" in query.upper(), (
                f"Every schema CREATE INDEX/CONSTRAINT must use IF NOT EXISTS so that "
                f"container restarts on a persistent volume do not raise "
                f"EquivalentSchemaRuleAlreadyExists.  Offending query: {query!r}"
            )

    async def test_ensure_schema_creates_uniqueness_constraint(self) -> None:
        """_ensure_schema creates a node_id+workspace uniqueness constraint.

        The constraint makes MERGE (n {node_id, workspace}) truly atomic under
        concurrent worker flushes — two MERGEs for the same (node_id, workspace)
        pair cannot produce two nodes.  This is the database-level enforcement
        that prevents the race condition where child and parent workers both
        flush stub nodes for the same session.
        """
        store = _make_store()
        store._schema_initialized = False

        mock_session = self._make_schema_session()
        store._driver.session = MagicMock(return_value=mock_session)

        await store._ensure_schema()

        all_queries = [
            call.args[0] for call in mock_session.run.call_args_list if call.args
        ]
        constraint_queries = [
            q for q in all_queries if "CONSTRAINT" in q.upper() and "node_id" in q
        ]
        assert constraint_queries, (
            f"Expected a uniqueness CONSTRAINT on (node_id, workspace) to be created "
            f"in _ensure_schema to prevent concurrent-worker duplicate nodes. "
            f"Queries issued: {all_queries}"
        )

    async def test_ensure_schema_creates_event_uniqueness_constraint(self) -> None:
        """_ensure_schema creates the Event (node_id, workspace) uniqueness constraint.

        Mirrors the Session constraint assertion to cover the second
        ``_create_constraint`` call after Steps 3 & 4 were collapsed onto the one
        shared helper — guarding the helper's name/statement parametrization.
        """
        store = _make_store()
        store._schema_initialized = False

        mock_session = self._make_schema_session()
        store._driver.session = MagicMock(return_value=mock_session)

        await store._ensure_schema()

        all_queries = [
            call.args[0] for call in mock_session.run.call_args_list if call.args
        ]
        event_constraint_queries = [
            q
            for q in all_queries
            if "CONSTRAINT" in q.upper() and "event_node_id_workspace_unique" in q
        ]
        assert event_constraint_queries, (
            f"Expected the Event uniqueness CONSTRAINT "
            f"(event_node_id_workspace_unique) to be created in _ensure_schema. "
            f"Queries issued: {all_queries}"
        )

    async def test_ensure_schema_deduplicates_session_and_event_nodes(self) -> None:
        """Step 1 must deduplicate duplicate Session AND Event nodes.

        Both Session and Event carry a uniqueness constraint (Steps 3 & 4). With
        the retry-until-established behavior, a duplicate Event node makes the
        Event ``CREATE CONSTRAINT`` fail ``ConstraintCreationFailed`` on every
        flush forever unless a dedup pass clears the duplicates first. The
        Session dedup already exists; the Event dedup must mirror it so the
        constraint retry can converge.
        """
        store = _make_store()
        store._schema_initialized = False

        mock_session = self._make_schema_session()
        store._driver.session = MagicMock(return_value=mock_session)

        await store._ensure_schema()

        all_queries = [
            call.args[0] for call in mock_session.run.call_args_list if call.args
        ]
        session_dedup = [
            q for q in all_queries if "MATCH (s:Session)" in q and "DETACH DELETE" in q
        ]
        event_dedup = [
            q for q in all_queries if "MATCH (e:Event)" in q and "DETACH DELETE" in q
        ]
        assert session_dedup, (
            f"Expected a Session dedup (MATCH (s:Session) ... DETACH DELETE) query. "
            f"Queries issued: {all_queries}"
        )
        assert event_dedup, (
            f"Expected an Event dedup (MATCH (e:Event) ... DETACH DELETE) query so "
            f"the Event uniqueness constraint retry can converge. "
            f"Queries issued: {all_queries}"
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


def test_sanitize_properties_strips_empty_string_at_fields():
    """_sanitize_properties removes *_at keys whose value is an empty string.

    Protects against SET n += row.props overwriting a previously valid
    timestamp on the existing node with "".
    """
    result = Neo4jGraphStore._sanitize_properties({"started_at": "", "name": "node"})
    assert "started_at" not in result
    assert result["name"] == "node"


def test_sanitize_properties_preserves_non_empty_at_fields():
    """_sanitize_properties keeps *_at keys with non-empty string values."""
    result = Neo4jGraphStore._sanitize_properties(
        {"started_at": "2026-03-18T14:55:17+00:00"}
    )
    assert result["started_at"] == "2026-03-18T14:55:17+00:00"


def test_sanitize_properties_passes_datetime_through():
    """_sanitize_properties keeps datetime values as-is (not str-coerced)."""
    dt = datetime(2026, 3, 18, 14, 55, 17, tzinfo=timezone.utc)
    result = Neo4jGraphStore._sanitize_properties({"started_at": dt})
    assert result["started_at"] is dt
    assert isinstance(result["started_at"], datetime)


# ---------------------------------------------------------------------------
# Datetime conversion to Neo4j temporal types is deferred.
# See DATETIME-MIGRATION.md at the workspace root for the full
# implementation and backfill strategy.
# ---------------------------------------------------------------------------


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


async def test_get_node_fallback_includes_labels():
    """Post-flush fallback must return node labels under a 'labels' key.

    The Neo4j fallback previously returned properties(n) only, which EXCLUDES
    labels. A handler reading a label-blind node resolves current_type to None
    and adds a second type label. The fallback must also return labels(n),
    merged into the dict under 'labels', matching the in-buffer shape.

    FAILS before fix  -> result has no 'labels' key.
    PASSES after fix  -> result['labels'] contains the node's labels.
    """
    store = _make_store()
    store._node_buffer = {}  # simulate post-flush: buffer is empty

    record = {
        "props": {"node_id": "child-1", "session_id": "child-1"},
        "lbls": ["Session", "ForkedSession", "SST_EVENT"],
    }
    mock_result = MagicMock()
    mock_result.records = [record]
    store._driver.execute_query = AsyncMock(return_value=mock_result)  # type: ignore[attr-defined]

    result = await store.get_node("child-1")

    assert result is not None
    assert "labels" in result, (
        f"get_node fallback must return a 'labels' key; got keys {sorted(result.keys())}"
    )
    assert "ForkedSession" in result["labels"]
    assert "Session" in result["labels"]


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

        await store.set_labels(
            "s1", remove_labels=["RootSession"], add_labels=["ForkedSession"]
        )

        assert len(store._label_patches) == 1
        patch = store._label_patches[0]
        assert patch["node_id"] == "s1"
        assert patch["remove"] == ["RootSession"]
        assert patch["add"] == ["ForkedSession"]

    async def test_flush_clears_label_patches(self):
        """After a successful flush, _label_patches is empty."""
        store = _make_store()
        store._label_patches = []

        await store.set_labels(
            "s1", remove_labels=["RootSession"], add_labels=["ForkedSession"]
        )

        mock_tx, mock_session = _make_flush_mocks()
        store._driver.session = MagicMock(return_value=mock_session)
        store._schema_initialized = True

        await store.flush()

        assert store._label_patches == []

    async def test_flush_restores_patches_on_failure(self):
        """If flush fails, _label_patches is restored for retry."""
        store = _make_store()
        store._label_patches = []

        await store.set_labels(
            "s1", remove_labels=["RootSession"], add_labels=["ForkedSession"]
        )

        mock_tx, mock_session = _make_flush_mocks()
        mock_tx.run = AsyncMock(side_effect=RuntimeError("neo4j down"))
        store._driver.session = MagicMock(return_value=mock_session)
        store._schema_initialized = True

        with pytest.raises(RuntimeError):
            await store.flush()

        assert len(store._label_patches) == 1
        assert store._label_patches[0]["node_id"] == "s1"

    async def test_set_labels_updates_node_buffer_immediately(self) -> None:
        """set_labels must update _node_buffer immediately so get_node() reflects the change.

        This is the regression test for the fork guard bug: _handle_fork calls set_labels
        to mark a session as ForkedSession, then _handle_start calls get_node() to check
        labels. Without this fix, get_node() returns stale labels and the fork guard fails.
        """
        store = _make_store()
        store._node_buffer["s1"] = {"labels": ["Session"], "status": "running"}
        await store.set_labels("s1", remove_labels=[], add_labels=["ForkedSession"])
        node = await store.get_node("s1")
        assert node is not None
        assert "ForkedSession" in node["labels"], (
            "set_labels must update _node_buffer immediately — get_node() returned stale labels"
        )
        assert "Session" in node["labels"]


# ---------------------------------------------------------------------------
# TestNeo4jGraphStoreFlushNoLabelMerge — label-free MERGE regression
# ---------------------------------------------------------------------------


class TestNeo4jGraphStoreFlushNoLabelMerge:
    """flush() MERGEs node identity on the universal :Node label.

    All node writers (Session and others) MERGE on (n:Node {node_id, workspace}) so a
    bare :Node placeholder created by the cross-session edge writer converges with the
    later typed write instead of splitting identity.  Concurrent-worker atomicity is
    guaranteed by the :Node(node_id, workspace) UNIQUENESS CONSTRAINT (it replaced the
    former :Session-keyed MERGE + :Session constraint for this purpose).  The :Session
    type label is applied via ``SET n:Session`` in the same MERGE statement; labels
    beyond Session (RootSession, SubSession, ForkedSession) are applied separately via
    MATCH ... SET n:Label.
    """

    def _make_store(self) -> Neo4jGraphStore:
        return _make_store(workspace="test-ws")

    async def test_flush_session_node_merge_includes_session_label(self) -> None:
        """flush() MERGE for Session nodes keys on :Node and SETs the Session label.

        Correct:  MERGE (n:Node {node_id: ..., workspace: ...}) SET ... n:Session
        Wrong:    MERGE (n:Session {node_id: ..., workspace: ...})   (splits identity
                  vs a bare :Node placeholder created by the cross-session edge writer)

        Identity MERGEs on the universal :Node label so a placeholder endpoint
        converges with this typed write; the :Node(node_id, workspace) uniqueness
        constraint makes concurrent MERGEs atomic (the role :Session used to play).
        The :Session type label is still guaranteed — applied via SET in the same
        statement.
        """
        import re

        store = self._make_store()
        await store.upsert_node(
            "parent-123", {"labels": ["Session"], "status": "running"}
        )

        mock_tx, mock_session = _make_flush_mocks()
        store._driver.session = MagicMock(return_value=mock_session)
        store._schema_initialized = True

        await store.flush()

        merge_queries = [
            str(c.args[0])
            for c in mock_tx.run.call_args_list
            if c.args and "MERGE" in str(c.args[0])
        ]
        assert merge_queries, "Expected at least one MERGE query during flush"
        # Session node identity MERGEs on :Node (converges with edge-writer placeholders;
        # atomic via the :Node uniqueness constraint) and SETs the :Session label.
        assert any(
            re.search(r"MERGE\s*\(n:Node\s*\{", q) and "n:Session" in q
            for q in merge_queries
        ), (
            f"flush() MUST MERGE Session-node identity on :Node and SET n:Session. "
            f"Queries issued: {merge_queries}"
        )

    async def test_flush_second_cycle_uses_session_label_merge_for_reclassified_node(
        self,
    ) -> None:
        """A node written as bare Session then re-written as RootSession:Session must use
        MERGE (n:Session {...}) in both flushes — Session label always present.

        Session base label is never removed, so MERGE (n:Session) always finds the same
        node across flush cycles. This prevents both cross-cycle and concurrent duplicates.
        """
        import re

        store = self._make_store()

        # Flush 1: bare Session (ensure_session_node pattern)
        await store.upsert_node(
            "parent-123", {"labels": ["Session"], "status": "running"}
        )
        mock_tx, mock_session = _make_flush_mocks()
        store._driver.session = MagicMock(return_value=mock_session)
        store._schema_initialized = True
        await store.flush()

        # Flush 2: same node re-classified as RootSession:Session
        await store.upsert_node(
            "parent-123", {"labels": ["RootSession", "Session"], "ended_at": "T1"}
        )
        mock_tx2, mock_session2 = _make_flush_mocks()
        store._driver.session = MagicMock(return_value=mock_session2)
        store._schema_initialized = True
        await store.flush()

        merge_queries_flush2 = [
            str(c.args[0])
            for c in mock_tx2.run.call_args_list
            if c.args and "MERGE" in str(c.args[0])
        ]
        assert merge_queries_flush2, "Expected MERGE queries in flush 2"
        # Both flush cycles MERGE identity on :Node (converges across cycles + with
        # edge-writer placeholders; atomic via the :Node uniqueness constraint).
        assert any(
            re.search(r"MERGE\s*\(n:Node\s*\{", q) for q in merge_queries_flush2
        ), (
            f"Flush 2 MUST MERGE Session-node identity on :Node. "
            f"Identity is always (node_id, workspace) on :Node, so this MERGE finds "
            f"the existing node across cycles. Queries seen: {merge_queries_flush2}"
        )

        # Labels must be applied separately via MATCH ... SET n:Label
        all_queries_flush2 = [
            str(c.args[0]) for c in mock_tx2.run.call_args_list if c.args
        ]
        set_label_queries = [
            q for q in all_queries_flush2 if "SET n:" in q and "MERGE" not in q
        ]
        assert any("RootSession" in q for q in set_label_queries), (
            f"Labels must be applied via MATCH ... SET n:Label (not in MERGE). "
            f"Queries seen: {all_queries_flush2}"
        )

    @pytest.mark.anyio
    async def test_flush_session_nodes_use_label_aware_merge(self) -> None:
        """Session nodes MERGE identity on :Node (SET n:Session), not label-free MERGE.

        Regression test: a label-free MERGE (n {...}) is unindexed (AllNodesScan) and
        unconstrained; identity MERGEs on :Node so it is index-backed and made atomic
        by the :Node(node_id, workspace) uniqueness constraint, preventing concurrent
        flushes (and edge-writer placeholders) from creating duplicates.
        """
        store = self._make_store()
        store._node_buffer["s1"] = {
            "labels": ["ForkedSession", "Session"],
            "session_id": "s1",
            "status": "running",
        }

        captured_queries: list[str] = []

        async def capture(query: str, **kwargs):
            captured_queries.append(query)
            return AsyncMock()

        mock_tx = AsyncMock()
        mock_tx.run = AsyncMock(side_effect=capture)
        mock_tx.commit = AsyncMock()

        async def _execute_write(fn, *args, **kwargs):
            # Drive the managed-transaction write function against mock_tx,
            # mirroring the real driver's execute_write(fn, *args) contract.
            return await fn(mock_tx, *args, **kwargs)

        mock_db_session = AsyncMock()
        mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
        mock_db_session.__aexit__ = AsyncMock(return_value=False)
        mock_db_session.execute_write = AsyncMock(side_effect=_execute_write)
        store._driver.session = MagicMock(return_value=mock_db_session)

        from unittest.mock import patch as mock_patch

        with mock_patch.object(store, "_ensure_schema", AsyncMock()):
            await store.flush()

        merge_queries = [
            q for q in captured_queries if "MERGE" in q and "row.node_id" in q
        ]
        assert merge_queries, "Must have at least one MERGE query"
        # Session-node identity MERGEs on :Node (index-backed + atomic via the :Node
        # uniqueness constraint) and SETs the :Session label — never a label-free MERGE.
        for q in merge_queries:
            assert "MERGE (n:Node {" in q and "n:Session" in q, (
                f"Session nodes must MERGE identity on :Node and SET n:Session. Got: {q}"
            )

    @pytest.mark.anyio
    async def test_ensure_schema_uses_label_based_constraint_syntax(self) -> None:
        """_ensure_schema must create FOR (n:Session) constraint, not label-free FOR (n)."""
        store = self._make_store()
        store._schema_initialized = False

        constraint_queries: list[str] = []

        async def capture(query: str, **kwargs):
            if "CONSTRAINT" in query:
                constraint_queries.append(query)
            return AsyncMock()

        mock_session = AsyncMock()
        mock_session.run = AsyncMock(side_effect=capture)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        store._driver.session = MagicMock(return_value=mock_session)

        await store._ensure_schema()

        assert constraint_queries, "Must have at least one constraint creation query"
        for q in constraint_queries:
            assert "FOR (n:" in q, (
                f"Constraint must use label-based FOR (n:Label) syntax, not label-free (n). Got: {q}"
            )
            assert "IS UNIQUE" in q, f"Must use IS UNIQUE syntax. Got: {q}"


# ---------------------------------------------------------------------------
# Startup schema callable — lifespan fix regression guard
# ---------------------------------------------------------------------------


def test_ensure_schema_is_callable_at_startup() -> None:
    """ensure_neo4j_schema must be importable and callable for lifespan use.

    This test is a regression guard for the concurrency bug where _ensure_schema()
    was called inside flush() transactions.  Under concurrent upload load, multiple
    flushes start before any constraint is committed, so the uniqueness constraint is
    never active when it is needed.

    The fix extracts ensure_neo4j_schema() as a module-level coroutine function so
    main.py's lifespan handler can call it once at startup, before the server accepts
    any requests.
    """
    import inspect

    from context_intelligence_server.neo4j_store import ensure_neo4j_schema

    assert inspect.iscoroutinefunction(ensure_neo4j_schema), (
        "ensure_neo4j_schema must be an async function so lifespan can await it"
    )


# ---------------------------------------------------------------------------
# Deadlock prevention: flush() must serialize concurrent callers via asyncio.Lock
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flush_lock_serializes_concurrent_calls() -> None:
    """Concurrent flush() calls must serialize — second waits for first to finish.

    Regression guard for the Neo4j deadlock: without an asyncio.Lock two
    concurrent flush() coroutines can open separate transactions on the same
    nodes simultaneously, causing a Neo4j deadlock.  The _flush_lock ensures
    the second caller blocks until the first has committed and released.

    Verification strategy: the first flush() holds its DB session open until
    an asyncio.Event is set.  The second flush() is launched concurrently.
    We assert that the second session is only opened AFTER the first has
    fully completed (i.e. "flush_1_done" precedes "session_2_active" in the
    execution log).
    """
    store = _make_store()
    store._schema_initialized = True  # skip schema so the lock is the only gate

    order: list[str] = []
    first_entered = asyncio.Event()
    release_first = asyncio.Event()

    session_n = [0]  # monotonic counter so each session gets a stable identity

    def make_session() -> object:
        """Return an async context-manager whose __aenter__ blocks on call 1."""
        session_n[0] += 1
        n = session_n[0]

        class ControlledSession:
            _tx: object = None

            async def __aenter__(self) -> "ControlledSession":
                order.append(f"session_{n}_start")
                if n == 1:
                    first_entered.set()
                    await release_first.wait()
                order.append(f"session_{n}_active")
                tx = AsyncMock()
                tx.commit = AsyncMock()
                tx.rollback = AsyncMock()
                self._tx = tx
                return self

            async def __aexit__(self, *args: object) -> None:
                pass

            async def execute_write(
                self, fn, *args: object, **kwargs: object
            ) -> object:
                # Driver-managed transaction: drive the write function against the
                # captured tx exactly as the real driver does (await fn(tx, *args)).
                return await fn(self._tx, *args, **kwargs)

        return ControlledSession()

    store._driver.session = MagicMock(side_effect=lambda **_kw: make_session())

    # Seed buffer so the first flush() does real work (not the early-exit path)
    store._node_buffer["n1"] = {"name": "first"}

    async def run_first() -> None:
        order.append("flush_1_called")
        await store.flush()
        order.append("flush_1_done")

    async def run_second() -> None:
        # Wait until the first flush is inside its (slow) session
        await first_entered.wait()
        # Give the second caller something to flush so it also opens a session
        store._node_buffer["n2"] = {"name": "second"}
        order.append("flush_2_called")
        await store.flush()
        order.append("flush_2_done")

    async def releaser() -> None:
        """Release the first session after a short delay."""
        await first_entered.wait()
        await asyncio.sleep(0.05)
        release_first.set()

    await asyncio.gather(run_first(), run_second(), releaser())

    # Both flushes must complete
    assert "flush_1_done" in order, f"flush_1 never finished. order={order}"
    assert "flush_2_done" in order, f"flush_2 never finished. order={order}"

    # Core invariant: the second session must NOT open until flush_1 is done.
    # Without the lock the second session opens while flush_1 is still blocked
    # (deadlock scenario); with the lock it waits.
    if "session_2_active" in order:
        assert order.index("flush_1_done") < order.index("session_2_active"), (
            "session_2 became active before flush_1 completed — "
            f"concurrent transactions detected. Execution order: {order}"
        )


# ---------------------------------------------------------------------------
# _convert_temporal_props (write-path ISO-string → datetime conversion)
# ---------------------------------------------------------------------------


def test_convert_temporal_props_converts_started_at() -> None:
    """_convert_temporal_props converts started_at ISO string to datetime in place."""
    props: dict = {"started_at": "2026-03-18T14:55:17+00:00"}
    result = _convert_temporal_props(props)
    assert result is None  # mutates in place, returns None
    assert isinstance(props["started_at"], datetime)
    assert props["started_at"] == datetime(2026, 3, 18, 14, 55, 17, tzinfo=timezone.utc)


def test_convert_temporal_props_converts_last_updated() -> None:
    """_convert_temporal_props converts last_updated ISO string to datetime."""
    props: dict = {"last_updated": "2026-01-01T00:00:01Z"}
    _convert_temporal_props(props)
    assert isinstance(props["last_updated"], datetime)


def test_convert_temporal_props_converts_edge_occurred_at() -> None:
    """_convert_temporal_props converts occurred_at ISO string to datetime (edge property)."""
    props: dict = {"occurred_at": "2026-01-01T00:00:01+00:00"}
    _convert_temporal_props(props)
    assert isinstance(props["occurred_at"], datetime)


def test_convert_temporal_props_malformed_string_unchanged_and_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """_convert_temporal_props leaves malformed timestamps unchanged and logs a WARNING."""
    props: dict = {"started_at": "not-a-timestamp"}
    with caplog.at_level("WARNING"):
        _convert_temporal_props(props)  # must not raise
    assert props["started_at"] == "not-a-timestamp"
    assert any(record.levelname == "WARNING" for record in caplog.records)


def test_convert_temporal_props_empty_string_passes_through() -> None:
    """_convert_temporal_props skips empty string values (passes through unchanged)."""
    props: dict = {"started_at": ""}
    _convert_temporal_props(props)
    assert props["started_at"] == ""


def test_convert_temporal_props_existing_datetime_untouched() -> None:
    """_convert_temporal_props leaves already-datetime values untouched (idempotent)."""
    dt = datetime(2026, 3, 18, 14, 55, 17, tzinfo=timezone.utc)
    props: dict = {"started_at": dt}
    _convert_temporal_props(props)
    assert props["started_at"] is dt


def test_convert_temporal_props_non_registered_key_untouched() -> None:
    """_convert_temporal_props skips keys not in the TEMPORAL_PROPS registry."""
    props: dict = {"name": "2026-03-18T14:55:17+00:00"}
    _convert_temporal_props(props)
    assert props["name"] == "2026-03-18T14:55:17+00:00"


# ---------------------------------------------------------------------------
# _normalize_temporal (read-path neo4j.time.DateTime → Python datetime)
# ---------------------------------------------------------------------------


def test_normalize_temporal_converts_neo4j_datetime() -> None:
    """_normalize_temporal converts a neo4j.time.DateTime to a Python datetime."""
    from neo4j.time import DateTime as Neo4jDateTime

    py = datetime(2026, 3, 18, 14, 55, 17, tzinfo=timezone.utc)
    n4 = Neo4jDateTime.from_native(py)
    result = _normalize_temporal(n4)
    assert isinstance(result, datetime)
    assert result == py


def test_normalize_temporal_passes_through_str() -> None:
    """_normalize_temporal returns strings unchanged."""
    assert _normalize_temporal("2026-01-01T00:00:01Z") == "2026-01-01T00:00:01Z"


def test_normalize_temporal_passes_through_python_datetime() -> None:
    """_normalize_temporal returns a Python datetime as-is (identity preserved)."""
    dt = datetime(2026, 3, 18, 14, 55, 17, tzinfo=timezone.utc)
    assert _normalize_temporal(dt) is dt


def test_normalize_temporal_passes_through_int() -> None:
    """_normalize_temporal returns integers unchanged."""
    assert _normalize_temporal(42) == 42


# ---------------------------------------------------------------------------
# discard_buffer (Phase B2 — COE blocker for line-by-line poison isolation)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_discard_buffer_clears_all_buffers_without_flushing():
    """discard_buffer clears node/edge/label buffers with no driver I/O."""
    store = _make_store()
    await store.upsert_node("n1", {"name": "Alice"})
    await store.upsert_edge("src", "dst", {"type": "KNOWS"})
    await store.set_labels("n1", remove_labels=[], add_labels=["Person"])

    # Sanity: buffers populated before discard
    assert store._node_buffer
    assert store._edge_buffer
    assert store._label_patches

    store._driver = AsyncMock()  # type: ignore[assignment]
    store.discard_buffer()

    assert store._node_buffer == {}
    assert store._edge_buffer == {}
    assert store._label_patches == []
    store._driver.session.assert_not_called()


# ---------------------------------------------------------------------------
# flush_chunk_rows / flush_chunk_bytes constructor params (Task 3)
# ---------------------------------------------------------------------------


def _make_store_chunked(rows: int, byts: int) -> Neo4jGraphStore:
    """Create a Neo4jGraphStore with explicit chunk-size knobs."""
    with patch(
        "context_intelligence_server.neo4j_store.AsyncGraphDatabase"
    ) as mock_adb:
        mock_driver = AsyncMock()
        mock_adb.driver.return_value = mock_driver
        store = Neo4jGraphStore(
            uri="bolt://localhost:7687",
            auth=("neo4j", "password"),
            workspace="test",
            flush_chunk_rows=rows,
            flush_chunk_bytes=byts,
        )
    return store


def test_init_stores_chunk_bounds():
    """flush_chunk_rows=50 and flush_chunk_bytes=1024 are stored unchanged."""
    store = _make_store_chunked(50, 1024)
    assert store._flush_chunk_rows == 50
    assert store._flush_chunk_bytes == 1024


def test_init_clamps_nonpositive_bounds_to_one():
    """Zero and negative values are clamped to 1 so chunks are never empty."""
    store_zero = _make_store_chunked(0, 0)
    assert store_zero._flush_chunk_rows == 1
    assert store_zero._flush_chunk_bytes == 1

    store_neg = _make_store_chunked(-5, -100)
    assert store_neg._flush_chunk_rows == 1
    assert store_neg._flush_chunk_bytes == 1


def test_init_defaults_when_params_omitted():
    """When flush_chunk_rows/bytes are omitted the defaults are 100 / 4_194_304."""
    store = _make_store()
    assert store._flush_chunk_rows == 100
    assert store._flush_chunk_bytes == 4_194_304


# ---------------------------------------------------------------------------
# _serialized_row_size tests
# ---------------------------------------------------------------------------


def test_serialized_row_size_uses_serialized_form_not_len():
    """_serialized_row_size measures JSON bytes, not element count.

    A len()-based estimator on a dict/list returns the key/element count (3 for
    fat below), which is blind to fat nested payloads.  The serialized form of
    fat is several thousand bytes.
    """
    from context_intelligence_server.neo4j_store import _serialized_row_size

    fat = {
        "messages": ["x" * 2000],
        "context_snapshot": {"a": "y" * 2000},
        "k": 1,
    }
    # len(fat) == 3; the serialized form is >> 3000 bytes
    assert _serialized_row_size(fat) > 3000


def test_serialized_row_size_handles_unjsonable_value():
    """Non-JSON-serializable values fall back to str() length and never crash."""
    from context_intelligence_server.neo4j_store import _serialized_row_size

    result = _serialized_row_size({"ts": datetime(2026, 6, 18)})
    assert result > 0


# ---------------------------------------------------------------------------
# _chunk_dict / _chunk_list tests
# ---------------------------------------------------------------------------


def test_chunk_dict_row_bound():
    """250 tiny rows at max_rows=100 → chunk sizes [100, 100, 50], no loss/dup."""
    from context_intelligence_server.neo4j_store import _chunk_dict

    snapshot = {str(i): {"v": i} for i in range(250)}
    # Use enormous max_bytes so only the row bound trips
    chunks = list(_chunk_dict(snapshot, max_rows=100, max_bytes=10_000_000))

    assert [len(c) for c in chunks] == [100, 100, 50]
    merged = {}
    for c in chunks:
        merged.update(c)
    assert merged == snapshot


def test_chunk_dict_byte_bound():
    """6 rows of ~2 KB blob each at max_bytes=5000 → all chunks len<=2, at least 3 chunks."""
    from context_intelligence_server.neo4j_store import _chunk_dict

    # Each value is ~2 KB; two rows together are ~4 KB which is < 5000, but three would exceed
    blob = "x" * 2000
    snapshot = {str(i): {"blob": blob} for i in range(6)}
    chunks = list(_chunk_dict(snapshot, max_rows=1000, max_bytes=5000))

    assert all(len(c) <= 2 for c in chunks), (
        f"Expected all chunks <=2 rows, got {[len(c) for c in chunks]}"
    )
    assert len(chunks) >= 3, f"Expected >=3 chunks, got {len(chunks)}"
    # No loss
    merged = {}
    for c in chunks:
        merged.update(c)
    assert merged == snapshot


def test_chunk_dict_one_row_floor():
    """Oversized row is yielded alone (one-row floor); next row goes in next chunk."""
    from context_intelligence_server.neo4j_store import _chunk_dict

    snapshot = {
        "big": "x" * 10000,
        "small": {"v": 1},
    }
    chunks = list(_chunk_dict(snapshot, max_rows=1000, max_bytes=1000))

    assert len(chunks) == 2, (
        f"Expected 2 chunks, got {len(chunks)}: {[list(c.keys()) for c in chunks]}"
    )
    assert list(chunks[0].keys()) == ["big"], (
        f"Expected chunk[0] to contain only 'big', got {list(chunks[0].keys())}"
    )
    assert list(chunks[1].keys()) == ["small"], (
        f"Expected chunk[1] to contain only 'small', got {list(chunks[1].keys())}"
    )


def test_chunk_dict_empty_yields_nothing():
    """An empty snapshot produces no chunks."""
    from context_intelligence_server.neo4j_store import _chunk_dict

    result = list(_chunk_dict({}, max_rows=100, max_bytes=4_194_304))
    assert result == []


def test_chunk_list_row_bound():
    """150 patches at max_rows=100 → chunks of [100, 50], flattened equals input."""
    from context_intelligence_server.neo4j_store import _chunk_list

    patches = [{"node_id": str(i), "label": f"L{i}"} for i in range(150)]
    # Use enormous max_bytes so only row bound trips
    chunks = list(_chunk_list(patches, max_rows=100, max_bytes=10_000_000))

    assert [len(c) for c in chunks] == [100, 50]
    flattened = [item for c in chunks for item in c]
    assert flattened == patches


# ---------------------------------------------------------------------------
# Coordinator wiring guard (Task 9)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_coordinator_every_execute_write_within_bounds() -> None:
    """Every execute_write call passes a node chunk within both row and byte bounds.

    Coordinator characterization guard: 35 nodes at rows=10 must produce 4
    execute_write calls for the node phase, each carrying ≤10 nodes and a
    total serialized size ≤10_000_000 bytes (or exactly 1 node for the
    single-oversized-row floor).  Does NOT re-test chunk-size math — that is
    Task 5's responsibility.  This test guards only that the coordinator wires
    chunk payloads to execute_write correctly.
    """
    from context_intelligence_server.neo4j_store import _serialized_row_size

    store = _make_store_chunked(rows=10, byts=10_000_000)
    store._schema_initialized = True

    # Populate 35 simple nodes — row bound of 10 produces 4 chunks: [10,10,10,5]
    store._node_buffer = {f"n{i}": {"name": f"node-{i}"} for i in range(35)}

    captured_payloads: list[tuple[dict, dict, list]] = []

    async def _capture(fn, *args, **kwargs):
        # fn=_write_batch; args = (nodes_chunk, edges_chunk, patches_chunk, workspace)
        nodes: dict = args[0]
        edges: dict = args[1]
        patches: list = args[2]
        captured_payloads.append((nodes, edges, patches))

    fake_session = AsyncMock()
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)
    fake_session.execute_write = AsyncMock(side_effect=_capture)
    store._driver.session = MagicMock(return_value=fake_session)

    await store.flush()

    assert captured_payloads, (
        "Expected execute_write to be called at least once (35 nodes → 4 node chunks)"
    )
    for nodes, _edges, _patches in captured_payloads:
        if not nodes:
            continue  # skip edge/patch-only calls (empty nodes dict)
        assert len(nodes) <= 10, f"Node chunk exceeded row bound: {len(nodes)} > 10"
        total_bytes = sum(_serialized_row_size(v) for v in nodes.values())
        assert total_bytes <= 10_000_000 or len(nodes) == 1, (
            f"Node chunk exceeded byte bound: {total_bytes} bytes across "
            f"{len(nodes)} nodes (expected ≤10_000_000 or single-row floor)"
        )


@pytest.mark.asyncio
async def test_coordinator_empty_buffer_makes_zero_calls() -> None:
    """flush() with empty buffers must not call execute_write at all.

    Early-exit guard: the coordinator short-circuits before opening a driver
    session when all three buffers (_node_buffer, _edge_buffer, _label_patches)
    are empty, so execute_write must never be invoked.
    """
    store = _make_store_chunked(rows=100, byts=4_194_304)
    store._schema_initialized = True

    fake_session = AsyncMock()
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)
    fake_session.execute_write = AsyncMock()
    store._driver.session = MagicMock(return_value=fake_session)

    # Buffers are empty by default — flush should early-exit with no DB calls
    await store.flush()

    fake_session.execute_write.assert_not_called()


# ---------------------------------------------------------------------------
# Coordinator phase ordering guard (Task 10 — A.4)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_coordinator_phase_ordering_nodes_patches_edges() -> None:
    """All node chunks precede all patch chunks which precede all edge chunks.

    Phase-ordering guard: rows=2 forces multiple chunks per phase (5 nodes →
    3 chunks, 3 patches → 2 chunks, 2 edges → 1 chunk).  The side_effect on
    execute_write records which phase each call belongs to; the assertion
    verifies there is no interleaving between phases and that all three phases
    are exercised.
    """
    store = _make_store_chunked(rows=2, byts=10_000_000)
    store._schema_initialized = True

    # Populate buffers to force multiple chunks per phase
    store._node_buffer = {f"n{i}": {"name": f"node-{i}"} for i in range(5)}
    store._label_patches = [
        {"node_id": f"n{i}", "add": ["X"], "remove": []} for i in range(3)
    ]
    store._edge_buffer = {
        ("n0", "n1"): {"type": "KNOWS"},
        ("n2", "n3"): {"type": "LINKS"},
    }

    order: list[str] = []

    async def _record_phase(fn, *args, **kwargs) -> None:
        # fn=_write_batch; positional args = (nodes_chunk, edges_chunk, patches_chunk, ...)
        nodes = args[0]
        edges = args[1]
        patches = args[2]
        if nodes:
            order.append("node")
        elif patches:
            order.append("patch")
        elif edges:
            order.append("edge")

    fake_session = AsyncMock()
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)
    fake_session.execute_write = AsyncMock(side_effect=_record_phase)
    store._driver.session = MagicMock(return_value=fake_session)

    await store.flush()

    # All three phases must be present
    assert "node" in order, "Expected at least one node-phase execute_write call"
    assert "patch" in order, "Expected at least one patch-phase execute_write call"
    assert "edge" in order, "Expected at least one edge-phase execute_write call"

    # No phase interleaving: order must equal its phase-sorted form
    phase_key = {"node": 0, "patch": 1, "edge": 2}
    assert order == sorted(order, key=lambda p: phase_key[p]), (
        f"Phase interleaving detected — actual call order: {order}"
    )


# ---------------------------------------------------------------------------
# Re-raise invariant — A.5 (Task 11)
# ---------------------------------------------------------------------------


def _seq_execute_write_failing_on(call_index: int) -> AsyncMock:
    """Return an execute_write mock that succeeds until call_index, then raises.

    Call indices 0..(call_index-1) complete normally (return None).
    Call at call_index raises RuntimeError('chunk boom').
    """
    call_count: list[int] = [0]

    async def _side_effect(fn, *args, **kwargs) -> None:  # noqa: ANN001
        idx = call_count[0]
        call_count[0] += 1
        if idx == call_index:
            raise RuntimeError("chunk boom")
        # success path — return None without executing the write function

    return AsyncMock(side_effect=_side_effect)


def _wire_session(store: Neo4jGraphStore, execute_write_mock: AsyncMock) -> None:
    """Wire a fake session boundary with the given execute_write mock onto store.

    Every call to ``store._driver.session(...)`` returns the same fake context
    manager whose ``__aenter__`` yields itself and whose ``execute_write`` is
    the provided mock.  This lets a single call-indexed mock accumulate all
    execute_write invocations across chunks.
    """
    fake_session = MagicMock()
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)
    fake_session.execute_write = execute_write_mock
    store._driver.session = MagicMock(return_value=fake_session)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fail_call,case_id",
    [
        (0, "first_chunk_fails"),
        (1, "later_chunk_same_phase_committed"),
        ("edge", "edge_after_nodes_committed"),
    ],
)
async def test_reraise_restores_full_snapshot_and_logs(
    fail_call: int | str,
    case_id: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Re-raise invariant: RuntimeError propagates, full snapshot restored, ERROR logged.

    Hard constraint #4: the coordinator must re-raise on any chunk failure and
    never return success after a partial flush.

    3 cases with materially different durable-progress state:
    - first_chunk_fails (fail_call=0): nothing committed to Neo4j
    - later_chunk_same_phase_committed (fail_call=1): first node chunk committed,
      second node chunk fails — partial within the node phase
    - edge_after_nodes_committed (fail_call='edge'): all 3 node chunks committed,
      first edge chunk fails — partial durable progress across phases

    In all 3 cases the FULL snapshot (nodes + edges) must be restored to the
    live buffers and an ERROR containing 'flush_chunk_failed' must be logged.
    """
    nodes = {
        "n0": {"name": "node-0"},
        "n1": {"name": "node-1"},
        "n2": {"name": "node-2"},
    }
    edges: dict = {("n0", "n1"): {"type": "KNOWS"}}

    # rows=1 => each row is its own chunk:
    #   Phase 1 nodes:   3 nodes  → 3 execute_write calls at indices 0, 1, 2
    #   Phase 2 patches: 0        → 0 calls
    #   Phase 3 edges:   1 edge   → 1 execute_write call at index 3
    store = _make_store_chunked(rows=1, byts=10_000_000)
    store._schema_initialized = True
    store._node_buffer = dict(nodes)
    store._edge_buffer = dict(edges)

    # Map 'edge' to the first edge call index (index 3 = after 3 node calls)
    actual_call_index: int = 3 if fail_call == "edge" else int(fail_call)  # type: ignore[arg-type]

    execute_write_mock = _seq_execute_write_failing_on(actual_call_index)
    _wire_session(store, execute_write_mock)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError, match="chunk boom"):
            await store.flush()

    # Full snapshot must be restored — not just uncommitted remainder
    assert store._node_buffer == nodes, (
        f"[{case_id}] _node_buffer not fully restored. "
        f"Expected {nodes}, got {store._node_buffer}"
    )
    assert store._edge_buffer == edges, (
        f"[{case_id}] _edge_buffer not fully restored. "
        f"Expected {edges}, got {store._edge_buffer}"
    )

    # An ERROR containing 'flush_chunk_failed' must be logged
    assert any("flush_chunk_failed" in r.message for r in caplog.records), (
        f"[{case_id}] Expected ERROR log containing 'flush_chunk_failed'. "
        f"Records: {[r.getMessage() for r in caplog.records]}"
    )


# ---------------------------------------------------------------------------
# TestSchemaConstraintAllowList
# ---------------------------------------------------------------------------


class TestSchemaConstraintAllowList:
    """Verified benign allow-list for schema constraint creation.

    Benign already-exists / concurrent-schema-race codes must be swallowed at
    DEBUG and execution continues.  Dangerous codes (ConstraintCreationFailed
    and anything else) must be surfaced at ERROR and continue — never re-raised
    into the flush/write path, where a re-raise would make the drain barrier
    count it as a flush failure and dead-letter real events.
    """

    @staticmethod
    def _driver_raising_on_constraint(code: str):
        """Build a mock driver whose CREATE CONSTRAINT statements raise Neo4jError(code).

        ``Neo4jError._hydrate_neo4j`` returns the correct Neo4jError subclass with
        ``.code`` set (``.code`` is a read-only property with no setter, so it
        cannot be assigned directly) and is catchable as ``except Neo4jError``.
        Non-constraint statements (dedup, index creation) succeed.
        """
        err = Neo4jError._hydrate_neo4j(code=code, message="schema failure")

        async def _run(query, *args, **kwargs):
            if "CREATE CONSTRAINT" in query:
                raise err
            if "RETURN count(n) AS remaining" in query:
                # Backfill verification count — model a fully-tagged graph (0
                # untagged) so the migration logs INFO, not the incomplete WARNING.
                # (A bare AsyncMock() would yield MagicMock-int == 1, a spurious
                # "backfill incomplete" warning unrelated to these constraint tests.)
                result = AsyncMock()
                result.single = AsyncMock(return_value={"remaining": 0})
                return result
            return AsyncMock()

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.run = AsyncMock(side_effect=_run)

        mock_driver = MagicMock()
        mock_driver.session = MagicMock(return_value=mock_session)
        return mock_driver

    async def test_benign_deadlock_swallowed_at_debug(self, caplog):
        """DeadlockDetected is benign: no raise, no WARNING/ERROR, a DEBUG constraint line."""
        driver = self._driver_raising_on_constraint(
            "Neo.TransientError.Transaction.DeadlockDetected"
        )

        with caplog.at_level(logging.DEBUG):
            await ensure_neo4j_schema(driver)  # MUST NOT raise

        assert not [r for r in caplog.records if r.levelno >= logging.WARNING], (
            "Benign DeadlockDetected must not log at WARNING or above. "
            f"Records: {[(r.levelname, r.getMessage()) for r in caplog.records]}"
        )
        assert any(
            r.levelno == logging.DEBUG and "constraint" in r.getMessage().lower()
            for r in caplog.records
        ), (
            "Expected a DEBUG record mentioning the constraint. "
            f"Records: {[(r.levelname, r.getMessage()) for r in caplog.records]}"
        )

    async def test_benign_equivalent_rule_swallowed_at_debug(self, caplog):
        """EquivalentSchemaRuleAlreadyExists is benign: no raise, no WARNING/ERROR, DEBUG line."""
        driver = self._driver_raising_on_constraint(
            "Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists"
        )

        with caplog.at_level(logging.DEBUG):
            await ensure_neo4j_schema(driver)  # MUST NOT raise

        assert not [r for r in caplog.records if r.levelno >= logging.WARNING], (
            "Benign EquivalentSchemaRuleAlreadyExists must not log at WARNING or above. "
            f"Records: {[(r.levelname, r.getMessage()) for r in caplog.records]}"
        )
        assert any(
            r.levelno == logging.DEBUG and "constraint" in r.getMessage().lower()
            for r in caplog.records
        ), (
            "Expected a DEBUG record mentioning the constraint. "
            f"Records: {[(r.levelname, r.getMessage()) for r in caplog.records]}"
        )

    async def test_constraint_creation_failed_surfaced_at_error_no_raise(self, caplog):
        """ConstraintCreationFailed is dangerous: no raise, ERROR logged, old wording gone."""
        driver = self._driver_raising_on_constraint(
            "Neo.ClientError.Schema.ConstraintCreationFailed"
        )

        with caplog.at_level(logging.DEBUG):
            await ensure_neo4j_schema(driver)  # MUST NOT raise

        assert any(r.levelno >= logging.ERROR for r in caplog.records), (
            "ConstraintCreationFailed must surface at ERROR. "
            f"Records: {[(r.levelname, r.getMessage()) for r in caplog.records]}"
        )
        assert not any(
            "pre-existing duplicates?" in r.getMessage() for r in caplog.records
        ), "The misleading '(pre-existing duplicates?)' wording must be dropped."

    async def test_constraint_failure_on_flush_path_does_not_dead_letter(self, caplog):
        """END-TO-END: a constraint failure on the flush path must not dead-letter events.

        The schema session raises ConstraintCreationFailed on CREATE CONSTRAINT but
        succeeds on index/dedup; the node-write transaction must still run and the
        node buffer must be cleared (success path), proving the schema error did not
        propagate out of flush() and trigger restore-on-failure + dead-lettering.
        """
        store = _make_store()
        store._schema_initialized = False

        # Schema session: raises ConstraintCreationFailed on CREATE CONSTRAINT,
        # succeeds on dedup/index statements.
        err = Neo4jError._hydrate_neo4j(
            code="Neo.ClientError.Schema.ConstraintCreationFailed",
            message="schema failure",
        )

        async def _schema_run(query, *args, **kwargs):
            if "CREATE CONSTRAINT" in query:
                raise err
            return AsyncMock()

        schema_session = AsyncMock()
        schema_session.__aenter__ = AsyncMock(return_value=schema_session)
        schema_session.run = AsyncMock(side_effect=_schema_run)

        mock_tx, write_session = _make_flush_mocks()

        store._driver.session = MagicMock(
            side_effect=[schema_session, write_session, write_session, write_session]
        )

        await store.upsert_node("n1", {"labels": ["Session"], "name": "test"})

        with caplog.at_level(logging.DEBUG):
            await store.flush()  # MUST NOT raise

        assert any(
            r.levelno >= logging.ERROR and "constraint" in r.getMessage().lower()
            for r in caplog.records
        ), (
            "Expected an ERROR record mentioning the constraint on the flush path. "
            f"Records: {[(r.levelname, r.getMessage()) for r in caplog.records]}"
        )
        assert mock_tx.run.await_count >= 1, (
            "Node-write transaction must still run despite the constraint failure."
        )
        assert store._node_buffer == {}, (
            "Buffer must be cleared (success path) — not restored on failure."
        )

    async def test_connectivity_driver_error_surfaced_at_error_no_raise(self, caplog):
        """ServiceUnavailable (a DriverError, NOT a Neo4jError) on CREATE CONSTRAINT.

        Connectivity errors (ServiceUnavailable / SessionExpired) inherit from
        DriverError, not Neo4jError.  They must be caught, logged at ERROR, and
        MUST NOT propagate out of ensure_neo4j_schema — otherwise a connectivity
        drop on the flush path would be counted as a flush failure and dead-letter
        real events.
        """
        from neo4j.exceptions import ServiceUnavailable

        err = ServiceUnavailable("connection refused")

        async def _run(query, *args, **kwargs):
            if "CREATE CONSTRAINT" in query:
                raise err
            return AsyncMock()

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.run = AsyncMock(side_effect=_run)

        mock_driver = MagicMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        with caplog.at_level(logging.DEBUG):
            await ensure_neo4j_schema(mock_driver)  # MUST NOT raise

        assert any(
            r.levelno >= logging.ERROR and "constraint" in r.getMessage().lower()
            for r in caplog.records
        ), (
            "ServiceUnavailable on constraint creation must surface at ERROR. "
            f"Records: {[(r.levelname, r.getMessage()) for r in caplog.records]}"
        )

    async def test_connectivity_error_on_flush_path_does_not_dead_letter(self, caplog):
        """END-TO-END: a ServiceUnavailable on the flush path must not dead-letter.

        The schema session raises ServiceUnavailable (a DriverError) on CREATE
        CONSTRAINT but succeeds on dedup/index; the node-write transaction must
        still run and the node buffer must be cleared (success path), proving the
        connectivity error did not propagate out of flush() and trigger
        restore-on-failure + dead-lettering.
        """
        from neo4j.exceptions import ServiceUnavailable

        store = _make_store()
        store._schema_initialized = False

        err = ServiceUnavailable("connection refused")

        async def _schema_run(query, *args, **kwargs):
            if "CREATE CONSTRAINT" in query:
                raise err
            return AsyncMock()

        schema_session = AsyncMock()
        schema_session.__aenter__ = AsyncMock(return_value=schema_session)
        schema_session.run = AsyncMock(side_effect=_schema_run)

        mock_tx, write_session = _make_flush_mocks()

        store._driver.session = MagicMock(
            side_effect=[schema_session, write_session, write_session, write_session]
        )

        await store.upsert_node("n1", {"labels": ["Session"], "name": "test"})

        with caplog.at_level(logging.DEBUG):
            await store.flush()  # MUST NOT raise

        assert any(
            r.levelno >= logging.ERROR and "constraint" in r.getMessage().lower()
            for r in caplog.records
        ), (
            "Expected an ERROR record mentioning the constraint on the flush path. "
            f"Records: {[(r.levelname, r.getMessage()) for r in caplog.records]}"
        )
        assert mock_tx.run.await_count >= 1, (
            "Node-write transaction must still run despite the connectivity error."
        )
        assert store._node_buffer == {}, (
            "Buffer must be cleared (success path) — not restored on failure."
        )

    async def test_connectivity_driver_error_on_index_surfaced_no_raise(self, caplog):
        """ServiceUnavailable (a DriverError) on CREATE INDEX (the _create_index path).

        The INDEX-creation steps run BEFORE the constraint steps, so a connectivity
        DriverError (ServiceUnavailable / SessionExpired, NOT a Neo4jError) during
        index creation would otherwise propagate out of ensure_neo4j_schema and
        dead-letter real events on the flush path — reached even before the
        constraint hardening. It must be caught, logged at ERROR, and MUST NOT
        propagate out of ensure_neo4j_schema.
        """
        from neo4j.exceptions import ServiceUnavailable

        err = ServiceUnavailable("connection refused")

        async def _run(query, *args, **kwargs):
            if "CREATE INDEX" in query:
                raise err
            return AsyncMock()

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.run = AsyncMock(side_effect=_run)

        mock_driver = MagicMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        with caplog.at_level(logging.DEBUG):
            await ensure_neo4j_schema(mock_driver)  # MUST NOT raise

        assert any(
            r.levelno >= logging.ERROR and "index" in r.getMessage().lower()
            for r in caplog.records
        ), (
            "ServiceUnavailable on index creation must surface at ERROR. "
            f"Records: {[(r.levelname, r.getMessage()) for r in caplog.records]}"
        )

    async def test_connectivity_error_on_index_step_does_not_dead_letter(self, caplog):
        """END-TO-END: a ServiceUnavailable on the INDEX step must not dead-letter.

        The schema session raises ServiceUnavailable (a DriverError) on CREATE
        INDEX but succeeds on dedup/constraint; the node-write transaction must
        still run and the node buffer must be cleared (success path), proving the
        connectivity error did not propagate out of flush() and trigger
        restore-on-failure + dead-lettering.
        """
        from neo4j.exceptions import ServiceUnavailable

        store = _make_store()
        store._schema_initialized = False

        err = ServiceUnavailable("connection refused")

        async def _schema_run(query, *args, **kwargs):
            if "CREATE INDEX" in query:
                raise err
            return AsyncMock()

        schema_session = AsyncMock()
        schema_session.__aenter__ = AsyncMock(return_value=schema_session)
        schema_session.run = AsyncMock(side_effect=_schema_run)

        mock_tx, write_session = _make_flush_mocks()

        store._driver.session = MagicMock(
            side_effect=[schema_session, write_session, write_session, write_session]
        )

        await store.upsert_node("n1", {"labels": ["Session"], "name": "test"})

        with caplog.at_level(logging.DEBUG):
            await store.flush()  # MUST NOT raise

        assert any(
            r.levelno >= logging.ERROR and "index" in r.getMessage().lower()
            for r in caplog.records
        ), (
            "Expected an ERROR record mentioning the index on the flush path. "
            f"Records: {[(r.levelname, r.getMessage()) for r in caplog.records]}"
        )
        assert mock_tx.run.await_count >= 1, (
            "Node-write transaction must still run despite the connectivity error."
        )
        assert store._node_buffer == {}, (
            "Buffer must be cleared (success path) — not restored on failure."
        )


# ---------------------------------------------------------------------------
# TestSchemaLatchOnSuccess
# ---------------------------------------------------------------------------


class TestSchemaLatchOnSuccess:
    """The ``_schema_initialized`` flag must latch True only on genuine full success.

    Regression for the data-integrity gap: when Neo4j is unreachable while schema
    init runs, ``ensure_neo4j_schema`` swallows the connectivity DriverError (it
    never re-raises, to avoid dead-lettering real events) and the uniqueness
    constraint is NOT created.  If the flag latched True anyway, schema init would
    never be retried for the worker's life — so when Neo4j returns, concurrent
    MERGE runs with no uniqueness backstop and duplicate Session/Event nodes
    accrue until process restart.  The flag must therefore stay False until the
    schema is fully established, letting a later flush retry and self-heal.
    """

    @staticmethod
    def _schema_session_raising_on(substring: str, err: Exception):
        """Schema session whose ``run`` raises *err* for queries containing *substring*."""

        async def _run(query, *args, **kwargs):
            if substring in query:
                raise err
            return AsyncMock()

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.run = AsyncMock(side_effect=_run)
        return session

    @staticmethod
    def _schema_session_all_ok():
        """Schema session whose ``run`` succeeds for every statement."""
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.run = AsyncMock(return_value=AsyncMock())
        return session

    async def test_ensure_schema_does_not_latch_when_constraint_uncreated_on_connectivity(
        self,
    ) -> None:
        """Flag must stay False when the constraint could not be created (connectivity)."""
        from neo4j.exceptions import ServiceUnavailable

        store = _make_store()
        store._schema_initialized = False
        degraded = self._schema_session_raising_on(
            "CREATE CONSTRAINT", ServiceUnavailable("connection refused")
        )
        store._driver.session = MagicMock(return_value=degraded)

        await store._ensure_schema()  # MUST NOT raise

        assert store._schema_initialized is False, (
            "Flag must NOT latch True when the uniqueness constraint could not be "
            "created (connectivity error swallowed) — otherwise schema init is "
            "never retried and duplicate Session/Event nodes accrue."
        )

    async def test_ensure_schema_retries_until_established_then_latches(self) -> None:
        """Degraded first run retries on the next flush, then latches and stops re-running."""
        from neo4j.exceptions import ServiceUnavailable

        store = _make_store()
        store._schema_initialized = False

        degraded = self._schema_session_raising_on(
            "CREATE CONSTRAINT", ServiceUnavailable("connection refused")
        )
        ok_session = self._schema_session_all_ok()
        store._driver.session = MagicMock(side_effect=[degraded, ok_session])

        # First flush: connectivity error on a schema step → flag stays False.
        await store._ensure_schema()
        assert store._schema_initialized is False, (
            "Degraded first run must leave the flag False so a later flush retries."
        )

        # Second flush: schema fully succeeds → flag latches True, and the schema
        # statements were genuinely re-attempted (retry, not skip).
        await store._ensure_schema()
        assert store._schema_initialized is True, (
            "Once the schema is fully established the flag must latch True."
        )
        assert any(
            "CREATE CONSTRAINT" in call.args[0]
            for call in ok_session.run.await_args_list
        ), "Schema statements must be re-attempted on the retry — not skipped."

        # Third flush: flag latched → schema must NOT be re-run.
        store._driver.session = MagicMock(side_effect=AssertionError("schema re-run"))
        await store._ensure_schema()  # MUST NOT touch the driver session

    async def test_ensure_neo4j_schema_returns_true_on_full_success(self) -> None:
        """Direct return value: True when every index/constraint is established."""
        session = self._schema_session_all_ok()
        driver = MagicMock()
        driver.session = MagicMock(return_value=session)

        result = await ensure_neo4j_schema(driver)

        assert result is True

    async def test_ensure_neo4j_schema_returns_false_on_connectivity_error(
        self,
    ) -> None:
        """Direct return value: False when a connectivity error prevents constraint creation."""
        from neo4j.exceptions import ServiceUnavailable

        session = self._schema_session_raising_on(
            "CREATE CONSTRAINT", ServiceUnavailable("connection refused")
        )
        driver = MagicMock()
        driver.session = MagicMock(return_value=session)

        result = await ensure_neo4j_schema(driver)

        assert result is False
