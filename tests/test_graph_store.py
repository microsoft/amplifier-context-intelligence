"""Tests for GraphStore and QueryableStore protocols.

Verifies that both protocols are @runtime_checkable and that isinstance()
checks correctly accept conforming classes and reject non-conforming ones.
"""

from __future__ import annotations

from typing import Any

from context_intelligence_server.graph_store import GraphStore, QueryableStore


# ---------------------------------------------------------------------------
# Minimal conforming implementations for isinstance checks
# ---------------------------------------------------------------------------


class MinimalGraphStore:
    """Conforming implementation of GraphStore with all required members."""

    @property
    def workspace(self) -> str:
        return "test-workspace"

    async def upsert_node(self, node_id: str, data: dict[str, Any]) -> None:
        pass

    async def upsert_edge(self, src_id: str, dst_id: str, data: dict[str, Any]) -> None:
        pass

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        return None

    async def get_edge(self, src_id: str, dst_id: str) -> dict[str, Any] | None:
        return None

    async def flush(self) -> None:
        pass

    def schedule_flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


class MissingUpsertNode:
    """Non-conforming: missing upsert_node method."""

    @property
    def workspace(self) -> str:
        return "test-workspace"

    # No upsert_node!

    async def upsert_edge(self, src_id: str, dst_id: str, data: dict[str, Any]) -> None:
        pass

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        return None

    async def get_edge(self, src_id: str, dst_id: str) -> dict[str, Any] | None:
        return None

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


class MinimalQueryableStore:
    """Conforming implementation of QueryableStore with all required members."""

    @property
    def workspace(self) -> str:
        return "test-workspace"

    @property
    def supported_dialects(self) -> frozenset[str]:
        return frozenset({"cypher", "sparql"})

    async def upsert_node(self, node_id: str, data: dict[str, Any]) -> None:
        pass

    async def upsert_edge(self, src_id: str, dst_id: str, data: dict[str, Any]) -> None:
        pass

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        return None

    async def get_edge(self, src_id: str, dst_id: str) -> dict[str, Any] | None:
        return None

    async def flush(self) -> None:
        pass

    def schedule_flush(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def execute_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        dialect: str = "cypher",
        workspace: str | None = None,
    ) -> list[dict[str, Any]]:
        return []


# ---------------------------------------------------------------------------
# GraphStore protocol tests
# ---------------------------------------------------------------------------


def test_graph_store_is_runtime_checkable():
    """GraphStore must be decorated with @runtime_checkable."""
    store = MinimalGraphStore()
    # If not runtime_checkable this raises TypeError; if it is, it returns bool
    result = isinstance(store, GraphStore)
    assert isinstance(result, bool)


def test_conforming_class_is_instance_of_graph_store():
    """A class implementing all GraphStore members passes isinstance."""
    store = MinimalGraphStore()
    assert isinstance(store, GraphStore)


def test_missing_upsert_node_fails_isinstance():
    """A class missing upsert_node does NOT satisfy GraphStore isinstance check."""
    store = MissingUpsertNode()
    assert not isinstance(store, GraphStore)


def test_non_object_not_instance_of_graph_store():
    """Plain objects without protocol members fail isinstance."""
    assert not isinstance("not-a-store", GraphStore)
    assert not isinstance(42, GraphStore)
    assert not isinstance(None, GraphStore)


# ---------------------------------------------------------------------------
# QueryableStore protocol tests
# ---------------------------------------------------------------------------


def test_queryable_store_is_runtime_checkable():
    """QueryableStore must be decorated with @runtime_checkable."""
    store = MinimalQueryableStore()
    result = isinstance(store, QueryableStore)
    assert isinstance(result, bool)


def test_conforming_class_is_instance_of_queryable_store():
    """A class implementing all QueryableStore members passes isinstance."""
    store = MinimalQueryableStore()
    assert isinstance(store, QueryableStore)


def test_queryable_store_extends_graph_store():
    """QueryableStore conforming class also satisfies GraphStore isinstance."""
    store = MinimalQueryableStore()
    assert isinstance(store, GraphStore)


def test_graph_store_only_class_not_queryable_store():
    """A class with only GraphStore members does NOT satisfy QueryableStore."""
    store = MinimalGraphStore()
    assert not isinstance(store, QueryableStore)


# ---------------------------------------------------------------------------
# Export tests
# ---------------------------------------------------------------------------


def test_graph_store_exported():
    """GraphStore is exported from graph_store module.

    The top-level import at the head of this file would raise ``ImportError``
    if ``GraphStore`` were missing; this assertion makes the intent explicit.
    """
    assert GraphStore is not None


def test_queryable_store_exported():
    """QueryableStore is exported from graph_store module.

    The top-level import at the head of this file would raise ``ImportError``
    if ``QueryableStore`` were missing; this assertion makes the intent explicit.
    """
    assert QueryableStore is not None


def test_no_graph_forest_name_references():
    """Verify graph_forest_name does not appear anywhere in graph_store.py."""
    import inspect
    import context_intelligence_server.graph_store as m

    source = inspect.getsource(m)
    assert "graph_forest_name" not in source
