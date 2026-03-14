"""Neo4j implementation of GraphStore and QueryableStore protocols.

Provides a workspace-scoped, buffered graph store backed by Neo4j.
Writes are buffered in memory; persistence is handled by ``flush`` (placeholder).
Both ``GraphStore`` and ``QueryableStore`` protocols are satisfied.

Canonical workspace naming is used throughout; all scoping is done via the
``workspace`` attribute exclusively.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from neo4j import AsyncGraphDatabase


class Neo4jGraphStore:
    """Neo4j-backed implementation of the GraphStore and QueryableStore protocols.

    Writes are buffered in memory (``_node_buffer``, ``_edge_buffer``) and
    are immediately visible to subsequent ``get_node`` / ``get_edge`` calls.
    ``flush`` and ``close`` are no-op placeholders until persistence is wired up.
    ``execute_query`` raises ``NotImplementedError`` until query support is added.
    """

    def __init__(
        self,
        uri: str,
        auth: tuple,
        database: str = "neo4j",
        workspace: str | None = None,
    ) -> None:
        """Initialise the store and create the async Neo4j driver.

        Args:
            uri:       Bolt/neo4j URI, e.g. ``bolt://localhost:7687``.
            auth:      ``(username, password)`` tuple.
            database:  Target Neo4j database name (default: ``"neo4j"``).
            workspace: Workspace to scope writes to.  ``None`` resolves to
                       ``"default"`` via the ``workspace`` property.
        """
        self._driver = AsyncGraphDatabase.driver(uri, auth=auth)
        self._database = database
        self._workspace = workspace
        self._node_buffer: dict[str, dict] = {}
        self._edge_buffer: dict[tuple, dict] = {}
        self._schema_initialized: bool = False
        self._closed: bool = False
        self._flush_task = None

    # ------------------------------------------------------------------
    # workspace property
    # ------------------------------------------------------------------

    @property
    def workspace(self) -> str:
        """Return the current workspace, defaulting to ``'default'`` when unset."""
        return self._workspace if self._workspace is not None else "default"

    @workspace.setter
    def workspace(self, value: str | None) -> None:
        """Set the workspace (``None`` will resolve to ``'default'`` on read)."""
        self._workspace = value

    # ------------------------------------------------------------------
    # supported_dialects property
    # ------------------------------------------------------------------

    @property
    def supported_dialects(self) -> frozenset[str]:
        """Return the set of supported query dialects."""
        return frozenset({"cypher"})

    # ------------------------------------------------------------------
    # Buffer operations
    # ------------------------------------------------------------------

    async def upsert_node(self, node_id: str, data: dict[str, Any]) -> None:
        """Buffer a node upsert (no I/O).

        If the node already exists in the buffer:
        - ``labels`` lists are *unioned* (no duplicates, original order preserved).
        - All other properties are merged with ``dict.update`` semantics
          (later call wins on conflict).

        If the node does not exist, it is stored as a shallow copy of *data*.
        """
        if node_id not in self._node_buffer:
            self._node_buffer[node_id] = {}

        existing = self._node_buffer[node_id]

        # Union labels, preserving insertion order and avoiding duplicates
        if "labels" in data:
            existing_labels: list = list(existing.get("labels", []))
            for label in data["labels"]:
                if label not in existing_labels:
                    existing_labels.append(label)
            existing["labels"] = existing_labels

        # Update all other properties (labels handled above, skip to avoid overwrite)
        for key, value in data.items():
            if key != "labels":
                existing[key] = value

    async def upsert_edge(self, src_id: str, dst_id: str, data: dict[str, Any]) -> None:
        """Buffer an edge upsert (no I/O).

        If the edge already exists in the buffer, properties are merged via
        ``dict.update`` semantics.  Otherwise the edge is stored as a new entry.
        """
        key = (src_id, dst_id)
        if key not in self._edge_buffer:
            self._edge_buffer[key] = {}
        self._edge_buffer[key].update(data)

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        """Return node data, checking the in-memory buffer first.

        If the node is found in the buffer, a *shallow copy* is returned so
        that callers cannot accidentally mutate the buffered state.

        If not found in the buffer, falls back to a Neo4j query.  Returns
        ``None`` if the node is absent from both.
        """
        if node_id in self._node_buffer:
            return dict(self._node_buffer[node_id])

        # Neo4j fallback
        try:
            result = await self._driver.execute_query(
                "MATCH (n) WHERE n.id = $id AND n.workspace = $workspace "
                "RETURN properties(n) AS props",
                {"id": node_id, "workspace": self.workspace},
                database_=self._database,
            )
            records = result.records
            if records and len(records) > 0:
                return dict(records[0]["props"])
        except Exception:  # noqa: BLE001
            pass

        return None

    async def get_edge(self, src_id: str, dst_id: str) -> dict[str, Any] | None:
        """Return edge data, checking the in-memory buffer first.

        If the edge is found in the buffer, a *shallow copy* is returned.

        If not found in the buffer, falls back to a Neo4j query.  Returns
        ``None`` if the edge is absent from both.
        """
        key = (src_id, dst_id)
        if key in self._edge_buffer:
            return dict(self._edge_buffer[key])

        # Neo4j fallback
        try:
            result = await self._driver.execute_query(
                "MATCH ()-[r]->() "
                "WHERE r.src_id = $src_id AND r.dst_id = $dst_id "
                "RETURN properties(r) AS props",
                {"src_id": src_id, "dst_id": dst_id},
                database_=self._database,
            )
            records = result.records
            if records and len(records) > 0:
                return dict(records[0]["props"])
        except Exception:  # noqa: BLE001
            pass

        return None

    # ------------------------------------------------------------------
    # Placeholder persistence / query methods
    # ------------------------------------------------------------------

    async def flush(self) -> None:
        """Placeholder: no-op until persistence is implemented."""

    async def close(self) -> None:
        """Placeholder: no-op until resource teardown is implemented."""

    async def execute_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        dialect: str = "cypher",
        workspace: str | None = None,
    ) -> list[dict[str, Any]]:
        """Placeholder: raises ``NotImplementedError`` until query support is added."""
        raise NotImplementedError("execute_query is not yet implemented")

    # ------------------------------------------------------------------
    # Static helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _sanitize_properties(props: dict[str, Any]) -> dict[str, Any]:
        """Convert a properties dict to Neo4j-compatible types.

        Rules:
        - ``None`` values are *skipped* (not included in output).
        - ``str``, ``int``, ``float``, ``bool`` are kept as-is.
        - ``list`` whose items are all primitives (str/int/float/bool) is kept.
        - ``list`` containing non-primitive items is JSON-serialised to a string.
        - ``dict`` values are JSON-serialised to a string.
        - Everything else is converted via ``str()``.
        """
        result: dict[str, Any] = {}
        for key, value in props.items():
            if value is None:
                continue
            if isinstance(value, (str, int, float, bool)):
                result[key] = value
            elif isinstance(value, list):
                if all(isinstance(item, (str, int, float, bool)) for item in value):
                    result[key] = value
                else:
                    result[key] = json.dumps(value)
            elif isinstance(value, dict):
                result[key] = json.dumps(value)
            else:
                result[key] = str(value)
        return result

    @staticmethod
    def _convert_timestamps(props: dict[str, Any]) -> dict[str, Any]:
        """Convert ``*_at`` ISO string fields to :class:`datetime` objects.

        Any property whose key ends with ``_at`` and whose value is a valid
        ISO 8601 string is replaced with the corresponding :class:`datetime`.
        Invalid strings are left unchanged.  The input dict is not mutated.
        """
        result = dict(props)
        for key, value in result.items():
            if key.endswith("_at") and isinstance(value, str):
                try:
                    result[key] = datetime.fromisoformat(value)
                except ValueError:
                    pass
        return result
