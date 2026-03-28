"""Neo4j implementation of GraphStore and QueryableStore protocols.

Provides a workspace-scoped, buffered graph store backed by Neo4j.
Writes are buffered in memory and persisted via UNWIND-based batch Cypher on flush.
Both ``GraphStore`` and ``QueryableStore`` protocols are satisfied.

Canonical workspace naming is used throughout; all scoping is done via the
``workspace`` attribute exclusively.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime
from typing import Any

from neo4j import AsyncGraphDatabase
from neo4j.exceptions import Neo4jError

_LOG = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cypher identifier validation
# ---------------------------------------------------------------------------

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DEFAULT_EDGE_TYPE = "RELATED"


def _validate_identifier(name: str, kind: str) -> None:
    """Raise ``ValueError`` if *name* is not a safe Neo4j label / relationship-type identifier.

    Accepts only ``[A-Za-z_][A-Za-z0-9_]*`` — i.e. no spaces, hyphens, parentheses,
    or other characters that could be exploited for Cypher injection.

    Args:
        name: The identifier string to validate.
        kind: Human-readable category used in the error message (e.g. ``"label"``).

    Raises:
        ValueError: If *name* contains unsafe characters.
    """
    if not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid Neo4j {kind} identifier: {name!r}")


class Neo4jGraphStore:
    """Neo4j-backed implementation of the GraphStore and QueryableStore protocols.

    Writes are buffered in memory (``_node_buffer``, ``_edge_buffer``) and flushed
    to Neo4j via UNWIND-based batch Cypher. Workspace-scoped; schema indexes are
    created idempotently on first flush.
    """

    def __init__(
        self,
        uri: str,
        auth: tuple | None = None,
        database: str = "neo4j",
        workspace: str | None = None,
    ) -> None:
        """Initialise the store and create the async Neo4j driver.

        Args:
            uri:       Bolt/neo4j URI, e.g. ``bolt://localhost:7687``.
            auth:      ``(username, password)`` tuple, or ``None`` for no-auth.
            database:  Target Neo4j database name (default: ``"neo4j"``).
            workspace: Workspace to scope writes to.  ``None`` resolves to
                       ``"default"`` via the ``workspace`` property.
        """
        self._driver = AsyncGraphDatabase.driver(uri, auth=auth)
        self._database = database
        self._workspace = workspace
        self._node_buffer: dict[str, dict[str, Any]] = {}
        self._edge_buffer: dict[tuple, dict[str, Any]] = {}
        self._label_patches: list[dict[str, Any]] = []
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
                "MATCH (n) WHERE n.node_id = $id AND n.workspace = $workspace "
                "RETURN properties(n) AS props",
                {"id": node_id, "workspace": self.workspace},
                database_=self._database,
            )
            records = result.records
            if records:
                return dict(records[0]["props"])
        except Neo4jError:
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
                "AND r.workspace = $workspace "
                "RETURN properties(r) AS props",
                {"src_id": src_id, "dst_id": dst_id, "workspace": self.workspace},
                database_=self._database,
            )
            records = result.records
            if records:
                return dict(records[0]["props"])
        except Neo4jError:
            pass

        return None

    def remove_edge(self, src_id: str, dst_id: str) -> None:
        """Remove a buffered edge from the edge buffer.

        No-op if the edge does not exist in the buffer.
        Note: edges already flushed to Neo4j are handled separately by the
        ownership integrity checker via explicit Cypher DELETE queries.
        """
        self._edge_buffer.pop((src_id, dst_id), None)

    # ------------------------------------------------------------------
    # Persistence methods
    # ------------------------------------------------------------------

    async def flush(self) -> None:
        """Persist all buffered writes to Neo4j using UNWIND-based batch Cypher.

        Optimistically snapshots and clears buffers before writing.  On any
        failure the transaction is rolled back and the buffers are restored
        (merging any writes that arrived during the flush attempt).
        """
        if not self._node_buffer and not self._edge_buffer:
            return  # early exit — nothing to write

        # Snapshot and clear buffers optimistically
        node_snapshot = self._node_buffer
        edge_snapshot = self._edge_buffer
        self._node_buffer = {}
        self._edge_buffer = {}

        success = False
        try:
            await self._ensure_schema()

            async with self._driver.session(database=self._database) as db_session:
                tx = await db_session.begin_transaction()
                try:
                    # ---- nodes ----
                    no_label_rows: list[dict[str, Any]] = []
                    labeled_groups: dict[str, list[dict[str, Any]]] = {}
                    multi_label_rows: list[dict[str, Any]] = []

                    for node_id, data in node_snapshot.items():
                        labels: list[str] = data.get("labels", [])
                        props = self._sanitize_properties(
                            {k: v for k, v in data.items() if k != "labels"}
                        )
                        props["workspace"] = self.workspace
                        row: dict[str, Any] = {"node_id": node_id, "props": props}

                        if not labels:
                            no_label_rows.append(row)
                        else:
                            primary = labels[0]
                            labeled_groups.setdefault(primary, []).append(row)
                            if len(labels) > 1:
                                multi_label_rows.append(
                                    {"node_id": node_id, "extra_labels": labels[1:]}
                                )

                    # Enrichment rows: nodes with no labels
                    if no_label_rows:
                        await tx.run(
                            "UNWIND $rows AS row "
                            "MERGE (n {node_id: row.node_id, workspace: row.props.workspace}) "
                            "SET n += row.props",
                            rows=no_label_rows,
                        )

                    # Primary-label groups
                    for label, rows in labeled_groups.items():
                        _validate_identifier(label, "label")
                        node_merge_query = (  # type: ignore[assignment]
                            f"UNWIND $rows AS row "
                            f"MERGE (n:{label} {{node_id: row.node_id, workspace: row.props.workspace}}) "
                            f"SET n += row.props"
                        )
                        await tx.run(node_merge_query, rows=rows)  # type: ignore[arg-type]

                    # Second pass: set additional labels for multi-label nodes
                    for item in multi_label_rows:
                        for extra_label in item["extra_labels"]:
                            _validate_identifier(extra_label, "label")
                        labels_str = ":".join(item["extra_labels"])
                        label_set_query = (  # type: ignore[assignment]
                            f"MATCH (n {{node_id: $node_id, workspace: $workspace}}) "
                            f"SET n:{labels_str}"
                        )
                        await tx.run(
                            label_set_query,  # type: ignore[arg-type]
                            node_id=item["node_id"],
                            workspace=self.workspace,
                        )

                    # ---- edges ----
                    edge_groups: dict[str, list[dict[str, Any]]] = {}
                    for (src_id, dst_id), data in edge_snapshot.items():
                        edge_type: str = data.get("type", _DEFAULT_EDGE_TYPE)
                        props = self._sanitize_properties(
                            {k: v for k, v in data.items() if k != "type"}
                        )
                        props["workspace"] = self.workspace
                        # Store src_id/dst_id on the relationship so the
                        # get_edge() fallback query (WHERE r.src_id = $src_id
                        # AND r.dst_id = $dst_id) can locate it after a flush.
                        props["src_id"] = src_id
                        props["dst_id"] = dst_id
                        row = {"src_id": src_id, "dst_id": dst_id, "props": props}
                        edge_groups.setdefault(edge_type, []).append(row)

                    for edge_type, rows in edge_groups.items():
                        _validate_identifier(edge_type, "edge_type")
                        edge_merge_query = (  # type: ignore[assignment]
                            f"UNWIND $rows AS row "
                            f"MATCH (src {{node_id: row.src_id, workspace: $workspace}}) "
                            f"MATCH (dst {{node_id: row.dst_id, workspace: $workspace}}) "
                            f"MERGE (src)-[r:{edge_type}]->(dst) "
                            f"SET r += row.props"
                        )
                        await tx.run(
                            edge_merge_query,  # type: ignore[arg-type]
                            rows=rows,
                            workspace=self.workspace,
                        )

                    await tx.commit()
                    success = True
                except Exception:
                    await tx.rollback()
                    raise
        finally:
            if not success:
                # Restore buffers: snapshot base + any new writes since flush started
                merged_nodes = dict(node_snapshot)
                merged_nodes.update(self._node_buffer)
                self._node_buffer = merged_nodes
                merged_edges = dict(edge_snapshot)
                merged_edges.update(self._edge_buffer)
                self._edge_buffer = merged_edges

    async def _ensure_schema(self) -> None:
        """Create Neo4j indexes idempotently (runs once per store instance).

        Creates node_id indexes on Session, OrchestratorRun, Step, ToolExecution,
        and Event labels, plus a named workspace index on Session.
        """
        if self._schema_initialized:
            return

        async with self._driver.session(database=self._database) as session:
            for label in (
                "Session",
                "OrchestratorRun",
                "Step",
                "ToolExecution",
                "Event",
            ):
                await session.run(
                    f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.node_id)"
                )
            await session.run(
                "CREATE INDEX idx_session_workspace IF NOT EXISTS "
                "FOR (n:Session) ON (n.workspace)"
            )

        self._schema_initialized = True

    def schedule_flush(self) -> None:
        """Schedule a background flush task if none is currently running."""
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.create_task(self._background_flush())

    async def _background_flush(self) -> None:
        """Invoke ``flush`` and log any exceptions (does not propagate)."""
        try:
            await self.flush()
        except Exception:
            _LOG.exception("Background flush failed")

    async def close(self) -> None:
        """Flush pending writes, await any background task, and close the driver.

        Handles event-loop mismatch gracefully when closing the driver from a
        different loop context.  Sets ``_closed`` on completion.
        """
        # Await any in-flight background flush
        if self._flush_task is not None and not self._flush_task.done():
            try:
                await self._flush_task
            except Exception:
                pass

        # Final flush to persist remaining buffer contents
        try:
            await self.flush()
        except Exception:
            _LOG.exception(
                "Final flush failed during close; buffered writes may be lost"
            )

        # Close the driver, ignoring event-loop mismatch errors
        try:
            await self._driver.close()
        except RuntimeError:
            pass

        self._closed = True

    async def execute_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        dialect: str = "cypher",
        workspace: str | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a Cypher query against Neo4j.

        Args:
            query:     Cypher query string.
            params:    Optional query parameters.
            dialect:   Must be ``"cypher"``; raises ``ValueError`` otherwise.
            workspace: Scope results to this workspace.  ``None`` uses the
                       store's own workspace.  ``"*"`` disables workspace
                       filtering.

        Returns:
            List of result row dicts.

        Raises:
            ValueError: If *dialect* is not in ``supported_dialects``.
        """
        if dialect not in self.supported_dialects:
            raise ValueError(
                f"Unsupported dialect: {dialect!r}. Supported: {self.supported_dialects}"
            )

        effective_workspace = workspace if workspace is not None else self.workspace
        query_params: dict[str, Any] = dict(params) if params else {}

        if effective_workspace != "*":
            query_params["workspace"] = effective_workspace

        async with self._driver.session(database=self._database) as session:
            result = await session.run(query, query_params)  # type: ignore[arg-type]
            data = await result.data()
            return [dict(record) for record in data]

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
