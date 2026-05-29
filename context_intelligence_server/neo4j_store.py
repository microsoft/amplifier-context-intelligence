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
from typing import Any, LiteralString, cast

from neo4j import AsyncGraphDatabase
from neo4j.exceptions import Neo4jError

_LOG = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cypher identifier validation
# ---------------------------------------------------------------------------

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DEFAULT_EDGE_TYPE = "RELATED"

# ---------------------------------------------------------------------------
# Temporal property registry
# ---------------------------------------------------------------------------
# Every property name in this set is stored as a Neo4j ZONED DATETIME.
#
# On WRITE: _convert_temporal_props() parses ISO strings to Python datetime;
#   the Neo4j driver then writes those datetime objects as ZONED DATETIME nodes.
# On READ:  _normalize_temporal() converts neo4j.time.DateTime back to Python
#   datetime objects.
#
# IMPORTANT: neo4j.time types must NEVER leave this module.
#   services.py, pipeline.py, and handlers deal in Python stdlib types only.
#
# RULE: add a name here whenever you add a temporal property to any handler.
#   Forgetting means the value lands as a plain string silently — no error,
#   no warning, just wrong data.
#
# Note: last_updated is the one field that does NOT follow the *_at convention
#   and is listed deliberately.  Do NOT replace this explicit set with a suffix
#   heuristic — a heuristic would silently miss last_updated.
#
# FUTURE: when the first duration-typed property arrives, convert this frozenset
#   to TEMPORAL_PROPERTIES: dict[str, type] and add neo4j.time.Duration to the
#   _sanitize_properties allow-list at that point — not before.
TEMPORAL_PROPS: frozenset[str] = frozenset({
    "started_at",
    "ended_at",
    "occurred_at",
    "completed_at",
    "last_updated",  # only temporal field NOT ending in _at
    "resumed_at",
    "cancelled_at",
    "last_loop_iteration_at",
    "loop_completed_at",
})


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


async def ensure_neo4j_schema(
    driver: Any,
    database: str = "neo4j",
    workspace: str = "",
) -> None:
    """Create Neo4j indexes and constraints idempotently.

    Intended to be called **once at server startup** (before any requests are
    accepted) so that the uniqueness constraint on Session nodes is active before
    concurrent ``flush()`` transactions execute ``MERGE``.

    Runs a deduplication pass *first* so that any pre-existing duplicate Session
    nodes (from a previous run without the constraint) do not block constraint
    creation.

    Args:
        driver:    An ``AsyncDriver`` instance created via
                   ``AsyncGraphDatabase.driver(...)``.
        database:  Target Neo4j database name (default: ``"neo4j"``).
        workspace: Reserved for future workspace-scoped schema; currently unused.
    """
    async with driver.session(database=database) as session:
        # ------------------------------------------------------------------
        # Step 1: deduplicate any pre-existing duplicate Session nodes.
        # For each duplicate (node_id, workspace) group keep the first node
        # (as returned by collect()) and DETACH DELETE the remainder.
        # This MUST run before constraint creation so a dirty graph does not
        # cause the CREATE CONSTRAINT statement to fail.
        # ------------------------------------------------------------------
        try:
            await session.run(
                "MATCH (s:Session) "
                "WITH s.node_id AS nid, s.workspace AS ws, collect(s) AS nodes "
                "WHERE size(nodes) > 1 "
                "UNWIND tail(nodes) AS duplicate "
                "DETACH DELETE duplicate"
            )
        except Exception as exc:  # noqa: BLE001
            _LOG.warning(
                "ensure_neo4j_schema: deduplication query failed (non-fatal): %s", exc
            )

        # ------------------------------------------------------------------
        # Step 2: create node_id indexes (idempotent via IF NOT EXISTS).
        # ------------------------------------------------------------------
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

        # ------------------------------------------------------------------
        # Step 3: uniqueness constraint on Session nodes.
        # Must run AFTER deduplication (step 1) so pre-existing duplicates do
        # not cause the constraint creation to fail.
        # Combined with MERGE (n:Session ...) in flush(), makes concurrent
        # MERGEs atomic and prevents duplicate Session nodes under load.
        # ------------------------------------------------------------------
        try:
            await session.run(
                "CREATE CONSTRAINT session_node_id_workspace_unique IF NOT EXISTS "
                "FOR (n:Session) REQUIRE (n.node_id, n.workspace) IS UNIQUE"
            )
        except Exception as exc:  # noqa: BLE001
            _LOG.warning(
                "ensure_neo4j_schema: could not create Session uniqueness constraint "
                "(pre-existing duplicates?): %s",
                exc,
            )


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
        self._flush_lock: asyncio.Lock = asyncio.Lock()

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

    async def set_labels(
        self, node_id: str, remove_labels: list[str], add_labels: list[str]
    ) -> None:
        """Buffer a label patch and immediately update _node_buffer.

        Two-phase effect:
        1. _node_buffer updated immediately — so get_node() reflects the change
           within the same flush cycle. Essential for fork guard and state machine
           logic that reads labels between handler calls.
        2. Patch queued in _label_patches — applied to Neo4j at flush time via
           explicit REMOVE/SET Cypher statements AFTER node writes.

        Each label in remove_labels is removed via REMOVE n:Label Cypher.
        Each label in add_labels is set via SET n:Label Cypher.
        """
        # Phase 1: update in-memory buffer immediately (same as GraphState.set_labels)
        if node_id not in self._node_buffer:
            self._node_buffer[node_id] = {}
        existing = self._node_buffer[node_id]
        current = set(existing.get("labels", []))
        existing["labels"] = sorted((current - set(remove_labels)) | set(add_labels))

        # Phase 2: queue patch for Neo4j flush
        self._label_patches.append(
            {
                "node_id": node_id,
                "remove": list(remove_labels),
                "add": list(add_labels),
            }
        )

    # ------------------------------------------------------------------
    # Persistence methods
    # ------------------------------------------------------------------

    async def flush(self) -> None:
        """Persist all buffered writes to Neo4j using UNWIND-based batch Cypher.

        Serializes callers via ``_flush_lock`` so that at most one Neo4j
        transaction is open at any time for this store.  Without the lock a
        concurrent caller (e.g. the 30-second periodic timer firing while
        ``_background_flush`` is mid-transaction) would open a second
        transaction on the same nodes, which Neo4j resolves as a deadlock.

        Optimistically snapshots and clears buffers before writing.  On any
        failure the transaction is rolled back and the buffers are restored
        (merging any writes that arrived during the flush attempt).

        All nodes are merged by (node_id, workspace) identity — label is never
        part of the MERGE key.  This prevents duplicate nodes when the same
        session is written with different type labels across flush cycles
        (e.g. bare Session created by a child worker, then RootSession written
        by the parent worker's session:end flush).
        """
        async with self._flush_lock:
            await self._flush_body()

    async def _flush_body(self) -> None:
        """Inner flush implementation — must only be called while _flush_lock is held."""
        if not self._node_buffer and not self._edge_buffer and not self._label_patches:
            return  # early exit — nothing to write

        # Snapshot and clear buffers optimistically
        node_snapshot = self._node_buffer
        edge_snapshot = self._edge_buffer
        self._node_buffer = {}
        self._edge_buffer = {}
        patch_snapshot = self._label_patches
        self._label_patches = []

        success = False
        try:
            await self._ensure_schema()

            async with self._driver.session(database=self._database) as db_session:
                tx = await db_session.begin_transaction()
                try:
                    # ---- nodes ---- (Session nodes use label-aware MERGE + uniqueness constraint)
                    session_rows: list[dict[str, Any]] = []  # have Session label
                    other_rows: list[dict[str, Any]] = []  # everything else
                    label_assignments: list[dict[str, Any]] = []

                    for node_id, data in node_snapshot.items():
                        labels: list[str] = data.get("labels", [])
                        props = self._sanitize_properties(
                            {k: v for k, v in data.items() if k != "labels"}
                        )
                        props["workspace"] = self.workspace
                        row: dict[str, Any] = {"node_id": node_id, "props": props}

                        if "Session" in labels:
                            session_rows.append(row)
                        else:
                            other_rows.append(row)

                        if labels:
                            for label in labels:
                                _validate_identifier(label, "label")
                            label_assignments.append(
                                {"node_id": node_id, "labels": sorted(set(labels))}
                            )

                    # Session nodes: MERGE by Session label + uniqueness constraint (atomic under concurrency)
                    if session_rows:
                        await tx.run(
                            "UNWIND $rows AS row "
                            "MERGE (n:Session {node_id: row.node_id, workspace: row.props.workspace}) "
                            "SET n += row.props",
                            rows=session_rows,
                        )

                    # Non-session nodes: label-free MERGE (no constraint needed — single-worker owned)
                    if other_rows:
                        await tx.run(
                            "UNWIND $rows AS row "
                            "MERGE (n {node_id: row.node_id, workspace: row.props.workspace}) "
                            "SET n += row.props",
                            rows=other_rows,
                        )

                    # Set all labels for labeled nodes (primary + extra in one SET per node).
                    # For Session nodes, Session label is already set by the MERGE above — this
                    # adds any additional type labels (RootSession, SubSession, ForkedSession, etc.)
                    for item in label_assignments:
                        labels_str = ":".join(item["labels"])
                        await tx.run(
                            cast(
                                LiteralString,
                                f"MATCH (n {{node_id: $node_id, workspace: $workspace}}) "
                                f"SET n:{labels_str}",
                            ),
                            node_id=item["node_id"],
                            workspace=self.workspace,
                        )

                    # ---- label patches (must run AFTER node writes — nodes must exist in Neo4j before MATCH) ----
                    for lp in patch_snapshot:
                        pid = lp["node_id"]
                        for label in lp.get("remove", []):
                            _validate_identifier(label, "label")
                            await tx.run(
                                cast(
                                    LiteralString,
                                    f"MATCH (n {{node_id: $node_id, workspace: $workspace}}) REMOVE n:{label}",
                                ),
                                node_id=pid,
                                workspace=self.workspace,
                            )
                        for label in lp.get("add", []):
                            _validate_identifier(label, "label")
                            await tx.run(
                                cast(
                                    LiteralString,
                                    f"MATCH (n {{node_id: $node_id, workspace: $workspace}}) SET n:{label}",
                                ),
                                node_id=pid,
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
                merged_patches = patch_snapshot + self._label_patches
                self._label_patches = merged_patches

    async def _ensure_schema(self) -> None:
        """Create Neo4j indexes and constraints idempotently (runs once per store instance).

        Safety-net for contexts where the FastAPI lifespan is not active (e.g. tests,
        CLI tools, or direct store use).  The primary schema-initialization path is
        ``ensure_neo4j_schema()`` called from the lifespan handler *before* the server
        starts accepting requests, which guarantees the uniqueness constraint is active
        before any concurrent ``flush()`` transactions execute ``MERGE``.
        """
        if self._schema_initialized:
            return

        await ensure_neo4j_schema(self._driver, self._database)
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
                # Never write empty-string timestamps — SET n += row.props would
                # overwrite a previously valid started_at on the existing node.
                if isinstance(value, str) and value == "" and key.endswith("_at"):
                    continue
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
