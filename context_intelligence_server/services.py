"""HookConfig, GraphState, and HookStateService primitives.

- HookConfig       — event-exclusion configuration wrapper
- GraphState       — in-memory property graph conforming to GraphStore protocol
- HookStateService — server-side hook state service (no external dependencies)
"""

from __future__ import annotations

import fnmatch
import logging
from datetime import datetime
from typing import Any

from context_intelligence_server.handlers.data_layer_2.state import DataLayer2State
from context_intelligence_server.handlers.data_layer_3.state import DataLayer3State

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HookConfig
# ---------------------------------------------------------------------------


class HookConfig:
    """Wraps raw hook configuration and provides exclusion helpers."""

    def __init__(self, raw_config: dict[str, Any]) -> None:
        self._raw_config = raw_config

    @property
    def exclude_events(self) -> set[str]:
        """Return the set of exclusion patterns (may contain wildcards)."""
        return set(self._raw_config.get("exclude_events", []))

    def is_excluded(self, event: str) -> bool:
        """Return True if *event* matches any exclusion pattern.

        Patterns support ``fnmatch`` wildcards, e.g. ``session-naming:*``
        matches ``session-naming:foo``.

        Iterates directly over the raw config list to avoid reconstructing
        a set on every call (this method may be invoked on every hook event).
        """
        for pattern in self._raw_config.get("exclude_events", []):
            if fnmatch.fnmatch(event, pattern):
                return True
        return False


# ---------------------------------------------------------------------------
# GraphState
# ---------------------------------------------------------------------------


class GraphState:
    """In-memory property graph conforming to the GraphStore protocol.

    All writes are buffered in memory.  ``flush`` and ``close`` are no-ops
    because there is no backing store — this implementation is purely in-memory.

    The ``workspace`` attribute is the canonical scoping identifier and is
    both readable and settable.
    """

    def __init__(self, workspace: str = "default") -> None:
        self._workspace = workspace
        self._created_by: str | None = None
        self._nodes: dict[str, dict[str, Any]] = {}
        self._edges: dict[tuple[str, str], dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # workspace property (getter + setter)
    # ------------------------------------------------------------------

    @property
    def workspace(self) -> str:
        """Workspace this store is bound to."""
        return self._workspace

    @workspace.setter
    def workspace(self, value: str) -> None:
        self._workspace = value

    # ------------------------------------------------------------------
    # created_by property (getter + setter)
    # ------------------------------------------------------------------

    @property
    def created_by(self) -> str | None:
        """Authenticated contributor id for provenance stamping (None when unset)."""
        return self._created_by

    @created_by.setter
    def created_by(self, value: str | None) -> None:
        self._created_by = value

    # ------------------------------------------------------------------
    # Node operations
    # ------------------------------------------------------------------

    async def upsert_node(self, node_id: str, data: dict[str, Any]) -> None:
        """Create or merge a node.

        Labels (``data["labels"]``) are union-merged with any existing labels.
        All other properties are dict-merged (new values win on conflict).
        """
        if node_id not in self._nodes:
            self._nodes[node_id] = {}

        existing = self._nodes[node_id]

        if "labels" in data:
            existing_labels: set[str] = set(existing.get("labels", []))
            new_labels: set[str] = set(data["labels"])
            existing["labels"] = sorted(existing_labels | new_labels)

        for key, value in data.items():
            if key != "labels":
                existing[key] = value

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        """Return a copy of node data or ``None`` if the node does not exist.

        Returns a shallow copy to prevent callers from silently corrupting the
        internal buffer by mutating the returned dict.
        """
        node = self._nodes.get(node_id)
        return dict(node) if node is not None else None

    # ------------------------------------------------------------------
    # Edge operations
    # ------------------------------------------------------------------

    async def upsert_edge(self, src_id: str, dst_id: str, data: dict[str, Any]) -> None:
        """Create or merge an edge between *src_id* and *dst_id*."""
        key = (src_id, dst_id)
        if key not in self._edges:
            self._edges[key] = {}
        self._edges[key].update(data)

    async def get_edge(self, src_id: str, dst_id: str) -> dict[str, Any] | None:
        """Return a copy of edge data or ``None`` if the edge does not exist.

        Returns a shallow copy to prevent callers from silently corrupting the
        internal buffer by mutating the returned dict.
        """
        edge = self._edges.get((src_id, dst_id))
        return dict(edge) if edge is not None else None

    def remove_edge(self, src_id: str, dst_id: str) -> None:
        """Remove an edge from the in-memory store.

        No-op if the edge does not exist.
        """
        self._edges.pop((src_id, dst_id), None)

    async def set_labels(
        self, node_id: str, remove_labels: list[str], add_labels: list[str]
    ) -> None:
        """Atomically remove specific labels and add new labels on a node.

        If the node does not exist, creates it with add_labels.
        Labels in remove_labels that are not present are silently skipped.
        Unlike upsert_node, this method CAN remove labels — it is the correct
        way to perform session type label reclassification.
        """
        if node_id not in self._nodes:
            self._nodes[node_id] = {}
        existing = self._nodes[node_id]
        current: set[str] = set(existing.get("labels", []))
        existing["labels"] = sorted((current - set(remove_labels)) | set(add_labels))

    # ------------------------------------------------------------------
    # Flush / close (no-ops for in-memory store)
    # ------------------------------------------------------------------

    def discard_buffer(self) -> None:
        """No-op: in-memory store has no flush buffer to discard."""

    async def flush(self) -> None:
        """No-op: no backing store to persist to."""

    async def close(self) -> None:
        """Call flush (no-op) before releasing — satisfies the GraphStore contract."""
        await self.flush()


# ---------------------------------------------------------------------------
# HookStateService
# ---------------------------------------------------------------------------


class HookStateService:
    """Server-side hook state service.

    Owns the graph store and the set of already-seen sessions.  The workspace
    is set directly at construction time.
    """

    def __init__(
        self,
        workspace: str = "default",
        graph_store: Any | None = None,
        *,
        created_by: str | None = None,
        raw_config: dict[str, Any] | None = None,
        blob_store: Any | None = None,
    ) -> None:
        self.config = HookConfig(raw_config or {})
        if graph_store is not None:
            self.graph = graph_store
        else:
            self.graph = GraphState()
        self.graph.workspace = workspace
        self.graph.created_by = created_by
        self.blob_store = blob_store
        self._seen_sessions: set[str] = set()
        self.data_layer_2 = DataLayer2State()
        self.data_layer_3 = DataLayer3State()

    # ------------------------------------------------------------------
    # Session node management
    # ------------------------------------------------------------------

    async def ensure_session_node(self, session_id: str, data: dict[str, Any]) -> None:
        """Idempotently create a Session node in the graph for *session_id*.

        Uses a two-tier lookup for replay resilience:

        1. Fast path — if *session_id* is already in the in-memory
           ``_seen_sessions`` cache, return immediately.
        2. Graph query — call ``graph.get_node(session_id)``.  If the node
           already exists (e.g. from a previous run), repopulate the cache and
           return without overwriting any data.  If the node is absent, create
           it with labels ``["Session"]`` and ``status = 'running'``.

        This method is a safety net that creates a minimal session node if it
        doesn't exist.  ``SessionHandler`` is the sole authority on session
        type labels (``RootSession``, ``SubSession``, ``ForkedSession``).
        ``ensure_session_node`` always creates a bare ``Session`` node;
        ``SessionHandler`` enriches it with the correct type label via a
        subsequent upsert.

        Only caches session_id after a successful write to ensure retry
        resilience on write failure.
        """
        # Tier 1: fast path — warm cache hit
        if session_id in self._seen_sessions:
            return

        # Tier 2: graph query — check durable state
        existing = await self.graph.get_node(session_id)
        if existing is not None:
            # Node already in graph. Also upsert a bare stub to this worker's buffer
            # so that the current worker's flush uses MERGE (idempotent) rather than
            # creating a second node.  This prevents the asyncio race condition where:
            #   1. Worker A flushes a stub node — tx is in-flight.
            #   2. Worker B calls get_node — falls through to Neo4j, finds the node.
            #   3. Without this upsert, Worker B's _node_buffer stays empty.
            #   4. Worker B's flush later issues a fresh MERGE → duplicate node.
            # upsert_node uses union-merge for labels, so existing type labels
            # (e.g. "RootSession") are preserved — this call never strips labels.
            await self.graph.upsert_node(
                session_id,
                {"labels": ["Session"], "status": "running", "session_id": session_id},
            )
            self._seen_sessions.add(session_id)
            return

        # Node absent from both cache and graph — create it as a bare Session node.
        # ensure_session_node is a safety net; SessionHandler is the sole authority
        # on session type labels (RootSession, SubSession, ForkedSession).
        node_data: dict[str, Any] = {
            "labels": ["Session"],
            "status": "running",
            "session_id": session_id,  # explicit property — enables direct query without HAS_EVENT traversal
        }
        # Kernel events carry the wall-clock under data["timestamp"]; older callers
        # may pass an explicit "started_at" — accept either, but never write an empty value.
        _ts = data.get("timestamp") or data.get("started_at")
        if _ts:
            node_data["started_at"] = _ts
        if "agent" in data:
            node_data["agent"] = data["agent"]

        await self.graph.upsert_node(session_id, node_data)
        self._seen_sessions.add(session_id)  # only cache after successful write

    async def touch_session(self, session_id: str, timestamp: str) -> None:
        """Update last_updated on the direct Session node only.

        Updates exactly one node — the session named by *session_id*.  There is
        deliberately NO ancestor/parent_id propagation: the previous parent-chain
        walk SET last_updated on the shared root :Session node for every child
        event, so many independent writers contended on that one node's exclusive
        lock — the source of the Neo4j deadlock that silently dropped events.
        Root/session attributes (started_at/status/parent_id) are written once at
        session:start by SessionHandler, and staleness reaping uses
        worker.last_event_time — neither depends on ancestor last_updated — so
        dropping propagation costs nothing while removing the contention hot spot.

        Skips the write when the stored last_updated is already at or ahead of
        *timestamp*.  Never raises — errors are logged at WARNING level.
        """
        try:
            node = await self.graph.get_node(session_id)
            if node is None:
                return
            current = node.get("last_updated")
            # Compare using stdlib datetime only; the store's read path normalises
            # driver DateTime objects to Python datetime, but the in-memory store returns
            # whatever was written (often a str), so coerce both sides defensively.
            # No driver-specific datetime types here.
            ts = (
                datetime.fromisoformat(timestamp)
                if isinstance(timestamp, str)
                else timestamp
            )
            current_dt = (
                datetime.fromisoformat(current) if isinstance(current, str) else current
            )
            if current_dt is not None and ts <= current_dt:
                return  # already at or ahead — nothing to write
            # Update only the direct node — never the ancestor/root chain.
            await self.graph.upsert_node(
                session_id,
                {"labels": ["Session"], "last_updated": timestamp},
            )
        except Exception:
            logger.warning(
                "touch_session failed for %s @ %s",
                session_id,
                timestamp,
                exc_info=True,
            )
