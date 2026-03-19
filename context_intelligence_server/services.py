"""SessionCursors, HookConfig, GraphState, and HookStateService primitives.

- SessionCursors   — per-session cursor tracking (dataclass)
- HookConfig       — event-exclusion configuration wrapper
- GraphState       — in-memory property graph conforming to GraphStore protocol
- HookStateService — server-side hook state service (no external dependencies)
"""

from __future__ import annotations

import dataclasses
import fnmatch
from typing import Any


# ---------------------------------------------------------------------------
# SessionCursors
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class SessionCursors:
    """Pointer-only state — no accumulators.

    All fields are reconstructable from ordered event replay.
    """

    current_run_id: str | None = None
    current_step_id: str | None = None
    prompt_preview: str = ""
    parallel_groups: dict[str, list[str]] = dataclasses.field(default_factory=dict)
    tool_call_map: dict[str, str] = dataclasses.field(default_factory=dict)


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

    # ------------------------------------------------------------------
    # Flush / close (no-ops for in-memory store)
    # ------------------------------------------------------------------

    def schedule_flush(self) -> None:
        """No-op: no background I/O for an in-memory store."""

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

    Owns the per-session cursor map, the graph store, and the set of already-seen
    sessions.  The workspace is set directly at construction time.
    """

    def __init__(
        self,
        workspace: str = "default",
        graph_store: Any | None = None,
        *,
        raw_config: dict[str, Any] | None = None,
        blob_store: Any | None = None,
    ) -> None:
        self.config = HookConfig(raw_config or {})
        if graph_store is not None:
            self.graph = graph_store
        else:
            self.graph = GraphState()
        self.graph.workspace = workspace
        self.blob_store = blob_store
        self._cursors: dict[str, SessionCursors] = {}
        self._seen_sessions: set[str] = set()

    # ------------------------------------------------------------------
    # Cursor management
    # ------------------------------------------------------------------

    def get_cursors(self, session_id: str) -> SessionCursors:
        """Return (creating if necessary) the SessionCursors for *session_id*.

        The same instance is returned on every subsequent call for the same id.
        """
        if session_id not in self._cursors:
            self._cursors[session_id] = SessionCursors()
        return self._cursors[session_id]

    def set_cursors(self, session_id: str, cursors: SessionCursors) -> None:
        """Replace the SessionCursors entry for *session_id* with *cursors*.

        Used to restore persisted cursor state into a freshly-created worker.
        """
        self._cursors[session_id] = cursors

    def remove_cursors(self, session_id: str) -> None:
        """Remove the SessionCursors entry for *session_id*.

        Safe to call when *session_id* has no entry — this is a no-op in that
        case.
        """
        self._cursors.pop(session_id, None)

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
           it with the appropriate labels (``Session + Root`` when no parent is
           present, ``Session + Subsession`` when *data* contains a
           ``parent_id`` or ``parent`` field) and set ``status = 'running'``.
        """
        # Tier 1: fast path — warm cache hit
        if session_id in self._seen_sessions:
            return

        # Tier 2: graph query — check durable state
        existing = await self.graph.get_node(session_id)
        if existing is not None:
            # Node already in graph; repopulate cache and skip creation
            self._seen_sessions.add(session_id)
            return

        # Node absent from both cache and graph — create it
        parent = data.get("parent_id") or data.get("parent")
        labels = ["Session", "Subsession" if parent else "Root"]

        node_data: dict[str, Any] = {
            "labels": labels,
            "status": "running",
        }
        if "started_at" in data:
            node_data["started_at"] = data["started_at"]

        await self.graph.upsert_node(session_id, node_data)
        self._seen_sessions.add(session_id)  # only cache after successful write
