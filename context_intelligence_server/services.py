"""HookConfig, GraphState, and HookStateService primitives.

- HookConfig       — event-exclusion configuration wrapper
- GraphState       — in-memory property graph conforming to GraphStore protocol
- HookStateService — server-side hook state service (no external dependencies)
"""

from __future__ import annotations

import fnmatch
import logging
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from context_intelligence_server.blob_store import BlobStore
from context_intelligence_server.graph_store import (
    GraphDeleteResult,
    SessionGraph,
    extract_blob_refs,
)
from context_intelligence_server.handlers.data_layer_2.state import DataLayer2State
from context_intelligence_server.handlers.data_layer_3.state import DataLayer3State

_GRAPH_EDGE_TYPES = frozenset({"HAS_SUBSESSION", "FORKED"})


def _parse_timestamp(value: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp string to ``datetime``; passes datetimes through.

    GraphState keeps whatever was written (usually a str); unlike
    Neo4jGraphStore there is no driver-side temporal normalisation, so this is
    the in-memory equivalent of that read-path conversion.
    """
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


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

    async def find_delegation_by_sub_session(
        self, sub_session_id: str, workspace: str
    ) -> dict[str, Any] | None:
        """Return a copy of the Delegation node whose sub_session_id matches, or None.

        Scans the in-memory node store for a node carrying the ``Delegation``
        label with a matching ``sub_session_id`` property -- the parent
        Delegation that spawned *sub_session_id*.

        ``GraphState`` is a single-workspace store (workspace is fixed at
        construction), so the *workspace* argument is accepted for parity with
        other ``GraphStore`` implementations (e.g. ``Neo4jGraphStore``) but is
        not used to filter here -- every node in this store already belongs to
        the same workspace.
        """
        for data in self._nodes.values():
            if (
                "Delegation" in data.get("labels", [])
                and data.get("sub_session_id") == sub_session_id
            ):
                return dict(data)
        return None

    async def resolve_session_graph(self, session_id: str) -> SessionGraph | None:
        """In-memory equivalent of ``Neo4jGraphStore.resolve_session_graph``.

        Walks ``HAS_SUBSESSION``/``FORKED`` edges up to the root, then back
        down to every descendant. See that method's docstring for the
        graph-subgraph (node/edge/blob) traversal rule.
        """
        start = self._nodes.get(session_id)
        if start is None or "Session" not in start.get("labels", []):
            return None

        parent_of: dict[str, str] = {}
        children: dict[str, list[str]] = {}
        outgoing: dict[str, list[str]] = {}
        for (src, dst), edata in self._edges.items():
            outgoing.setdefault(src, []).append(dst)
            if edata.get("type") in _GRAPH_EDGE_TYPES:
                parent_of[dst] = src
                children.setdefault(src, []).append(dst)

        # Walk up to the root (tree structure: at most one parent per node).
        root_id = session_id
        visited_up = {root_id}
        while root_id in parent_of:
            root_id = parent_of[root_id]
            if root_id in visited_up:
                break  # defensive cycle guard; graph is acyclic by construction
            visited_up.add(root_id)

        # Walk down from the root to every descendant.
        session_ids: set[str] = set()
        stack = [root_id]
        while stack:
            nid = stack.pop()
            if nid in session_ids:
                continue
            session_ids.add(nid)
            stack.extend(children.get(nid, []))

        # Graph subgraph: expand outward from every graph session, stopping
        # at (but including) any :SST_CONCEPT node.
        graph_nodes: set[str] = set()
        stack = list(session_ids)
        while stack:
            nid = stack.pop()
            if nid in graph_nodes:
                continue
            graph_nodes.add(nid)
            node_data = self._nodes.get(nid) or {}
            if "SST_CONCEPT" in node_data.get("labels", []):
                continue
            stack.extend(outgoing.get(nid, []))

        edge_count = sum(
            1
            for (src, dst) in self._edges
            if src in graph_nodes and dst in graph_nodes
        )

        blob_refs: set[str] = set()
        for nid in graph_nodes:
            blob_refs |= extract_blob_refs(self._nodes.get(nid) or {})

        root_props = self._nodes.get(root_id) or {}
        # GraphState has no per-node created_by stamp (unlike Neo4jGraphStore's
        # `ON CREATE SET n.created_by`) -- fall back to the store-level value.
        created_by = root_props.get("created_by") or self._created_by
        started_at = _parse_timestamp(root_props.get("started_at"))
        working_dir = root_props.get("working_dir")

        last_change: datetime | None = None
        for sid in session_ids:
            props = self._nodes.get(sid) or {}
            candidate = _parse_timestamp(
                props.get("last_updated")
                or props.get("ended_at")
                or props.get("started_at")
            )
            if candidate is not None and (
                last_change is None or candidate > last_change
            ):
                last_change = candidate

        return SessionGraph(
            root_id=root_id,
            session_ids=frozenset(session_ids),
            blob_refs=frozenset(blob_refs),
            node_count=len(graph_nodes),
            edge_count=edge_count,
            created_by=created_by,
            started_at=started_at,
            last_change=last_change,
            subsession_count=len(session_ids) - 1,
            workspace=self._workspace,
            working_dir=working_dir if isinstance(working_dir, str) else None,
        )

    async def delete_session_graph(self, session_id: str) -> GraphDeleteResult | None:
        """In-memory equivalent of ``Neo4jGraphStore.delete_session_graph``.

        Reuses ``resolve_session_graph`` to find the graph, then repeats its
        exact traversal (up to the root, back down, then outward stopping at
        but not past ``:SST_CONCEPT``) to partition the reachable nodes into
        OWNED (removed) vs boundary concept (kept) -- see that method's
        docstring for the traversal rule this must never diverge from.

        Raises ``RuntimeError`` if, after removal, any owned node still exists
        or any boundary concept node was wrongly removed -- the same gate
        ``Neo4jGraphStore.delete_session_graph`` enforces.
        """
        graph = await self.resolve_session_graph(session_id)
        if graph is None:
            return None

        parent_of: dict[str, str] = {}
        children: dict[str, list[str]] = {}
        outgoing: dict[str, list[str]] = {}
        for (src, dst), edata in self._edges.items():
            outgoing.setdefault(src, []).append(dst)
            if edata.get("type") in _GRAPH_EDGE_TYPES:
                parent_of[dst] = src
                children.setdefault(src, []).append(dst)

        owned: set[str] = set()
        concept: set[str] = set()
        stack = list(graph.session_ids)
        visited: set[str] = set()
        while stack:
            nid = stack.pop()
            if nid in visited:
                continue
            visited.add(nid)
            node_data = self._nodes.get(nid) or {}
            if "SST_CONCEPT" in node_data.get("labels", []):
                concept.add(nid)
                continue
            owned.add(nid)
            stack.extend(outgoing.get(nid, []))

        relationships_deleted = sum(
            1 for (src, dst) in self._edges if src in owned or dst in owned
        )

        for nid in owned:
            self._nodes.pop(nid, None)
        self._edges = {
            key: data
            for key, data in self._edges.items()
            if key[0] not in owned and key[1] not in owned
        }

        survivors = [nid for nid in owned if nid in self._nodes]
        if survivors:
            raise RuntimeError(
                f"delete_session_graph: owned node(s) survived deletion for "
                f"graph root {graph.root_id!r}: {survivors!r}"
            )
        missing_concepts = [nid for nid in concept if nid not in self._nodes]
        if missing_concepts:
            raise RuntimeError(
                f"delete_session_graph: shared concept node(s) were wrongly "
                f"deleted for graph root {graph.root_id!r}: {missing_concepts!r}"
            )

        return GraphDeleteResult(
            root_id=graph.root_id,
            nodes_deleted=len(owned),
            relationships_deleted=relationships_deleted,
        )

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

    async def ensure_session_node(
        self,
        session_id: str,
        data: dict[str, Any],
        *,
        working_dir: str | None = None,
    ) -> None:
        """Idempotently create a Session node in the graph for *session_id*.

        *working_dir* is the folder the session ran in, read off the event
        envelope.  It is applied POPULATE-IF-MISSING: written when the event
        supplies one and the node does not already carry one, never overwritten
        once set.  That rule is enforced twice — here (so an already-populated
        node is left alone) and again at the Neo4j MERGE via ``coalesce``
        (so a concurrent writer or a replayed batch cannot clobber it either).

        Populate-if-missing is what makes backfill work: a Session node created
        before working_dir was recorded — or created as a bare reference by a
        delegation/fork edge — is filled in by the first later event that
        carries one, including a re-import through the upload CLI.

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
        # Tier 1: fast path — warm cache hit.
        # Safe with respect to working_dir: a worker only ever drains events for
        # its OWN session (worker_key == session_id), and the hook stamps the
        # same working_dir on every event of a session. So the FIRST call for
        # this worker's own session already carries the value if it will ever
        # carry one, and it is applied below before the cache is warmed. Nodes
        # this worker stubs for OTHER sessions (a parent_id or a delegation's
        # sub_session_id) are populated when that session's own worker runs.
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
            stub_data: dict[str, Any] = {
                "labels": ["Session"],
                "status": "running",
                "session_id": session_id,
            }
            # Populate-if-missing backfill. This is the branch a re-import lands
            # in (the node survives from the original ingest) and the branch a
            # worker respawned by crash recovery lands in. Only write when this
            # event supplies a working_dir AND the stored node still lacks one,
            # so an already-attributed session is never re-attributed.
            if working_dir and not existing.get("working_dir"):
                stub_data["working_dir"] = working_dir
            await self.graph.upsert_node(session_id, stub_data)
            self._seen_sessions.add(session_id)
            return

        # Node absent from both cache and graph — create it as a bare Session node.
        # ensure_session_node is a safety net; SessionHandler is the sole authority
        # on session type labels (RootSession, SubSession, ForkedSession).
        #
        # "StubSession" marks this node as created by a reference (delegation,
        # fork/start parent) BEFORE its own lifecycle events arrived.  If those
        # events never arrive, the node stays bare forever — indistinguishable
        # by label from a node about to be enriched.  StubSession makes that
        # permanently-orphaned state observable.  It is a plain marker, not a
        # terminal label: SessionLabelStateMachine.classify() removes it the
        # moment a real terminal label (RootSession/SubSession/ForkedSession/
        # IncompleteSession) is assigned via genuine lifecycle enrichment.
        node_data: dict[str, Any] = {
            "labels": ["Session", "StubSession"],
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
        # Attribute the session to the folder it ran in. Absent/None leaves the
        # property unset so a later event (or a re-import) can populate it.
        if working_dir:
            node_data["working_dir"] = working_dir

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


# ---------------------------------------------------------------------------
# Blob-size composition
# ---------------------------------------------------------------------------


async def total_blob_size(blob_store: BlobStore, blob_refs: Iterable[str]) -> int:
    """Sum the byte size of every ``ci-blob://`` URI in *blob_refs*.

    Composes ``BlobStore.size()`` over the graph's authoritative blob-ref set
    (``SessionGraph.blob_refs``) -- the size lookup goes through the
    ``BlobStore`` Protocol, never a raw filesystem stat, per the abstraction
    principle in docs/02-server-design.md. A missing blob contributes 0 (same
    idempotent-on-missing contract as ``BlobStore.size``/``delete_session``).
    """
    total = 0
    for uri in blob_refs:
        total += await blob_store.size(uri)
    return total
