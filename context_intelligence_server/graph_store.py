"""GraphStore and QueryableStore protocol definitions.

Non-negotiable guarantees for all conforming implementations:

1.  **Workspace isolation** — every write is scoped to the workspace set at
    construction; data from one workspace is never visible to another through
    normal read methods.
2.  **Point-lookup workspace-agnosticism** — ``get_node`` and ``get_edge`` may
    resolve lookups from any buffered workspace data, but never leak cross-
    workspace data via list/query operations.
3.  **Buffer-only writes** — ``upsert_node`` and ``upsert_edge`` MUST NOT
    perform any I/O; they only append to an in-memory buffer.
4.  **Buffer-first reads** — ``get_node`` and ``get_edge`` check the in-memory
    buffer before hitting the backing store; callers see their own writes
    immediately, even before a flush.
5.  **Flush semantics** — ``flush`` persists all buffered writes to the backing
    store atomically (best-effort); after a successful flush, the buffer is
    cleared.
6.  **Flush failure isolation** — failures inside ``flush`` MUST NOT propagate
    as exceptions to event handlers; implementations must swallow or log errors
    internally.
7.  **Close calls flush** — ``close`` MUST call ``flush`` before releasing any
    resources, ensuring no buffered writes are silently discarded.
8.  **Dialect enforcement** — ``execute_query`` raises ``ValueError`` when the
    requested dialect is not in ``supported_dialects``.
9.  **Default workspace scoping** — passing ``workspace=None`` to
    ``execute_query`` restricts results to the store's own workspace.
10. **Wildcard workspace** — passing ``workspace="*"`` to ``execute_query``
    disables workspace filtering entirely, returning data across all
    workspaces.
11. **Canonical workspace naming** — this module and all conforming
    implementations must use the term ``workspace`` exclusively; the legacy
    forest-scoping identifier has been retired and must not appear anywhere.
12. **Runtime checkability** — both ``GraphStore`` and ``QueryableStore`` are
    decorated with ``@runtime_checkable`` so that ``isinstance`` checks work
    at runtime without instantiating the protocol.
13. **Buffer discard semantics** — ``discard_buffer`` drops all buffered writes
    without persisting them.  It MUST NOT perform I/O and MUST NOT raise.  It is
    the dead-letter primitive used to isolate a poison write so it does not
    remain resident and re-enter the next flush.  In-memory implementations with
    no backing store may treat this as a no-op.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

_BLOB_REF_KEY = "$blob_ref"


@dataclass(frozen=True)
class SessionFamily:
    """Whole-family resolution result backing the session-summary facts.

    A session graph spans many session nodes (root + subsessions + forks),
    never just the one passed in (see B1 in
    docs/context-intelligence-delete-session-data.md). ``session_ids`` and
    ``blob_refs`` are the authoritative sets: a later delete operation reuses
    this exact resolution so its dry-run preview and its apply step can never
    disagree.

    ``node_count``/``edge_count`` and ``blob_refs`` cover the family's own
    subgraph only -- traversal stops at (but includes) any ``:SST_CONCEPT``
    node, since concept nodes (Agent/Orchestrator/Recipe) are shared across
    sessions and are never owned by one family.
    """

    root_id: str
    session_ids: frozenset[str]
    blob_refs: frozenset[str]
    node_count: int
    edge_count: int
    created_by: str | None
    started_at: datetime | None
    last_change: datetime | None
    subsession_count: int
    workspace: str
    working_dir: str | None


def extract_blob_refs(props: dict[str, Any]) -> frozenset[str]:
    """Return the distinct ``ci-blob://`` URIs referenced anywhere in *props*.

    Blob-offloaded fields (``blob_processor.py``) are written as
    ``{"$blob_ref": uri}``. In-memory stores keep that nested-dict shape;
    Neo4j has no nested-map property type, so ``Neo4jGraphStore._sanitize_properties``
    JSON-serialises the same dict to a string. Both shapes are handled here so
    every ``GraphStore`` implementation can share one extraction routine.
    """
    refs: set[str] = set()
    for value in props.values():
        candidate: Any = value
        if isinstance(candidate, str) and _BLOB_REF_KEY in candidate:
            try:
                candidate = json.loads(candidate)
            except ValueError:
                continue
        if isinstance(candidate, dict):
            ref = candidate.get(_BLOB_REF_KEY)
            if isinstance(ref, str):
                refs.add(ref)
    return frozenset(refs)


@runtime_checkable
class GraphStore(Protocol):
    """Protocol for a workspace-scoped, buffered graph store.

    Conforming classes must implement all properties and async methods defined
    here.  Writes are buffered in memory and persisted only when ``flush`` is
    called.  All writes are scoped to the ``workspace`` set at construction;
    point lookups are workspace-agnostic within the buffer.
    """

    @property
    def workspace(self) -> str:
        """Workspace this store is bound to (set at construction, read-only)."""
        ...

    async def upsert_node(self, node_id: str, data: dict[str, Any]) -> None:
        """Buffer a node upsert.

        MUST NOT perform any I/O.  The node is immediately visible to
        subsequent ``get_node`` calls via the in-memory buffer.
        """
        ...

    async def upsert_edge(self, src_id: str, dst_id: str, data: dict[str, Any]) -> None:
        """Buffer an edge upsert.

        MUST NOT perform any I/O.  The edge is immediately visible to
        subsequent ``get_edge`` calls via the in-memory buffer.
        """
        ...

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        """Return node data, checking the in-memory buffer first.

        Returns ``None`` if the node is not found in either the buffer or the
        backing store.
        """
        ...

    async def get_edge(self, src_id: str, dst_id: str) -> dict[str, Any] | None:
        """Return edge data, checking the in-memory buffer first.

        Returns ``None`` if the edge is not found in either the buffer or the
        backing store.
        """
        ...

    async def find_delegation_by_sub_session(
        self, sub_session_id: str, workspace: str
    ) -> dict[str, Any] | None:
        """Return the Delegation node whose ``sub_session_id`` property matches, or ``None``.

        Used to find the Delegation that *spawned* a given session -- i.e. the
        Delegation node ``D`` where ``D.sub_session_id == sub_session_id``. This
        is the parent-Delegation lookup the self-delegation resolver needs to
        find the real agent behind a ``agent == "self"`` delegation, since the
        correct source of truth is the parent Delegation node, never the parent
        Session node (which structurally never carries an ``agent`` property).

        Checks the in-memory buffer first, consistent with ``get_node``/
        ``get_edge`` buffer-first semantics. Returns ``None`` if no matching
        Delegation node is found in either the buffer or the backing store.
        """
        ...

    async def resolve_session_family(self, session_id: str) -> SessionFamily | None:
        """Resolve the whole session family (root + all descendants) for *session_id*.

        If *session_id* is not itself a root, first expands UP to the root by
        walking ``HAS_SUBSESSION``/``FORKED`` edges, then returns the root plus
        every descendant session -- never just the passed session alone (B1).
        A sub-session id and its root id MUST resolve to the identical family.

        Returns ``None`` if *session_id* does not resolve to any known
        ``:Session`` node.
        """
        ...

    async def flush(self) -> None:
        """Persist all buffered writes to the backing store.

        Failure MUST NOT propagate as an exception to event handlers;
        implementations must handle errors internally (log and swallow).
        After a successful flush the buffer is cleared.
        """
        ...

    async def close(self) -> None:
        """Release resources held by this store.

        MUST call ``flush`` before releasing any resources so that no buffered
        writes are silently discarded.
        """
        ...


@runtime_checkable
class QueryableStore(GraphStore, Protocol):
    """Protocol for a graph store that also supports arbitrary query execution.

    Extends ``GraphStore`` with a dialect-aware query interface.  The
    ``supported_dialects`` property advertises which query languages are
    available; ``execute_query`` raises ``ValueError`` for unsupported dialects.
    """

    @property
    def supported_dialects(self) -> frozenset[str]:
        """Set of query dialect identifiers supported by this store.

        Example: ``frozenset({"cypher", "sparql"})``.
        """
        ...

    async def execute_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        dialect: str = "cypher",  # protocol default; implementations may override
        workspace: str | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a query against the store.

        Args:
            query:     The query string in the specified dialect.
            params:    Optional query parameters.
            dialect:   Query language to use.  Raises ``ValueError`` if not in
                       ``supported_dialects``.
            workspace: Workspace to scope results to.
                       - ``None``  → scope to this store's own workspace.
                       - ``"*"``   → disable workspace filtering (all data).
                       - any str   → filter to the named workspace.

        Returns:
            A list of result row dicts.

        Raises:
            ValueError: If *dialect* is not in ``supported_dialects``.
        """
        ...
