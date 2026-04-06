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
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


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

    async def flush(self) -> None:
        """Persist all buffered writes to the backing store.

        Failure MUST NOT propagate as an exception to event handlers;
        implementations must handle errors internally (log and swallow).
        After a successful flush the buffer is cleared.
        """
        ...

    def schedule_flush(self) -> None:
        """Schedule a background flush without blocking.

        Implementations must ensure that at most one background flush runs at a
        time — if a flush is already in progress, this call is a no-op or defers
        until the current flush completes.  In-memory implementations may treat
        this as a no-op since all writes are immediately visible.
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
