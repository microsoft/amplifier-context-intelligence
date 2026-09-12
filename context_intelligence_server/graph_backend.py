"""GraphBackend protocol -- the single owner of graph-storage connections.

``GraphStore`` (see ``graph_store.py``) is the port for *using* a graph: buffer
writes, read nodes, flush. It deliberately says nothing about how a store gets
connected to its storage technology, and a store that has to answer that
question for itself is a store that leaks its technology into every caller that
constructs one.

``GraphBackend`` is the port for *obtaining* those stores. It is the one object
in the server permitted to know that the graph lives in Neo4j, that reaching it
costs a connection pool, and that the pool has to be opened before use and
closed after. Everything else asks a backend for a ``GraphStore`` and gets back
an object that satisfies a protocol -- never a driver, never a URL, never a
credential.

Two rules make that guarantee real, and both are structural rather than
advisory:

1.  **Nothing but a backend implementation constructs a connection.** A
    ``GraphStore`` implementation takes an already-built connection; it has no
    code path that builds one. A leak cannot be re-introduced by a caller that
    simply forgets to inject, because there is nothing to forget -- the
    parameter is required.
2.  **A backend hands out stores, never connections.** The three accessors
    below return ``GraphStore``/``QueryableStore``. There is no public accessor
    that returns the underlying connection, so no consumer can acquire one to
    hold, pass on, or close out of turn.

The maintenance surface (``ensure_schema``, ``health``, ``diagnose``,
``repair``) exists for the same reason. Those operations genuinely need the
connection rather than a workspace-scoped store, and without a home on this
protocol every one of them would be an excuse for the application entrypoint
and the CLI to reach for a raw driver again -- which is exactly how the
previous topology ended up with four independent construction sites.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from context_intelligence_server.graph_store import GraphStore, QueryableStore

__all__ = ["BackendHealth", "GraphBackend"]


@dataclass(frozen=True)
class BackendHealth:
    """Technology-neutral health snapshot for a graph backend.

    Reported by ``/status``. The field names here are deliberately neutral
    (``write_connected``, not ``neo4j_connected``) so the health *concept*
    carries no technology. The ``/status`` response keys remain the existing
    ``neo4j_*`` names -- those are a published wire contract and are mapped at
    the edge, in the endpoint, rather than propagated inward.

    ``url`` and ``browser_url`` are operator-facing strings the backend chooses
    to disclose about itself; they are opaque to every consumer.
    """

    write_connected: bool
    read_connected: bool
    url: str
    browser_url: str


@runtime_checkable
class GraphBackend(Protocol):
    """Owns graph-storage connections and hands out ``GraphStore`` instances.

    Lifecycle is explicit and symmetric: ``start`` before any store is
    requested, ``aclose`` when the process is done. ``aclose`` MUST be
    idempotent, because the shutdown path that calls it also runs on the
    failure path where ``start`` itself raised part-way through.
    """

    async def start(self) -> None:
        """Open the backend's connections. Call once, before any store use.

        MUST leave no connection open if it raises: a backend that fails
        half-way through opening is responsible for releasing whatever it had
        already acquired, so a failed startup cannot leak sockets.
        """
        ...

    async def aclose(self) -> None:
        """Release every connection this backend owns. Idempotent."""
        ...

    def session_store(
        self, *, workspace: str, created_by: str | None = None
    ) -> GraphStore:
        """Return a workspace-scoped store for one session's ingest path.

        The returned store shares this backend's write connection. Closing it
        flushes its buffer and releases nothing else -- connection lifetime is
        the backend's concern, never a per-session one.
        """
        ...

    def admin_store(self) -> GraphStore:
        """Return an unscoped store for whole-graph mutation (e.g. deletion).

        Workspace is resolved per operation from the data itself, so no
        workspace is bound here.
        """
        ...

    def query_store(self) -> QueryableStore:
        """Return an unscoped store for read-intent queries.

        This is what an arbitrary-query endpoint executes through, so that the
        endpoint depends on ``QueryableStore.execute_query`` rather than on a
        driver session in a specific query dialect.
        """
        ...

    async def ensure_schema(self, *, fail_on_data_conflict: bool = False) -> bool:
        """Idempotently establish the backend's schema objects.

        Args:
            fail_on_data_conflict: When True, raise rather than log-and-swallow
                if existing data blocks a constraint. Cold start passes True
                (nothing is written yet, so refusing to boot loses nothing);
                the self-healing write path passes False.

        Returns:
            True when the schema is fully established.
        """
        ...

    async def health(self) -> BackendHealth:
        """Report connectivity. MUST NOT raise -- unreachable reads as False."""
        ...

    async def diagnose(self) -> dict[str, int]:
        """Read-only storage health counts. No writes."""
        ...

    async def repair(self) -> dict[str, int]:
        """Run the backend's idempotent repair migrations."""
        ...
