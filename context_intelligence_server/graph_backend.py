"""GraphBackend protocol -- the single owner of graph-storage connections.

``GraphStore`` (``graph_store.py``) is the port for *using* a graph. This is the
port for *obtaining* one, plus the connection lifecycle and the maintenance
operations that need a connection rather than a workspace-scoped store.

Two rules, both structural rather than advisory:

1.  Only a backend implementation constructs a connection. A ``GraphStore``
    takes an already-open one as a required argument.
2.  A backend hands out stores, never connections. No accessor here returns a
    driver.

Why it is shaped this way: ``docs/architecture/README.md`` -> "Graph backend and
Neo4j client topology".
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from context_intelligence_server.graph_store import GraphStore, QueryableStore

__all__ = ["BackendHealth", "GraphBackend"]


@dataclass(frozen=True)
class BackendHealth:
    """Technology-neutral health snapshot, reported by ``/status``.

    Field names carry no technology; ``/status``'s ``neo4j_*`` response keys are
    a published wire contract and are mapped at the endpoint, not propagated
    inward. ``url``/``browser_url`` are operator-facing and opaque to consumers.
    """

    write_connected: bool
    read_connected: bool
    url: str
    browser_url: str


@runtime_checkable
class GraphBackend(Protocol):
    """Owns graph-storage connections and hands out ``GraphStore`` instances."""

    async def start(self) -> None:
        """Open the backend's connections, once, before any store is requested.

        MUST leave nothing open if it raises, so a failed startup cannot leak.
        """
        ...

    async def aclose(self) -> None:
        """Release every connection this backend owns.

        MUST be idempotent: the shutdown path that calls it also runs on the
        failure path where ``start`` raised part-way through.
        """
        ...

    def session_store(
        self, *, workspace: str, created_by: str | None = None
    ) -> GraphStore:
        """A workspace-scoped store for one session's ingest path."""
        ...

    def admin_store(self) -> GraphStore:
        """An unscoped store for whole-graph mutation; workspace resolved per call."""
        ...

    def query_store(self) -> QueryableStore:
        """An unscoped store carrying read intent, for arbitrary queries."""
        ...

    async def ensure_schema(self, *, fail_on_data_conflict: bool = False) -> bool:
        """Idempotently establish schema objects; True when fully established.

        ``fail_on_data_conflict``: raise, rather than log-and-swallow, when
        existing data blocks a constraint. Cold start passes True (nothing is
        written yet, so refusing to boot loses nothing); the self-healing write
        path passes False.
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
