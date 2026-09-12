"""Neo4j implementation of the ``GraphBackend`` port.

This module is the ONLY place in the server that owns a Neo4j driver. Every
other module -- the application entrypoint, the session registry, the deletion
routes, the doctor CLI -- asks this object for a ``GraphStore`` and never sees
a driver, a bolt URL, or a credential.

Two pools, matching the two logical clients the configuration defines:

* **write** (``neo4j.admin``) -- schema DDL, per-session ingest flushes,
  whole-graph deletion.
* **read** (``neo4j.cypher_query``) -- the arbitrary-query endpoint and the
  read-only deletion summary.

Both are bounded by ``neo4j_max_connection_pool_size`` and carry an acquisition
budget equal to ``neo4j_lock_timeout``, so a caller waiting on a saturated pool
fails on the same budget as one waiting on a blocked transaction.

Rationale and history: ``docs/architecture/README.md``.
"""

from __future__ import annotations

import logging
from typing import Any

from neo4j import AsyncGraphDatabase

from context_intelligence_server.config import Neo4jClientConfig, Settings
from context_intelligence_server.graph_backend import BackendHealth
from context_intelligence_server.graph_store import GraphStore, QueryableStore
from context_intelligence_server.neo4j_store import (
    Neo4jGraphStore,
    diagnose,
    ensure_neo4j_schema,
    run_repair,
)

_LOG = logging.getLogger(__name__)

__all__ = ["Neo4jGraphBackend"]


def _build_bounded_driver(
    config: Neo4jClientConfig,
    *,
    max_connection_pool_size: int,
    connection_acquisition_timeout: float | None = None,
) -> Any:
    """Construct an AsyncGraphDatabase driver with a bounded connection pool.

    The ONE call to ``AsyncGraphDatabase.driver`` in the server. Every pool the
    process opens is built here, with the same bounding kwargs, so two drivers
    cannot silently diverge in how they are bounded -- and a grep for driver
    construction has exactly one hit to audit.

    PRIVATE, and deliberately so: a public factory that returns a raw driver is
    a public bypass of the whole ownership boundary. A future module could
    import it and recreate the old multi-pool topology without ever touching
    ``Neo4jGraphBackend``. Only this module may call it.

    Args:
        max_connection_pool_size:
            Hard cap on concurrent bolt connections for this driver. Without a
            cap the neo4j driver defaults to 100 per pool, which is only safe
            while the number of pools is itself bounded.
        connection_acquisition_timeout:
            How long a caller waits for a free pooled connection before
            failing. Set to the configured lock timeout so waiting on a
            saturated pool fails on the same budget as waiting on a blocked
            transaction, rather than parking indefinitely. ``None`` leaves the
            driver default.

    ``max_connection_lifetime`` is deliberately not set: the neo4j driver
    already recycles pooled connections at 3600 s by default, so passing it
    would be a knob that changes nothing.
    """
    kwargs: dict[str, Any] = {
        "max_connection_pool_size": max_connection_pool_size,
        # Explicit auto-retry budget for transient errors (e.g. deadlocks) so
        # the managed-transaction retry window is deliberate and reviewable
        # rather than relying on the driver default implicitly.
        "max_transaction_retry_time": 30.0,
    }
    if (
        connection_acquisition_timeout is not None
        and connection_acquisition_timeout > 0
    ):
        kwargs["connection_acquisition_timeout"] = connection_acquisition_timeout
    return AsyncGraphDatabase.driver(config.url, auth=config.auth, **kwargs)


class Neo4jGraphBackend:
    """``GraphBackend`` over Neo4j. Owns every driver in the process."""

    def __init__(
        self,
        *,
        write_config: Neo4jClientConfig,
        read_config: Neo4jClientConfig,
        max_connection_pool_size: int,
        lock_timeout: float | None = None,
        database: str = "neo4j",
        flush_chunk_rows: int = 100,
        flush_chunk_bytes: int = 4_194_304,
        browser_url: str = "",
    ) -> None:
        self._write_config = write_config
        self._read_config = read_config
        self._max_connection_pool_size = max_connection_pool_size
        self._lock_timeout = lock_timeout if lock_timeout and lock_timeout > 0 else None
        self._database = database
        self._flush_chunk_rows = flush_chunk_rows
        self._flush_chunk_bytes = flush_chunk_bytes
        self._browser_url = browser_url
        self._write_driver: Any | None = None
        self._read_driver: Any | None = None

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_settings(cls, settings: Settings) -> Neo4jGraphBackend:
        """Build a backend from resolved settings.

        The single place server configuration is turned into graph-storage
        connection parameters. Callers pass settings, not bolt URLs.
        """
        return cls(
            write_config=settings.resolve_neo4j_admin(),
            read_config=settings.resolve_neo4j_query(),
            max_connection_pool_size=settings.neo4j_max_connection_pool_size,
            lock_timeout=settings.neo4j_lock_timeout,
            flush_chunk_rows=settings.neo4j_flush_chunk_rows,
            flush_chunk_bytes=settings.neo4j_flush_chunk_bytes,
            browser_url=settings.neo4j_browser_url,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Open both pools. Idempotent; leaks nothing if the second build raises.

        The write driver is built first and released explicitly if building the
        read driver fails. Without that, a partially-started backend would hold
        an unreferenced pool for the life of the process -- the same class of
        leak this module exists to eliminate, merely relocated to the startup
        path.
        """
        if self._write_driver is not None and self._read_driver is not None:
            return
        if self._write_driver is None:
            self._write_driver = _build_bounded_driver(
                self._write_config,
                max_connection_pool_size=self._max_connection_pool_size,
                connection_acquisition_timeout=self._lock_timeout,
            )
        try:
            if self._read_driver is None:
                self._read_driver = _build_bounded_driver(
                    self._read_config,
                    max_connection_pool_size=self._max_connection_pool_size,
                    connection_acquisition_timeout=self._lock_timeout,
                )
        except Exception:
            await self.aclose()
            raise
        _LOG.info(
            "graph_backend_started write_url=%s read_url=%s pool_size=%d",
            self._write_config.url,
            self._read_config.url,
            self._max_connection_pool_size,
        )

    async def aclose(self) -> None:
        """Close both pools. Idempotent, and never raises on a double close.

        Runs on the normal shutdown path AND on the startup-failure path, so a
        driver that is already gone -- or whose event loop has moved on -- must
        not turn teardown into a second failure.
        """
        for attr in ("_write_driver", "_read_driver"):
            driver = getattr(self, attr)
            if driver is None:
                continue
            setattr(self, attr, None)
            try:
                await driver.close()
            except RuntimeError:  # event-loop mismatch on interpreter teardown
                pass
            except Exception:  # noqa: BLE001 - shutdown must not be derailed
                _LOG.exception("graph_backend_close_failed driver=%s", attr)

    # ------------------------------------------------------------------
    # Store accessors
    # ------------------------------------------------------------------

    def _require(self, driver: Any | None, role: str) -> Any:
        if driver is None:
            raise RuntimeError(
                f"Neo4jGraphBackend.start() must be awaited before requesting a "
                f"{role} store"
            )
        return driver

    def session_store(
        self, *, workspace: str, created_by: str | None = None
    ) -> GraphStore:
        """A workspace-scoped store on the shared write pool, for one session."""
        store = Neo4jGraphStore(
            driver=self._require(self._write_driver, "session"),
            database=self._database,
            workspace=workspace,
            flush_chunk_rows=self._flush_chunk_rows,
            flush_chunk_bytes=self._flush_chunk_bytes,
            neo4j_lock_timeout=self._lock_timeout,
        )
        store.created_by = created_by
        return store

    def admin_store(self) -> GraphStore:
        """An unscoped store on the write pool, for whole-graph mutation."""
        return Neo4jGraphStore(
            driver=self._require(self._write_driver, "admin"),
            database=self._database,
            neo4j_lock_timeout=self._lock_timeout,
        )

    def query_store(self) -> QueryableStore:
        """An unscoped store on the read pool, carrying read access intent."""
        return Neo4jGraphStore(
            driver=self._require(self._read_driver, "query"),
            database=self._database,
            default_access_mode=self._read_config.access_mode,
        )

    # ------------------------------------------------------------------
    # Maintenance surface
    # ------------------------------------------------------------------

    async def ensure_schema(self, *, fail_on_data_conflict: bool = False) -> bool:
        """Create indexes and constraints idempotently on the write pool."""
        return await ensure_neo4j_schema(
            self._require(self._write_driver, "schema"),
            database=self._database,
            fail_on_data_conflict=fail_on_data_conflict,
        )

    async def health(self) -> BackendHealth:
        """Verify both pools. Never raises -- unreachable reads as False."""
        return BackendHealth(
            write_connected=await self._verify(self._write_driver),
            read_connected=await self._verify(self._read_driver),
            url=self._write_config.url,
            browser_url=self._browser_url,
        )

    @staticmethod
    async def _verify(driver: Any | None) -> bool:
        if driver is None:
            return False
        try:
            await driver.verify_connectivity()
            return True
        except Exception:  # noqa: BLE001 - health must never 500 the caller
            return False

    async def diagnose(self) -> dict[str, int]:
        """Read-only graph health counts (untagged + duplicate nodes)."""
        return await diagnose(
            self._require(self._write_driver, "diagnose"), database=self._database
        )

    async def repair(self) -> dict[str, int]:
        """Run dedup + label backfill + schema DDL. Idempotent."""
        return await run_repair(
            self._require(self._write_driver, "repair"), database=self._database
        )
