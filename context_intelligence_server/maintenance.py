"""Maintenance-mode coordinator -- the single seam for gate + status + op state.

This module owns ALL maintenance-mode state: the tri-state live constraint
probe, the current-mode derivation, the single-flight maintenance-operation
record, and the structured 503 response. It is deliberately the ONLY place
this state lives so that the HTTP gate (``maintenance_gate_middleware``), the
drain-loop gate (``registry.drain_worker``), and the ``/status`` /
``GET /admin/maintenance`` surfaces can never drift from one another.

Constraints (do not violate):
- This module MUST NOT import ``registry`` -- ``registry`` imports this
  module, never the reverse.
- This module MUST NOT contain graph-mutation logic. Migration/repair logic
  lives in ``neo4j_store.run_repair``; this module only reads (the
  constraint-presence probe) and tracks in-process op state.

Swappability (council D2, multi-replica deferred): every call site touches
only ``gate_closed`` / ``status`` / ``try_begin_op`` / ``finish_op`` /
``current_op``. Replacing the in-process op record with a graph-store-backed
lock node later means reimplementing the body of those five methods --
zero call-site changes.

No driver bound -- the load-bearing no-regression property: tests that never
run ``lifespan`` never call ``bind_driver``, so the probe always returns
``None`` (unknown) and the gate is never closed. This keeps the ENTIRE
existing unit-test suite's behavior unchanged.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import Request
from fastapi.responses import JSONResponse, Response

from context_intelligence_server.config import get_settings
from context_intelligence_server.neo4j_store import count_untagged_nodes

logger = logging.getLogger("context_intelligence_server")

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

MaintenanceMode = Literal["healthy", "maintenance", "degraded", "unknown"]
OpState = Literal["unknown", "running", "succeeded", "failed"]


@dataclass(frozen=True)
class OpRecord:
    """Snapshot of the (per-process) maintenance operation state.

    ``state`` initializes to "unknown" (council D4) so "never ran" is
    distinguishable from "ran, record lost to a crash" (which would also
    read as a false "succeeded" if initialized there instead).
    """

    state: OpState
    run_id: str | None  # uuid4 hex; freshness marker AND future fencing token
    started_at: str | None  # ISO-8601 UTC
    completed_at: str | None  # ISO-8601 UTC; set ONLY on the genuine-execution path
    records_affected: int | None
    error: str | None  # human-readable; persists across "failed"


@dataclass(frozen=True)
class MaintenanceStatus:
    """Snapshot of the current maintenance mode, returned by ``status()``."""

    mode: MaintenanceMode
    constraint_present: bool | None  # None == probe could not answer
    reason: str | None  # human-readable cause of the current mode
    started_at: str | None  # when the CURRENT maintenance window opened
    elapsed_seconds: float | None  # None when not in maintenance
    op: OpRecord
    # Live (TTL-cached) untagged-node count backing the `degraded` term.
    # Defaulted so existing MaintenanceStatus(...) constructions in tests keep
    # working; None == the probe could not answer (or no driver bound).
    untagged_nodes: int | None = None


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CONSTRAINT_NAME = "node_node_id_workspace_unique"
_PROBE_CYPHER = (
    "SHOW CONSTRAINTS YIELD name "
    f"WHERE name = '{_CONSTRAINT_NAME}' RETURN count(*) AS c"
)
_PROBE_TTL_SECONDS = 5.0  # default; overridable per-instance via bind_driver()
_RETRY_AFTER_DEFAULT = (
    30  # fallback; live value is settings.maintenance_retry_after_seconds
)

# Allow-list, not deny-list: a deny-list is scatter-and-miss (spec sec 3a-1).
# Any route added later to the app is blocked-by-default -- the safe direction.
MAINTENANCE_ALLOW_LIST: frozenset[str] = frozenset(
    {"/status", "/version", "/admin/maintenance", "/docs", "/openapi.json"}
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


# ---------------------------------------------------------------------------
# The seam
# ---------------------------------------------------------------------------


class MaintenanceCoordinator:
    """The seam. See module docstring for the swappability contract."""

    def __init__(self) -> None:
        self._driver: Any = None
        self._boot_untagged: int | None = None
        self._probe_ttl_seconds: float = _PROBE_TTL_SECONDS

        # TTL-cached, single-flight probe state (constraint presence).
        self._probe_lock = asyncio.Lock()
        self._cache_populated: bool = False
        self._cached_present: bool | None = None
        self._cache_expires_at: float = 0.0  # monotonic clock

        # TTL-cached, single-flight probe state for the untagged-node count --
        # a SEPARATE cache from the constraint probe above (independent fate:
        # a count failure must not poison the constraint signal, and vice
        # versa). This is what de-latches the degraded/untagged half of the
        # health signal: it self-clears within one TTL after an out-of-band
        # repair, exactly like the constraint probe, instead of staying pinned
        # to the boot-time snapshot until a restart. count_untagged_nodes is
        # O(1) via Neo4j's counts store (see its docstring), so probing it on
        # the cached path is as cheap as the constraint catalog read.
        self._untagged_lock = asyncio.Lock()
        self._untagged_cache_populated: bool = False
        self._cached_untagged: int | None = None
        self._untagged_cache_expires_at: float = 0.0  # monotonic clock

        # Op state -- init "unknown" (council D4: never-run != crash-lost).
        self._op = OpRecord(
            state="unknown",
            run_id=None,
            started_at=None,
            completed_at=None,
            records_affected=None,
            error=None,
        )

        # Maintenance-window bookkeeping (for /status + transition logging).
        self._window_started_at: str | None = None
        self._window_started_monotonic: float | None = None

        # WS-3c: strong references to in-flight maintenance-op background
        # tasks. asyncio only holds a WEAK reference to a task created via
        # ``asyncio.create_task`` -- without an external strong ref the task
        # can be garbage-collected mid-run. ``retain_task`` holds it here and
        # a completion callback discards it so this set never grows unbounded.
        self._background_tasks: set[asyncio.Task[Any]] = set()

    # -- binding --------------------------------------------------------

    def bind_driver(
        self,
        driver: Any,
        *,
        untagged: int | None = None,
        probe_ttl_seconds: float | None = None,
    ) -> None:
        """Wire the live admin Neo4j driver (called from ``lifespan``).

        ``untagged`` is the boot-time untagged-node count already computed by
        ``main._record_schema_health`` -- injected here so this module never
        imports ``main`` (would be circular). ``probe_ttl_seconds`` overrides
        the default TTL (normally sourced from
        ``settings.maintenance_probe_ttl_seconds`` by the caller).

        Tests that never call this leave the driver unbound: the probe then
        always returns ``None`` (unknown) and the gate stays OPEN -- the
        load-bearing no-regression property for the existing suite.
        """
        self._driver = driver
        self._boot_untagged = untagged
        if probe_ttl_seconds is not None:
            self._probe_ttl_seconds = probe_ttl_seconds
        # Rebinding invalidates any cached constraint probe result.
        self._cache_populated = False
        self._cached_present = None
        self._cache_expires_at = 0.0
        # Seed the untagged cache with the boot-time count so the FIRST
        # /status (before any live re-probe) reports the same value the old
        # boot snapshot did -- then let it expire after one TTL so the live
        # probe takes over and de-latches it. Seeding as populated (not cold)
        # keeps the existing TTL-cache tests' hit-counts unchanged for the
        # first in-window call.
        self._cached_untagged = untagged
        self._untagged_cache_populated = True
        self._untagged_cache_expires_at = time.monotonic() + self._probe_ttl_seconds

    # -- the constraint probe --------------------------------------------

    async def _run_probe(self) -> bool | None:
        """One live catalog read. Tri-state: True / False / None (unknown).

        ``SHOW CONSTRAINTS`` is a catalog read -- no data scan, no dependence
        on graph size. Unlike ``count_untagged_nodes``/``count_duplicate_nodes``
        (neo4j_store.py), this MUST be cheap enough to run on the request
        path; it must never become an ``AllNodesScan``.
        """
        if self._driver is None:
            return None
        try:
            async with self._driver.session() as session:
                result = await session.run(_PROBE_CYPHER)
                count = 0
                async for record in result:
                    count = record["c"]
                return count > 0
        except Exception as exc:  # noqa: BLE001 -- connectivity probe, not confirmed bad state
            logger.warning("maintenance_probe_failed error=%s", exc)
            return None

    async def _probe_constraint_present(self) -> bool | None:
        """TTL-cached, single-flight wrapper around ``_run_probe``.

        Double-checked locking: the fast path (warm cache) never touches the
        lock, so N concurrent callers with a warm cache never contend on it.
        Only callers that observe an expired/empty cache take the lock, and
        the re-check immediately after acquiring collapses concurrent
        expiry-time callers into exactly one live probe.
        """
        now = time.monotonic()
        if self._cache_populated and now < self._cache_expires_at:
            return self._cached_present
        async with self._probe_lock:
            now = time.monotonic()
            if self._cache_populated and now < self._cache_expires_at:
                return self._cached_present
            result = await self._run_probe()
            self._cached_present = result
            self._cache_populated = True
            self._cache_expires_at = time.monotonic() + self._probe_ttl_seconds
            return result

    # -- the untagged-node probe (de-latches the degraded half) ----------

    async def _run_untagged_probe(self) -> int | None:
        """One live untagged-node count. Tri-state: int / None (unknown).

        ``count_untagged_nodes`` is O(1) via Neo4j's counts store (total minus
        :Node count), NOT the ``WHERE NOT n:Node`` AllNodesScan -- safe on the
        cached request path. A probe failure returns None (unknown, no
        ``degraded`` signal fabricated) exactly as the constraint probe does;
        it is caught here so it can never poison the independent constraint
        signal.
        """
        if self._driver is None:
            return None
        try:
            return await count_untagged_nodes(self._driver)
        except Exception as exc:  # noqa: BLE001 -- connectivity probe, not confirmed bad state
            logger.warning("maintenance_untagged_probe_failed error=%s", exc)
            return None

    async def _probe_untagged(self) -> int | None:
        """TTL-cached, single-flight wrapper around ``_run_untagged_probe``.

        Same double-checked-locking shape as ``_probe_constraint_present`` but
        with its OWN cache/lock so the two probes never share fate. Seeded at
        ``bind_driver`` with the boot count, then live-refreshed each TTL --
        this is what lets an out-of-band repair clear ``degraded`` without a
        restart (the latch this fixes).
        """
        now = time.monotonic()
        if self._untagged_cache_populated and now < self._untagged_cache_expires_at:
            return self._cached_untagged
        async with self._untagged_lock:
            now = time.monotonic()
            if self._untagged_cache_populated and now < self._untagged_cache_expires_at:
                return self._cached_untagged
            result = await self._run_untagged_probe()
            self._cached_untagged = result
            self._untagged_cache_populated = True
            self._untagged_cache_expires_at = time.monotonic() + self._probe_ttl_seconds
            return result

    # -- mode derivation (single source of truth for /status + the gate) --

    def _handle_transition(
        self, mode: MaintenanceMode, reason: str | None, run_id: str | None
    ) -> None:
        """Detect + log open<->closed transitions exactly once each.

        No ``await`` anywhere in this method: it cannot be preempted
        mid-execution by another coroutine, so two concurrent callers
        observing the same transition can never both log it (whichever runs
        first flips ``_window_started_at``; the second then sees the
        already-updated state and no-ops).
        """
        is_maintenance = mode == "maintenance"
        was_maintenance = self._window_started_at is not None
        if is_maintenance and not was_maintenance:
            self._window_started_at = _now_iso()
            self._window_started_monotonic = time.monotonic()
            logger.info(
                "maintenance_entered",
                extra={
                    "reason": reason,
                    "run_id": run_id,
                    "trigger": "op" if self._op.state == "running" else "constraint",
                },
            )
        elif not is_maintenance and was_maintenance:
            duration = (
                time.monotonic() - self._window_started_monotonic
                if self._window_started_monotonic is not None
                else None
            )
            logger.info(
                "maintenance_completed",
                extra={
                    "reason": reason,
                    "run_id": run_id,
                    "duration_seconds": duration,
                },
            )
            self._window_started_at = None
            self._window_started_monotonic = None

    async def _derive_mode(
        self,
    ) -> tuple[MaintenanceMode, str | None, bool | None, int | None]:
        """The ONE place mode is computed. Both ``gate_closed`` and
        ``status`` call this so a transition is caught no matter which
        surface is being polled (spec sec 2.4)."""
        op = self._op
        constraint_present = await self._probe_constraint_present()
        # untagged is only load-bearing for the `degraded` term, which only
        # applies when the constraint IS present. Probing it only in that
        # branch keeps the absent/unknown/op-running paths (and their existing
        # TTL-cache hit-count tests) untouched, and avoids a needless count on
        # a graph we already know is in maintenance.
        untagged: int | None = None

        if op.state == "running":
            mode: MaintenanceMode = "maintenance"
            reason = "maintenance operation in progress"
        elif constraint_present is False:
            mode = "maintenance"
            reason = ":Node uniqueness constraint absent -- migration required"
        elif constraint_present is None:
            mode = "unknown"
            reason = "constraint probe could not determine graph state"
        else:
            # constraint present: consult the LIVE (TTL-cached) untagged count
            # so an out-of-band repair de-latches degraded->healthy with no
            # restart. None (probe could not answer) is NOT coerced to
            # degraded -- no evidence, so healthy stands.
            untagged = await self._probe_untagged()
            if untagged is not None and untagged > 0:
                mode = "degraded"
                reason = f"{untagged} node(s) lacking the :Node label"
            else:
                mode = "healthy"
                reason = None

        self._handle_transition(mode, reason, op.run_id)
        return mode, reason, constraint_present, untagged

    # -- public seam ------------------------------------------------------

    async def gate_closed(self) -> bool:
        """True iff ingest/query must be refused right now.

        ``gate_closed() == op_running (live) OR constraint_absent (TTL-cached)``.
        ``degraded`` and ``unknown`` do NOT close the gate -- only
        ``mode == "maintenance"`` does (D-C, D-E).
        """
        mode, _reason, _constraint_present, _untagged = await self._derive_mode()
        return mode == "maintenance"

    async def status(self) -> MaintenanceStatus:
        """Full snapshot for ``/status`` and ``GET /admin/maintenance``."""
        mode, reason, constraint_present, untagged = await self._derive_mode()
        elapsed = (
            time.monotonic() - self._window_started_monotonic
            if self._window_started_monotonic is not None
            else None
        )
        return MaintenanceStatus(
            mode=mode,
            constraint_present=constraint_present,
            reason=reason,
            started_at=self._window_started_at,
            elapsed_seconds=elapsed,
            op=self._op,
            untagged_nodes=untagged,
        )

    def try_begin_op(self) -> str | None:
        """Synchronous single-flight CAS -- begin an op iff none is running.

        No ``await`` between the check and the set: in asyncio this makes
        the check-and-set atomic (D-G / MUST-FIX #3). Per-process only;
        ``run_id`` is the future multi-replica fencing token.
        """
        if self._op.state == "running":
            return None
        run_id = uuid.uuid4().hex
        self._op = OpRecord(
            state="running",
            run_id=run_id,
            started_at=_now_iso(),
            completed_at=None,
            records_affected=None,
            error=None,
        )
        return run_id

    def finish_op(
        self, run_id: str, *, records_affected: int | None, error: str | None
    ) -> None:
        """Record the outcome of the op started by ``try_begin_op``.

        Sets ``completed_at`` -- the ONLY place it is written, which is what
        makes ``completed_at`` a genuine freshness marker (D-H). A
        run_id mismatch (stale/foreign completion signal) is logged and
        ignored rather than corrupting the current op record.
        """
        if self._op.run_id != run_id:
            logger.warning(
                "maintenance_finish_op_run_id_mismatch expected=%s got=%s",
                self._op.run_id,
                run_id,
            )
            return
        self._op = OpRecord(
            state="failed" if error else "succeeded",
            run_id=run_id,
            started_at=self._op.started_at,
            completed_at=_now_iso(),
            records_affected=records_affected,
            error=error,
        )

    def current_op(self) -> OpRecord:
        return self._op

    def retain_task(self, task: asyncio.Task[Any]) -> None:
        """Hold a strong reference to an in-flight maintenance-op task.

        ``asyncio.create_task`` only returns a task the event loop tracks
        weakly; with no other strong reference, the task object can be
        garbage-collected mid-run (a well-known asyncio footgun -- see the
        "Important" note in the stdlib ``asyncio.create_task`` docs). The
        caller (``routers/admin.py``) MUST call this immediately after
        creating the task. The completion callback discards the reference
        once the task finishes, so this set never grows unbounded.
        """
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)


# Module singleton -- the ONE coordinator instance shared by the HTTP gate,
# the drain-loop gate, and /status.
coordinator: MaintenanceCoordinator = MaintenanceCoordinator()


# ---------------------------------------------------------------------------
# The structured 503 (one producer)
# ---------------------------------------------------------------------------


def maintenance_response(status: MaintenanceStatus, retry_after: int) -> JSONResponse:
    """The ONE producer of the maintenance 503.

    Deliberately NOT ``HTTPException(503, detail=...)`` -- that renders
    ``{"detail": ...}``, the wrong contract for this response.
    """
    schema_health = "unknown" if status.constraint_present is None else "degraded"
    return JSONResponse(
        status_code=503,
        content={
            "status": "maintenance",
            "reason": status.reason,
            "retry_after": retry_after,
            "schema_health": schema_health,
            "maintenance_started_at": status.started_at,
        },
        headers={"Retry-After": str(retry_after)},
    )


# ---------------------------------------------------------------------------
# HTTP gate middleware
# ---------------------------------------------------------------------------


async def maintenance_gate_middleware(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Allow-list middleware: refuse every non-allow-listed path while the
    coordinator reports ``mode == "maintenance"``.

    Registered on ``app`` itself (not the auth-wrapped ASGI app) so it cannot
    be bypassed by the bare ``app`` entrypoint -- see WS-3a spec sec 3a-1.
    Blocks (non-exhaustive; see docs/maintenance-mode.md once shipped):
    ``POST /events``, ``POST /cypher``, ``GET /blobs/*``, ``GET/POST
    /queues/*`` (including dead-letter replay), and all ``/admin/*`` except
    ``/admin/maintenance``.
    """
    if request.url.path in MAINTENANCE_ALLOW_LIST:
        return await call_next(request)
    st = await coordinator.status()
    if st.mode == "maintenance":
        retry_after = getattr(
            get_settings(), "maintenance_retry_after_seconds", _RETRY_AFTER_DEFAULT
        )
        return maintenance_response(st, retry_after)
    return await call_next(request)
