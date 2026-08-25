"""FastAPI application entrypoint for the Context Intelligence Server."""

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, nullcontext, suppress
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from neo4j import READ_ACCESS, WRITE_ACCESS, AsyncGraphDatabase

from context_intelligence_server import __version__
from context_intelligence_server.auth import (
    _EXEMPT_PATHS,
    BearerTokenMiddleware,
    EntraResolver,
    StaticKeyResolver,
)
from context_intelligence_server.authz import (  # noqa: F401 — re-exported for tests/routes
    _is_write_capable,
    require_read,
    require_write,
)
from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.config import Neo4jClientConfig, Settings, get_settings
from context_intelligence_server.idempotency import (
    EventIdempotencyCache,
    KeyedAsyncLocks,
)
from context_intelligence_server.identity_store import IdentityStore
from context_intelligence_server.logging_config import setup_logging
from context_intelligence_server.models import (
    CypherRequest,
    EventRequest,
    EventResponse,
)
from context_intelligence_server.neo4j_store import (
    build_bounded_neo4j_driver,
    count_untagged_nodes,
    ensure_neo4j_schema,
)
from context_intelligence_server.registry import SessionRegistry
from context_intelligence_server.routers.admin import router as admin_router
from context_intelligence_server.routers.queues import router as queues_router
from context_intelligence_server.routers.version import router as version_router
from context_intelligence_server.status import boot_state, build_status_response
from context_intelligence_server.writer_lease import (
    WriterLeaseConflict,
    shutdown_lease_io,
    writer_lease,
)

_settings = get_settings()

logger = logging.getLogger("context_intelligence_server")


def _neo4j_access_const(mode: str) -> str:
    """Map our config string ("READ"/"WRITE") to the driver's access-mode constant."""
    return READ_ACCESS if mode == "READ" else WRITE_ACCESS


def build_neo4j_driver(config: Neo4jClientConfig) -> Any:
    """Construct the pool-bounded admin AsyncGraphDatabase driver.

    Shared by ``lifespan()`` (the admin driver, on every server boot) and
    ``doctor.run_doctor()`` (the CLI), so the two entry points can never
    construct the connection differently. Delegates the actual driver
    construction to ``build_bounded_neo4j_driver`` so the pool-bounding kwargs
    have one source of truth, shared with ``SessionRegistry``'s driver.
    """
    return build_bounded_neo4j_driver(
        config,
        max_connection_pool_size=_settings.neo4j_max_connection_pool_size,
        max_connection_lifetime=_settings.neo4j_max_connection_lifetime,
    )


# Module-level live identity-map stores. Exactly one is non-None at a time --
# the other is reset to None so accessors return an unambiguous result.
_api_key_store: IdentityStore | None = None
_entra_identity_store: IdentityStore | None = None


def get_api_key_store() -> IdentityStore | None:
    """Return the live API-key IdentityStore, or None when entra mode is active.

    The /admin router calls this to mutate the keystore (PUT/DELETE entries).
    The returned store's flat_dict is the SAME object used by StaticKeyResolver,
    so any put() or delete() is visible to the resolver immediately.
    """
    return _api_key_store


def get_entra_identity_store() -> IdentityStore | None:
    """Return the live Entra-identity IdentityStore, or None when static mode is active.

    The /admin router calls this to mutate the identity map (PUT/DELETE entries).
    The returned store's flat_dict is the SAME object used by EntraResolver,
    so any put() or delete() is visible to the resolver immediately.
    """
    return _entra_identity_store


# Last-resort workspace sentinel when no head line resolves a workspace.
# Dispatching under it still isolates the bad line and drains the rest.
_RECOVERY_FALLBACK_WORKSPACE = "unknown-recovered"


def _head_is_resumable(raw: bytes) -> bool:
    """Total predicate: does ``raw`` parse to a dict with a workspace?

    Shared by ``_recover_one_session`` and ``QueueManager.classify_session``
    (injected as a pure callable so the queue never learns the event schema).
    Never raises -- valid-but-non-dict JSON must not escape as an AttributeError.
    """
    try:
        obj = json.loads(raw)
    except (ValueError, TypeError):
        return False
    if not isinstance(obj, dict):
        return False
    try:
        return bool(obj.get("workspace", ""))
    except (AttributeError, TypeError):
        return False


def _parse_workspace_and_creator(raw: str | bytes) -> tuple[str, str | None] | None:
    """Return ``(workspace, created_by)`` iff ``raw`` parses to a dict with a
    non-empty workspace, else ``None``. Total -- never raises."""
    try:
        obj = json.loads(raw)
    except (ValueError, TypeError):
        return None
    if not isinstance(obj, dict):
        return None
    try:
        workspace = obj.get("workspace", "")
        created_by = obj.get("created_by")
    except (AttributeError, TypeError):
        return None
    if not workspace:
        return None
    return workspace, created_by


def _recover_one_session(
    sid: str,
    first_line: str | bytes,
    get_or_create: Any,
    first_log_line: bytes | None = None,
    *,
    recovered: bool = True,
) -> bool:
    """Parse the first queued line for *sid* and respawn a drainer when valid.

    Falls back to ``first_log_line`` (byte-0) when ``first_line`` doesn't
    resolve a workspace, then to the ``_RECOVERY_FALLBACK_WORKSPACE``
    sentinel -- so an unparseable head never blocks recovery of the data
    behind it. Returns True if a drainer was (re)spawned, False if skipped.
    """
    parsed = _parse_workspace_and_creator(first_line)
    if parsed is not None:
        workspace, created_by = parsed
        get_or_create(sid, workspace, created_by=created_by, recovered=recovered)
        return True

    if first_log_line is not None:
        byte0_parsed = _parse_workspace_and_creator(first_log_line)
        if byte0_parsed is not None:
            workspace, created_by = byte0_parsed
            logger.warning("recovery_fallback_workspace session=%s source=byte0", sid)
            get_or_create(sid, workspace, created_by=created_by, recovered=recovered)
            return True
        # Last resort: dispatch under the sentinel anyway -- the drainer
        # dead-letters the unparseable head and drains everything behind it.
        logger.warning("recovery_fallback_workspace session=%s source=sentinel", sid)
        get_or_create(
            sid,
            _RECOVERY_FALLBACK_WORKSPACE,
            created_by=None,
            recovered=recovered,
        )
        return True

    logger.warning(
        "recovery_skipped session=%s: torn or empty workspace in first line",
        sid,
    )
    return False


@dataclass
class TopupResult:
    """Result of one ``_crash_recovery_topup`` pass.

    ``dispatched``: sessions dispatched this pass (idempotent, so an upper
    bound on newly-spawned drainers). ``recovered``: total size of this
    pass's ``recover()`` report, before ceiling slicing. ``deferred``:
    ``recovered`` minus how many were processed (0 when unbounded).
    """

    dispatched: int
    recovered: int
    deferred: int


async def _crash_recovery_topup(respawn_limit: int | None) -> TopupResult:
    """One bounded crash-recovery pass: respawn drainers for up to
    ``respawn_limit`` recovered sessions (all of them when ``None``).

    Shared by the boot-time recovery and the periodic sweep. Safe to call
    repeatedly -- ``get_or_create`` is idempotent, and ``recover()`` only
    reports sessions with undrained data, so live recovered drainers stay
    bounded as the deferred tail advances. Falls back to the session's
    byte-0 line when the head doesn't resolve a workspace.
    """
    recovered = await registry.queue_manager.recover()
    to_process = recovered if respawn_limit is None else recovered[:respawn_limit]
    deferred_count = (
        0 if respawn_limit is None else max(0, len(recovered) - respawn_limit)
    )
    dispatched = 0
    for sid in to_process:
        try:
            # Guarded here (not inside read_batch, which must stay loud for
            # the live drainer's hot path) so one bad key can't halt the pass.
            batch = await registry.queue_manager.read_batch(sid, max_items=1)
        except (OSError, ValueError):
            logger.exception("crash_recovery_topup_read_failed session=%s", sid)
            continue
        if not batch.lines:
            # recover()/read_batch disagreement (e.g. a concurrent compaction
            # advanced the offset) -- not a loss, just no longer recoverable.
            logger.warning(
                "recovery_skipped_empty_batch session=%s reason=empty_batch",
                sid,
            )
            continue
        # An upper bound on newly-spawned drainers: get_or_create is
        # idempotent, so "dispatched" may include already-live workers.
        dispatched_ok = _recover_one_session(
            sid, batch.lines[0], registry.get_or_create
        )
        if not dispatched_ok:
            first_log_line = await registry.queue_manager.read_first_line(sid)
            dispatched_ok = _recover_one_session(
                sid,
                batch.lines[0],
                registry.get_or_create,
                first_log_line=first_log_line,
            )
        if dispatched_ok:
            dispatched += 1
    if deferred_count:
        # WARNING (not INFO): a deferred backlog must never be silently
        # undiscoverable. Names the exact counts and the setting to raise.
        logger.warning(
            "lifespan_startup: crash-recovery respawn cap reached "
            "(crash_recovery_respawn_limit=%d): %d/%d respawned this pass, "
            "%d session(s) deferred to a later pass (untouched on disk, "
            "still fully recoverable). Raise crash_recovery_respawn_limit "
            "to respawn more per pass.",
            respawn_limit,
            dispatched,
            len(to_process),
            deferred_count,
        )
    return TopupResult(
        dispatched=dispatched, recovered=len(recovered), deferred=deferred_count
    )


async def _ensure_schema_ready() -> None:
    """Attempt Neo4j schema init once; a no-op once already ready.

    Sets ``app.state.schema_ready`` on success. A connectivity failure
    (Neo4j unreachable) is logged and swallowed here -- schema stays
    not-ready, retried later (boot's sweep phase) instead of crash-looping
    the server. Raises ``RuntimeError`` only for a genuine data conflict
    (graph reachable but un-migrated) -- the one refusal this still
    preserves, now recorded via ``boot_state.fail()`` by the caller instead
    of aborting ASGI startup.
    """
    if getattr(app.state, "schema_ready", False):
        return
    try:
        await ensure_neo4j_schema(app.state.neo4j_driver, fail_on_data_conflict=True)
    except RuntimeError:
        raise  # genuine data conflict: fatal, let the caller record it
    except Exception as exc:  # noqa: BLE001 - Neo4j unreachable, not fatal
        logger.warning(
            "schema_init_unreachable: Neo4j not reachable, will retry: %s", exc
        )
        return
    # Catches nodes lacking the :Node label (the other un-migrated shape the
    # constraint above can't see). A probe failure is logged at DEBUG, not
    # treated as confirmed-bad -- the flush path's self-heal still covers it.
    try:
        untagged = await count_untagged_nodes(app.state.neo4j_driver)
    except Exception as exc:  # noqa: BLE001 - connectivity probe, not a confirmed bad state
        logger.debug(
            "schema_init: untagged-node probe skipped (graph unreachable?): %s", exc
        )
        untagged = 0
    if untagged:
        raise RuntimeError(
            f"Neo4j graph has {untagged} node(s) lacking the :Node label "
            "(un-migrated). Cold start refuses to boot to avoid duplicating "
            "them on write. Run: context-intelligence-server doctor --fix"
        )
    app.state.schema_ready = True
    logger.info("lifespan_startup: Neo4j schema initialized")


async def _crash_recovery_sweep_loop(interval: int, respawn_limit: int) -> None:
    """Periodically top the recovered-drainer pool back up to the ceiling so
    a finite ``crash_recovery_respawn_limit`` cannot permanently strand the
    deferred backlog. Also the retry mechanism for a schema that wasn't
    ready at boot: each tick retries schema init first, and only tops up
    (and marks boot ready) once it succeeds. A single failed tick is logged
    and retried; ``CancelledError`` propagates for clean shutdown.
    """
    while True:
        try:
            await asyncio.sleep(interval)
            if not app.state.schema_ready:
                try:
                    await _ensure_schema_ready()
                except Exception as exc:  # noqa: BLE001 - retried next tick
                    logger.warning(
                        "crash_recovery_sweep: schema still not ready, will retry: %s",
                        exc,
                    )
                else:
                    if app.state.schema_ready:
                        logger.info(
                            "crash_recovery_sweep: schema now ready -- "
                            "draining deferred backlog"
                        )
            # Drainer start stays gated on schema; disk-only work below
            # (expire) does not and must run every tick regardless.
            if app.state.schema_ready:
                result = await _crash_recovery_topup(respawn_limit)
                if result.dispatched:
                    logger.info(
                        "crash_recovery_sweep: dispatched %d recovered session(s) "
                        "(ceiling=%d) -- draining deferred backlog",
                        result.dispatched,
                        respawn_limit,
                    )
                if boot_state.phase == "awaiting_schema":
                    boot_state.finish()
            # Live counters here (unlike boot) must record_purged expired
            # records, or the accepted/written residual latches at +n.
            expire_result = await registry.queue_manager.expire_dead_letters(
                time.time(),
                _settings.dead_letter_retention_seconds,
                _settings.dead_letter_expiry_enabled,
            )
            if expire_result["expired_records"]:
                registry.record_purged(expire_result["expired_records"])
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "crash_recovery_sweep: tick failed, will retry: %s",
                exc,
                exc_info=True,
            )


async def _writer_lease_boot() -> None:
    """The only writer-lease call on the synchronous boot path.

    Only ``WriterLeaseConflict`` (the intended enforce-mode refusal) is
    allowed to escape. The heartbeat task starts whenever
    ``writer_lease_mode != "off"``, even after a failed acquire -- that is
    the re-arm mechanism a transient fault depends on.
    """
    try:
        await writer_lease.acquire(_settings, lambda: registry.queues_dir_path)
    except WriterLeaseConflict:
        raise  # the ONE intended abort (enforce mode only)
    except Exception as exc:  # noqa: BLE001 - a detector must never crash-loop the server
        logger.error(
            "writer_lease: boot acquire failed -- the writer-lease detector "
            "is NOT ARMED for this process: %r",
            exc,
        )
        writer_lease.mark_unarmed(repr(exc), _settings.writer_lease_mode)
    if _settings.writer_lease_mode != "off":
        app.state.lease_task = asyncio.create_task(writer_lease.heartbeat_loop())


async def _boot_reclaim() -> None:
    """Log-then-delete every un-resumable/already-drained key, resume with
    fallback for a recoverable-but-unparseable head, and reset a bounded
    bad-offset key. Skips any key with a live registry worker. Classify
    always runs; the actual unlink/reset only runs when ``reclaim_enabled``.
    """
    qm = registry.queue_manager
    # Module-level `_settings`, not a fresh get_settings() -- keeps this in
    # sync with test monkeypatches bound to the same object.
    settings = _settings
    boot_state.reclaim_enabled = settings.reclaim_enabled
    # Iterate the QueueManager's own directory, not settings.queues_path --
    # the two can differ (tests do this routinely).
    keys = sorted(p.stem for p in qm.queues_dir.glob("*.log"))
    reclaimed = 0
    reclaimed_bytes = 0
    kept = 0
    failed = 0
    for key in keys:
        if registry.has_worker(key):
            kept += 1
            continue
        try:
            c = await qm.classify_session(key, _head_is_resumable)
        except (OSError, ValueError) as exc:  # pragma: no cover -- defence in depth
            logger.error("boot_reclaim_classify_failed session=%s error=%s", key, exc)
            failed += 1
            continue
        if c.verdict.value == "resumable":
            kept += 1
            if c.reason == "fallback_workspace":
                if c.fallback_source == "byte0":
                    boot_state.fallback_workspace_byte0 += 1
                elif c.fallback_source == "sentinel":
                    boot_state.fallback_workspace_sentinel += 1
            continue
        if c.verdict.value == "unreadable":
            failed += 1
            logger.warning("boot_reclaim_kept reason=%s session=%s", c.reason, key)
            continue
        if c.verdict.value == "keep":
            kept += 1
            logger.warning("boot_reclaim_kept reason=%s session=%s", c.reason, key)
            continue
        # verdict in (unresumable, drained, reset_offset): actionable.
        # drained is the same evidence delete_drained already acts on
        # unconditionally at session finalize -- safe to auto-reclaim
        # regardless of reclaim_enabled. unresumable/reset_offset stay
        # gated: they can act on a log whose offset was merely unreadable.
        if c.verdict.value != "drained" and not settings.reclaim_enabled:
            logger.warning(
                "boot_reclaimed reason=%s path=%s session=%s bytes=%d action=dry_run",
                c.reason,
                Path(settings.queues_path) / f"{key}.log",
                key,
                c.size,
            )
            kept += 1
            continue
        ok = await qm.reclaim(c, partial(registry.has_worker, key))
        if ok:
            reclaimed += 1
            reclaimed_bytes += c.size
        else:
            kept += 1
    # reclaim_orphans itself gates on reclaim_enabled and reports only real
    # unlinks (0 when disabled) -- no further gating needed here.
    orphan_result = await qm.reclaim_orphans(_start_time, settings.reclaim_enabled)
    reclaimed += orphan_result["reclaimed"]
    reclaimed_bytes += orphan_result["reclaimed_bytes"]
    failed += orphan_result["failed"]
    boot_state.reclaimed += reclaimed
    boot_state.reclaimed_bytes += reclaimed_bytes
    boot_state.kept += kept
    boot_state.failed += failed
    logger.info(
        "boot_reclaim_summary reclaimed=%d bytes=%d kept=%d failed=%d mode=%s",
        reclaimed,
        reclaimed_bytes,
        kept,
        failed,
        "live" if settings.reclaim_enabled else "dry_run",
    )


async def _phase_run(coro: Any) -> Any:
    """Run one boot-phase awaited call under ``boot_phase_timeout_seconds``.

    A hung mount-touching call (a blocking stat/read on a degraded mount)
    would otherwise leave ``boot_state.phase`` stuck pre-ready forever,
    latching /status's spool/metrics at null. On timeout this raises
    ``TimeoutError`` -- left to propagate to ``_boot_reconcile``'s own
    except-Exception handler, which records it via ``boot_state.fail()``
    exactly like any other phase failure. ``<= 0`` disables the timeout
    (unbounded wait, pre-existing behavior).
    """
    timeout = _settings.boot_phase_timeout_seconds
    if timeout is not None and timeout > 0:
        return await asyncio.wait_for(coro, timeout=timeout)
    return await coro


async def _boot_reconcile() -> None:
    """The backgrounded, exception-safe boot-recovery body.

    Runs schema -> heal -> reclaim -> expire -> reconcile -> seed -> topup ->
    sweep, then phase=ready. Spawned from ``lifespan``, not awaited, so the
    server serves its first request while this still runs. Any exception is
    recorded via ``boot_state.fail()``; the server keeps serving.

    ``schema`` is the one phase gating something real: drainer start
    (``topup``) requires ``app.state.schema_ready``, since the Session/:Node
    uniqueness constraints must be active before any flush() MERGE. The
    disk-only phases (heal/reclaim/expire/reconcile/seed) need no schema and
    run regardless. A schema left not-ready (Neo4j unreachable) is retried
    by the periodic sweep, not by blocking this pass.
    """
    boot_state.begin()
    # Defensive: a direct call (bypassing lifespan's own init) must not
    # AttributeError on the topup-phase read below.
    app.state.schema_ready = getattr(app.state, "schema_ready", False)
    try:
        boot_state.phase = "schema"
        await _ensure_schema_ready()

        boot_state.phase = "heal"
        _heal_result = await _phase_run(registry.queue_manager.heal_torn_tails())
        logger.info("lifespan_startup: heal_torn_tails result=%s", _heal_result)

        boot_state.phase = "reclaim"
        await _phase_run(_boot_reclaim())

        boot_state.phase = "expire"
        # Runs before recovery_seed_counts, so expired lines are simply never
        # counted into accepted_seed -- record_purged must not be called here.
        await _phase_run(
            registry.queue_manager.expire_dead_letters(
                time.time(),
                _settings.dead_letter_retention_seconds,
                _settings.dead_letter_expiry_enabled,
            )
        )

        boot_state.phase = "reconcile"
        await _phase_run(registry.queue_manager.recovery_reconcile_dead())

        boot_state.phase = "seed"
        (
            _accepted_seed,
            _written_seed,
        ) = await _phase_run(registry.queue_manager.recovery_seed_counts())
        registry.seed_counters(_accepted_seed, _written_seed)

        boot_state.phase = "topup"
        respawn_limit = _settings.crash_recovery_respawn_limit
        if app.state.schema_ready:
            result = await _phase_run(_crash_recovery_topup(respawn_limit))
            boot_state.resumed += result.dispatched
            boot_state.deferred += result.deferred
            logger.info(
                "lifespan_startup: crash recovery respawned %d/%d drainers",
                result.dispatched,
                result.recovered,
            )
        else:
            logger.warning(
                "crash_recovery_topup_skipped phase=topup reason=schema_not_ready "
                "-- drainers deferred until Neo4j schema init succeeds "
                "(retried by the periodic sweep)"
            )

        boot_state.phase = "sweep"
        _sweep_interval = _settings.crash_recovery_sweep_interval_seconds
        if respawn_limit is not None and _sweep_interval > 0:
            app.state.sweep_task = asyncio.create_task(
                _crash_recovery_sweep_loop(_sweep_interval, respawn_limit)
            )
            logger.info(
                "crash_recovery_sweep: enabled (interval=%ds, ceiling=%d) -- "
                "deferred backlog will drain progressively, not just on restart",
                _sweep_interval,
                respawn_limit,
            )
        if app.state.schema_ready:
            # Finish unconditionally here (after starting the loop), so a
            # forever-running sweep never leaves phase stuck at "sweep".
            boot_state.finish()
        else:
            # Schema never came up this pass -- stay visibly NOT ready
            # (never silently reported as "ready") until the sweep loop
            # above retries schema + topup and marks it ready itself.
            boot_state.phase = "awaiting_schema"
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # a boot hook must never crash-loop the server
        failed_step = boot_state.phase
        boot_state.fail(failed_step, exc)
        logger.exception(
            "boot_reconcile_failed phase=failed failed_step=%s", failed_step
        )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application lifespan: configure logging and create shared Neo4j driver."""
    setup_logging()
    _admin = _settings.resolve_neo4j_admin()
    _query = _settings.resolve_neo4j_query()
    logger.info(
        "lifespan_startup: creating Neo4j drivers admin_url=%s query_url=%s query_access_mode=%s",
        _admin.url,
        _query.url,
        _query.access_mode,
    )
    # Admin (read/write): schema init + all mutation paths. Shares
    # build_neo4j_driver() with doctor.run_doctor() so the two never diverge.
    app.state.neo4j_driver = build_neo4j_driver(_admin)
    # Cypher-query (read-intent): /cypher + dashboard reads.
    app.state.neo4j_query_driver = AsyncGraphDatabase.driver(
        _query.url, auth=_query.auth
    )
    # Stash the resolved query access_mode so /cypher opens READ sessions without
    # re-resolving settings on every request.
    app.state.neo4j_query_access_mode = _query.access_mode
    # Schema init (indexes + the Session/:Node uniqueness constraints) no
    # longer runs synchronously here -- a Neo4j connectivity failure must
    # never raise out of lifespan (ASGI startup abort -> crash-loop). It now
    # runs as _boot_reconcile's first phase ("schema"), backgrounded like
    # the rest of boot recovery; app.state.schema_ready gates drainer start
    # (_crash_recovery_topup) until it succeeds.
    app.state.schema_ready = False
    # Acquires the lease before any boot-recovery pass mutates the shared
    # directory; awaited synchronously so an enforce-mode refusal can't be
    # silently downgraded by _boot_reconcile's own exception-safety.
    await _writer_lease_boot()
    # Every share-reading recovery pass moves off the critical path to first
    # request: spawned as a background task, not awaited, so /status and
    # /version answer while it still runs.
    boot_state.begin()
    app.state.boot_task = asyncio.create_task(_boot_reconcile())
    try:
        yield
    finally:
        # Ordering is load-bearing: sweep stops before reconcile (it can
        # re-enter its work), and every task stops before the drivers close.
        _sweep_task = getattr(app.state, "sweep_task", None)
        if _sweep_task is not None:
            _sweep_task.cancel()
            with suppress(asyncio.CancelledError):
                await _sweep_task
        _boot_task = getattr(app.state, "boot_task", None)
        if _boot_task is not None:
            _boot_task.cancel()
            with suppress(asyncio.CancelledError):
                await _boot_task
        # Released last; its heartbeat is cancelled first so no in-flight
        # tick can regain the gate mid-shutdown. release() never raises.
        _lease_task = getattr(app.state, "lease_task", None)
        if _lease_task is not None:
            _lease_task.cancel()
            with suppress(asyncio.CancelledError):
                await _lease_task
        await writer_lease.release()
        shutdown_lease_io()
        logger.info("lifespan_shutdown: closing Neo4j drivers")
        await app.state.neo4j_driver.close()
        await app.state.neo4j_query_driver.close()
        # The registry's shared per-session driver is independent of the two
        # above (its own pool, built from settings.resolve_neo4j_admin() the
        # first time a session is created) -- close it here too so no bolt
        # connection outlives the process.
        await registry.close_neo4j_driver()


app = FastAPI(
    title="Context Intelligence Server",
    version=__version__,
    lifespan=lifespan,
    # Headless server: Swagger UI is the dev surface; ReDoc is a redundant
    # second doc UI, intentionally left off.
    docs_url="/docs",
    redoc_url=None,
    openapi_url="/openapi.json",
)
app.include_router(admin_router)
app.include_router(version_router)
app.include_router(queues_router)
_start_time = time.time()
registry = SessionRegistry()
# Expose the registry singleton on app.state so routers can read it without
# importing the module-level name (avoids a circular import).
app.state.registry = registry
idempotency_cache = EventIdempotencyCache()
# Serializes the seen()->append->store() sequence per idempotency_key so
# concurrent same-key requests cannot both durably append (see post_events).
_idempotency_locks = KeyedAsyncLocks()

# Session-less events are keyed by a per-workspace sentinel stem so that events
# from distinct workspaces never collide in one durable log.
_NO_SESSION_PREFIX = "_no_session__"


def _workspace_slug(workspace: str) -> str:
    """Return a filesystem-safe slug for a workspace (session-less log stem)."""
    slug = re.sub(r"[^a-z0-9]+", "-", (workspace or "").lower()).strip("-")
    return slug or "default"


def _validate_data_timestamp(data: dict[str, Any]) -> None:
    """Raise HTTPException(400) if data['timestamp'] is missing, empty, or
    not ISO-8601. This is the ingest boundary check -- reject malformed
    payloads with a clear 400 instead of dead-lettering them later.
    """
    value = data.get("timestamp")
    if value is None or not isinstance(value, str) or not value.strip():
        raise HTTPException(
            status_code=400,
            detail="data.timestamp is required and must be a non-empty ISO-8601 string",
        )
    try:
        datetime.fromisoformat(value)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"data.timestamp must be a valid ISO-8601 string; got {value!r}",
        )


def _assert_admin_not_exempt() -> None:
    """/admin/* must never be in any exempt set.

    Raises ``RuntimeError`` if any ``/admin`` path or prefix appears in
    ``_EXEMPT_PATHS`` or ``_EXEMPT_PREFIXES`` -- a defence-in-depth check
    against accidentally shipping an unauthenticated admin surface.
    """
    import context_intelligence_server.auth as _auth_module

    # Check the exact-path exempt set.
    for path in _auth_module._EXEMPT_PATHS:
        if path == "/admin" or path.startswith("/admin/"):
            raise RuntimeError(
                f"Security invariant violated: /admin path {path!r} found in "
                f"auth._EXEMPT_PATHS.  The /admin surface MUST be "
                f"authenticated — remove it from the exempt set immediately."
            )

    # Check prefix exempt tuple.
    for prefix in _auth_module._EXEMPT_PREFIXES:
        if prefix == "/admin" or prefix.startswith("/admin/") or prefix == "/admin":
            raise RuntimeError(
                f"Security invariant violated: /admin prefix {prefix!r} found in "
                f"auth._EXEMPT_PREFIXES.  The /admin surface MUST be authenticated "
                f"— remove it from the exempt prefix list immediately."
            )


def _assert_neo4j_clients_explicit(settings: Settings) -> None:
    """The deployed profile must declare structured neo4j.admin /
    neo4j.cypher_query clients explicitly. When
    ``neo4j_require_explicit_clients`` is True, refuse to boot on a silent
    fallback to legacy flat neo4j_* fields.
    """
    if settings.neo4j_require_explicit_clients and settings.neo4j is None:
        raise RuntimeError(
            "Neo4j config invariant violated: neo4j_require_explicit_clients=True but "
            "the structured `neo4j` block (admin + cypher_query) is absent — the server "
            "would silently fall back to legacy neo4j_* fields. The deployed profile MUST "
            "declare both clients explicitly (doc 11 §Backward-compatibility). Set the "
            "`neo4j` block in amplifier-online.yaml / server-config.yaml, or unset "
            "neo4j_require_explicit_clients for a dev/transition deploy."
        )


def create_asgi_app(
    settings: Settings | None = None,
    *,
    _jwks_client: Any = None,
) -> BearerTokenMiddleware:
    """Return the ASGI app wrapped with auth middleware.

    *settings* defaults to the module-level ``_settings``; tests pass an
    explicit instance to exercise a config without touching the live cache.
    *_jwks_client* injects a JWKS client for ``auth_mode="entra"`` tests only.

    An empty keystore/identity map is a supported bootstrap state: the
    server boots fail-closed and every request 401/403s until populated via
    the /admin API, unless ``allow_unauthenticated=True`` with no
    credentials configured (wide-open, logged loudly).
    """
    global _api_key_store, _entra_identity_store

    # Structural assertion: runs before middleware construction so the
    # failure is loud and immediate.
    _assert_admin_not_exempt()

    s = settings if settings is not None else _settings
    _assert_neo4j_clients_explicit(s)

    # Reset both stores; the active mode sets exactly one below. app.state.*
    # mirrors the globals so /admin can read them without importing main.
    _api_key_store = None
    _entra_identity_store = None
    app.state.api_key_store = None
    app.state.entra_identity_store = None

    # Store auth/admin config on app.state so dependencies can read it
    # without importing main, and test-specific settings take effect.
    app.state.auth_mode = s.auth_mode
    app.state.admin_api_key_configured = s.resolve_admin_api_key_digest() is not None
    app.state.entra_admin_role = s.entra_admin_role
    # Service-capability role names for require_write / require_read.
    app.state.service_data_role = s.service_data_role
    app.state.reader_role = s.reader_role

    # Admin-key digest for the middleware (static mode only): checked against
    # the bearer token's sha256 before the resolver, so the admin key
    # authenticates even though it isn't in the data keystore.
    admin_api_key_digest: str | None = s.resolve_admin_api_key_digest()
    if s.admin_api_key is not None and s.admin_api_key_sha256 is not None:
        logger.warning(
            "Both admin_api_key and admin_api_key_sha256 are configured; using "
            "admin_api_key_sha256 (digest at rest) and IGNORING the raw "
            "admin_api_key. Remove the raw admin_api_key from your config."
        )
    elif s.admin_api_key is not None:
        logger.warning(
            "admin_api_key is configured as a RAW token, which stores the secret "
            "in plaintext at rest. This is DEPRECATED. Store its SHA-256 digest in "
            "admin_api_key_sha256 instead (see docs/managing-api-keys.md): "
            'python3 -c "import hashlib,sys;print(hashlib.sha256('
            'sys.argv[1].encode()).hexdigest())" "<token>"'
        )

    if s.auth_mode == "entra":
        # Build and load the entra identity store.
        entra_store = IdentityStore(Path(s.entra_identities_store_path))
        entra_store.load()
        if not entra_store.path.exists():
            # First boot: seed from config, converting flat {oid: contributor_id}
            # to the rich {oid: {"id": contributor_id}} format IdentityStore expects.
            config_map = s.build_identity_map()
            if config_map:
                rich_seed = {oid: {"id": cid} for oid, cid in config_map.items()}
                entra_store.seed(rich_seed)
        _entra_identity_store = entra_store
        app.state.entra_identity_store = entra_store

        # Supported bootstrap state, not an error -- without this the empty
        # map would be silent and look like a misconfiguration.
        if not entra_store.flat_dict:
            logger.warning(
                "entra identity map is EMPTY at startup (0 bound oids) — server "
                "is UP and serving, but every delegated (human) token will "
                "receive 403 until identities are onboarded. Bind the first user "
                "with an IdentityAdmin-role token via PUT /admin/identities/{oid} "
                "(store=%s). This is expected on a fresh /data volume.",
                s.entra_identities_store_path,
            )

        # Disjointness invariant: each oid belongs to exactly one identity
        # source. Built here (not inline in EntraResolver) so the overlap can
        # be checked before construction, failing loud at startup.
        _service_id_map = s.build_service_identity_map()
        _entra_oids = set(entra_store.flat_dict.keys())
        _service_oids = set(_service_id_map.keys())
        _overlap = _entra_oids & _service_oids
        if _overlap:
            raise RuntimeError(
                f"Boot invariant violated (B4): oid(s) {sorted(_overlap)!r} appear "
                f"in both entra_identities and service_identities. Each oid must "
                f"belong to exactly one identity source. Fix the config to remove "
                f"the overlap before restarting."
            )

        # EntraResolver raises at construction if the JWKS prefetch fails
        # (fail-closed). Pass the live flat_dict so /admin mutations are
        # visible immediately, no restart required.
        resolver: StaticKeyResolver | EntraResolver = EntraResolver(
            s.azure_client_id,  # type: ignore[arg-type] -- validated non-None by config
            s.azure_tenant_id,  # type: ignore[arg-type] -- validated non-None by config
            entra_store.flat_dict,  # live reference -- mutations visible immediately
            service_identity_map=_service_id_map,  # pre-built, disjointness verified
            service_data_role=s.service_data_role,  # role gate
            reader_role=s.reader_role,  # role gate
            entra_admin_role=s.entra_admin_role,  # role gate
            jwks_client=_jwks_client,
        )
        # Entra mode: admin is via roles claim, not admin_api_key_digest.
        admin_api_key_digest = None
    else:
        # Build and load the API-key store.
        key_store = IdentityStore(Path(s.api_keys_store_path))
        key_store.load()
        if not key_store.path.exists():
            # First boot: seed from config, converting flat {sha256: contributor_id}
            # to the rich {sha256: {"id": contributor_id}} format.
            config_ks = s.build_keystore()
            if config_ks:
                rich_seed = {digest: {"id": cid} for digest, cid in config_ks.items()}
                key_store.seed(rich_seed)
        _api_key_store = key_store
        app.state.api_key_store = key_store

        # Supported bootstrap state (fail-closed, not fail-open): server is
        # up but every request 401s until keys are onboarded.
        if not key_store.flat_dict:
            if s.resolve_admin_api_key_digest() is not None:
                logger.warning(
                    "static keystore is EMPTY at startup (0 bound keys) — server "
                    "is UP but fail-CLOSED; every request will 401 until keys are "
                    "onboarded. Add the first key with the admin token via "
                    "PUT /admin/keys/{sha256hash} (store=%s). Expected on a fresh "
                    "/data volume.",
                    s.api_keys_store_path,
                )
            else:
                logger.warning(
                    "static keystore is EMPTY at startup (0 bound keys) AND no "
                    "admin_api_key/admin_api_key_sha256 is configured — server is "
                    "UP but fail-CLOSED and CANNOT be bootstrapped at runtime (the "
                    "/admin API is unreachable without an admin key: every token "
                    "401s at the middleware before require_admin runs). Set "
                    "admin_api_key/admin_api_key_sha256 to enable runtime "
                    "onboarding, or add api_keys in config and restart. (store=%s)",
                    s.api_keys_store_path,
                )

        # Pass key_store.flat_dict (the LIVE dict) so the resolver sees any
        # put()/delete() made by /admin immediately, no restart required.
        resolver = StaticKeyResolver(key_store.flat_dict)

    # Fires only on the explicit allow_unauthenticated opt-out combined with
    # no credentials configured; an empty store alone boots fail-closed instead.
    if s.allow_unauthenticated and not resolver.auth_enabled:
        logger.warning(
            "allow_unauthenticated=True AND no credentials configured — the "
            "server is WIDE OPEN: EVERY request is admitted UNAUTHENTICATED. "
            "This opt-out is for TEST/DEV ONLY and must NEVER be set in "
            "production. Configure api_key/api_keys (static) or entra_identities "
            "(entra) and unset allow_unauthenticated to enforce authentication."
        )

    # Log admin capability status for operator visibility.
    if s.auth_mode == "static":
        _admin_status = (
            "enabled"
            if s.resolve_admin_api_key_digest() is not None
            else "disabled (admin_api_key/admin_api_key_sha256 not set)"
        )
    else:
        _admin_status = (
            f"enabled (role={s.entra_admin_role!r})"
            if s.entra_admin_role
            else "disabled (entra_admin_role not configured)"
        )
    logger.info(
        "create_asgi_app: auth_mode=%s admin_api=%s",
        s.auth_mode,
        _admin_status,
    )

    # Store the admin-key digest on app.state so the /admin router can read
    # it without importing main; None in entra mode, sha256 in static mode.
    app.state.admin_api_key_digest = admin_api_key_digest

    return BearerTokenMiddleware(
        app,
        resolver=resolver,
        exempt_paths=_EXEMPT_PATHS,
        admin_api_key_digest=admin_api_key_digest,
        allow_unauthenticated=s.allow_unauthenticated,
    )


# Module-level ASGI app used by Gunicorn: context_intelligence_server.main:asgi_app
# Lazily constructed (PEP 562 __getattr__) so bare import / --help / --version
# don't trigger create_asgi_app()'s auth guard; get_asgi_app() builds-and-caches.
_asgi_app: BearerTokenMiddleware | None = None


def get_asgi_app() -> BearerTokenMiddleware:
    """Return the module-level ASGI app, constructing it on first call.

    Internal code must call this rather than referencing a bare ``asgi_app``
    global -- that lookup would not go through ``__getattr__``.
    """
    global _asgi_app
    if _asgi_app is None:
        _asgi_app = create_asgi_app()
    return _asgi_app


def __getattr__(name: str) -> Any:
    """PEP 562 module-level lazy attribute access for ``asgi_app``.

    Only intercepts ``asgi_app`` (the sole lazily-constructed module
    attribute); anything else is a genuine ``AttributeError``, matching
    normal module attribute-access semantics.
    """
    if name == "asgi_app":
        return get_asgi_app()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# require_write, require_read, _is_write_capable live in authz.py (avoids a
# circular import) and are re-exported here for existing imports from main.


@app.get("/status")
async def get_status(request: Request) -> dict[str, Any]:
    response = build_status_response(registry, _start_time)
    response["neo4j_connected"] = await _check_driver_connected(
        request.app, "neo4j_driver"
    )
    # Surface the query (read-intent) driver's connectivity too, so a
    # misconfigured cypher_query client shows up here, not on first /cypher.
    response["neo4j_query_connected"] = await _check_driver_connected(
        request.app, "neo4j_query_driver"
    )
    response["neo4j_url"] = _settings.resolve_neo4j_admin().url
    response["neo4j_browser_url"] = _settings.neo4j_browser_url
    # Gated on boot being OVER (ready or failed), not SUCCEEDED -- gating on
    # `ready` alone would permanently null the spool alarm after any reconcile failure.
    response["boot"] = boot_state.snapshot()
    # Pure in-memory (never touches disk), so this is safe at every boot
    # phase. Kept out of `spool`, which is a live cache dict returned by reference.
    response["writer_lease"] = writer_lease.snapshot()
    if boot_state.phase in ("ready", "failed"):
        # /status is unauthenticated: only aggregate-only conservation
        # metrics, no per-key table or dead-letter listing.
        response["metrics"] = await registry.pipeline_metrics()
        # Same contract: aggregate integers only, cheap (stat-only,
        # short-TTL cached) even with a huge spool.
        response["spool"] = await registry.queue_manager.spool_stats()
    else:
        # While booting, /status performs zero disk reads. metrics/spool stay
        # present but null, so an absent key is never confused with a version skew.
        response["metrics"] = None
        response["spool"] = None
        response["status_detail"] = {"reason": "booting"}
    # Surface auth mode/admin capability so operators can confirm admin is
    # enabled without tailing logs -- boolean flags only, no credentials.
    _auth_mode = getattr(request.app.state, "auth_mode", _settings.auth_mode)
    _admin_key_set = getattr(
        request.app.state,
        "admin_api_key_configured",
        _settings.resolve_admin_api_key_digest() is not None,
    )
    _entra_admin_role = getattr(
        request.app.state, "entra_admin_role", _settings.entra_admin_role
    )
    response["auth"] = {
        "mode": _auth_mode,
        "admin_api_enabled": (
            _admin_key_set if _auth_mode == "static" else bool(_entra_admin_role)
        ),
        # Surface role names (not secrets) so operators can confirm what's
        # configured without exposing credential values.
        **(
            {
                "entra_admin_role": _entra_admin_role,
                "reader_role": getattr(
                    request.app.state, "reader_role", _settings.reader_role
                ),
                "service_data_role": getattr(
                    request.app.state, "service_data_role", _settings.service_data_role
                ),
            }
            if _auth_mode == "entra"
            else {}
        ),
    }
    return response


async def _check_driver_connected(app_instance: FastAPI, attr_name: str) -> bool:
    """Check a Neo4j driver's connectivity via verify_connectivity().

    *attr_name* names the app.state attribute holding the driver -- either
    "neo4j_driver" (admin) or "neo4j_query_driver" (cypher_query). Defensive:
    returns False (never raises, never 500s /status) when the driver is
    absent or verify_connectivity() raises for any reason.
    """
    driver = getattr(app_instance.state, attr_name, None)
    if driver is None:
        return False
    try:
        await driver.verify_connectivity()
        return True
    except Exception:  # noqa: BLE001 -- status must never 500
        return False


@app.post(
    "/events",
    status_code=202,
    response_model=EventResponse,
    dependencies=[Depends(require_write)],
)
async def post_events(
    request: EventRequest, http_request: Request, replay: bool = False
) -> EventResponse:
    # Read contributor_id injected by auth middleware (None when auth not configured).
    contributor_id: str | None = http_request.scope.get("state", {}).get(
        "contributor_id"
    )
    session_id = request.data.get("session_id", "")
    # Validate data.timestamp at the ingest boundary (fail loud, not silent dead-letter).
    # Real Amplifier clients always supply this field; 400 only hits malformed payloads.
    _validate_data_timestamp(request.data)
    # Serialize seen->append->store per key so concurrent same-key requests
    # cannot both append; store only after a successful append.
    dedup_key = request.idempotency_key if not replay else None
    lock_ctx = _idempotency_locks.acquire(dedup_key) if dedup_key else nullcontext()
    async with lock_ctx:
        if dedup_key and idempotency_cache.seen(dedup_key):
            logger.info(
                "event_duplicate_skipped: event=%s session_id=%s",
                request.event,
                session_id,
            )
            return EventResponse(status="duplicate", session_id=session_id or None)
        # Empty session_id maps to a per-workspace sentinel stem so session-less
        # events from distinct workspaces never collide in one log.
        worker_key = session_id or (
            _NO_SESSION_PREFIX + _workspace_slug(request.workspace)
        )
        # Spawn (or reuse) the sticky drainer keyed by worker_key.
        registry.get_or_create(worker_key, request.workspace, created_by=contributor_id)
        # Re-parse raw bytes (not the pydantic model) so client extra fields
        # survive; stamp created_by server-side, overwriting any spoofed value.
        body = await http_request.body()
        body_obj = json.loads(body)
        body_obj["created_by"] = contributor_id  # overwrite, never setdefault
        body = json.dumps(body_obj, separators=(",", ":")).encode()
        await registry.queue_manager.append(worker_key, body)
        # Bytes are on disk: the key may be burned now (a failed append
        # simply never reaches this line -- the lock is still released,
        # via the `async with`, WITHOUT storing).
        if dedup_key:
            idempotency_cache.store(dedup_key)
        registry.record_accepted()  # count the durably-accepted event
        return EventResponse(status="queued", session_id=session_id or None)


@app.get("/blobs/{session_id}", dependencies=[Depends(require_read)])
async def list_blobs(session_id: str) -> JSONResponse:
    blob_store = AsyncDiskBlobStore(root=_settings.blob_path)
    uris = await blob_store.list(session_id)
    return JSONResponse(content={"session_id": session_id, "blobs": uris})


@app.get("/blobs/{session_id}/{key}", dependencies=[Depends(require_read)])
async def get_blob(session_id: str, key: str) -> JSONResponse:
    blob_store = AsyncDiskBlobStore(root=_settings.blob_path)
    uri = f"ci-blob://{session_id}/{key}"
    try:
        content = await blob_store.read(uri)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Blob not found: {uri}")
    return JSONResponse(content=content)


@app.post("/cypher", dependencies=[Depends(require_read)])
async def post_cypher(body: CypherRequest, request: Request) -> Response:
    """Proxy a Cypher query to Neo4j and return the results as JSON."""
    driver = request.app.state.neo4j_query_driver
    access_mode = request.app.state.neo4j_query_access_mode
    params = dict(body.params)
    if body.workspace is not None and body.workspace != "*":
        params["workspace"] = body.workspace
    rows: list[dict] = []
    try:
        async with driver.session(
            default_access_mode=_neo4j_access_const(access_mode)
        ) as session:
            result = await session.run(body.query, params)
            async for record in result:
                rows.append(dict(record))
        serialized = json.dumps({"results": rows}, default=str)
        return Response(content=serialized, media_type="application/json")
    except Exception as exc:  # noqa: BLE001 -- catch all Neo4j and serialization errors
        raise HTTPException(status_code=500, detail=str(exc))


def main(argv: list[str] | None = None) -> None:
    """CLI entrypoint.

    No subcommand (or the explicit ``serve``) starts the ingestion server --
    the systemd unit and macOS launchd agent invoke the bare console script
    with no arguments, dispatching to ``serve``.

    ``doctor [--fix]`` diagnoses (and repairs) Neo4j graph health; see
    ``context_intelligence_server.doctor``.
    """
    parser = argparse.ArgumentParser(prog="context-intelligence-server")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("serve", help="Start the ingestion server (default).")
    doctor_parser = subparsers.add_parser(
        "doctor", help="Diagnose (and optionally repair) Neo4j graph health."
    )
    doctor_parser.add_argument(
        "--fix",
        action="store_true",
        help=(
            "Repair detected issues (duplicate-node dedup + :Node label "
            "backfill) instead of reporting only."
        ),
    )

    args = parser.parse_args(argv)

    if args.command in (None, "serve"):
        run()
        return

    # Deferred import: doctor.py imports build_neo4j_driver back from this
    # module, so a top-level import here would be circular.
    from context_intelligence_server import doctor as _doctor

    sys.exit(asyncio.run(_doctor.run_doctor(fix=args.fix)))


def _effective_worker_count() -> int:
    """Return the worker count gunicorn will actually honor from WEB_CONCURRENCY.

    WEB_CONCURRENCY is gunicorn's own env override for the worker count, so it
    is the single source of truth for how many worker processes will run. Unset
    means 1; a non-integer value is treated as 1 (with a warning) rather than
    crashing on a malformed env var.
    """
    raw = os.environ.get("WEB_CONCURRENCY")
    if raw is None:
        return 1
    try:
        return int(raw)
    except ValueError:
        logger.warning(
            "WEB_CONCURRENCY=%r is not an integer; treating effective workers as 1",
            raw,
        )
        return 1


def _validate_single_worker(workers: int | None = None) -> int:
    """Fail loud unless exactly one worker will run; return that worker count.

    The durable drainer assumes exactly one drainer per session per process, so
    more than one worker process would split a session's drainer across
    processes and reintroduce the loss this design eliminates. When ``workers``
    is None the effective count is read from WEB_CONCURRENCY (the value gunicorn
    honors) so the guard and the live config can never diverge.
    """
    effective = workers if workers is not None else _effective_worker_count()
    if effective != 1:
        raise RuntimeError(
            f"context-intelligence-server requires exactly one worker, got {effective}. "
            "The durable drainer assumes one drainer per session per process; unset "
            "WEB_CONCURRENCY or set WEB_CONCURRENCY=1. Multi-process operation needs a "
            "distributed backend (Open Q7)."
        )
    return effective


def run() -> None:
    """Start the server using gunicorn + uvicorn worker for graceful SIGTERM shutdown."""
    from gunicorn.app.base import BaseApplication

    # Fail loud if WEB_CONCURRENCY would run != 1 worker; the same value
    # feeds gunicorn below so the guard and config can never diverge.
    workers = _validate_single_worker()

    class _App(BaseApplication):
        def load_config(self) -> None:
            for key, value in {
                "bind": f"{_settings.server_host}:{_settings.server_port}",
                "workers": workers,
                "worker_class": "uvicorn.workers.UvicornWorker",
                "timeout": _settings.gunicorn_worker_timeout,
                "graceful_timeout": _settings.gunicorn_graceful_timeout,
                "loglevel": _settings.log_level.lower(),
            }.items():
                self.cfg.set(key, value)

        def load(self) -> Any:
            # get_asgi_app(), not the bare `asgi_app` global -- this is where
            # lazy construction happens and the auth guard still fires.
            return get_asgi_app()

    _App().run()
