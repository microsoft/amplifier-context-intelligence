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
from contextlib import asynccontextmanager, suppress
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
from context_intelligence_server.idempotency import EventIdempotencyCache
from context_intelligence_server.identity_store import IdentityStore
from context_intelligence_server.logging_config import setup_logging
from context_intelligence_server.models import (
    CypherRequest,
    EventRequest,
    EventResponse,
)
from context_intelligence_server.neo4j_store import (
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
    """Construct an AsyncGraphDatabase driver from a resolved Neo4j client config.

    Shared by ``lifespan()`` (the admin driver, on every server boot) and
    ``doctor.run_doctor()`` (the CLI), so the two entry points can never
    construct the connection differently.
    """
    return AsyncGraphDatabase.driver(config.url, auth=config.auth)


# ---------------------------------------------------------------------------
# Module-level live identity-map stores (T3)
#
# Set by create_asgi_app() so the future /admin router can mutate the active
# store without needing to carry a reference through the middleware chain.
# Exactly ONE of these is non-None at any time — whichever mode is active.
# The other is always reset to None so accessors return an unambiguous result.
# ---------------------------------------------------------------------------
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


# The last-resort workspace for a recovered session
# whose head line does NOT resolve a workspace even after the byte-0
# fallback. Dispatching under this sentinel is strictly better than deleting
# the file outright: a drainer spawns, its own dead-letter-and-advance
# isolates the unparseable line, and every event BEHIND it still reaches the
# graph. See ``_head_is_resumable`` / ``_parse_workspace_and_creator`` below.
_RECOVERY_FALLBACK_WORKSPACE = "unknown-recovered"


def _head_is_resumable(raw: bytes) -> bool:
    """Total predicate: does ``raw`` parse to a dict with a workspace?

    Shared by ``_recover_one_session`` (recovery dispatch, via
    ``_parse_workspace_and_creator``) and ``QueueManager.classify_session``
    (boot-safety reclaim, injected as a pure ``Callable[[bytes], bool]`` -- the queue
    must not learn the event schema). ONE definition, two consumers. NEVER
    RAISES: valid-but-non-dict JSON (``123``, ``null``, ``"str"``, ``[]``)
    must not escape as an ``AttributeError`` from ``obj.get(...)`` -- this
    predicate is a total function on the entire boot path.
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

    Extracted from the lifespan startup recovery loop so tests can exercise the
    real parsing/dispatch logic rather than reimplementing it inline.

    The queue-read step is handled by the caller (the lifespan loop or the test)
    so this function is pure -- no I/O, fully synchronous.

    Args:
        sid:            Session id being recovered.
        first_line:     The first raw log line (bytes from QueueManager or str
                        from tests).  ``json.loads`` accepts both.
        get_or_create:  The registry callable -- ``registry.get_or_create`` in
                        production or a spy in tests.
        first_log_line: (optional) the session's BYTE-0 line, read by
                        the caller via ``QueueManager.read_first_line`` --
                        used ONLY when ``first_line`` fails to resolve a
                        workspace. Omitting it (the default) preserves the
                        original behaviour exactly: a bad/torn head line is
                        skipped with no fallback attempted.
        recovered:      Passed straight through to ``get_or_create``
                        so a dispatched-from-recovery worker is exit-eligible
                        once its backlog drains. Defaults to True because
                        every real caller of this function IS the recovery
                        path.

    Returns:
        True  -- drainer was (re)spawned via *get_or_create*.
        False -- session skipped (empty/torn workspace, or malformed JSON
                 line, with no fallback available or all fallbacks exhausted).
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
        # Sentinel: the LAST resort (a structural
        # graph-identity cost, not a cosmetic one). Dispatch anyway: the
        # drainer dead-letters the unparseable head and drains everything
        # behind it, strictly better than destroying that data outright.
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
    """Result of one ``_crash_recovery_topup`` pass (M-2).

    ``dispatched``: sessions DISPATCHED to get_or_create this pass (an upper
    bound on newly-spawned drainers -- get_or_create is idempotent).
    ``recovered``: the total size of ``recover()``'s report THIS pass, before
    any ceiling slicing -- lets the boot path report ``deferred`` to
    ``/status`` without a second ``recover()`` scan (~2.5 GiB on the real
    corpus).
    ``deferred``: ``recovered`` minus how many were actually processed this
    pass (0 when the ceiling is ``None``, i.e. unbounded).
    """

    dispatched: int
    recovered: int
    deferred: int


async def _crash_recovery_topup(respawn_limit: int | None) -> TopupResult:
    """One bounded crash-recovery pass: respawn drainers for up to
    ``respawn_limit`` recovered sessions (all of them when ``None``).

    This is the shared body of the boot-time recovery and the periodic sweep
    (the only recovery-dispatch body -- the former lifespan inline loop was
    a duplicate and has been removed). It is SAFE to call repeatedly on a
    live server because respawn is idempotent -- ``registry.get_or_create``
    returns the existing worker for a session that already has a live
    drainer (no duplicate drainer, no reset). And because ``recover()``
    reports only sessions that still have undrained data, a session drops
    out the moment it finishes, so the number of live RECOVERED drainers
    stays <= ``respawn_limit`` per dispatch pass (live recovered drainers
    are also bounded independently via the drain loop's own dry-exit, not
    this ceiling -- since a drained-out-but-still-registered drainer stays
    alive between passes).

    When the head line does not resolve a workspace, this now tries the
    session's byte-0 line before giving up on it entirely -- the data
    behind an unparseable head is still recoverable (see
    ``_recover_one_session``'s docstring).
    """
    recovered = await registry.queue_manager.recover()
    to_process = recovered if respawn_limit is None else recovered[:respawn_limit]
    deferred_count = (
        0 if respawn_limit is None else max(0, len(recovered) - respawn_limit)
    )
    dispatched = 0
    for sid in to_process:
        try:
            # Guarded HERE, at the boot/sweep call site -- NOT
            # inside read_batch itself, which is also the LIVE drainer's hot
            # path and must stay loud on a real read failure. Only the
            # recovery-dispatch loop needs to degrade past one bad key.
            batch = await registry.queue_manager.read_batch(sid, max_items=1)
        except (OSError, ValueError):
            # Cheap tightening: attach the traceback (was
            # message-only) so a repeating read failure yields a traceback.
            logger.exception("crash_recovery_topup_read_failed session=%s", sid)
            continue
        if not batch.lines:
            # recover() reported this session as having a
            # complete unprocessed line, but read_batch found none --
            # recover()/read_batch disagreement (e.g. a concurrent
            # compaction/drain advanced the offset between the two calls).
            # Not a loss -- the session is simply no longer recoverable by
            # the time this pass reached it -- but a silent `continue` here
            # was indistinguishable from "recover() was simply wrong".
            logger.warning(
                "recovery_skipped_empty_batch session=%s reason=empty_batch",
                sid,
            )
            continue
        # NOTE: _recover_one_session returns True whenever it dispatched to
        # get_or_create, whether or not a drainer already existed (get_or_create
        # is idempotent). So this count is "sessions dispatched this pass", an
        # upper bound on newly-spawned drainers -- fine for an INFO log.
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
        # Loud on purpose (WARNING, not INFO): a deferred backlog must never
        # be a silent, un-discoverable fact -- that silence is exactly what
        # let the 38 GB spool go unnoticed for two days in the incident this
        # guards against. Names the exact counts and the setting to raise.
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


async def _crash_recovery_sweep_loop(interval: int, respawn_limit: int) -> None:
    """Periodically top the recovered-drainer pool back up to the ceiling so a
    finite ``crash_recovery_respawn_limit`` cannot permanently strand the
    deferred backlog (the tail only advances as head sessions finish draining).

    Started (from ``_boot_reconcile``, after the topup) whenever a
    finite ceiling is configured and the interval is > 0. A single failed
    tick must never kill the loop, so the body is guarded (CancelledError
    propagates for clean shutdown; everything else is logged and the loop
    continues).
    """
    while True:
        try:
            await asyncio.sleep(interval)
            result = await _crash_recovery_topup(respawn_limit)
            if result.dispatched:
                logger.info(
                    "crash_recovery_sweep: dispatched %d recovered session(s) "
                    "(ceiling=%d) -- draining deferred backlog",
                    result.dispatched,
                    respawn_limit,
                )
            # Sweep-tick dead-letter
            # expiry. Unlike the boot phase, counters are LIVE here, so the
            # expired records MUST be reported via record_purged -- they
            # were counted into `accepted` at ingest but never `written`;
            # dropping them from disk without dropping them from `accepted`
            # would latch the residual at +n forever.
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
            # Cheap tightening: attach the traceback (was
            # message-only) so a repeating sweep-tick failure yields one.
            logger.warning(
                "crash_recovery_sweep: tick failed, will retry: %s",
                exc,
                exc_info=True,
            )


async def _writer_lease_boot() -> None:
    """The ONLY writer-lease call on the synchronous boot path. Guarded so
    that exactly ONE exception type -- the
    intended `enforce`-mode refusal -- can escape.

    The heartbeat task is created whenever ``writer_lease_mode != "off"``,
    INCLUDING after a failed acquire -- that IS the re-arm mechanism a
    transient fault depends on. ``mark_unarmed`` also fills
    ``writer_lease.mode`` if ``acquire()`` failed before setting it, so
    ``/status`` never shows ``mode: null`` while a lease task is running.
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
    """The reclaim pass -- log-then-delete every un-resumable /
    already-drained key, resume-with-fallback for a recoverable-but-
    unparseable-head key, and reset a bounded bad-offset key.

    Iterates `*.log` STEMS only -- a log-less key (only a
    `.dead.jsonl`) belongs to ``reclaim_orphans``, never classified here.

    Ownership gate: skip any key with a LIVE registry worker, checked
    FRESH on the event loop immediately before classifying -- the registry
    owns live sessions; this pass only ever touches unowned keys.

    Runs classify UNCONDITIONALLY; ``reclaim``/``reclaim_orphans`` (the
    actual unlink/reset) run ONLY when ``reclaim_enabled`` -- the
    documented dry-run-first deploy sequence.
    """
    qm = registry.queue_manager
    # Use the MODULE-LEVEL `_settings` (same object `_boot_reconcile` and
    # every test's `monkeypatch.setattr(main_module._settings, ...)` reads),
    # not a fresh `get_settings()` call -- if anything upstream ever clears
    # the settings lru_cache, a fresh call could return a DIFFERENT object
    # than the one already bound to `_settings`, silently detaching this
    # function from a test's (or an operator's live-reload's) mutation.
    settings = _settings
    boot_state.reclaim_enabled = settings.reclaim_enabled
    # Iterate the QueueManager's OWN directory (qm.queues_dir), not a
    # path recomputed from settings.queues_path -- registry.queue_manager may
    # be pointed at a directory that differs from the live Settings
    # singleton's queues_path (tests do this routinely), and this must never
    # silently scan the wrong directory.
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
        if not settings.reclaim_enabled:
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
    # `reclaim_enabled` MUST reach `reclaim_orphans` itself -- it, not just
    # this function's own telemetry aggregation, gates the actual unlink.
    # Previously, orphan `.offset`/`.offset.tmp` and stale
    # `.torn-*.bin` quarantine sidecars were unlinked unconditionally every
    # boot, invisibly, even under the `reclaim_enabled=False` safety
    # default. `reclaim_orphans` now reports `reclaimed`/`reclaimed_bytes`
    # that ALREADY reflect only real unlinks (0 when disabled), so no
    # further gating is needed here.
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


async def _boot_reconcile() -> None:
    """The backgrounded, EXCEPTION-SAFE boot-recovery body.

    Runs heal -> reclaim -> reconcile -> seed -> bounded topup -> (start the
    sweep, then set phase=ready). Spawned as a task from ``lifespan``
    immediately after the migration-health guard, NOT awaited -- the server
    serves its first request while this still runs. On ANY exception:
    ``boot_state.fail()`` + a loud traceback; the server KEEPS SERVING.
    """
    boot_state.begin()
    try:
        boot_state.phase = "heal"
        _heal_result = await registry.queue_manager.heal_torn_tails()
        logger.info("lifespan_startup: heal_torn_tails result=%s", _heal_result)

        boot_state.phase = "reclaim"
        await _boot_reclaim()

        boot_state.phase = "expire"
        # Boot-phase dead-letter expiry
        # runs BEFORE recovery_reconcile_dead / recovery_seed_counts. Uses
        # dead_letter_expiry_enabled (a SEPARATE flag from reclaim_enabled)
        # -- NOT reclaim_enabled. Deliberately does
        # NOT call registry.record_purged: this runs before
        # recovery_seed_counts, so expired lines are simply never counted
        # into accepted_seed in the first place (recovery_seed_counts
        # derives `dead` from disk). Calling record_purged here would
        # subtract records that were never added, driving the residual
        # negative.
        await registry.queue_manager.expire_dead_letters(
            time.time(),
            _settings.dead_letter_retention_seconds,
            _settings.dead_letter_expiry_enabled,
        )

        boot_state.phase = "reconcile"
        await registry.queue_manager.recovery_reconcile_dead()

        boot_state.phase = "seed"
        (
            _accepted_seed,
            _written_seed,
        ) = await registry.queue_manager.recovery_seed_counts()
        registry.seed_counters(_accepted_seed, _written_seed)

        boot_state.phase = "topup"
        respawn_limit = _settings.crash_recovery_respawn_limit
        result = await _crash_recovery_topup(respawn_limit)
        boot_state.resumed += result.dispatched
        boot_state.deferred += result.deferred
        logger.info(
            "lifespan_startup: crash recovery respawned %d/%d drainers",
            result.dispatched,
            result.recovered,
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
        # C2 (v1.3.1): starting a non-returning sweep loop must NOT leave
        # phase stuck at "sweep" -- unconditionally finish here, AFTER the
        # (forever-running) loop is started.
        boot_state.finish()
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
    # Admin (read/write): schema init + all mutation paths. Keep the existing
    # app.state.neo4j_driver NAME so nothing that reads it silently breaks.
    # build_neo4j_driver() is the SAME helper doctor.run_doctor() uses, so the
    # server and the doctor CLI can never construct this connection differently.
    app.state.neo4j_driver = build_neo4j_driver(_admin)
    # Cypher-query (read-intent): /cypher + dashboard reads.
    app.state.neo4j_query_driver = AsyncGraphDatabase.driver(
        _query.url, auth=_query.auth
    )
    # Stash the resolved query access_mode so /cypher opens READ sessions without
    # re-resolving settings on every request.
    app.state.neo4j_query_access_mode = _query.access_mode
    # Initialize schema (indexes + uniqueness constraints) BEFORE the server starts
    # accepting requests.  This ensures the Session uniqueness constraint is active
    # before any concurrent flush() transactions execute MERGE, which prevents the
    # duplicate-Session-node race condition observed under concurrent upload load.
    logger.info(
        "lifespan_startup: initializing Neo4j schema (indexes + uniqueness constraints)"
    )
    # Cold start FAILS LOUD on schema/data corruption that requires
    # `doctor --fix` -- an un-migrated graph (duplicate legacy nodes OR
    # nodes lacking the universal :Node label). Nothing has been written yet
    # at cold start, so refusing to boot loses no data: this is the safest
    # possible moment to surface an impossible state as an un-missable
    # signal rather than a log line someone greps for later. Contrast with
    # the flush path (Neo4jGraphStore._ensure_schema), which must keep
    # self-healing and never raise (Salil's blocker -- raising there would
    # dead-letter real in-flight activity records). fail_on_data_conflict=True
    # here mirrors run_repair's contract: a :Node constraint data conflict
    # raises a RuntimeError naming `doctor --fix` instead of being logged
    # and swallowed.
    await ensure_neo4j_schema(app.state.neo4j_driver, fail_on_data_conflict=True)
    logger.info("lifespan_startup: Neo4j schema initialized")
    # Fail-loud migration-health guard: duplicate nodes are already caught
    # above by the :Node constraint (fail_on_data_conflict=True); this catches
    # the OTHER un-migrated shape the constraint can't see on its own --
    # nodes that simply lack the :Node label altogether, which violate no
    # constraint and so raise nothing by themselves. O(1) via the counts
    # store (see count_untagged_nodes) -- this must never regress into the
    # AllNodesScan stall PR #67 removed from the write path.
    #
    # A connectivity/probe failure here is NOT the same as "confirmed
    # un-migrated" -- it means graph state could not be determined, not that
    # it was determined to be bad -- so it is logged at DEBUG and swallowed
    # rather than treated as a corruption finding; the flush path's
    # self-heal still covers a genuinely dirty graph once it becomes
    # reachable.
    try:
        untagged = await count_untagged_nodes(app.state.neo4j_driver)
    except Exception as exc:  # noqa: BLE001 - connectivity probe, not a confirmed bad state
        _LOG_MSG = "startup migration-health probe skipped (graph unreachable?): %s"
        logger.debug(_LOG_MSG, exc)
        untagged = 0
    if untagged:
        raise RuntimeError(
            f"Neo4j graph has {untagged} node(s) lacking the :Node label "
            "(un-migrated). Cold start refuses to boot to avoid duplicating "
            "them on write. Run: context-intelligence-server doctor --fix"
        )
    # The writer-lease detector acquires the queue-directory
    # lease BEFORE any boot-recovery pass runs. `_boot_reconcile` mutates
    # the shared directory destructively (heal truncates, reclaim unlinks),
    # so a fresh-foreign-lease refusal (enforce mode) must gate those passes
    # rather than race them. Awaited synchronously here -- NOT part of
    # `_boot_reconcile` -- because that task's own exception-safety would
    # silently downgrade "refuse to boot" into "log a line and boot anyway".
    await _writer_lease_boot()
    # Boot-safety hardening: every SHARE-READING recovery
    # pass -- heal, reclaim, reconcile-dead, seed-counts, the crash-recovery
    # topup, and the periodic sweep -- moves OFF the critical path to first
    # request. `yield` moves up to immediately after the migration-health
    # guard above (M-8); `_boot_reconcile` runs those passes as a single
    # supervised background task, spawned but NOT awaited, so `/status` and
    # `/version` answer while it is still running. The Neo4j schema guard
    # above stays synchronous and fail-loud on purpose (O(1), no share I/O,
    # a deliberate cold-start refusal on an un-migrated graph) -- the
    # boot-recovery pass does not touch it.
    boot_state.begin()
    app.state.boot_task = asyncio.create_task(_boot_reconcile())
    try:
        yield
    finally:
        # Every background task lives on app.state so this
        # `finally` can reach it even if `_boot_reconcile` crashed before
        # creating the sweep task (hence the getattr guard). Ordering is
        # load-bearing: sweep stops before the one-shot reconcile (it can
        # re-enter its work), and EVERY task stops before the drivers
        # close -- a reconcile mid-Neo4j-write must never find a closed
        # driver.
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
        # The writer lease is released LAST among
        # app-owned tasks, and its own heartbeat task is cancelled FIRST --
        # so no in-flight tick can regain the in-flight gate mid-shutdown.
        # release()/shutdown_lease_io() never raise (owner-gated release,
        # OSError + WriterLeaseBusy swallowed inside release()) -- shutdown
        # never raises, though a healthy-but-still-in-flight op can leave
        # the lease on disk even on a clean exit (an honest
        # limit bounded by the staleness window, same as after a SIGKILL).
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


app = FastAPI(
    title="Context Intelligence Server",
    version=__version__,
    lifespan=lifespan,
    # Headless server: no browser-facing UI. The OpenAPI contract + Swagger UI
    # are the developer surface and are always registered; ReDoc is a redundant
    # second doc UI and is intentionally left off (docs_url=None equivalent).
    docs_url="/docs",
    redoc_url=None,
    openapi_url="/openapi.json",
)
app.include_router(admin_router)
app.include_router(version_router)
app.include_router(queues_router)
_start_time = time.time()
registry = SessionRegistry()
# Expose the registry singleton on app.state so routers can read it via
# request.app.state.registry instead of importing the module-level name
# (avoids a circular import between main and the routers package).
app.state.registry = registry
idempotency_cache = EventIdempotencyCache()

# Session-less events are keyed by a per-workspace sentinel stem so that events
# from distinct workspaces never collide in one durable log.
_NO_SESSION_PREFIX = "_no_session__"


def _workspace_slug(workspace: str) -> str:
    """Return a filesystem-safe slug for a workspace (session-less log stem)."""
    slug = re.sub(r"[^a-z0-9]+", "-", (workspace or "").lower()).strip("-")
    return slug or "default"


def _validate_data_timestamp(data: dict[str, Any]) -> None:
    """Raise HTTPException(400) if data['timestamp'] is missing, empty, or not ISO-8601.

    This is the ingest boundary check (Option A). Real Amplifier clients always
    supply data.timestamp (verified: 224,530 events on disk, 0 missing). This
    guard rejects only malformed/hand-rolled payloads with a clear 400, instead
    of accepting them silently and dead-lettering them later when the graph
    drainer calls make_node_id() on an empty string.
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
    """Startup assertion (TB-07): /admin/* must NEVER be in any exempt set.

    Called by ``create_asgi_app`` before constructing the middleware.
    Raises ``RuntimeError`` if any ``/admin`` path or prefix appears in
    ``_EXEMPT_PATHS`` or ``_EXEMPT_PREFIXES``, because that would make the
    admin API accessible without authentication.

    This is a defence-in-depth structural check: it is impossible to
    accidentally ship an unauthenticated admin surface.
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
    """Startup assertion: the deployed profile MUST declare the
    structured neo4j.admin / neo4j.cypher_query clients explicitly.

    When settings.neo4j_require_explicit_clients is True, refuse to boot if the
    server silently fell back to the legacy flat neo4j_* fields (settings.neo4j is
    None). Back-compat fallback is allowed ONLY when the flag is False (dev / test /
    transition). This makes a silent partial-config fallback impossible in the
    deployed profile.
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

    This is the single strategy-selection point.  *settings* defaults to
    the module-level ``_settings`` (the cached production config).  Pass an
    explicit :class:`~context_intelligence_server.config.Settings` instance
    from tests to exercise specific configurations without touching the live
    cached settings.

    *_jwks_client* is an injectable JWKS client used **only** when
    ``auth_mode="entra"`` — intended for tests that need to construct an
    :class:`~context_intelligence_server.auth.EntraResolver` without making
    real network calls.  Production deployments leave it as ``None``; the
    resolver builds a real ``PyJWKClient`` internally.

    Startup behavior on an EMPTY store:
        An empty keystore (static) or empty identity map (entra) NO LONGER
        raises — it is a supported bootstrap state. The server BOOTS
        fail-CLOSED and logs a loud startup WARNING; every request 401/403s
        until the store is populated at runtime via the /admin API. Wide-open
        pass-through is reachable ONLY via the explicit
        ``settings.allow_unauthenticated=True`` opt-out combined with no
        credentials configured, which additionally logs a "WIDE OPEN" warning.

    Raises:
        RuntimeError: (TB-07) When any ``/admin`` path or prefix appears in an
            auth-exempt set.  The admin API surface must never be unguarded.
    """
    global _api_key_store, _entra_identity_store

    # TB-07 structural assertion: /admin must not be in any exempt set.
    # This runs before any middleware construction so the failure is loud and
    # immediate — no request ever reaches an unauthenticated /admin endpoint.
    _assert_admin_not_exempt()

    s = settings if settings is not None else _settings
    _assert_neo4j_clients_explicit(s)

    # Reset both stores; the active mode sets exactly one of them below.
    # app.state.* mirrors the module-level globals so the /admin router can
    # access the live stores via request.app.state without importing from main
    # (which would create a circular import).
    _api_key_store = None
    _entra_identity_store = None
    app.state.api_key_store = None
    app.state.entra_identity_store = None

    # T5: store auth/admin config on app.state so the require_admin dependency
    # can read it without importing from main (avoids circular import) and so
    # test-specific settings (passed via create_asgi_app(settings=...)) take
    # effect without relying on the module-level cached get_settings().
    app.state.auth_mode = s.auth_mode
    app.state.admin_api_key_configured = s.resolve_admin_api_key_digest() is not None
    app.state.entra_admin_role = s.entra_admin_role
    # M2: service capability role names for require_write / require_read deps.
    app.state.service_data_role = s.service_data_role
    app.state.reader_role = s.reader_role

    # Compute the admin-key digest for the middleware (static mode only).
    # The middleware checks the bearer token's sha256 against this digest BEFORE
    # calling the resolver, so the admin key can authenticate even though it is
    # not in the data keystore (ROB F1).
    #
    # Storage-at-rest is resolved by Settings: the RECOMMENDED admin_api_key_sha256
    # (digest at rest) is used verbatim; the legacy raw admin_api_key (DEPRECATED,
    # plaintext at rest) is hashed by the resolver.  Surface the deprecation and
    # precedence as one-time startup warnings so operators can migrate.
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
            # First boot: seed in-process map from config.  Converts the flat
            # {oid -> contributor_id} from build_identity_map() to the rich
            # {oid -> {"id": contributor_id}} format that IdentityStore expects.
            config_map = s.build_identity_map()
            if config_map:
                rich_seed = {oid: {"id": cid} for oid, cid in config_map.items()}
                entra_store.seed(rich_seed)
        _entra_identity_store = entra_store
        app.state.entra_identity_store = entra_store

        # Bootstrap visibility: announce an EMPTY identity map loudly at startup.
        # This is a SUPPORTED state, not an error — the server is up and serving.
        # Delegated (human) tokens will 403 until an IdentityAdmin role-holder
        # binds the first oid via PUT /admin/identities/{oid}. Without this line
        # an empty map would be silent and look like a misconfiguration.
        if not entra_store.flat_dict:
            logger.warning(
                "entra identity map is EMPTY at startup (0 bound oids) — server "
                "is UP and serving, but every delegated (human) token will "
                "receive 403 until identities are onboarded. Bind the first user "
                "with an IdentityAdmin-role token via PUT /admin/identities/{oid} "
                "(store=%s). This is expected on a fresh /data volume.",
                s.entra_identities_store_path,
            )

        # B4: boot disjointness invariant — each oid must belong to exactly one
        # identity source.  Building the service map here (not inline in the
        # EntraResolver call) lets us check the overlap BEFORE construction so
        # the server fails loudly at startup rather than silently misbehaving.
        # This is cheap hygiene: B1 already keeps app tokens off the human map
        # at request time; this prevents a same-oid-in-both misconfiguration.
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

        # EntraResolver raises RuntimeError at construction if the JWKS
        # prefetch fails (eager fail-closed guard from §8b / crusty gate).
        # Pass entra_store.flat_dict (the LIVE dict) so the resolver sees
        # any put()/delete() made by /admin immediately, no restart required.
        resolver: StaticKeyResolver | EntraResolver = EntraResolver(
            s.azure_client_id,  # type: ignore[arg-type]  — validated non-None by config
            s.azure_tenant_id,  # type: ignore[arg-type]  — validated non-None by config
            entra_store.flat_dict,  # live reference — mutations visible immediately
            service_identity_map=_service_id_map,  # B4: pre-built, disjointness verified
            service_data_role=s.service_data_role,  # M2: role gate
            reader_role=s.reader_role,  # M2: role gate
            entra_admin_role=s.entra_admin_role,  # M2: role gate
            jwks_client=_jwks_client,
        )
        # Entra mode does not use admin_api_key_digest (admin via roles claim).
        admin_api_key_digest = None
    else:
        # Build and load the API-key store.
        key_store = IdentityStore(Path(s.api_keys_store_path))
        key_store.load()
        if not key_store.path.exists():
            # First boot: seed from config.  Converts the flat
            # {sha256_hex -> contributor_id} from build_keystore() to the
            # rich {sha256_hex -> {"id": contributor_id}} format.
            config_ks = s.build_keystore()
            if config_ks:
                rich_seed = {digest: {"id": cid} for digest, cid in config_ks.items()}
                key_store.seed(rich_seed)
        _api_key_store = key_store
        app.state.api_key_store = key_store

        # Bootstrap visibility: announce an EMPTY keystore loudly at startup.
        # This is a SUPPORTED state (fail-CLOSED, not fail-open) — the server
        # is up and serving, but every request 401s until keys are onboarded.
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

    # Wide-open warning: fires ONLY on the explicit allow_unauthenticated
    # opt-out combined with no credentials configured. An empty keystore/map
    # ALONE no longer triggers this (and no longer refuses to start) — it now
    # boots fail-closed instead (see the empty-map/keystore warnings above).
    if s.allow_unauthenticated and not resolver.auth_enabled:
        logger.warning(
            "allow_unauthenticated=True AND no credentials configured — the "
            "server is WIDE OPEN: EVERY request is admitted UNAUTHENTICATED. "
            "This opt-out is for TEST/DEV ONLY and must NEVER be set in "
            "production. Configure api_key/api_keys (static) or entra_identities "
            "(entra) and unset allow_unauthenticated to enforce authentication."
        )

    # Log admin capability status for operator visibility (E: status surfacing).
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

    # T6: store the admin-key digest on app.state so the /admin router handlers
    # can read it without importing from main (no circular import) and so that
    # test-specific settings are honoured.  In entra mode admin_api_key_digest
    # has already been set to None above (line ~385); in static mode it is the
    # sha256 of admin_api_key (or None when admin_api_key is not configured).
    app.state.admin_api_key_digest = admin_api_key_digest

    return BearerTokenMiddleware(
        app,
        resolver=resolver,
        exempt_paths=_EXEMPT_PATHS,
        admin_api_key_digest=admin_api_key_digest,
        allow_unauthenticated=s.allow_unauthenticated,
    )


# Module-level ASGI app used by Gunicorn: context_intelligence_server.main:asgi_app
# The raw `app` is kept for internal use and testing against un-authed routes.
#
# LAZY construction (PEP 562 module __getattr__), NOT built at import time.
#
# create_asgi_app() enforces the auth guard: it raises RuntimeError when no
# authentication is configured at all (see its docstring / _assert_* helpers).
# That guard is correct and must NOT be weakened. The problem was *timing*:
# this module used to call create_asgi_app() unconditionally at import time,
# which meant the console-script entry point (`context-intelligence-server`)
# imports `main` to reach `main()`, so even `--help`/`--version` constructed
# the whole ASGI app and hit the guard. An operator with a broken/absent
# config couldn't ask the binary what version it was -- exactly when they
# most need to.
#
# `_asgi_app` is the cache; `get_asgi_app()` builds-and-caches on first call;
# `__getattr__` makes `context_intelligence_server.main.asgi_app` /
# `from context_intelligence_server.main import asgi_app` keep working for
# anything that reads the module attribute directly (gunicorn's `load()`,
# tests) -- construction (and therefore the auth guard) now happens on first
# access instead of at import time. Actually serving (`run()` -> `_App.load()`
# -> `get_asgi_app()`) still triggers it, so an unconfigured server still
# fails loud exactly as before -- only bare import / --help / --version are
# spared.
_asgi_app: BearerTokenMiddleware | None = None


def get_asgi_app() -> BearerTokenMiddleware:
    """Return the module-level ASGI app, constructing it on first call.

    This is the single lazy-construction point. Internal code (``_App.load()``
    below) MUST call this function rather than referencing a bare ``asgi_app``
    global -- a bare name reference is a normal global-variable lookup and
    would NOT go through ``__getattr__``, so it would raise ``NameError``
    once the unconditional module-level assignment is removed.
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


# ---------------------------------------------------------------------------
# M2 — service capability dependencies (moved to authz.py to avoid circular import)
#
# require_write, require_read, _is_write_capable are imported from
# context_intelligence_server.authz at the top of this file (re-exported here
# so tests and existing imports from main still work).
# ---------------------------------------------------------------------------


@app.get("/status")
async def get_status(request: Request) -> dict[str, Any]:
    response = build_status_response(registry, _start_time)
    response["neo4j_connected"] = await _check_driver_connected(
        request.app, "neo4j_driver"
    )
    # Additive (Concern B, council review): surface the query (read-intent)
    # driver's connectivity too, so a misconfigured cypher_query client shows
    # up here instead of on the first /cypher call.
    response["neo4j_query_connected"] = await _check_driver_connected(
        request.app, "neo4j_query_driver"
    )
    response["neo4j_url"] = _settings.resolve_neo4j_admin().url
    response["neo4j_browser_url"] = _settings.neo4j_browser_url
    # M-16: the boot-progress block is
    # ALWAYS present. Gate on boot IS OVER (ready or failed), not boot
    # SUCCEEDED -- `failed` is a terminal phase and the server keeps
    # ingesting new events, so gating on `ready` alone would permanently
    # null the spool-byte alarm for the rest of the process's life on any
    # reconcile failure, reintroducing the exact "38 GB spool grew with
    # zero signal" incident through a new door.
    response["boot"] = boot_state.snapshot()
    # The writer-lease detector is an ALWAYS-present top-level
    # block, at every boot phase -- pure in-memory (writer_lease.snapshot()
    # never touches disk or calls get_settings()), so this never violates
    # the zero-disk-during-boot contract. Deliberately NOT projected into
    # `spool`: that dict is spool_stats()'s live cache OBJECT
    # returned BY REFERENCE, and mutating it would corrupt a shared,
    # already-verified surface.
    response["writer_lease"] = writer_lease.snapshot()
    if boot_state.phase in ("ready", "failed"):
        # Byte-for-byte today's code, unreachable while actively booting.
        # Additive, aggregate-only conservation metrics. /status is
        # unauthenticated, so this block must NOT carry the per-key table or
        # the dead-letter listing — both are authenticated-only.
        response["metrics"] = await registry.pipeline_metrics()
        # Additive, aggregate-only spool footprint (incident: a 38 GB /
        # 583-file durable spool grew completely unnoticed -- the only
        # symptom was a graph that had silently stopped updating). Same
        # /status contract as `metrics` above: two aggregate integers only,
        # no session ids, no workspace names, no per-key table. Cheap by
        # construction (stat-only, short-TTL cached) -- see
        # QueueManager.spool_stats() for why this is safe on every poll even
        # with a huge spool.
        response["spool"] = await registry.queue_manager.spool_stats()
    else:
        # While actively booting (heal/reclaim/reconcile/seed/topup/
        # sweep-not-yet-ready), `/status` is LEAN by construction -- it
        # performs ZERO disk reads on this request path (neither
        # `pipeline_metrics`/`derive_all_stats` nor `spool_stats` is called
        # at all; `build_status_response` above reads only in-memory
        # registry state). Less data during boot is the accepted trade
        # (user directive): richer boot-time status is deferred to a future
        # maintenance mode. `metrics`/`spool` stay PRESENT and `null` rather
        # than absent, so an absent key is never confused with a version
        # skew.
        response["metrics"] = None
        response["spool"] = None
        response["status_detail"] = {"reason": "booting"}
    # T5 (E): surface auth mode and admin-API capability so operators can
    # confirm admin is enabled without tailing startup logs.  /status is
    # unauthenticated — only config-level boolean flags are exposed here
    # (no credential values, no key hashes, no token details).
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
        # Surface the role names (not secrets) so operators can confirm which
        # roles are configured without exposing credential values.  Additive:
        # existing fields (mode, admin_api_enabled, entra_admin_role) are
        # unchanged; reader_role and service_data_role are new in M2.
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
    # Idempotency. The CHECK stays BEFORE the durable append -- a genuine
    # duplicate must not persist a second log line. The STORE moved to AFTER a
    # successful append, so a key is never burned for an event that is not on
    # disk; a failed append leaves the key unburned and the client's retry is
    # honoured. ``dedup_key`` is None on the replay path, which (as before)
    # neither checks NOR stores.
    dedup_key = request.idempotency_key if not replay else None
    if dedup_key and idempotency_cache.seen(dedup_key):
        logger.info(
            "event_duplicate_skipped: event=%s session_id=%s",
            request.event,
            session_id,
        )
        return EventResponse(status="duplicate", session_id=session_id or None)
    # Empty session_id maps to a per-workspace sentinel stem so session-less
    # events from distinct workspaces never collide in one log.
    worker_key = session_id or (_NO_SESSION_PREFIX + _workspace_slug(request.workspace))
    # Spawn (or reuse) the sticky drainer keyed by worker_key.
    registry.get_or_create(worker_key, request.workspace, created_by=contributor_id)
    # Re-parse the raw validated body bytes, stamp created_by (server-assigned,
    # unconditional overwrite — kills any client-supplied spoofed value), then
    # re-serialize compact JSON before persisting to the durable queue.
    # IMPORTANT: re-parse raw bytes (not the pydantic model) so client extra
    # fields are preserved. body() is cached by Starlette after the first read.
    body = await http_request.body()
    body_obj = json.loads(body)
    body_obj["created_by"] = contributor_id  # overwrite, never setdefault
    body = json.dumps(body_obj, separators=(",", ":")).encode()
    await registry.queue_manager.append(worker_key, body)
    # The bytes are on disk: NOW the key may be burned. No try/except is
    # needed anywhere -- "release on failure" is simply not reaching this line.
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

    INVARIANT: no subcommand (or the explicit ``serve`` subcommand) starts the
    ingestion server. This MUST hold because the systemd unit (and the
    macOS launchd agent) invoke the bare console script
    ``context-intelligence-server`` with NO arguments -- that call dispatches
    to ``serve`` unchanged.

    ``doctor [--fix]`` diagnoses (and, with ``--fix``, repairs) Neo4j graph
    health -- the two O(graph-size) migration scans (dedup + :Node backfill)
    that used to run unconditionally at cold start now live ONLY here, never
    on server boot. See ``context_intelligence_server.doctor``.
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
    # module, so importing it at module load time (rather than here, inside
    # main()) would be a circular import at import time. By the time main()
    # runs, this module has already finished executing top-to-bottom.
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

    # Read WEB_CONCURRENCY and fail loud if it would run != 1 worker. The same
    # value is fed into gunicorn below so the guard and the live config are one
    # source of truth (they can never diverge).
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
            # get_asgi_app() (not the bare `asgi_app` global) -- this is
            # where lazy construction actually happens for a real serve,
            # and where the auth guard still fires if unconfigured.
            return get_asgi_app()

    _App().run()
