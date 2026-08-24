"""Writer-lease DETECTOR -- not a mutex.

Durable append-log framing is correct only while exactly one process writes
the queue directory (per-key serialization is in-process, ``queue_manager.py``).
A rolling/blue-green overlap on a shared mount can silently violate that.

``enforce`` (default): refuses boot against a LIVE foreign lease, takes over a
STALE one. ``detect`` (opt-in): best-effort acquire + heartbeat, latches a
conflict on ``/status``, never refuses to boot. ``off``: disabled.

Honest limits: staleness tolerance is heartbeat * multiplier, not a mutex; a
share fault degrades to "not armed" rather than crash-looping; never
constructs the queue directory (pure path read); all I/O runs on a private
single-thread executor so a hung mount leaks at most one thread; clean
shutdown releases (bounded, best-effort) -- the staleness window is the
backstop only when release itself fails or is skipped (e.g. a crash).
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import os
import socket
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal, Protocol

from context_intelligence_server.lease_store import (
    LeaseRecord,
    LeaseStore,
    create_lease_store,
)
from context_intelligence_server.status import SERVER_VERSION

logger = logging.getLogger("context_intelligence_server")


class WriterLeaseSettings(Protocol):
    """The six fields `WriterLease.acquire` reads.

    Structural (not nominal) on purpose: `acquire()` needs nothing else from
    a settings object, and the real `Settings` model (config.py) satisfies
    this Protocol by construction. Tests may pass any duck-typed stub
    carrying just these six fields without needing the full pydantic
    model."""

    writer_lease_mode: Literal["off", "detect", "enforce"]
    writer_lease_heartbeat_seconds: float
    writer_lease_staleness_multiplier: float
    writer_lease_confirm_delay_seconds: float
    writer_lease_acquire_timeout_seconds: float
    writer_lease_force_acquire: bool


_LEASE_VERSION = 1

# Private, single-thread executor: all lease I/O runs here, never on the
# shared default pool the append/commit path and `spool_stats` also use.
_LEASE_IO = concurrent.futures.ThreadPoolExecutor(
    max_workers=1, thread_name_prefix="writer-lease-io"
)


def shutdown_lease_io() -> None:
    """Shut the private lease-I/O executor down (lifespan's `finally`).

    ``wait=False`` is deliberate: waiting would hang shutdown on exactly the
    hung mount this design bounds. ``cancel_futures=True``
    drops anything still queued (there is at most one slot, so this is at
    most one item).
    """
    _LEASE_IO.shutdown(wait=False, cancel_futures=True)


class WriterLeaseConflict(RuntimeError):
    """Raised ONLY when an `enforce`-mode boot must refuse. The one intended
    abort -- see `_acquire_once`'s `refuse` parameter, which is the ONLY
    place either raise site is reachable from."""


class WriterLeaseBusy(RuntimeError):
    """Raised by `_io()` when a previous lease op has not yet completed (the
    one-slot in-flight gate is closed), or when submitting a new op to the
    private executor itself failed. Handled identically to a filesystem
    fault by every caller -- never a conflict, never wedges anything."""


def _now() -> float:
    return time.time()


class WriterLease:
    """One process's writer-lease detector. Module-level singleton below.

    `__init__` performs NO I/O and reads NO settings -- only
    `uuid.uuid4()` / `os.getpid()` / `socket.gethostname()` / `time.time()`,
    so importing this module (and therefore `main`) stays exactly as cheap
    and side-effect-free as it is today. Every settings-derived field is
    `None` until `acquire()` assigns it, which it does as the FIRST thing it
    does, before any fallible step -- so a prelude
    death can never leave the object half-built.
    """

    def __init__(self) -> None:
        # Identity -- literals only, no I/O.
        self.owner: str = uuid.uuid4().hex
        self.host: str = socket.gethostname()
        self.pid: int = os.getpid()
        self.started_at: float = time.time()

        # Settings-derived state -- None until acquire()'s I/O-free prelude.
        self.mode: str | None = None
        self.heartbeat_seconds: float | None = None
        self.staleness_seconds: float | None = None
        self.force_acquire: bool = False
        self._confirm_delay: float | None = None
        self._acquire_timeout: float | None = None
        self._dir_source: Callable[[], Path] | None = None

        self._store: LeaseStore | None = None

        # Observable state.
        self.acquired: bool = False
        self.ever_acquired: bool = False
        self.conflict: bool = False
        self.conflict_source: str | None = None  # "boot" | "reacquire" | "runtime"
        self.observed_owner: str | None = None
        self.observed_at: float | None = None
        self.took_over_stale: bool = False
        self.superseded_owner: str | None = None
        self.superseded_age_seconds: float | None = None
        self.error: str | None = None
        self.last_renewed: float | None = None

        # The one-slot in-flight gate.
        self._io_inflight: bool = False

    def _build_record(self, heartbeat: float) -> LeaseRecord:
        """The lease record this process would write at *heartbeat* -- pure
        identity, no I/O. The store persists it; the detector owns it."""
        return LeaseRecord(
            owner=self.owner,
            host=self.host,
            pid=self.pid,
            started_at=self.started_at,
            heartbeat=heartbeat,
            revision=os.environ.get("CONTAINER_APP_REVISION"),
            server_version=SERVER_VERSION,
            lease_version=_LEASE_VERSION,
        )

    # -----------------------------------------------------------------
    # The dedicated single-thread executor + one-slot in-flight gate.
    # -----------------------------------------------------------------

    async def _io(self, fn: Callable[[], Any]) -> Any:
        """Run ONE blocking lease op on the private single-thread executor.

        The gate flips False->True synchronously on the event-loop thread
        with no await between the check and the set, so two concurrent
        submissions are unrepresentable. It is cleared by the FUTURE'S
        done-callback -- NOT by the awaiting coroutine -- so a `wait_for`
        cancellation leaves the gate CLOSED until the syscall actually
        returns; that is what makes the <=1-leaked-thread bound exact.

        If `submit` itself raises (a dead/shutdown executor), no future is
        ever created and the done-callback would never fire -- so THIS is
        the one path that clears the gate directly, converting the failure
        into `WriterLeaseBusy` rather than a permanent silent disarm.
        """
        if self._io_inflight:
            raise WriterLeaseBusy("lease I/O still in flight (mount not responding)")
        self._io_inflight = True
        try:
            fut = _LEASE_IO.submit(fn)
        except Exception as exc:
            self._io_inflight = False
            raise WriterLeaseBusy(f"failed to submit lease I/O: {exc!r}") from exc
        fut.add_done_callback(lambda _f: setattr(self, "_io_inflight", False))
        return await asyncio.wrap_future(fut)

    # -----------------------------------------------------------------
    # Boot acquisition
    # -----------------------------------------------------------------

    async def acquire(
        self, settings: WriterLeaseSettings, dir_source: Callable[[], Path]
    ) -> None:
        """Acquire the lease at boot. Raises `WriterLeaseConflict` ONLY in
        `enforce` mode against a fresh foreign (or unreadable) lease, or on
        losing the confirm-handshake race in `enforce` mode. Every other
        fault -- OSError, a hung mount, a busy gate -- is absorbed and
        surfaced via `error`/`conflict`, never raised.
        """
        # I/O-free prelude: attribute reads only. `_dir_source` assigned
        # first so a later prelude failure still leaves a real re-arm source.
        # The store resolves the directory lazily per op, so this constructs
        # nothing and reads no path here.
        self._dir_source = dir_source
        self._store = create_lease_store(dir_source)
        self.mode = settings.writer_lease_mode
        self.heartbeat_seconds = settings.writer_lease_heartbeat_seconds
        self.staleness_seconds = (
            self.heartbeat_seconds * settings.writer_lease_staleness_multiplier
        )
        self.force_acquire = settings.writer_lease_force_acquire
        self._confirm_delay = settings.writer_lease_confirm_delay_seconds
        self._acquire_timeout = settings.writer_lease_acquire_timeout_seconds

        if self.force_acquire:
            # Log on every boot while set so it's never silently forgotten.
            logger.warning(
                "writer_lease: FORCE_ACQUIRE IS ENABLED -- the boot refusal is "
                "disabled for this process. Unset "
                "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_WRITER_LEASE_FORCE_ACQUIRE."
            )

        if self.mode == "off":
            logger.info("writer_lease: mode=off -- detector disabled")
            return

        refuse = self.mode == "enforce" and not self.force_acquire
        await self._acquire_once(refuse=refuse, source="boot")

    async def _acquire_once(self, *, refuse: bool, source: str) -> None:
        """One bounded acquire attempt. Never raises except
        `WriterLeaseConflict` when `refuse=True` (boot's `enforce` path only;
        `tick()`'s re-arm always passes `refuse=False`)."""
        assert self._acquire_timeout is not None
        try:
            await asyncio.wait_for(
                self._acquire_once_inner(refuse=refuse, source=source),
                timeout=self._acquire_timeout,
            )
        except WriterLeaseConflict:
            raise
        except TimeoutError:
            self._apply_fault_policy(
                f"acquire timed out after {self._acquire_timeout}s"
            )
        except (OSError, WriterLeaseBusy) as exc:
            self._apply_fault_policy(repr(exc))

    def _apply_fault_policy(self, error_repr: str) -> None:
        """A share fault or busy gate is EVIDENCE OF NOTHING -- never a
        conflict. Continue boot; the detector is simply unarmed for this
        process until the next tick's retry."""
        logger.error(
            "writer_lease: acquire failed on a filesystem error -- the "
            "writer-lease detector is NOT ARMED for this process: %s",
            error_repr,
        )
        self.acquired = False
        self.error = error_repr
        # self.conflict is deliberately left UNTOUCHED here.

    async def _acquire_once_inner(self, *, refuse: bool, source: str) -> None:
        assert self._dir_source is not None
        assert self.staleness_seconds is not None
        assert self._confirm_delay is not None
        assert self._store is not None

        rec = await self._io(self._store.read)
        if rec is not None and rec.owner != self.owner:
            age = 0.0 if rec.unreadable else (_now() - rec.heartbeat)
            if age < self.staleness_seconds:
                # Fresh (or unreadable, or future-dated) foreign lease.
                if refuse:
                    msg = self._refusal_message(rec, age)
                    logger.error("writer_lease_refused_boot %s", msg)
                    raise WriterLeaseConflict(msg)
                self._latch_conflict(source, rec.owner)
                logger.error(
                    "writer_lease_conflict at boot: taking over a FRESH foreign "
                    "lease owner=%s host=%s pid=%s revision=%s age=%.1fs -- TWO "
                    "WRITERS ARE SHARING THIS DIRECTORY -- concurrent writers "
                    "corrupt the append log",
                    rec.owner,
                    rec.host,
                    rec.pid,
                    rec.revision,
                    age,
                )
            else:
                self.took_over_stale = True
                self.superseded_owner = rec.owner
                self.superseded_age_seconds = age
                logger.warning(
                    "writer_lease: took over a STALE lease owner=%s age=%.1fs",
                    rec.owner,
                    age,
                )

        heartbeat = _now()
        store = self._store
        await self._io(lambda: store.write(self._build_record(heartbeat)))
        await asyncio.sleep(self._confirm_delay)
        rec2 = await self._io(self._store.read)
        if rec2 is None or rec2.owner != self.owner:
            if refuse:
                msg = f"lost the acquire race to owner={rec2.owner if rec2 else None}"
                logger.error("writer_lease_refused_boot %s", msg)
                raise WriterLeaseConflict(msg)
            self._latch_conflict(source, rec2.owner if rec2 else None)
            self.acquired = False
            logger.error(
                "writer_lease_conflict: lost the acquire race to owner=%s",
                rec2.owner if rec2 else None,
            )
            return

        self.acquired = True
        self.ever_acquired = True
        self.last_renewed = heartbeat
        logger.info("writer lease acquired owner=%s", self.owner)

    def _latch_conflict(self, source: str, observed_owner: str | None) -> None:
        self.conflict = True
        self.observed_owner = observed_owner
        self.observed_at = _now()
        # Upgrade ladder: boot/reacquire -> runtime, never back.
        if self.conflict_source != "runtime":
            self.conflict_source = source

    def _refusal_message(self, rec: LeaseRecord, age: float) -> str:
        assert self._dir_source is not None
        assert self.staleness_seconds is not None
        return (
            "Refusing to boot: another writer holds the queue-directory lease.\n"
            f"  dir           = {self._dir_source()}\n"
            f"  foreign owner = {rec.owner} (host={rec.host} pid={rec.pid} "
            f"revision={rec.revision} version={rec.server_version})\n"
            f"  lease age     = {age:.1f}s  (stale after "
            f"{self.staleness_seconds:.1f}s)\n"
            "This server serializes durable appends IN-PROCESS; two processes "
            "writing the same queue directory corrupts it. Wait "
            "for the previous revision to drain and exit, or set "
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_WRITER_LEASE_FORCE_ACQUIRE="
            "true for ONE boot if you are certain the previous writer is gone."
        )

    # -----------------------------------------------------------------
    # Heartbeat / runtime conflict detection
    # -----------------------------------------------------------------

    async def tick(self) -> None:
        """ONE observation + at most one renewal. Never raises (this is what
        lets `heartbeat_loop` stay a thin, supervised wrapper)."""
        if self.mode is None or self.mode == "off":
            return

        if not self.acquired:
            if self.ever_acquired:
                # Held-then-lost: keep reading, but never write again --
                # renewing would ping-pong the lease with the peer.
                await self._observe_only()
                return
            # Never-acquired: nothing to protect, so try again this tick.
            await self._acquire_once(refuse=False, source="reacquire")
            return

        await self._renew_once()

    async def _renew_once(self) -> None:
        """The 'we hold it' branch -- also bounded by the acquire timeout
        so a hung mount gives the heartbeat loop ONE
        bounded WARNING per tick and re-arms, rather than wedging forever
        inside `tick()`."""
        assert self._acquire_timeout is not None
        try:
            await asyncio.wait_for(
                self._renew_once_inner(), timeout=self._acquire_timeout
            )
        except TimeoutError:
            logger.warning(
                "writer_lease: tick timed out after %.1fs -- will retry",
                self._acquire_timeout,
            )
        except (OSError, WriterLeaseBusy) as exc:
            logger.warning("writer_lease: tick failed (%s), will retry", exc)

    async def _renew_once_inner(self) -> None:
        assert self._store is not None
        rec = await self._io(self._store.read)
        if rec is None or rec.owner != self.owner:
            self.conflict = True
            self.conflict_source = "runtime"  # unconditional upgrade
            self.observed_owner = rec.owner if rec else None
            self.observed_at = _now()
            self.acquired = False  # we LOST it; ever_acquired stays True
            logger.error(
                "writer_lease_conflict: lease taken by owner=%s",
                self.observed_owner,
            )
            return
        heartbeat = _now()
        store = self._store
        await self._io(lambda: store.write(self._build_record(heartbeat)))
        self.last_renewed = heartbeat

    async def _observe_only(self) -> None:
        """Held-then-lost: read-only, best-effort, bounded. Never writes."""
        assert self._acquire_timeout is not None
        assert self._store is not None
        try:
            rec = await asyncio.wait_for(
                self._io(self._store.read), timeout=self._acquire_timeout
            )
        except (OSError, WriterLeaseBusy, TimeoutError):
            return
        if rec is not None:
            self.observed_owner = rec.owner
            self.observed_at = _now()

    async def heartbeat_loop(self) -> None:
        """Sleep -> tick -> forever, supervised.

        Interval is read once before the loop; an unset value (prelude never
        ran) is a loud single-shot return rather than an uncapped busy-loop."""
        interval = self.heartbeat_seconds
        if not interval or interval <= 0:
            logger.error(
                "writer_lease: heartbeat loop NOT STARTED -- heartbeat_seconds "
                "is unset (acquire() never completed its prelude). The "
                "writer-lease detector is NOT ARMED for this process."
            )
            self.error = "heartbeat loop not started: heartbeat_seconds unset"
            return
        while True:
            try:
                await asyncio.sleep(interval)
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "writer_lease_heartbeat: tick failed, will retry: %s",
                    exc,
                    exc_info=True,
                )

    # -----------------------------------------------------------------
    # Shutdown
    # -----------------------------------------------------------------

    async def release(self) -> None:
        """Owner-gated, best-effort release. A failed release is not a
        failed shutdown -- the next boot just waits out the staleness window.
        Bounded by the acquire timeout so a hung mount can never block
        shutdown."""
        if self.mode is None or self.mode == "off" or self._store is None:
            return
        store = self._store
        timeout = self._acquire_timeout if self._acquire_timeout is not None else 5.0
        try:
            await asyncio.wait_for(
                self._io(lambda: store.delete_if_owned(self.owner)),
                timeout=timeout,
            )
        except TimeoutError:
            logger.warning("writer_lease: release timed out after %.1fs", timeout)
        except (OSError, WriterLeaseBusy) as exc:
            logger.warning("writer_lease: release failed (best-effort): %s", exc)

    def mark_unarmed(self, error_repr: str, mode: str | None = None) -> None:
        """Called when a fault escapes `acquire()`'s own fault policy.
        `conflict` is left untouched."""
        self.acquired = False
        self.error = error_repr
        if self.mode is None and mode is not None:
            self.mode = mode

    # -----------------------------------------------------------------
    # /status
    # -----------------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """Pure in-memory dict build -- no I/O, so it cannot raise on the
        unauthenticated health path."""
        last_renewed = self.last_renewed
        lease_age = None if last_renewed is None else max(0.0, _now() - last_renewed)
        return {
            "mode": self.mode,
            "acquired": self.acquired,
            "owner": self.owner,
            "conflict": self.conflict,
            "conflict_source": self.conflict_source,
            "observed_owner": self.observed_owner,
            "observed_at": self.observed_at,
            "took_over_stale": self.took_over_stale,
            "superseded_owner": self.superseded_owner,
            "superseded_age_seconds": self.superseded_age_seconds,
            "force_acquire": self.force_acquire,
            "error": self.error,
            "last_renewed": last_renewed,
            "lease_age_seconds": lease_age,
            "heartbeat_seconds": self.heartbeat_seconds,
            "staleness_seconds": self.staleness_seconds,
        }


# Module singleton. No I/O at import, so a bare ASGI test client that never
# runs the real lifespan still gets a coherent, non-lying /status.
writer_lease: WriterLease = WriterLease()
