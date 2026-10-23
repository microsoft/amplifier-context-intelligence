"""One server-owned, explicitly confirmed stored-age job per canonical blob root."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Callable, Iterator, TypeVar

from context_intelligence_server import blob_retention as r
from context_intelligence_server.blob_coordination import coordinator_for, wait_filesystem

T = TypeVar("T")
SNAPSHOT = "current-job.json"
TERMINAL = {"completed", "cancelled", "failed", "interrupted"}


class JobConflict(r.RetentionError):
    pass


class JobNotFound(r.RetentionError):
    pass


class RetentionService:
    """Async control plane; all filesystem work uses a dedicated single thread.

    A request disappearing never cancels a confirmed remote job. Only an explicit
    cancel or shutdown stops it, between fully durable admitted batches.
    """

    def __init__(self, path: Path | str):
        # Canonical before creation, so coordinator, lock and snapshot root agree.
        self.path = Path(path).expanduser().resolve()
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="blob-maintenance")
        self.guard = asyncio.Lock()
        self.job: dict | None = None
        self.report: r.Report | None = None
        self.task: asyncio.Task | None = None
        self.cancel_requested = threading.Event()
        self.accepting = True
        self.closed = False
        # Terminal outcome written to the durable snapshot BEFORE it becomes public.
        self._terminal: dict | None = None
        self._lifetime: r.RootLock | None = None
        self._coordinator = None
        self._ready_deadline = 0.0
        self._last_wall = 0

    @classmethod
    async def open(cls, path: Path | str) -> RetentionService:
        service = cls(path)
        try:
            await service._work(service._initialize, coordinated=False)
        except BaseException:
            await service._work(service._release, coordinated=False)
            await asyncio.to_thread(service.executor.shutdown, wait=True)
            raise
        return service

    async def _work(self, operation: Callable[[], T], *, exclusive: bool = False, coordinated: bool = True) -> T:
        def run() -> T:
            if not coordinated:
                return operation()
            assert self._coordinator is not None
            with self._coordinator.lease(exclusive=exclusive):
                return operation()

        future = asyncio.get_running_loop().run_in_executor(self.executor, run)
        # Service tasks are stopped cooperatively, never Task.cancel(). A caller
        # cancelled during initialize/control I/O must still wait for real exit.
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            await asyncio.shield(future)
            raise

    def _initialize(self) -> None:
        self._coordinator = coordinator_for(self.path)
        with self._coordinator.condition:
            if self._coordinator.service is not None:
                raise JobConflict("A retention service already owns this blob root")
            self._coordinator.service = self
        self._lifetime = r.RootLock(self.path, create_root=True)
        self._lifetime.__enter__()
        self.path = self._lifetime.root.path
        self._load()

    def _release(self) -> None:
        if self._lifetime is not None:
            if self._lifetime.fd is not None:
                self._lifetime.__exit__()
            self._lifetime = None
        if self._coordinator is not None:
            with self._coordinator.condition:
                if self._coordinator.service is self:
                    self._coordinator.service = None

    @staticmethod
    @contextmanager
    def _metadata(root: r.Root) -> Iterator[r.Ledger]:
        # Snapshot validation must not trigger hot SQLite rollback before consent.
        ledger = r.Ledger(root)
        try:
            try:
                ledger.fd = os.open(r.METADATA, r._DIR, dir_fd=root.fd)
            except FileNotFoundError:
                yield ledger
                return
            ledger._check()
            yield ledger
        finally:
            ledger.__exit__()

    @staticmethod
    def _validate_foreign_report(value: object, stored_root: str) -> None:
        """Shape-check a foreign terminal snapshot's report before it is ignored.

        Mirrors the fields of ``r.Report`` without depending on the model. The
        stored root is compared as a string only; it is never resolved or opened.
        """
        counters = (
            "files",
            "logical_bytes",
            "allocated_bytes",
            "retained",
            "skipped",
            "changed",
            "errors",
            "deleted",
        )
        recognized = {"root", "cutoff_ns", "recovery_required", "samples", *counters}

        def is_int(item: object) -> bool:
            return isinstance(item, int) and not isinstance(item, bool)

        if (
            not isinstance(value, dict)
            or not set(value) <= recognized
            or value.get("root") != stored_root
            or not is_int(value.get("cutoff_ns"))
            or any(
                name in value and (not is_int(value[name]) or value[name] < 0)
                for name in counters
            )
            or not isinstance(value.get("recovery_required", False), bool)
            or not isinstance(value.get("samples", []), list)
            or not all(isinstance(item, str) for item in value.get("samples", []))
        ):
            raise r.RetentionError("Invalid retention job snapshot")

    def _load(self) -> None:
        with self._coordinator.lease(), r.Root(self.path) as root, self._metadata(root) as ledger:
            if ledger.fd is None:
                return
            try:
                fd = os.open(SNAPSHOT, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=ledger.fd)
            except FileNotFoundError:
                return
            with os.fdopen(fd, "rb") as stream:
                st = os.fstat(stream.fileno())
                if not r._regular(st) or st.st_uid != os.geteuid() or st.st_mode & 0o077 or st.st_size > 65536:
                    raise r.RetentionError("Unsafe retention job snapshot")
                saved = json.load(stream)
            # A snapshot from another canonical root (the blob directory was copied
            # or renamed) may only be finished history. A running/cancelling job
            # there would be resumed against the wrong root, so it is rejected.
            # The stored root is only ever compared as a string: a malformed value
            # must fail closed here, never be treated as "some other root". It is
            # never resolved, opened or required to exist (the old root is gone).
            stored = saved.get("root")
            if (
                not isinstance(stored, str)
                or not stored
                or "\x00" in stored
                or not os.path.isabs(stored)
            ):
                raise r.RetentionError("Invalid retention job snapshot")
            foreign = stored != str(self.path)
            allowed = TERMINAL if foreign else TERMINAL | {"running", "cancelling"}
            if saved.get("state") not in allowed:
                raise r.RetentionError("Invalid retention job snapshot")
            if (saved.get("version") != 1 or saved.get("confirmed") is not True
                    or str(uuid.UUID(saved["id"])) != saved["id"]):
                raise r.RetentionError("Invalid retention job snapshot")
            if foreign:
                # Ignored history must still be a well-formed report of ITS root.
                self._validate_foreign_report(saved.get("report"), stored)
                return
            # Fully validated into locals before anything is published on self.
            report = r.Report(**saved.pop("report"))
            self.report = report
            self.job = saved
            if saved["state"] not in TERMINAL:
                saved["state"] = "interrupted"
                saved["error"] = "Server restarted; deletion was NOT resumed"
                # Persist below only after the read-only connection is closed.
                interrupted = True
            else:
                interrupted = False
        if interrupted:
            with self._coordinator.lease(exclusive=True):
                self._save()

    def _save(self) -> None:
        """Atomic, durable ONE snapshot; called only for confirmed jobs under EX."""
        assert self.job is not None and self.job["confirmed"]
        saved = {**self.job, **(self._terminal or {}), "report": asdict(self.report)}
        body = json.dumps(saved, separators=(",", ":")).encode()
        with r.Root(self.path) as root:
            try:
                os.mkdir(r.METADATA, 0o700, dir_fd=root.fd)
                os.fsync(root.fd)
            except FileExistsError:
                pass
            # Validate metadata without requiring writable ledger recovery.
            with self._metadata(root) as ledger:
                assert ledger.fd is not None
                self._write_snapshot(ledger, body)

    @staticmethod
    def _write_snapshot(ledger: r.Ledger, body: bytes) -> None:
        name = "current-job.tmp"
        assert ledger.fd is not None
        # A prior crash may leave this private regular file. Never follow it.
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=ledger.fd)
        with os.fdopen(fd, "wb") as stream:
            st = os.fstat(stream.fileno())
            if not r._regular(st) or st.st_uid != os.geteuid() or st.st_mode & 0o077:
                raise r.RetentionError("Unsafe retention checkpoint temporary file")
            os.ftruncate(stream.fileno(), 0)
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        ledger._check()
        os.replace(name, SNAPSHOT, src_dir_fd=ledger.fd, dst_dir_fd=ledger.fd)
        os.fsync(ledger.fd)
        ledger._check()

    def status(self, job_id: str | None = None) -> dict:
        if self.job is None or (job_id is not None and self.job["id"] != job_id):
            raise JobNotFound("No matching current blob-prune job")
        return {**self.job, "report": asdict(self.report)}

    def _clock(self) -> None:
        now = time.time_ns()
        if now < self._last_wall:
            raise r.RetentionError("Clock moved backwards; job stopped without widening cutoff")
        self._last_wall = now

    async def preview(self, older_than: str) -> dict:
        async with self.guard:
            if not self.accepting:
                raise JobConflict("Server is shutting down")
            if ((self.task is not None and not self.task.done())
                    or (self.job is not None and self.job["state"] not in TERMINAL)):
                raise JobConflict("Cancel the current preview/job before replacing it")
            started = time.time_ns()
            cutoff = r.cutoff_for(r.duration_ns(older_than), started)
            self._last_wall = started
            self.report = r.Report(str(self.path), cutoff)
            self.job = {
                "version": 1, "id": str(uuid.uuid4()), "root": str(self.path),
                "state": "previewing", "confirmed": False, "older_than": older_than,
                "cutoff_ns": cutoff, "preview_started_ns": started,
                "error": None, "phase": "scan", "windows": 0, "recovery_rows": 0,
                "preview_kind": "live estimate; apply rescans the frozen cutoff",
            }
            self.cancel_requested.clear()
            self.task = asyncio.create_task(self._preview())
            return self.status()

    def _validate_preview(self) -> None:
        try:
            with r.Root(self.path) as root, r.Ledger(root):
                pass
        except sqlite3.OperationalError as exc:
            if getattr(exc, "sqlite_errorcode", None) != sqlite3.SQLITE_READONLY_ROLLBACK:
                raise
            self.report.recovery_required = True

    async def _scan(self, *, applying: bool) -> None:
        cursor = r.ScanCursor()
        while not cursor.done and not self.cancel_requested.is_set():
            self._clock()
            items = await self._work(lambda: r.scan_window(
                self.path, cursor, self.report, self.job["preview_started_ns"],
            ))
            self.job["windows"] += 1
            if applying and items and not self.cancel_requested.is_set():
                await self._batch(items)
            else:
                await asyncio.sleep(0.001)

    async def _preview(self) -> None:
        try:
            await self._work(self._validate_preview)
            await self._scan(applying=False)
            if self.cancel_requested.is_set():
                self.job["state"] = "cancelled"
            else:
                self.job["state"] = "ready"
                self.job["phase"] = "ready"
                self._ready_deadline = time.monotonic() + 900
                self.job["expires_ns"] = time.time_ns() + 900 * 10**9
        except Exception as exc:
            self.job["state"] = "failed"
            self.job["error"] = self._error(exc)

    @staticmethod
    def _error(exc: Exception) -> str:
        # Never include payloads, credentials, or arbitrary DB statements.
        return str(exc)[:512] if isinstance(exc, r.RetentionError) else f"Filesystem/ledger failure ({type(exc).__name__})"

    async def start(self, job_id: str, confirmation: str) -> dict:
        async with self.guard:
            self.status(job_id)
            if not self.accepting or self.job["state"] != "ready":
                raise JobConflict("Only a current ready preview may start")
            if confirmation != "prune":
                raise JobConflict("Exact confirmation 'prune' is required")
            try:
                self._clock()
            except r.RetentionError as exc:
                self.job["state"] = "failed"
                self.job["error"] = str(exc)
                raise JobConflict(str(exc)) from exc
            if time.monotonic() >= self._ready_deadline or time.time_ns() >= self.job["expires_ns"]:
                raise JobConflict("Preview expired; cancel it and request a new preview")
            self.job["confirmed"] = True
            self.job["state"] = "running"
            self.job["phase"] = "checkpoint"
            self.task = asyncio.create_task(self._apply())
            return self.status()

    async def _batch(self, items: list[r.Candidate], *, recovery: bool = False) -> None:
        # Already admitted work finishes before cancellation is honored.
        self._clock()
        def admitted() -> None:
            if not self.cancel_requested.is_set():
                r.apply_batch(self.path, items, self.report, recovery=recovery)
        await self._work(admitted, exclusive=True)
        await self._work(self._save, exclusive=True)
        await asyncio.sleep(0.05)

    async def _apply(self) -> None:
        try:
            # Consent precedes metadata; checkpoint precedes ledger recovery/unlink.
            await self._work(self._save, exclusive=True)
            self.report = r.Report(str(self.path), self.job["cutoff_ns"])

            # Writable open recovers a genuine hot rollback journal only here.
            def initialize_ledger() -> None:
                with r.Root(self.path) as root, r.Ledger(root, writable=True):
                    pass

            await self._work(initialize_ledger, exclusive=True)
            self.job["phase"] = "recovery"
            after = ""
            while not self.cancel_requested.is_set():
                self._clock()
                after, items, count = await self._work(
                    lambda: r.recovery_page(
                        self.path,
                        after,
                        self.job["cutoff_ns"],
                        self.job["preview_started_ns"],
                    )
                )
                self.job["recovery_rows"] += count
                for offset in range(0, len(items), 32):
                    if self.cancel_requested.is_set():
                        break
                    await self._batch(items[offset : offset + 32], recovery=True)
                if count < 512:
                    break
                await asyncio.sleep(0.001)
            self.job["phase"] = "scan"
            await self._scan(applying=True)
            self._clock()
            outcome = {
                "state": "cancelled" if self.cancel_requested.is_set() else "completed",
                "phase": "finished",
            }
        except Exception as exc:
            outcome = {"state": "failed", "error": self._error(exc)}
        # Clients may acknowledge a terminal state, so it becomes public only after
        # its durable checkpoint: stay running (finalizing) while the save is in flight.
        job = self.job
        assert job is not None
        outcome.setdefault("phase", job["phase"])
        job["phase"] = "finalizing"
        self._terminal = outcome
        try:
            await self._work(self._save, exclusive=True)
        except Exception as exc:
            outcome = {
                **outcome,
                "state": "failed",
                "error": f"Checkpoint failed: {self._error(exc)}",
            }
        finally:
            self._terminal = None
        job.update(outcome)

    async def cancel(self, job_id: str) -> dict:
        async with self.guard:
            self.status(job_id)
            # The outcome is already decided while its final checkpoint is in flight.
            if self.job["state"] not in TERMINAL and self._terminal is None:
                self.cancel_requested.set()
                if self.job["state"] == "ready":
                    self.job["state"] = "cancelled"
                else:
                    self.job["state"] = "cancelling"
            return self.status()

    async def stop(self) -> None:
        self.accepting = False
        if self.job is not None:
            await self.cancel(self.job["id"])
        if self.task is not None:
            await asyncio.shield(self.task)

    async def close(self) -> None:
        if self.closed:
            return
        await self.stop()
        await wait_filesystem()
        # Join the real maintenance thread before dropping external exclusion.
        # Drainers have stopped submitting and ordinary I/O has finished, so the
        # ordinary pool cannot be occupied by our lease waiters at this point.
        await asyncio.to_thread(self.executor.shutdown, wait=True)
        await asyncio.to_thread(self._release)
        self.closed = True