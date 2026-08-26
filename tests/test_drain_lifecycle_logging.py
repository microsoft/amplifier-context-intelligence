"""Logging-completeness tests: each test asserts that a specific
drain/session-lifecycle event emits exactly one structured log line (right
level, `session=`/`reason=` tokens, session id promoted via `extra`). No
test here asserts on behavior, metrics, or /status -- other files own that.

Harness: reuses the fakes/helpers from tests/test_drain_supervision.py.
No real Neo4j is used anywhere in this file.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import context_intelligence_server.main as main_module
from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.registry import SessionRegistry
from tests.test_drain_supervision import (
    _accumulate,
    _cancel_and_await,
    _FlakyGraph,
    _line,
    _make_worker,
    _pump,
    _start_supervised,
)

pytestmark = pytest.mark.integration

LOGGER_NAME = "context_intelligence_server"


def _has(
    caplog: pytest.LogCaptureFixture,
    *,
    level: int,
    contains: list[str],
    session_id: str | None = None,
) -> bool:
    """True iff some captured record is at ``level`` and its message contains
    every string in ``contains`` (and, if given, carries ``session_id`` via
    the JsonFormatter-promoted ``extra`` field)."""
    for r in caplog.records:
        if r.levelno != level:
            continue
        msg = r.getMessage()
        if not all(token in msg for token in contains):
            continue
        if session_id is not None and getattr(r, "session_id", None) != session_id:
            continue
        return True
    return False


# ---------------------------------------------------------------------------
# drain_worker CANCELLED (two distinct sites)
# ---------------------------------------------------------------------------


class TestG1DrainWorkerCancelled:
    async def test_g1a_cancelled_during_dispatch_logs_info_site_dispatch(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Cancel while inside the inner try (dispatch/flush) -- the
        registry.py inner ``except asyncio.CancelledError`` block."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d3-g1-dispatch"
        graph = _FlakyGraph()
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        started = asyncio.Event()
        release = asyncio.Event()

        async def _blocking_process(
            worker: object, event: str, data: object, handlers: object
        ) -> None:
            started.set()
            await release.wait()  # never set -- cancellation always wins here

        await qm.append(sid, _line("e1", "/ws", {"session_id": sid}))

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                side_effect=_blocking_process,
            ),
            caplog.at_level(logging.INFO, logger=LOGGER_NAME),
        ):
            task = _start_supervised(reg, worker)
            await started.wait()  # deterministic: task is now inside dispatch
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await _pump()

        assert _has(
            caplog,
            level=logging.INFO,
            contains=["drain_worker_cancelled", "site=dispatch"],
            session_id=sid,
        ), [r.getMessage() for r in caplog.records]
        assert worker.store_closed is True
        assert sid not in reg.active_sessions()

    async def test_g1b_cancelled_while_idle_logs_info_site_loop(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Cancel while idle (no data, polling) -- the registry.py OUTER
        ``except asyncio.CancelledError`` block (never the inner one, since
        an empty batch never reaches the dispatch/flush try)."""
        reg = SessionRegistry()
        sid = "d3-g1-loop"
        graph = _FlakyGraph()
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                side_effect=_accumulate,
            ),
            caplog.at_level(logging.INFO, logger=LOGGER_NAME),
        ):
            task = _start_supervised(reg, worker)
            await asyncio.sleep(0)
            await asyncio.sleep(0)  # let it settle into the idle poll
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await _pump()

        assert _has(
            caplog,
            level=logging.INFO,
            contains=["drain_worker_cancelled", "site=loop"],
            session_id=sid,
        ), [r.getMessage() for r in caplog.records]
        assert worker.store_closed is True
        assert sid not in reg.active_sessions()


# ---------------------------------------------------------------------------
# remove() cancelling a still-live drain task
# ---------------------------------------------------------------------------


class TestG2Remove:
    async def test_g2_remove_live_task_logs_info(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        reg = SessionRegistry()
        sid = "d3-g2-remove"
        graph = _FlakyGraph()
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                side_effect=_accumulate,
            ),
            caplog.at_level(logging.INFO, logger=LOGGER_NAME),
        ):
            task = _start_supervised(reg, worker)
            await asyncio.sleep(0)
            assert not task.done(), "the drainer must still be live for this test"
            reg.remove(sid)
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await _pump()

        assert _has(
            caplog,
            level=logging.INFO,
            contains=["drain_worker_remove", "had_live_task=True"],
            session_id=sid,
        ), [r.getMessage() for r in caplog.records]
        assert sid not in reg.active_sessions()


# ---------------------------------------------------------------------------
# start_drain() respawning an already-registered worker
# ---------------------------------------------------------------------------


class TestG3Respawn:
    async def test_g3_start_drain_respawn_logs_info(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A worker whose previous task is done (cancelled, not crashed) is
        respawned: only this case should log ``drainer_respawned``, not the
        brand-new-spawn ``drainer_spawned``."""
        reg = SessionRegistry()
        sid = "d3-g3-respawn"
        graph = _FlakyGraph()
        worker = _make_worker(sid, graph)

        async def _noop() -> None:
            return None

        old_task = asyncio.create_task(_noop())
        await old_task  # done, cancelled() is False, exception() is None
        worker.task = old_task

        with (
            patch(
                "context_intelligence_server.registry.process_event",
                side_effect=_accumulate,
            ),
            caplog.at_level(logging.INFO, logger=LOGGER_NAME),
        ):
            reg.start_drain(worker)
            assert worker.task is not old_task, (
                "start_drain must build a fresh task for a done-but-not-crashed worker"
            )
            new_task = worker.task
            assert new_task is not None
            await _cancel_and_await(new_task)

        assert _has(
            caplog,
            level=logging.INFO,
            contains=["drainer_respawned"],
            session_id=sid,
        ), [r.getMessage() for r in caplog.records]
        # No behavior change: drainer_spawned (the pre-existing brand-new-spawn
        # log) must NOT have fired for this respawn.
        assert not any("drainer_spawned" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# _finalize_session leaving an orphaned worker (two sites)
# ---------------------------------------------------------------------------


class TestG4FinalizeOrphan:
    async def test_g4a_first_pass_tail_flush_failed_logs_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The FIRST _drain_to_eof call inside _finalize_session fails ->
        the recoverable orphan (a respawn/next-drain retries finalize)."""
        reg = SessionRegistry()
        sid = "d3-g4-tail-flush-failed"
        graph = _FlakyGraph()
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        handlers = object()
        with (
            patch.object(reg, "_drain_to_eof", AsyncMock(return_value=False)),
            caplog.at_level(logging.WARNING, logger=LOGGER_NAME),
        ):
            await reg._finalize_session(worker, handlers)

        assert _has(
            caplog,
            level=logging.WARNING,
            contains=[
                "finalize_orphan",
                "reason=tail_flush_failed",
                "recoverable=respawn",
            ],
            session_id=sid,
        ), [r.getMessage() for r in caplog.records]
        # No behavior change: still registered, store not closed (respawn will retry).
        assert sid in reg.active_sessions()
        assert graph.closed is False

    async def test_g4b_delete_retry_exhausted_permanent_orphan_logs_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """delete_drained refuses every attempt and the late-tail re-drain
        also fails -> the permanent-retention orphan."""
        reg = SessionRegistry()
        qm = reg.queue_manager
        sid = "d3-g4-permanent"
        graph = _FlakyGraph()
        worker = _make_worker(sid, graph)
        reg._register_for_test(worker)

        handlers = object()
        with (
            patch.object(reg, "_drain_to_eof", AsyncMock(side_effect=[True, False])),
            patch.object(qm, "delete_drained", AsyncMock(return_value=False)),
            caplog.at_level(logging.WARNING, logger=LOGGER_NAME),
        ):
            await reg._finalize_session(worker, handlers)

        assert _has(
            caplog,
            level=logging.ERROR,
            contains=[
                "finalize_orphan",
                "reason=delete_retry_exhausted",
                "permanent=true",
            ],
            session_id=sid,
        ), [r.getMessage() for r in caplog.records]
        assert sid in reg.active_sessions(), "permanent orphan stays registered"
        assert graph.closed is False


# ---------------------------------------------------------------------------
# QueueManager.dead_letter()'s own write failing
# ---------------------------------------------------------------------------


class TestG5DeadLetterWriteFailure:
    async def test_g5_dead_letter_write_oserror_logs_error_and_reraises(
        self,
        caplog: pytest.LogCaptureFixture,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        qm = QueueManager(queues_dir=tmp_path / "queues")
        sid = "d3-g5-dead-letter"
        injected = OSError(errno.EIO, "Input/output error")
        monkeypatch.setattr(qm, "_write_record", MagicMock(side_effect=injected))

        with (
            caplog.at_level(logging.ERROR, logger=LOGGER_NAME),
            pytest.raises(OSError) as ei,
        ):
            await qm.dead_letter(sid, b"bad-line", "boom")

        assert ei.value is injected, "propagation must be the SAME exception object"
        matches = [
            r
            for r in caplog.records
            if r.levelno == logging.ERROR
            and "dead_letter_write_failed" in r.getMessage()
        ]
        assert matches, [r.getMessage() for r in caplog.records]
        rec = matches[0]
        assert sid in rec.getMessage()
        assert rec.exc_info is not None, "dead_letter_write_failed must carry exc_info"


# ---------------------------------------------------------------------------
# reclaim_orphans()'s two swallowed mtime-stat OSErrors
# ---------------------------------------------------------------------------


class TestG6ReclaimOrphansMtimeStatFailure:
    @pytest.mark.parametrize(
        ("artifact_suffix", "log_token"),
        [
            ("log.torn-123.bin", "torn_sidecar"),
            ("log.compact.tmp", "orphan_compact_tmp"),
        ],
    )
    async def test_mtime_stat_failure_on_reclaim_artifact_logged(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
        artifact_suffix: str,
        log_token: str,
    ) -> None:
        qdir = tmp_path / "queues"
        qdir.mkdir()
        artifact = qdir / f"d3-g6.{artifact_suffix}"
        artifact.write_bytes(b"x")

        orig_stat = Path.stat

        def _fake_stat(self: Path, *a: object, **kw: object) -> object:
            if self.name == artifact.name:
                raise OSError(errno.EIO, "Input/output error")
            return orig_stat(self, *a, **kw)  # type: ignore[misc]

        monkeypatch.setattr(Path, "stat", _fake_stat)
        qm = QueueManager(queues_dir=qdir)

        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            result = await qm.reclaim_orphans(
                before_ts=time.time() + 3600, enabled=False
            )

        assert result["failed"] == 1
        matches = [
            r
            for r in caplog.records
            if r.levelno == logging.ERROR
            and "boot_reclaim_failed" in r.getMessage()
            and log_token in r.getMessage()
            and artifact.name in r.getMessage()
        ]
        assert matches, [r.getMessage() for r in caplog.records]
        assert matches[0].exc_info is not None, (
            f"boot_reclaim_failed ({log_token} mtime stat) must carry exc_info"
        )


# ---------------------------------------------------------------------------
# Crash-recovery topup: recover()/read_batch disagreement
# ---------------------------------------------------------------------------


class TestG8RecoverySkippedEmptyBatch:
    async def test_g8_recover_reports_session_but_read_batch_empty_logs_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        sid = "d3-g8-empty-batch"
        qm = main_module.registry.queue_manager
        with (
            patch.object(qm, "recover", AsyncMock(return_value=[sid])),
            caplog.at_level(logging.WARNING, logger=LOGGER_NAME),
        ):
            result = await main_module._crash_recovery_topup(None)

        assert result.dispatched == 0
        assert any(
            r.levelno == logging.WARNING
            and "recovery_skipped_empty_batch" in r.getMessage()
            and sid in r.getMessage()
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]
