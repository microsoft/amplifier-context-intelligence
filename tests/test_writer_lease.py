"""Writer-lease detector tests. No real Neo4j is used anywhere in this file.

The detector is a DETECTOR, not a mutex: it acquires a `.writer.lease`
sibling before boot recovery runs; `detect` mode latches + surfaces
conflicts without ever refusing boot; `enforce` mode additionally refuses a
fresh foreign lease. It never constructs a QueueManager, never crash-loops
on a share fault or hung mount, and bounds lease I/O to a private executor
so a stalled detector can never starve the append/commit path.
"""

from __future__ import annotations

import asyncio
import errno
import json
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

import context_intelligence_server.main as main_module
from context_intelligence_server.config import Settings
from context_intelligence_server.main import lifespan
from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.status import boot_state
from context_intelligence_server.writer_lease import (
    WriterLease,
    WriterLeaseBusy,
    WriterLeaseConflict,
)

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class _LeaseSettings:
    """A minimal duck-typed settings stub carrying only the six fields
    ``WriterLease.acquire`` reads. Isolates the lease-mechanics tests from
    the full ``Settings`` model (which is exercised directly by the config
    validator test at the bottom of this file)."""

    writer_lease_mode: Literal["off", "detect", "enforce"] = "detect"
    writer_lease_heartbeat_seconds: float = 5.0
    writer_lease_staleness_multiplier: float = 3.0
    writer_lease_confirm_delay_seconds: float = 0.0
    writer_lease_acquire_timeout_seconds: float = 5.0
    writer_lease_force_acquire: bool = False


def _settings(**overrides: object) -> _LeaseSettings:
    return _LeaseSettings(**overrides)  # type: ignore[arg-type]


def _read_lease(directory: Path) -> dict:
    return json.loads((directory / ".writer.lease").read_text(encoding="utf-8"))


async def _clear_app_task(name: str) -> None:
    if hasattr(main_module.app.state, name):
        delattr(main_module.app.state, name)


class _NoOpGraph:
    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None

    def discard_buffer(self) -> None:
        return None


def _reset_lease(lease: WriterLease) -> None:
    """Reset a WriterLease instance's fields to fresh-`__init__` values IN
    PLACE (never replaces the object identity, since ``main.py`` imported
    ``writer_lease`` by name at module-import time)."""
    fresh = WriterLease()
    for name in vars(fresh):
        setattr(lease, name, getattr(fresh, name))


@pytest.fixture(autouse=True)
def _isolate_module_singleton():
    """The `main_module.writer_lease` singleton is process-wide, so tests
    that exercise it directly (rather than a locally-constructed
    `WriterLease()`) must not leak state into each other."""
    _reset_lease(main_module.writer_lease)
    yield
    _reset_lease(main_module.writer_lease)


# `_restore_lease_io` (the process-wide `_LEASE_IO` executor guard) now lives
# in tests/conftest.py as an autouse fixture, so it protects every test
# module regardless of collection order -- not just this one.


async def _drive_lifespan(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[MagicMock, AsyncMock]:
    """Return the patches needed to drive the real ``lifespan()`` without a
    real Neo4j -- mirrors ``tests/test_boot_safety.py``'s own
    pattern exactly, so this file's lifespan integration tests use the SAME
    boot-driving convention the boot-safety suite already established."""
    mock_driver = MagicMock()
    mock_driver.close = AsyncMock()
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", 0)
    monkeypatch.setattr(
        main_module._settings, "crash_recovery_sweep_interval_seconds", 0
    )
    return mock_driver, AsyncMock()


# ---------------------------------------------------------------------------
# (a) Clean boot acquires
# ---------------------------------------------------------------------------


async def test_clean_boot_acquires(tmp_path: Path) -> None:
    lease = WriterLease()
    await lease.acquire(_settings(), lambda: tmp_path)

    assert lease.acquired is True
    assert lease.ever_acquired is True
    assert lease.conflict is False
    assert lease.error is None

    path = tmp_path / ".writer.lease"
    assert path.exists()
    text = path.read_text(encoding="utf-8")
    assert text.endswith("\n")
    assert text.count("\n") == 1  # ONE newline-terminated line

    record = json.loads(text)
    assert record["lease_version"] == 1
    assert record["owner"] == lease.owner
    assert abs(record["heartbeat"] - time.time()) < 2.0
    assert record["host"]
    assert isinstance(record["pid"], int) and record["pid"] > 0
    assert record["server_version"]


# ---------------------------------------------------------------------------
# (b) DETECT never refuses -- even against a FRESH foreign lease
# ---------------------------------------------------------------------------


async def test_detect_never_refuses_fresh_foreign_lease(tmp_path: Path) -> None:
    """An unconditional raise here would deadlock every rolling deploy --
    `detect` mode must acquire over a fresh foreign lease instead."""
    peer = WriterLease()
    await peer.acquire(_settings(), lambda: tmp_path)

    lease = WriterLease()
    # Must NOT raise -- this is the deadlock guard.
    await lease.acquire(_settings(writer_lease_mode="detect"), lambda: tmp_path)

    assert lease.acquired is True
    assert lease.conflict is True
    assert lease.conflict_source == "boot"
    assert lease.observed_owner == peer.owner

    # The deliberate take-over: on-disk owner is now us.
    record = _read_lease(tmp_path)
    assert record["owner"] == lease.owner


# ---------------------------------------------------------------------------
# (c) ENFORCE refuses against a fresh foreign lease; boot_task never created
# ---------------------------------------------------------------------------


async def test_enforce_refuses_fresh_foreign_lease(tmp_path: Path) -> None:
    peer = WriterLease()
    await peer.acquire(_settings(), lambda: tmp_path)

    lease = WriterLease()
    with pytest.raises(WriterLeaseConflict) as ei:
        await lease.acquire(_settings(writer_lease_mode="enforce"), lambda: tmp_path)
    assert peer.owner in str(ei.value)

    # A refusal must not overwrite the on-disk owner.
    record = _read_lease(tmp_path)
    assert record["owner"] == peer.owner


async def test_enforce_refusal_aborts_real_startup_before_boot_task(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    queues_dir = tmp_path / "queues"
    queues_dir.mkdir()
    peer = WriterLease()
    await peer.acquire(_settings(), lambda: queues_dir)

    monkeypatch.setattr(
        "context_intelligence_server.registry.get_settings",
        lambda: type(
            "S",
            (),
            {
                "queues_path": str(queues_dir),
                "write_concurrency": 8,
                "max_delivery_attempts": 5,
            },
        )(),
    )
    monkeypatch.setattr(main_module._settings, "writer_lease_mode", "enforce")
    main_module.registry._queue_manager = None
    await _clear_app_task("boot_task")
    await _clear_app_task("lease_task")

    mock_driver = MagicMock()
    mock_driver.close = AsyncMock()
    with (
        patch("context_intelligence_server.main.setup_logging"),
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ),
        patch("context_intelligence_server.main.ensure_neo4j_schema", new=AsyncMock()),
        pytest.raises(WriterLeaseConflict),
    ):
        async with lifespan(main_module.app):
            pytest.fail("lifespan must not reach yield in enforce+fresh-foreign")

    assert not hasattr(main_module.app.state, "boot_task")


# ---------------------------------------------------------------------------
# (d) Acquire over a STALE lease; took_over_stale surfaced
# ---------------------------------------------------------------------------


async def test_acquire_over_stale_lease_takes_over_and_latches(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    peer = WriterLease()
    await peer.acquire(_settings(), lambda: tmp_path)
    # Rewrite the peer's heartbeat far in the past (data-driven staleness --
    # no sleep-and-hope).
    stale_record = _read_lease(tmp_path)
    stale_record["heartbeat"] = time.time() - 3600
    (tmp_path / ".writer.lease").write_text(
        json.dumps(stale_record) + "\n", encoding="utf-8"
    )

    lease = WriterLease()
    with caplog.at_level("WARNING"):
        await lease.acquire(_settings(), lambda: tmp_path)

    assert lease.acquired is True
    assert lease.took_over_stale is True
    assert lease.superseded_owner == peer.owner
    assert lease.superseded_age_seconds is not None
    assert lease.superseded_age_seconds > lease.staleness_seconds  # type: ignore[operator]
    assert any("STALE" in r.message for r in caplog.records)

    record = _read_lease(tmp_path)
    assert record["owner"] == lease.owner


async def test_took_over_stale_surfaces_on_status(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    lease = main_module.writer_lease
    peer = WriterLease()
    await peer.acquire(_settings(), lambda: tmp_path)
    stale_record = _read_lease(tmp_path)
    stale_record["heartbeat"] = time.time() - 3600
    (tmp_path / ".writer.lease").write_text(
        json.dumps(stale_record) + "\n", encoding="utf-8"
    )

    await lease.acquire(_settings(), lambda: tmp_path)
    assert lease.took_over_stale is True

    boot_state.phase = "ready"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main_module.app), base_url="http://test"
    ) as client:
        response = await client.get("/status")
    body = response.json()
    assert body["writer_lease"]["took_over_stale"] is True
    assert body["writer_lease"]["superseded_owner"] == peer.owner


# ---------------------------------------------------------------------------
# (e) Heartbeat renews
# ---------------------------------------------------------------------------


async def test_heartbeat_renews(tmp_path: Path) -> None:
    lease = WriterLease()
    await lease.acquire(_settings(), lambda: tmp_path)
    h0 = _read_lease(tmp_path)["heartbeat"]

    await asyncio.sleep(0.01)
    await lease.tick()

    h1 = _read_lease(tmp_path)["heartbeat"]
    assert h1 > h0
    assert lease.conflict is False
    assert lease.acquired is True


async def test_heartbeat_loop_calls_tick(tmp_path: Path) -> None:
    lease = WriterLease()
    await lease.acquire(
        _settings(writer_lease_heartbeat_seconds=0.05), lambda: tmp_path
    )
    evt = asyncio.Event()
    orig_tick = lease.tick

    async def _wrapped_tick() -> None:
        await orig_tick()
        evt.set()

    lease.tick = _wrapped_tick  # type: ignore[method-assign]
    task = asyncio.create_task(lease.heartbeat_loop())
    try:
        await asyncio.wait_for(evt.wait(), 2.0)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


# ---------------------------------------------------------------------------
# (f) Foreign overwrite mid-run -> writer_lease_conflict on real /status
#     within one heartbeat -- THE ACCEPTANCE TEST
# ---------------------------------------------------------------------------


async def test_foreign_overwrite_surfaces_conflict_on_status(tmp_path: Path) -> None:
    lease = main_module.writer_lease
    await lease.acquire(_settings(), lambda: tmp_path)
    assert lease.acquired is True

    peer = WriterLease()
    await peer.acquire(_settings(), lambda: tmp_path)  # real second identity steals it

    await lease.tick()
    assert lease.conflict is True
    assert lease.conflict_source == "runtime"
    assert lease.observed_owner == peer.owner
    assert lease.acquired is False  # lost it
    assert lease.ever_acquired is True

    boot_state.phase = "ready"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main_module.app), base_url="http://test"
    ) as client:
        response = await client.get("/status")
    body = response.json()
    assert body["writer_lease"]["conflict"] is True
    # The spool projection is UNTOUCHED by this change -- boot-verified shape.
    assert set(body["spool"].keys()) == {
        "pending_sessions",
        "spool_bytes_total",
        "corrupt_offsets",
    }


async def test_conflict_latches_and_stops_renewing(tmp_path: Path) -> None:
    lease = WriterLease()
    await lease.acquire(_settings(), lambda: tmp_path)
    peer = WriterLease()
    await peer.acquire(_settings(), lambda: tmp_path)
    await lease.tick()
    assert lease.conflict is True

    (tmp_path / ".writer.lease").unlink()
    await lease.tick()
    await lease.tick()

    assert lease.conflict is True
    assert not (tmp_path / ".writer.lease").exists()  # no steal-back


# ---------------------------------------------------------------------------
# (g) Clean shutdown releases; never destroys a foreign lease
# ---------------------------------------------------------------------------


async def test_clean_shutdown_releases(tmp_path: Path) -> None:
    lease = WriterLease()
    await lease.acquire(_settings(), lambda: tmp_path)
    await lease.release()
    assert not (tmp_path / ".writer.lease").exists()


async def test_release_never_destroys_a_foreign_lease(tmp_path: Path) -> None:
    lease = WriterLease()
    await lease.acquire(_settings(), lambda: tmp_path)
    peer = WriterLease()
    await peer.acquire(_settings(), lambda: tmp_path)  # peer steals it

    await lease.release()
    assert (tmp_path / ".writer.lease").exists()
    assert _read_lease(tmp_path)["owner"] == peer.owner


# ---------------------------------------------------------------------------
# (h) Acquire race: exactly one wins
# ---------------------------------------------------------------------------


async def test_acquire_race_exactly_one_wins(tmp_path: Path) -> None:
    """Only `enforce` mode raises on the LOST confirm-race branch (`detect`'s
    deliberate take-over means BOTH sides would otherwise report success,
    which is correct `detect` behaviour, not a race failure -- see
    ``test_detect_never_refuses_fresh_foreign_lease``)."""
    for _ in range(5):
        d = tmp_path / f"race-{_}"
        d.mkdir()
        a = WriterLease()
        b = WriterLease()
        settings = _settings(
            writer_lease_mode="enforce", writer_lease_confirm_delay_seconds=0.2
        )
        results = await asyncio.gather(
            a.acquire(settings, lambda directory=d: directory),
            b.acquire(settings, lambda directory=d: directory),
            return_exceptions=True,
        )
        winners = [r for r in results if r is None]
        losers = [r for r in results if isinstance(r, Exception)]
        assert len(winners) == 1, results
        assert len(losers) == 1, results
        assert isinstance(losers[0], WriterLeaseConflict)
        winner = a if results[0] is None else b
        assert _read_lease(d)["owner"] == winner.owner


# ---------------------------------------------------------------------------
# (i) R1 -- the detector constructs NO QueueManager (the merged-line-corruption guard)
# ---------------------------------------------------------------------------


async def test_r1_no_queue_manager_constructed_by_d6(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The detector's acquire/tick/release path must never trigger
    QueueManager construction, or a concurrent construction race can
    reproduce torn/merged-line append corruption."""
    queues_dir = tmp_path / "queues"
    queues_dir.mkdir()
    main_module.registry._queue_manager = None

    def _fail_init(self: object, queues_dir: Path) -> None:
        pytest.fail("QueueManager.__init__ must never be called by the detector")

    monkeypatch.setattr(QueueManager, "__init__", _fail_init)

    lease = WriterLease()
    await lease.acquire(_settings(), lambda: main_module.registry.queues_dir_path)
    await lease.tick()
    await lease.tick()

    assert main_module.registry._queue_manager is None
    assert lease.acquired is True
    assert (queues_dir / ".writer.lease").exists()

    await lease.release()
    assert main_module.registry._queue_manager is None
    assert not (queues_dir / ".writer.lease").exists()


async def test_r1_exactly_one_queue_manager_under_concurrent_construction(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    queues_dir = tmp_path / "queues"
    monkeypatch.setattr(
        "context_intelligence_server.registry.get_settings",
        lambda: type(
            "S",
            (),
            {
                "queues_path": str(queues_dir),
                "write_concurrency": 8,
                "max_delivery_attempts": 5,
            },
        )(),
    )
    main_module.registry._queue_manager = None

    construct_count = 0
    real_init = QueueManager.__init__

    def _counting_init(self: QueueManager, queues_dir: Path) -> None:
        nonlocal construct_count
        construct_count += 1
        real_init(self, queues_dir)

    monkeypatch.setattr(QueueManager, "__init__", _counting_init)

    lease = WriterLease()

    async def _boot_touch() -> None:
        await lease.acquire(_settings(), lambda: main_module.registry.queues_dir_path)

    async def _reconcile_touch() -> None:
        for _ in range(20):
            _ = main_module.registry.queue_manager
            await asyncio.sleep(0)

    await asyncio.gather(_boot_touch(), _reconcile_touch())

    assert construct_count == 1
    main_module.registry._queue_manager = None


# ---------------------------------------------------------------------------
# (j) Cold boot: dir absent -> acquired False -> re-arms once dir exists
# ---------------------------------------------------------------------------


async def test_cold_boot_dir_absent_rearms_once_dir_exists(tmp_path: Path) -> None:
    missing_dir = tmp_path / "does-not-exist-yet"
    lease = WriterLease()

    await lease.acquire(_settings(), lambda: missing_dir)
    assert lease.acquired is False
    assert lease.conflict is False
    assert lease.error is not None
    assert not missing_dir.exists()  # the detector never creates the directory

    missing_dir.mkdir(parents=True)  # stands in for boot recovery's mkdir
    await lease.tick()

    assert lease.acquired is True
    assert lease.conflict is False
    assert _read_lease(missing_dir)["owner"] == lease.owner


# ---------------------------------------------------------------------------
# (k) Share-fault OSError at boot -> continue in every mode
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["off", "detect", "enforce"])
async def test_share_fault_at_boot_continues_in_every_mode(
    mode: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lease = WriterLease()

    def _boom() -> None:
        raise OSError(errno.ESTALE, "stale file handle")

    monkeypatch.setattr(lease, "_read", _boom)

    await lease.acquire(_settings(writer_lease_mode=mode), lambda: tmp_path)

    assert lease.acquired is False
    if mode == "off":
        assert lease.error is None
    else:
        assert lease.conflict is False
        assert lease.error is not None


async def test_writer_lease_boot_wrapper_survives_any_exception(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The main.py-level guard: only WriterLeaseConflict may escape."""
    await _clear_app_task("lease_task")
    monkeypatch.setattr(main_module._settings, "writer_lease_mode", "detect")

    async def _raise_runtime(*_a: object, **_kw: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(main_module.writer_lease, "acquire", _raise_runtime)
    await main_module._writer_lease_boot()
    assert main_module.writer_lease.error is not None
    assert main_module.app.state.lease_task is not None
    main_module.app.state.lease_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await main_module.app.state.lease_task

    async def _raise_conflict(*_a: object, **_kw: object) -> None:
        raise WriterLeaseConflict("boom")

    monkeypatch.setattr(main_module.writer_lease, "acquire", _raise_conflict)
    with pytest.raises(WriterLeaseConflict):
        await main_module._writer_lease_boot()


# ---------------------------------------------------------------------------
# (l) Hung mount -> wait_for times out -> lifespan/heartbeat always returns
# ---------------------------------------------------------------------------


async def test_hung_mount_acquire_times_out(tmp_path: Path) -> None:
    lease = WriterLease()
    blocker = threading.Event()

    def _hang() -> None:
        blocker.wait(timeout=5.0)

    monkeypatch_target = lease
    monkeypatch_target._read = _hang  # type: ignore[method-assign]

    start = time.monotonic()
    await lease.acquire(
        _settings(writer_lease_acquire_timeout_seconds=0.1), lambda: tmp_path
    )
    elapsed = time.monotonic() - start

    assert lease.acquired is False
    assert lease.error is not None
    assert "time" in lease.error.lower()
    assert elapsed < 2.0  # upper bound only -- never a lower-bound sleep assertion
    blocker.set()


async def test_hung_mount_does_not_starve_shared_pool(tmp_path: Path) -> None:
    """F2's bound: a stalled detector can never starve the append/commit path,
    which run on the SHARED default executor."""
    lease = WriterLease()
    blocker = threading.Event()

    def _hang() -> None:
        blocker.wait(timeout=3.0)

    lease._read = _hang  # type: ignore[method-assign]

    await lease.acquire(
        _settings(
            writer_lease_acquire_timeout_seconds=0.1,
            writer_lease_heartbeat_seconds=0.05,
        ),
        lambda: tmp_path,
    )
    task = asyncio.create_task(lease.heartbeat_loop())
    try:
        await asyncio.sleep(0.5)

        qm = QueueManager(queues_dir=tmp_path / "shared-pool-check")
        start = time.monotonic()
        await asyncio.wait_for(qm.append("sid-1", b"hello"), timeout=1.0)
        elapsed = time.monotonic() - start
        assert elapsed < 1.0
    finally:
        blocker.set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


# ---------------------------------------------------------------------------
# (m) A failed submit() must release the gate (R3)
# ---------------------------------------------------------------------------


async def test_submit_fail_releases_gate(tmp_path: Path) -> None:
    from context_intelligence_server import writer_lease as wl_module

    lease = WriterLease()
    await lease.acquire(_settings(), lambda: tmp_path)

    real_submit = wl_module._LEASE_IO.submit
    calls = {"n": 0}

    def _boom_once(*args: object, **kwargs: object) -> object:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("shutdown")
        return real_submit(*args, **kwargs)  # type: ignore[arg-type]

    with (
        patch.object(wl_module._LEASE_IO, "submit", side_effect=_boom_once),
        pytest.raises(WriterLeaseBusy),
    ):
        await lease._io(lease._read)
    assert lease._io_inflight is False

    # Re-arm: unpatched, a subsequent op succeeds.
    await lease.tick()
    assert lease.conflict is False


# ---------------------------------------------------------------------------
# (n) Shutdown never raises, even with the gate busy
# ---------------------------------------------------------------------------


async def test_shutdown_never_raises_with_busy_gate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Forces a genuinely busy lease-I/O gate (real outstanding future on the
    single-worker executor, `_io_inflight` held True) at the exact moment
    shutdown calls `release()`, so `release()` must swallow the resulting
    `WriterLeaseBusy` rather than let it escape."""
    from context_intelligence_server import writer_lease as wl_module

    queues_dir = tmp_path / "queues"
    queues_dir.mkdir()

    monkeypatch.setattr(
        "context_intelligence_server.registry.get_settings",
        lambda: type(
            "S",
            (),
            {
                "queues_path": str(queues_dir),
                "write_concurrency": 8,
                "max_delivery_attempts": 5,
            },
        )(),
    )
    main_module.registry._queue_manager = None
    monkeypatch.setattr(main_module._settings, "writer_lease_mode", "detect")
    # Keep the heartbeat from ticking during the forced-busy window so it
    # cannot itself contend for the one-slot gate and mask the scenario.
    monkeypatch.setattr(main_module._settings, "writer_lease_heartbeat_seconds", 999.0)
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", 0)
    monkeypatch.setattr(
        main_module._settings, "crash_recovery_sweep_interval_seconds", 0
    )
    await _clear_app_task("boot_task")
    await _clear_app_task("lease_task")
    await _clear_app_task("sweep_task")

    mock_driver = MagicMock()
    mock_driver.close = AsyncMock()

    blocker = threading.Event()

    def _hang() -> None:
        blocker.wait(timeout=5.0)

    hung_future = None
    try:
        with (
            patch("context_intelligence_server.main.setup_logging"),
            patch(
                "context_intelligence_server.main.AsyncGraphDatabase.driver",
                return_value=mock_driver,
            ),
            patch(
                "context_intelligence_server.main.ensure_neo4j_schema",
                new=AsyncMock(),
            ),
        ):
            async with lifespan(main_module.app):
                await main_module.app.state.boot_task
                assert main_module.writer_lease.acquired is True

                # Force the adverse state right before we fall out of this
                # block (which runs lifespan's `finally`): a real
                # outstanding future holding the executor's one worker
                # thread, and the gate flag itself closed.
                hung_future = wl_module._LEASE_IO.submit(_hang)
                main_module.writer_lease._io_inflight = True
                # Falling out of the `async with` here triggers shutdown:
                # sweep/boot/lease tasks cancelled, then
                # `writer_lease.release()` -- which must swallow the
                # WriterLeaseBusy this forced state causes `_io()` to raise.
        # Reaching here without an exception IS the assertion: shutdown
        # never raises WriterLeaseBusy/OSError past `lifespan`'s finally,
        # even with a genuinely busy gate at release() time.
    finally:
        blocker.set()
        if hung_future is not None:
            hung_future.result(timeout=5.0)
        main_module.writer_lease._io_inflight = False

    assert (
        main_module.app.state.lease_task.cancelled()
        or main_module.app.state.lease_task.done()
    )


# ---------------------------------------------------------------------------
# (o) /status writer_lease block present during boot, ZERO disk reads
# ---------------------------------------------------------------------------


async def test_status_writer_lease_present_during_boot_zero_disk_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Instruments disk I/O directly (patches `Path.read_text`, filtered to
    this lease's own path) rather than only asserting on the JSON body, so a
    regression that reads the lease file from disk on every request is caught
    even if `/status`'s JSON output looks unchanged."""
    lease = main_module.writer_lease
    await lease.acquire(_settings(), lambda: tmp_path)

    lease_path = lease.path
    read_calls = {"n": 0}
    real_read_text = Path.read_text

    def _counting_read_text(
        self: Path, encoding: str | None = None, errors: str | None = None
    ) -> str:
        if self == lease_path:
            read_calls["n"] += 1
        return real_read_text(self, encoding=encoding, errors=errors)

    monkeypatch.setattr(Path, "read_text", _counting_read_text)

    boot_state.phase = "reclaim"  # actively booting
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main_module.app), base_url="http://test"
    ) as client:
        response = await client.get("/status")

    assert response.status_code == 200
    body = response.json()
    assert body["spool"] is None  # the boot-safety lean contract intact
    assert "writer_lease" in body
    assert body["writer_lease"]["acquired"] is True
    assert body["writer_lease"]["owner"] == lease.owner
    assert read_calls["n"] == 0, (
        "writer_lease.snapshot() (and /status while actively booting) must "
        "be pure in-memory -- it must never read the on-disk lease file. "
        f"Observed {read_calls['n']} disk read(s) of {lease_path}."
    )


async def test_status_writer_lease_never_500s_when_unacquired() -> None:
    lease = main_module.writer_lease
    saved = (
        lease.mode,
        lease.acquired,
        lease.heartbeat_seconds,
        lease.staleness_seconds,
        lease.last_renewed,
    )
    lease.mode = None
    lease.acquired = False
    lease.heartbeat_seconds = None
    lease.staleness_seconds = None
    lease.last_renewed = None
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main_module.app), base_url="http://test"
        ) as client:
            response = await client.get("/status")
        assert response.status_code == 200
        assert response.json()["writer_lease"]["acquired"] is False
    finally:
        (
            lease.mode,
            lease.acquired,
            lease.heartbeat_seconds,
            lease.staleness_seconds,
            lease.last_renewed,
        ) = saved


# ---------------------------------------------------------------------------
# Config validators (writer-lease settings)
# ---------------------------------------------------------------------------


def test_writer_lease_config_defaults() -> None:
    s = Settings()
    assert s.writer_lease_mode == "detect"
    assert s.writer_lease_heartbeat_seconds == 5.0
    assert s.writer_lease_staleness_multiplier == 3.0
    assert s.writer_lease_confirm_delay_seconds == 1.0
    assert s.writer_lease_acquire_timeout_seconds == 5.0
    assert s.writer_lease_force_acquire is False


def test_writer_lease_config_validators_fail_loud() -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings(writer_lease_mode="enforced")  # pyright: ignore[reportArgumentType] -- typo, not a legal value
    with pytest.raises(ValidationError):
        Settings(writer_lease_staleness_multiplier=1.5)
    with pytest.raises(ValidationError):
        Settings(writer_lease_heartbeat_seconds=0)
    with pytest.raises(ValidationError):
        Settings(writer_lease_confirm_delay_seconds=-1.0)
    with pytest.raises(ValidationError):
        Settings(
            writer_lease_acquire_timeout_seconds=0.5,
            writer_lease_confirm_delay_seconds=1.0,
        )


# ---------------------------------------------------------------------------
# Collision guard: the writer-lease files never collide with other boot-path scans
# ---------------------------------------------------------------------------


async def test_no_collision_with_existing_session_scans(tmp_path: Path) -> None:
    lease = WriterLease()
    await lease.acquire(_settings(), lambda: tmp_path)

    qm = QueueManager(queues_dir=tmp_path)
    (tmp_path / "sess-1.log").write_bytes(b'{"event":"x","workspace":"/w"}\n')
    (tmp_path / "sess-1.offset").write_text("0", encoding="utf-8")

    keys = sorted(p.stem for p in qm.queues_dir.glob("*.log"))
    assert "sess-1" in keys
    assert ".writer" not in keys

    active = await qm.active_sessions()
    assert ".writer.lease" not in active
    assert (tmp_path / ".writer.lease").exists()
