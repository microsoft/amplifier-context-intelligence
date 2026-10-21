"""Quarantine, its queue acknowledgement and an operator retry never interleave.

PR 126 blocker: the poison drainer published a receipt as ``quarantined`` and
only then acknowledged the old queue line.  A retry landing in between reused
the still-queued line, and the old acknowledgement later read as proof that the
retry had been written.  The fix is one recovery-only lock shared by the
drainer (receipt + acknowledgement), the operator retry, reconciliation and
operator quarantine, plus a durable-state fence for a quarantined delivery
whose line is still queued.

``tests/integration/test_recovery_poison_ack_retry.py`` proves the original
bug end to end through the HTTP routes and a live worker.  These tests pin the
individual guarantees that make it hold, against the real receipt store and
queue files (only the graph is a stub).  Lock contention is detected with an
event set by the contended ``acquire`` itself, never with wall-clock sleeps.

Names the break each test catches:

* ``test_poison_*``            quarantine receipt or its ack runs outside the
                               shared lock / pause-guard-first ordering.
* ``test_cancelled_drainer_*`` the lock is dropped when the drainer's await is
                               cancelled while receipt/ack I/O is in flight.
* ``test_failed_quarantine_*`` the ack is attempted after a receipt failure, or
                               the lock leaks.
* ``test_retry_refuses_*``     a retry archives/resets a quarantined delivery
                               whose old line is still queued (failed ack or
                               restart), or the refusal is only in memory.
* ``test_retry_waits_*`` / ``test_reconcile_*`` / ``test_operator_quarantine_*``
                               that operation proceeds without the shared lock.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from context_intelligence_server.registry import (
    LiveFirstFlushGate,
    SessionRegistry,
    SessionWorker,
)
from context_intelligence_server.services import HookStateService
from tests.test_native_recovery import (
    _admission_body,
    _source_handle,
    _source_headers,
    _write_lease,
)
from tests.test_recovery_review_fixes import (
    SESSION,
    _admit,
    _audit_path,
    _dead_path,
    _drain_once,
    _graph_worker,
    _lease_app_with_one_quarantined,
    _lease_settings,
    _legacy_body,
    _permit_settings,
    _recovery_body,
)

Mode = Literal["permit", "lease"]
MODES = pytest.mark.parametrize("mode", ["permit", "lease"])

BOUND = 5.0  # hard upper bound for every wait; nothing waits unbounded
LOOP_TURNS = 50  # scheduler turns (not wall-clock) for a cancelled task to unwind


class _ContentionSignalLock(asyncio.Lock):
    """asyncio.Lock that announces the moment someone has to wait for it."""

    def __init__(self) -> None:
        super().__init__()
        self.contended = asyncio.Event()

    async def acquire(self) -> Literal[True]:
        if self.locked():
            self.contended.set()
        return await super().acquire()


async def _contended_or_done(
    lock: _ContentionSignalLock | _ParkOnNthAcquire, task: asyncio.Future
) -> None:
    """Return once ``task`` blocked on ``lock`` or finished, whichever is first."""
    waiter = asyncio.ensure_future(lock.contended.wait())
    try:
        await asyncio.wait(
            [waiter, task], timeout=BOUND, return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


# ---------------------------------------------------------------------------
# Registry: quarantine receipt + queue acknowledgement are one locked unit
# ---------------------------------------------------------------------------

UNPARSEABLE = b"this is not json"
POISON = json.dumps(
    {
        "event": "session:start",
        "workspace": "one",
        "data": {"session_id": SESSION, "timestamp": "2026-01-01T00:00:00"},
    }
).encode()


class _PoisonPathProbe:
    """Records the poison path's steps and whether the shared lock was held."""

    def __init__(self, lock: asyncio.Lock) -> None:
        self.lock = lock
        self.steps: list[tuple[str, bool]] = []

    def note(self, step: str) -> None:
        self.steps.append((step, self.lock.locked()))

    def names(self) -> list[str]:
        return [name for name, _held in self.steps]


def _recovery_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    lock: asyncio.Lock,
    *,
    receipt_hook: Any,
) -> tuple[SessionRegistry, _PoisonPathProbe]:
    """A real recovery registry and queue, instrumented but not stubbed."""
    registry = SessionRegistry.for_recovery(
        queues_path=tmp_path / "recovery",
        shared_write_semaphore=asyncio.Semaphore(1),
        max_delivery_attempts=1,
        flush_gate=LiveFirstFlushGate(1),
        quarantine_retry_lock=lock,
    )
    probe = _PoisonPathProbe(lock)

    @asynccontextmanager
    async def pause_guard() -> AsyncIterator[None]:
        probe.note("guard_enter")
        try:
            yield
        finally:
            probe.note("guard_exit")

    registry.recovery_write_guard = pause_guard
    registry.on_record_quarantined = receipt_hook(probe)
    registry._process_one = AsyncMock(side_effect=ValueError("deterministic poison"))

    queue = registry.queue_manager
    real_commit = queue.commit

    async def commit(session_id: str, offset: int) -> Any:
        probe.note("commit")
        return await real_commit(session_id, offset)

    monkeypatch.setattr(queue, "commit", commit)
    return registry, probe


def _recording_hook(probe: _PoisonPathProbe) -> Any:
    async def hook(_worker: Any, _record: Any) -> None:
        probe.note("receipt")

    return hook


async def _isolate(registry: SessionRegistry, raw: bytes) -> Any:
    await registry.queue_manager.append(SESSION, raw)
    batch = await registry.queue_manager.read_batch(SESSION, 1)
    worker, _svc = _graph_worker()
    return await registry._handle_exhausted_batch(worker, batch, handlers=MagicMock())


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [UNPARSEABLE, POISON], ids=["unparseable", "poison"])
async def test_poison_receipt_and_ack_run_under_the_shared_lock_after_the_pause_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, raw: bytes
) -> None:
    lock = asyncio.Lock()
    registry, probe = _recovery_registry(
        tmp_path, monkeypatch, lock, receipt_hook=_recording_hook
    )

    await _isolate(registry, raw)

    # pause guard first (lock not yet held), then receipt and ack inside the
    # lock, then the lock is released before the guard is.
    assert probe.steps == [
        ("guard_enter", False),
        ("receipt", True),
        ("commit", True),
        ("guard_exit", False),
    ]
    assert (await registry.queue_manager.read_batch(SESSION, 5)).records == []


@pytest.mark.asyncio
async def test_cancelled_drainer_keeps_the_lock_until_receipt_and_ack_settle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Shutdown cancelling the drainer must not release a retry mid-acknowledgement."""
    lock = asyncio.Lock()
    receipt_entered, receipt_release = asyncio.Event(), asyncio.Event()

    def slow_receipt(probe: _PoisonPathProbe) -> Any:
        async def hook(_worker: Any, _record: Any) -> None:
            receipt_entered.set()
            await receipt_release.wait()
            probe.note("receipt")

        return hook

    registry, probe = _recovery_registry(
        tmp_path, monkeypatch, lock, receipt_hook=slow_receipt
    )
    await registry.queue_manager.append(SESSION, POISON)
    batch = await registry.queue_manager.read_batch(SESSION, 1)
    worker, _svc = _graph_worker()
    drainer = asyncio.ensure_future(
        registry._handle_exhausted_batch(worker, batch, handlers=MagicMock())
    )
    try:
        await asyncio.wait_for(receipt_entered.wait(), BOUND)

        drainer.cancel()
        for _ in range(LOOP_TURNS):
            await asyncio.sleep(0)

        assert lock.locked(), "lock released while receipt/ack I/O was in flight"
        assert not drainer.done()
        assert "commit" not in probe.names()

        receipt_release.set()
        done, _pending = await asyncio.wait([drainer], timeout=BOUND)
        assert done and drainer.cancelled()
    finally:
        receipt_release.set()
        drainer.cancel()
        await asyncio.gather(drainer, return_exceptions=True)

    # The cancellation did not abandon the pair: receipt, then ack, both done.
    assert probe.names() == ["guard_enter", "receipt", "commit", "guard_exit"]
    assert not lock.locked()
    assert (await registry.queue_manager.read_batch(SESSION, 5)).records == []


@pytest.mark.asyncio
async def test_failed_quarantine_receipt_never_acknowledges_and_frees_the_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lock = asyncio.Lock()

    def failing_receipt(probe: _PoisonPathProbe) -> Any:
        async def hook(_worker: Any, _record: Any) -> None:
            probe.note("receipt")
            raise RuntimeError("receipt store unavailable")

        return hook

    registry, probe = _recovery_registry(
        tmp_path, monkeypatch, lock, receipt_hook=failing_receipt
    )

    with pytest.raises(RuntimeError, match="receipt store unavailable"):
        await _isolate(registry, POISON)

    assert probe.steps == [
        ("guard_enter", False),
        ("receipt", True),
        ("guard_exit", False),
    ]
    assert not lock.locked()
    # No acknowledgement: the line stays replayable for the next drain.
    assert len((await registry.queue_manager.read_batch(SESSION, 5)).records) == 1


# ---------------------------------------------------------------------------
# Retry: fence for a quarantined delivery whose old line is still queued
# ---------------------------------------------------------------------------


async def _quarantined_but_unacknowledged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Any, Any, Any, Any]:
    """Lease-mode app: receipt quarantined and dead-lettered, old line NOT acked.

    This is the durable state left by a failed acknowledgement (or a crash
    between the receipt transition and the offset rename).
    """
    from context_intelligence_server.main import app, create_asgi_app
    from context_intelligence_server.routers.admin import require_admin

    asgi_app = create_asgi_app(settings=_lease_settings(tmp_path))
    reg = app.state.recovery_registry
    monkeypatch.setattr(reg, "get_or_create", MagicMock())
    app.dependency_overrides[require_admin] = lambda: None
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app), base_url="http://test"
    )
    _write_lease(tmp_path / "lease.json")
    accepted = await client.post("/recovery/events", json=_legacy_body())
    assert accepted.status_code == 202, accepted.text
    batch = await reg.queue_manager.read_batch(SESSION, 1)
    worker = SessionWorker(
        session_id=SESSION, workspace="one", services=HookStateService(workspace="one")
    )

    real_commit = reg.queue_manager.commit

    async def failing_commit(*_args: Any, **_kwargs: Any) -> None:
        raise OSError("disk error during the poison acknowledgement")

    monkeypatch.setattr(reg.queue_manager, "commit", failing_commit)
    with (
        patch(
            "context_intelligence_server.registry.process_event",
            new=AsyncMock(side_effect=ValueError("deterministic poison")),
        ),
        pytest.raises(OSError, match="poison acknowledgement"),
    ):
        await reg._handle_exhausted_batch(worker, batch, handlers=MagicMock())
    monkeypatch.setattr(reg.queue_manager, "commit", real_commit)

    receipt = await app.state.recovery_receipts.next_quarantined()
    origin = {"source_handle": receipt["source_handle"], **receipt["origin"]}
    # Preconditions that make the refusal below meaningful, not vacuous.
    assert await app.state.recovery_receipts.status() == {"quarantined": 1}
    assert await reg.queue_manager.recovery_origin_commit_state(origin) == "queued"
    assert _dead_path(tmp_path).read_bytes().count(b"\n") == 1
    return app, asgi_app, client, origin


@pytest.mark.asyncio
@pytest.mark.parametrize("reopen", [False, True], ids=["same-process", "after-restart"])
async def test_retry_refuses_while_old_delivery_is_unacknowledged_then_redelivers_after_ack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reopen: bool
) -> None:
    from context_intelligence_server.main import create_asgi_app
    from context_intelligence_server.routers.admin import require_admin

    app, _asgi, client, origin = await _quarantined_but_unacknowledged(
        tmp_path, monkeypatch
    )
    try:
        if reopen:
            # A genuinely fresh receipt store, queue manager and registry over
            # the same files: the refusal must come from durable state.
            old_registry = app.state.recovery_registry
            await client.aclose()
            asgi_app = create_asgi_app(settings=_lease_settings(tmp_path))
            assert app.state.recovery_registry is not old_registry
            app.dependency_overrides[require_admin] = lambda: None
            monkeypatch.setattr(
                app.state.recovery_registry, "get_or_create", MagicMock()
            )
            client = httpx.AsyncClient(
                transport=httpx.ASGITransport(app=asgi_app), base_url="http://test"
            )
        reg = app.state.recovery_registry
        receipts = app.state.recovery_receipts
        queue = reg.queue_manager

        archived: list[dict[str, Any]] = []
        real_archive = queue.archive_recovery_dead_letters

        async def archive_spy(requested: dict[str, Any]) -> int:
            archived.append(requested)
            return await real_archive(requested)

        monkeypatch.setattr(queue, "archive_recovery_dead_letters", archive_spy)

        refused = await client.post("/admin/recovery/retry-quarantined")

        assert refused.json() == {"retried": 0}
        assert archived == []  # evidence and payload untouched
        assert await receipts.status() == {"quarantined": 1}
        assert _dead_path(tmp_path).read_bytes().count(b"\n") == 1
        assert not _audit_path(tmp_path).exists()
        assert await queue.recovery_origin_commit_state(origin) == "queued"

        # The old acknowledgement finally lands; only now may a retry proceed.
        old_line = await queue.read_batch(SESSION, 1)
        await queue.commit(SESSION, old_line.end_offset)
        assert await queue.recovery_origin_commit_state(origin) == "committed"

        _write_lease(tmp_path / "lease.json", "lease-retry")
        accepted = await client.post("/admin/recovery/retry-quarantined")

        assert accepted.json() == {"retried": 1}
        assert len(archived) == 1
        assert await receipts.status() == {"enqueued": 1}
        # A fresh raw line, not the old one: written needs a real flush.
        assert len((await queue.read_batch(SESSION, 5)).records) == 1
        svc = await _drain_once(reg)
        assert svc.graph.flush.await_count == 1
        assert await receipts.status() == {"written": 1}
    finally:
        app.dependency_overrides.pop(require_admin, None)
        await client.aclose()


# ---------------------------------------------------------------------------
# Every state-changing recovery operation takes the same shared lock
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_waits_for_the_shared_lock_before_archiving_or_resetting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from context_intelligence_server.main import _retry_quarantined_native_recovery
    from context_intelligence_server.routers.admin import require_admin

    app, client, reg = await _lease_app_with_one_quarantined(tmp_path, monkeypatch)
    try:
        shared = _ContentionSignalLock()
        app.state.recovery_quarantine_retry_lock = shared
        receipts = app.state.recovery_receipts
        archived: list[dict[str, Any]] = []
        real_archive = reg.queue_manager.archive_recovery_dead_letters

        async def archive_spy(requested: dict[str, Any]) -> int:
            archived.append(requested)
            return await real_archive(requested)

        monkeypatch.setattr(
            reg.queue_manager, "archive_recovery_dead_letters", archive_spy
        )
        _write_lease(tmp_path / "lease.json", "lease-retry")

        await shared.acquire()
        retry = asyncio.ensure_future(_retry_quarantined_native_recovery(app))
        try:
            await _contended_or_done(shared, retry)
            assert shared.contended.is_set(), "retry ran without the shared lock"
            assert archived == []
            assert await receipts.status() == {"quarantined": 1}
        finally:
            shared.release()

        # No self-deadlock: the retry holds the lock through its own bridge.
        assert await asyncio.wait_for(retry, BOUND) is True
        assert await receipts.status() == {"enqueued": 1}
    finally:
        app.dependency_overrides.pop(require_admin, None)
        await client.aclose()


@pytest.mark.asyncio
async def test_reconcile_captures_no_evidence_snapshot_before_holding_the_shared_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A snapshot taken before the lock could predate a quarantine in flight."""
    from context_intelligence_server.main import _reconcile_native_recovery_outbox
    from context_intelligence_server.routers.admin import require_admin

    app, client, _reg = await _lease_app_with_one_quarantined(tmp_path, monkeypatch)
    try:
        shared = _ContentionSignalLock()
        app.state.recovery_quarantine_retry_lock = shared
        receipts = app.state.recovery_receipts
        snapshots: list[str] = []
        real_pending_outbox = receipts.pending_outbox

        async def pending_outbox_spy(*args: Any, **kwargs: Any) -> Any:
            snapshots.append("pending_outbox")
            return await real_pending_outbox(*args, **kwargs)

        monkeypatch.setattr(receipts, "pending_outbox", pending_outbox_spy)

        await shared.acquire()
        reconcile = asyncio.ensure_future(_reconcile_native_recovery_outbox(app))
        try:
            await _contended_or_done(shared, reconcile)
            assert shared.contended.is_set(), (
                f"reconcile ran without the shared lock; snapshots={snapshots}"
            )
            assert snapshots == []
        finally:
            shared.release()

        await asyncio.wait_for(reconcile, BOUND)
        assert snapshots, "reconcile never read the outbox once it held the lock"
    finally:
        app.dependency_overrides.pop(require_admin, None)
        await client.aclose()


@pytest.mark.asyncio
async def test_operator_quarantine_waits_for_the_shared_lock_before_mutating(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from context_intelligence_server.main import (
        _quarantine_pending_native_recovery,
        app,
        create_asgi_app,
    )
    from context_intelligence_server.routers.admin import require_admin

    asgi_app = create_asgi_app(settings=_permit_settings(tmp_path))
    reg = app.state.recovery_registry
    monkeypatch.setattr(reg, "get_or_create", MagicMock())
    app.dependency_overrides[require_admin] = lambda: None
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=asgi_app),
            base_url="http://test",
            headers=_source_headers(),
        ) as client:
            await _admit(client, _recovery_body())
            assert (await client.post("/admin/recovery/pause")).json() == {
                "paused": True
            }
            shared = _ContentionSignalLock()
            app.state.recovery_quarantine_retry_lock = shared
            receipts = app.state.recovery_receipts

            await shared.acquire()
            quarantine = asyncio.ensure_future(
                _quarantine_pending_native_recovery(app, _source_handle(), 0)
            )
            try:
                await _contended_or_done(shared, quarantine)
                assert shared.contended.is_set(), (
                    "operator quarantine mutated receipt/queue without the shared lock"
                )
                assert await receipts.status() == {"enqueued": 1}
            finally:
                shared.release()

            assert await asyncio.wait_for(quarantine, BOUND) == "quarantined"
            assert await receipts.status() == {"quarantined": 1}
    finally:
        app.dependency_overrides.pop(require_admin, None)


# ---------------------------------------------------------------------------
# Event admission: the shared lock comes first and nothing else is held
# ---------------------------------------------------------------------------

LIVE_EVENT = (
    b'{"event":"session:start","workspace":"one","data":{"session_id":"live-session"}}'
)


class _ParkOnNthAcquire(asyncio.Lock):
    """Lets the first N-1 acquisitions through; the Nth parks until released.

    ``contended`` is set when the Nth acquisition arrives, so
    ``_contended_or_done`` can race it against the request finishing.
    """

    def __init__(self) -> None:
        super().__init__()
        self.contended = asyncio.Event()
        self.proceed = asyncio.Event()
        self._target: int | None = None
        self._calls = 0

    def arm(self, nth: int) -> None:
        self._calls = 0
        self._target = nth
        self.contended.clear()

    async def acquire(self) -> Literal[True]:
        self._calls += 1
        if self._calls == self._target:
            self.contended.set()
            await self.proceed.wait()
        return await super().acquire()


async def _recovery_app_and_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: Mode
) -> tuple[Any, httpx.AsyncClient]:
    from context_intelligence_server.main import app, create_asgi_app

    settings = (
        _permit_settings(tmp_path) if mode == "permit" else _lease_settings(tmp_path)
    )
    asgi_app = create_asgi_app(settings=settings)
    monkeypatch.setattr(app.state.recovery_registry, "get_or_create", MagicMock())
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app),
        base_url="http://test",
        headers=_source_headers() if mode == "permit" else {},
    )
    return app, client


async def _prepare_event(
    client: httpx.AsyncClient, tmp_path: Path, mode: Mode
) -> tuple[dict[str, Any], dict[str, str]]:
    """Everything a ``POST /recovery/events`` needs, short of sending it."""
    if mode == "permit":
        body = _recovery_body()
        permit = await client.post("/recovery/admissions", json=_admission_body(body))
        assert permit.status_code == 201, permit.text
        return body, {"X-Recovery-Permit": permit.json()["permit"]}
    _write_lease(tmp_path / "lease.json")
    return _legacy_body(), {}


@pytest.mark.asyncio
@MODES
async def test_event_admission_waits_for_the_shared_lock_holding_no_admission_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: Mode
) -> None:
    """Admission classifies under the shared lock, and takes it before the gate.

    Without this, a second submission could classify ``new`` before a first
    one's duplicate repair ran, then forge a ``written`` receipt for a retry.
    Holding the admission gate while waiting would also stall live ingest.
    """
    app, client = await _recovery_app_and_client(tmp_path, monkeypatch, mode)
    shared = _ParkOnNthAcquire()
    event: asyncio.Future[httpx.Response] | None = None
    try:
        app.state.recovery_quarantine_retry_lock = shared
        body, headers = await _prepare_event(client, tmp_path, mode)
        # Acquisition 1 is the initial reconcile, which stays outside the
        # critical section; acquisition 2 is the admission itself.
        shared.arm(2)
        event = asyncio.ensure_future(
            client.post("/recovery/events", json=body, headers=headers)
        )
        await _contended_or_done(shared, event)
        assert shared.contended.is_set(), (
            "event admission completed without taking the shared lock"
        )
        assert await app.state.recovery_receipts.status() == {}

        # The request is waiting, yet holds no gate: live ingest still finishes.
        live = app.state.registry.queue_manager
        try:
            await asyncio.wait_for(
                app.state.append_live_record("live-session", LIVE_EVENT), BOUND
            )
        except TimeoutError:
            pytest.fail("live append blocked behind a waiting event admission")
        pending = await live.read_batch("live-session", 1)
        assert len(pending.records) == 1
        await live.commit("live-session", pending.end_offset)

        shared.proceed.set()
        response = await asyncio.wait_for(event, BOUND)
        assert response.status_code == 202, response.text
        assert response.json()["status"] == "queued"
        assert await app.state.recovery_receipts.status() == {"enqueued": 1}
    finally:
        shared.proceed.set()
        if event is not None:
            event.cancel()
            await asyncio.gather(event, return_exceptions=True)
        await client.aclose()


@pytest.mark.asyncio
@MODES
async def test_cancelled_event_admission_keeps_the_lock_until_its_receipt_operation_settles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: Mode
) -> None:
    app, client = await _recovery_app_and_client(tmp_path, monkeypatch, mode)
    receipts = app.state.recovery_receipts
    entered, release = asyncio.Event(), asyncio.Event()
    real_admit_source = receipts.admit_source

    async def held_admit_source(*args: Any, **kwargs: Any) -> Any:
        entered.set()
        await release.wait()
        return await real_admit_source(*args, **kwargs)

    monkeypatch.setattr(receipts, "admit_source", held_admit_source)
    shared = asyncio.Lock()
    app.state.recovery_quarantine_retry_lock = shared
    event: asyncio.Future[httpx.Response] | None = None
    try:
        body, headers = await _prepare_event(client, tmp_path, mode)
        event = asyncio.ensure_future(
            client.post("/recovery/events", json=body, headers=headers)
        )
        await asyncio.wait_for(entered.wait(), BOUND)

        event.cancel()
        for _ in range(LOOP_TURNS):
            await asyncio.sleep(0)
        assert shared.locked(), (
            "lock released while the receipt operation was in flight"
        )
        assert not event.done()

        release.set()
        done, _pending = await asyncio.wait([event], timeout=BOUND)
        assert done and event.cancelled()
    finally:
        release.set()
        if event is not None:
            event.cancel()
            await asyncio.gather(event, return_exceptions=True)
        await client.aclose()

    # Settled, then released: the next holder is not locked out, and the
    # admission it protected completed rather than being left half-applied.
    await asyncio.wait_for(shared.acquire(), BOUND)
    shared.release()
    assert await receipts.status() == {"enqueued": 1}


@pytest.mark.asyncio
async def test_duplicate_admission_repairs_a_missing_append_exactly_once_and_never_resurrects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preservation guard for the duplicate branch of ``_admit_native_recovery``.

    A receipt admitted whose queue append never happened (a crash) is repaired
    by the duplicate branch's own reconcile, exactly once; repeating the
    duplicate is a no-op, and once the receipt is terminal a duplicate never
    appends again.  This is a characterization test: it passes before and after
    the admission-serialization change and fails if that repair is lost.
    """
    from context_intelligence_server import main as main_module
    from context_intelligence_server.main import app, create_asgi_app

    asgi_app = create_asgi_app(settings=_lease_settings(tmp_path))
    reg = app.state.recovery_registry
    monkeypatch.setattr(reg, "get_or_create", MagicMock())
    receipts = app.state.recovery_receipts
    queue = reg.queue_manager

    captured: list[dict[str, Any]] = []
    real_admit = main_module._admit_native_recovery

    async def admit_spy(app_instance: Any, receipt: dict[str, Any], *rest: Any) -> str:
        captured.append(receipt)
        return await real_admit(app_instance, receipt, *rest)

    monkeypatch.setattr(main_module, "_admit_native_recovery", admit_spy)
    real_append = queue.append
    append_fails = True

    async def flaky_append(session_id: str, raw: bytes) -> None:
        if append_fails:
            raise OSError("simulated crash before the queue append")
        await real_append(session_id, raw)

    monkeypatch.setattr(queue, "append", flaky_append)
    shared = app.state.recovery_quarantine_retry_lock

    async def duplicate_admission() -> str:
        # Every production caller holds the shared lock (see the handler).
        async with shared:
            return await main_module._admit_native_recovery(app, captured[0])

    def queued_lines() -> int:
        log = tmp_path / "recovery" / f"{SESSION}.log"
        return log.read_bytes().count(b"\n") if log.exists() else 0

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        _write_lease(tmp_path / "lease.json")
        crashed = await client.post("/recovery/events", json=_legacy_body())
        assert crashed.status_code == 500
    assert captured and queued_lines() == 0
    assert len(await receipts.pending_outbox()) == 1  # admitted, never appended

    append_fails = False
    assert await duplicate_admission() == "duplicate"
    assert queued_lines() == 1
    assert await receipts.status() == {"enqueued": 1}

    assert await duplicate_admission() == "duplicate"  # repeated: nothing new
    assert queued_lines() == 1

    # Terminal receipt + acknowledged line: a duplicate must not resurrect it.
    origin = {"source_handle": captured[0]["source_handle"], **captured[0]["origin"]}
    assert await receipts.mark(
        origin, "quarantined", source_handle=captured[0]["source_handle"]
    )
    old_line = await queue.read_batch(SESSION, 1)
    await queue.commit(SESSION, old_line.end_offset)
    assert await duplicate_admission() == "duplicate"
    assert queued_lines() == 1
    assert await receipts.status() == {"quarantined": 1}
