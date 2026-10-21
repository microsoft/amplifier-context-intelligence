"""Regression tests for PR 126 Copilot recovery review threads.

Covers terminal-record flush eligibility (r4329246576), lease-mode quarantine
retry (r4329246777) and pause/quarantine/resume serialization (r4329246839).
All graph access is mocked; storage is isolated under ``tmp_path``.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Literal, cast
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from context_intelligence_server.config import Settings
from tests.test_native_recovery import (
    _admission_body,
    _recovery_body,
    _source_handle,
    _source_headers,
)

SESSION = "recovery-session"


def _permit_settings(tmp_path: Path) -> Settings:
    return Settings(
        api_key=None,
        api_keys=None,
        api_keys_store_path=str(tmp_path / "keys.json"),
        allow_unauthenticated=True,
        queues_path=str(tmp_path / "live"),
        recovery=cast(
            Any,
            {
                "enabled": True,
                "receipt_store_path": str(tmp_path / "receipts.sqlite3"),
                "queues_path": str(tmp_path / "recovery"),
            },
        ),
    )


async def _wait_until(predicate: Any, timeout: float = 3.0) -> bool:
    """Poll *predicate* so assertions wait on a signal, not on a fixed sleep."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return bool(predicate())


async def _terminal_quarantine_scenario(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    event: str,
    *,
    quarantine: bool = True,
) -> dict[str, Any]:
    """Park a drainer at pause, optionally quarantine its record, then resume.

    With ``quarantine=False`` this is the positive control: the identical
    harness, minus the operator action, must flush.  That proves the harness
    can observe a flush at all, so "zero flushes" is not vacuous.
    """
    from context_intelligence_server.main import app, create_asgi_app
    from context_intelligence_server.registry import SessionWorker
    from context_intelligence_server.routers.admin import require_admin
    from context_intelligence_server.services import HookStateService

    asgi_app = create_asgi_app(settings=_permit_settings(tmp_path))
    reg = app.state.recovery_registry
    monkeypatch.setattr(reg, "get_or_create", MagicMock())
    # Real _process_batch, so safe_count/terminal_at come from TERMINAL_EVENTS;
    # only per-event handler dispatch is stubbed.
    reg._process_one = AsyncMock()
    app.dependency_overrides[require_admin] = lambda: None
    body = _recovery_body()
    body["event"] = event
    parked, release = asyncio.Event(), asyncio.Event()

    async def hold() -> None:
        parked.set()
        await release.wait()

    reg.pre_flush = hold
    out: dict[str, Any] = {}
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=asgi_app),
            base_url="http://test",
            headers=_source_headers(),
        ) as client:
            permit = await client.post(
                "/recovery/admissions", json=_admission_body(body)
            )
            assert permit.status_code == 201
            accepted = await client.post(
                "/recovery/events",
                json=body,
                headers={"X-Recovery-Permit": permit.json()["permit"]},
            )
            assert accepted.status_code == 202
            svc = HookStateService(workspace="one")
            graph = MagicMock()
            graph.flush = AsyncMock()
            svc.graph = graph
            worker = SessionWorker(session_id=SESSION, workspace="one", services=svc)
            qm = reg.queue_manager
            drain = asyncio.create_task(reg.drain_worker(worker, flush_timeout=0.2))
            await asyncio.wait_for(parked.wait(), timeout=2)
            assert (await client.post("/admin/recovery/pause")).json() == {
                "paused": True
            }
            if quarantine:
                quarantined = await client.post(
                    "/admin/recovery/quarantine-pending",
                    json={"source_handle": _source_handle(), "ordinal": 0},
                )
                out["quarantine"] = quarantined.json()
                out[
                    "receipts_after_quarantine"
                ] = await app.state.recovery_receipts.status()
                out["offset_after_quarantine"] = qm._offset_path(SESSION).read_text()
            release.set()
            assert (await client.post("/admin/recovery/resume")).json() == {
                "paused": False
            }
            # Wait for the drainer's observable outcome rather than a sleep:
            # a flush when eligible, a buffer discard when quarantined.
            if quarantine:
                await _wait_until(lambda: graph.discard_buffer.call_count >= 1)
            else:
                await _wait_until(lambda: graph.flush.await_count >= 1)
            await asyncio.sleep(0.05)  # let any erroneous late flush surface
            out["flush_awaits"] = graph.flush.await_count
            out["discard_calls"] = graph.discard_buffer.call_count
            path = qm._offset_path(SESSION)
            out["offset_after_resume"] = path.read_text() if path.exists() else None
            out["receipts_final"] = await app.state.recovery_receipts.status()
            drain.cancel()
            await asyncio.gather(drain, return_exceptions=True)
    finally:
        app.dependency_overrides.pop(require_admin, None)
    return out


@pytest.mark.asyncio
@pytest.mark.parametrize("event", ["session:start", "session:end"])
async def test_quarantined_record_never_flushes_after_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, event: str
) -> None:
    """Terminal-only and non-terminal batches get the same eligibility fence."""
    out = await _terminal_quarantine_scenario(tmp_path, monkeypatch, event)

    assert out["quarantine"] == {"quarantined": 1}
    assert out["flush_awaits"] == 0, out
    assert out["discard_calls"] >= 1, out
    # The operator's terminal receipt is not rewound, and the queue offset the
    # quarantine committed is not moved backwards (a drained spool may delete
    # its offset file, which is also not a rewind).
    assert out["receipts_final"] == out["receipts_after_quarantine"], out
    after = out["offset_after_resume"]
    assert after is None or int(after) >= int(out["offset_after_quarantine"]), out


@pytest.mark.asyncio
@pytest.mark.parametrize("event", ["session:start", "session:end"])
async def test_unquarantined_record_flushes_in_the_same_harness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, event: str
) -> None:
    """Positive control: identical pause/resume harness, no quarantine -> flush.

    Without this, the zero-flush assertions above could pass merely because the
    harness never lets any drainer reach the graph.
    """
    out = await _terminal_quarantine_scenario(
        tmp_path, monkeypatch, event, quarantine=False
    )

    assert out["flush_awaits"] >= 1, out
    assert out["discard_calls"] == 0, out
    assert out["receipts_final"] == {"written": 1}, out


# ---------------------------------------------------------------------------
# r4329246839 -- quarantine must hold the pause condition end to end
# ---------------------------------------------------------------------------


async def _admit(client: httpx.AsyncClient, body: dict[str, Any]) -> None:
    permit = await client.post("/recovery/admissions", json=_admission_body(body))
    assert permit.status_code == 201
    accepted = await client.post(
        "/recovery/events",
        json=body,
        headers={"X-Recovery-Permit": permit.json()["permit"]},
    )
    assert accepted.status_code == 202


def _graph_worker() -> tuple[Any, Any]:
    from context_intelligence_server.registry import SessionWorker
    from context_intelligence_server.services import HookStateService

    svc = HookStateService(workspace="one")
    svc.graph = MagicMock()
    svc.graph.flush = AsyncMock()
    return SessionWorker(session_id=SESSION, workspace="one", services=svc), svc


@pytest.mark.asyncio
async def test_resume_waits_for_inflight_quarantine_and_target_never_flushes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Resume cannot slip between the paused check and the terminal receipt."""
    from context_intelligence_server.main import app, create_asgi_app
    from context_intelligence_server.routers.admin import require_admin

    asgi_app = create_asgi_app(settings=_permit_settings(tmp_path))
    reg = app.state.recovery_registry
    monkeypatch.setattr(reg, "get_or_create", MagicMock())
    reg._process_one = AsyncMock()
    app.dependency_overrides[require_admin] = lambda: None
    in_dead_letter, release_dead_letter = asyncio.Event(), asyncio.Event()
    queue = reg.queue_manager
    real_dead_letter = queue.dead_letter

    async def barrier_dead_letter(*args: Any, **kwargs: Any) -> Any:
        in_dead_letter.set()
        await release_dead_letter.wait()
        return await real_dead_letter(*args, **kwargs)

    monkeypatch.setattr(queue, "dead_letter", barrier_dead_letter)
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
            worker, svc = _graph_worker()
            drain = asyncio.create_task(reg.drain_worker(worker, flush_timeout=0.2))
            await asyncio.sleep(0.1)  # drainer parks on the pause

            quarantine = asyncio.create_task(
                client.post(
                    "/admin/recovery/quarantine-pending",
                    json={"source_handle": _source_handle(), "ordinal": 0},
                )
            )
            await asyncio.wait_for(in_dead_letter.wait(), timeout=2)
            resume = asyncio.create_task(client.post("/admin/recovery/resume"))
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(resume), timeout=0.3)
            assert app.state.recovery_paused is True
            assert svc.graph.flush.await_count == 0

            release_dead_letter.set()
            quarantined, resumed = await asyncio.wait_for(
                asyncio.gather(quarantine, resume), timeout=5
            )
            assert quarantined.json() == {"quarantined": 1}
            assert resumed.json() == {"paused": False}
            await asyncio.sleep(0.5)
            assert svc.graph.flush.await_count == 0
            assert await app.state.recovery_receipts.status() == {"quarantined": 1}
            drain.cancel()
            await asyncio.gather(drain, return_exceptions=True)
    finally:
        app.dependency_overrides.pop(require_admin, None)


@pytest.mark.asyncio
async def test_quarantine_outcomes_not_paused_none_and_already_terminal(
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
    selected = {"source_handle": _source_handle(), "ordinal": 0}
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=asgi_app),
            base_url="http://test",
            headers=_source_headers(),
        ) as client:
            await _admit(client, _recovery_body())

            # Unpaused: the decision is made inside the guard -> 409, no change.
            refused = await client.post(
                "/admin/recovery/quarantine-pending", json=selected
            )
            assert refused.status_code == 409
            assert (
                await _quarantine_pending_native_recovery(
                    app, selected["source_handle"], 0
                )
                == "not_paused"
            )
            assert await app.state.recovery_receipts.status() == {"enqueued": 1}

            assert (await client.post("/admin/recovery/pause")).json() == {
                "paused": True
            }
            # Unknown receipt while paused: nothing to do, not an error.
            unknown = await client.post(
                "/admin/recovery/quarantine-pending",
                json={"source_handle": selected["source_handle"], "ordinal": 99},
            )
            assert unknown.status_code == 200 and unknown.json() == {"quarantined": 0}

            # A receipt already terminalized is not quarantined a second time.
            first = await client.post(
                "/admin/recovery/quarantine-pending", json=selected
            )
            assert first.json() == {"quarantined": 1}
            again = await client.post(
                "/admin/recovery/quarantine-pending", json=selected
            )
            assert again.json() == {"quarantined": 0}
            assert await app.state.recovery_receipts.status() == {"quarantined": 1}
    finally:
        app.dependency_overrides.pop(require_admin, None)


class _TracingLock(asyncio.Lock):
    """asyncio.Lock that records, per task, what that task already held."""

    def __init__(
        self, name: str, log: list[tuple[str, str, tuple[str, ...]]], held: dict
    ) -> None:
        super().__init__()
        self.name = name
        self._log = log
        self._held = held

    async def acquire(self) -> Literal[True]:
        task = asyncio.current_task()
        self._log.append(("acquire", self.name, tuple(self._held.get(task, ()))))
        await super().acquire()
        self._held.setdefault(task, []).append(self.name)
        return True

    def release(self) -> None:
        names = self._held.get(asyncio.current_task(), [])
        if self.name in names:
            names.remove(self.name)
        super().release()


def _install_lock_tracing(app: Any) -> list[tuple[str, str, tuple[str, ...]]]:
    """Swap the pause condition's lock and the outbox lock for tracing locks."""
    log: list[tuple[str, str, tuple[str, ...]]] = []
    held: dict = {}
    app.state.recovery_pause_condition = asyncio.Condition(
        _TracingLock("condition", log, held)
    )
    app.state.recovery_outbox_lock = _TracingLock("outbox", log, held)
    return log


def _order_violations(
    log: list[tuple[str, str, tuple[str, ...]]],
) -> list[tuple[str, str, tuple[str, ...]]]:
    """Any acquisition of the condition by a task already holding the outbox."""
    return [entry for entry in log if entry[1] == "condition" and "outbox" in entry[2]]


@pytest.mark.asyncio
async def test_lock_tracer_flags_outbox_then_condition() -> None:
    """Self-check: the tracer would catch the forbidden reverse order.

    This guards the strict test below against being vacuous.  It exercises the
    tracer only, not product code.
    """
    log: list[tuple[str, str, tuple[str, ...]]] = []
    held: dict = {}
    condition = asyncio.Condition(_TracingLock("condition", log, held))
    outbox = _TracingLock("outbox", log, held)
    async with outbox, condition:
        pass
    assert _order_violations(log) == [("acquire", "condition", ("outbox",))]

    log.clear()
    async with condition, outbox:
        pass
    assert _order_violations(log) == []
    assert ("acquire", "outbox", ("condition",)) in log


@pytest.mark.asyncio
async def test_recovery_lock_acquisition_order_is_condition_before_outbox(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Across admit, pause, quarantine, flush, resume and reconcile, no task
    ever takes the pause condition while holding the outbox lock."""
    from context_intelligence_server.main import (
        _quarantine_pending_native_recovery,
        _reconcile_native_recovery_outbox,
        app,
        create_asgi_app,
    )
    from context_intelligence_server.routers.admin import require_admin

    asgi_app = create_asgi_app(settings=_permit_settings(tmp_path))
    log = _install_lock_tracing(app)
    reg = app.state.recovery_registry
    monkeypatch.setattr(reg, "get_or_create", MagicMock())
    reg._process_one = AsyncMock()
    app.dependency_overrides[require_admin] = lambda: None
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=asgi_app),
            base_url="http://test",
            headers=_source_headers(),
        ) as client:
            await _admit(client, _recovery_body())  # outbox only
            assert (await client.post("/admin/recovery/pause")).json() == {
                "paused": True
            }
            worker, svc = _graph_worker()
            drain = asyncio.create_task(reg.drain_worker(worker, flush_timeout=0.2))
            await asyncio.sleep(0.1)  # drainer waits on the pause condition
            outcome = await asyncio.wait_for(
                _quarantine_pending_native_recovery(app, _source_handle(), 0),
                timeout=5,
            )
            assert outcome == "quarantined"
            assert (await client.post("/admin/recovery/resume")).json() == {
                "paused": False
            }
            await _wait_until(lambda: svc.graph.discard_buffer.call_count >= 1)
            await asyncio.wait_for(_reconcile_native_recovery_outbox(app), timeout=5)
            drain.cancel()
            await asyncio.gather(drain, return_exceptions=True)
    finally:
        app.dependency_overrides.pop(require_admin, None)

    assert _order_violations(log) == [], log
    # Non-vacuous: both orders' prerequisites were actually exercised.
    assert ("acquire", "outbox", ("condition",)) in log, log  # quarantine
    assert ("acquire", "outbox", ()) in log, log  # admission / reconcile
    assert ("acquire", "condition", ()) in log, log  # pause / flush guard


# ---------------------------------------------------------------------------
# r4329246777 -- legacy lease mode (permit_required: false) quarantine retry
# ---------------------------------------------------------------------------


def _lease_settings(tmp_path: Path) -> Settings:
    return Settings(
        api_key=None,
        api_keys=None,
        api_keys_store_path=str(tmp_path / "keys.json"),
        allow_unauthenticated=True,
        queues_path=str(tmp_path / "live"),
        recovery=cast(
            Any,
            {
                "enabled": True,
                "permit_required": False,
                "guard_lease_path": str(tmp_path / "lease.json"),
                "receipt_store_path": str(tmp_path / "receipts.sqlite3"),
                "queues_path": str(tmp_path / "recovery"),
            },
        ),
    )


def _legacy_body() -> dict[str, Any]:
    return {
        "event": "session:start",
        "workspace": "one",
        "data": {"session_id": SESSION, "timestamp": "2026-01-01T00:00:00"},
        "origin": {
            "session_id": SESSION,
            "ordinal": 0,
            "source_line_sha256": "a" * 64,
            "source_stream_sha256": "b" * 64,
        },
    }


async def _lease_app_with_one_quarantined(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    raise_app_exceptions: bool = True,
) -> tuple[Any, httpx.AsyncClient, Any]:
    """Real lease-mode app whose single receipt is quarantined by poison."""
    from unittest.mock import patch

    from context_intelligence_server.main import app, create_asgi_app
    from context_intelligence_server.registry import SessionWorker
    from context_intelligence_server.routers.admin import require_admin
    from context_intelligence_server.services import HookStateService
    from tests.test_native_recovery import _write_lease

    asgi_app = create_asgi_app(settings=_lease_settings(tmp_path))
    reg = app.state.recovery_registry
    monkeypatch.setattr(reg, "get_or_create", MagicMock())
    app.dependency_overrides[require_admin] = lambda: None
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(
            app=asgi_app, raise_app_exceptions=raise_app_exceptions
        ),
        base_url="http://test",
    )
    _write_lease(tmp_path / "lease.json")
    accepted = await client.post("/recovery/events", json=_legacy_body())
    assert accepted.status_code == 202, accepted.text
    batch = await reg.queue_manager.read_batch(SESSION, 1)
    worker = SessionWorker(
        session_id=SESSION, workspace="one", services=HookStateService(workspace="one")
    )
    with patch(
        "context_intelligence_server.registry.process_event",
        new=AsyncMock(side_effect=ValueError("deterministic poison")),
    ):
        await reg._handle_exhausted_batch(worker, batch, handlers=MagicMock())
    assert await app.state.recovery_receipts.status() == {"quarantined": 1}
    return app, client, reg


async def _drain_once(reg: Any) -> Any:
    from context_intelligence_server.registry import SessionWorker
    from context_intelligence_server.services import HookStateService

    svc = HookStateService(workspace="one")
    svc.graph = MagicMock()
    svc.graph.flush = AsyncMock()
    worker = SessionWorker(session_id=SESSION, workspace="one", services=svc)
    reg._process_one = AsyncMock()
    assert await reg._drain_to_eof(worker, MagicMock())
    return svc


@pytest.mark.asyncio
async def test_lease_mode_retry_requeues_real_record_and_writes_once_on_drain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from context_intelligence_server.routers.admin import require_admin
    from tests.test_native_recovery import _write_lease

    app, client, reg = await _lease_app_with_one_quarantined(tmp_path, monkeypatch)
    try:
        assert app.state.recovery_admission_gate is None  # lease mode
        _write_lease(tmp_path / "lease.json", "lease-retry")
        retried = await client.post("/admin/recovery/retry-quarantined")
        assert retried.json() == {"retried": 1}

        # Enqueued with a REAL raw record, and not written before a drain.
        assert await app.state.recovery_receipts.status() == {"enqueued": 1}
        assert len((await reg.queue_manager.read_batch(SESSION, 5)).records) == 1

        svc = await _drain_once(reg)
        assert svc.graph.flush.await_count == 1
        assert await app.state.recovery_receipts.status() == {"written": 1}

        again = await client.post("/admin/recovery/retry-quarantined")
        assert again.json() == {"retried": 0}  # duplicate retry is a no-op
        assert await app.state.recovery_receipts.status() == {"written": 1}
    finally:
        app.dependency_overrides.pop(require_admin, None)
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("blocker", ["paused", "live_pending"])
async def test_lease_mode_retry_is_live_first_and_pause_aware(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, blocker: str
) -> None:
    from context_intelligence_server.routers.admin import require_admin

    app, client, reg = await _lease_app_with_one_quarantined(tmp_path, monkeypatch)
    try:
        if blocker == "paused":
            assert (await client.post("/admin/recovery/pause")).json() == {
                "paused": True
            }
        else:
            await app.state.registry.queue_manager.append(
                "live-session",
                b'{"event":"session:start","workspace":"one","data":{"session_id":"live-session"}}',
            )
        retried = await client.post("/admin/recovery/retry-quarantined")
        assert retried.json() == {"retried": 0}
        assert await app.state.recovery_receipts.status() == {"quarantined": 1}
        assert (await reg.queue_manager.read_batch(SESSION, 5)).records == []
    finally:
        app.dependency_overrides.pop(require_admin, None)
        await client.aclose()


@pytest.mark.asyncio
async def test_lease_mode_stale_outbox_snapshot_never_marks_retry_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A retry made durable after the pre-lock snapshot is deferred, not lost.

    The quarantined delivery's old committed queue line must not be read as the
    new delivery's completion.
    """
    from context_intelligence_server.main import _reconcile_native_recovery_outbox
    from context_intelligence_server.routers.admin import require_admin

    app, client, reg = await _lease_app_with_one_quarantined(tmp_path, monkeypatch)
    try:
        receipts = app.state.recovery_receipts
        receipt = await receipts.next_quarantined()
        origin = {"source_handle": receipt["source_handle"], **receipt["origin"]}
        await reg.queue_manager.archive_recovery_dead_letters(origin)
        assert await receipts.requeue_quarantined(
            receipt["source_handle"], receipt["origin"]["ordinal"]
        )

        real_pending_outbox = receipts.pending_outbox
        calls = {"n": 0}

        async def stale_first(*args: Any, **kwargs: Any) -> Any:
            calls["n"] += 1
            return [] if calls["n"] == 1 else await real_pending_outbox(*args, **kwargs)

        monkeypatch.setattr(receipts, "pending_outbox", stale_first)
        await _reconcile_native_recovery_outbox(app)

        assert calls["n"] >= 2  # the locked pass did see the retry receipt
        assert await receipts.status() == {"retry_pending": 1}
        assert (await reg.queue_manager.read_batch(SESSION, 5)).records == []

        # A fresh pass has the audit evidence and bridges it correctly.
        monkeypatch.setattr(receipts, "pending_outbox", real_pending_outbox)
        await _reconcile_native_recovery_outbox(app)
        assert await receipts.status() == {"enqueued": 1}
        assert len((await reg.queue_manager.read_batch(SESSION, 5)).records) == 1
    finally:
        app.dependency_overrides.pop(require_admin, None)
        await client.aclose()


# ---------------------------------------------------------------------------
# Retry-audit reader must fail closed (review BLOCKER)
# ---------------------------------------------------------------------------

AUDIT_SUFFIX = ".recovery-retry-audit.jsonl"
_BAD_AUDIT_LINES = {
    "invalid_json": b"{not json\n",
    "non_utf8": b"\xff\xfe\n",
    "not_an_object": b"[]\n",
    "payload_not_a_string": b'{"payload": 7}\n',
    "payload_not_json": b'{"payload": "{broken"}\n',
    # Valid JSON, structurally an envelope, but it proves nothing about any
    # origin.  These are the shapes the first reader accepted as "not a match".
    "payload_empty_object": b'{"payload": "{}"}\n',
    "payload_json_not_object": b'{"payload": "7"}\n',
    "origin_not_a_dict": b'{"payload": "{\\"_recovery_origin\\": 7}"}\n',
    "origin_missing_fields": (
        b'{"payload": "{\\"_recovery_origin\\": {\\"ordinal\\": 0}}"}\n'
    ),
    "binary_payload_only": b'{"payload_b64": "AAAA"}\n',
}


def _audit_path(tmp_path: Path) -> Path:
    return tmp_path / "recovery" / f"{SESSION}{AUDIT_SUFFIX}"


async def _assert_retry_left_pending_and_unwritten(
    app: Any, reg: Any, response: httpx.Response
) -> None:
    """The failed retry must neither complete, ack, flush nor drop the work."""
    assert response.status_code == 500, response.text
    assert await app.state.recovery_receipts.status() == {"retry_pending": 1}
    assert (await reg.queue_manager.read_batch(SESSION, 5)).records == []
    svc = await _drain_once(reg)  # nothing queued -> nothing may flush
    assert svc.graph.flush.await_count == 0
    assert await app.state.recovery_receipts.status() == {"retry_pending": 1}


async def _assert_restored_evidence_yields_one_new_delivery(app: Any, reg: Any) -> None:
    from context_intelligence_server.main import _reconcile_native_recovery_outbox

    await _reconcile_native_recovery_outbox(app)
    assert await app.state.recovery_receipts.status() == {"enqueued": 1}
    assert len((await reg.queue_manager.read_batch(SESSION, 5)).records) == 1
    svc = await _drain_once(reg)
    assert svc.graph.flush.await_count == 1
    assert await app.state.recovery_receipts.status() == {"written": 1}


@pytest.mark.asyncio
async def test_lease_retry_unreadable_audit_fails_closed_then_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from context_intelligence_server.routers.admin import require_admin
    from tests.test_native_recovery import _write_lease

    app, client, reg = await _lease_app_with_one_quarantined(
        tmp_path, monkeypatch, raise_app_exceptions=False
    )
    try:
        _write_lease(tmp_path / "lease.json", "lease-retry")
        real_open = Path.open
        denied: list[str] = []

        def open_denying_audit(self: Path, *args: Any, **kwargs: Any) -> Any:
            if self.name.endswith(AUDIT_SUFFIX):
                denied.append(self.name)
                raise PermissionError(13, "Permission denied", str(self))
            return real_open(self, *args, **kwargs)

        monkeypatch.setattr(Path, "open", open_denying_audit)
        response = await client.post("/admin/recovery/retry-quarantined")
        assert denied, "fault injection never reached the audit reader"
        await _assert_retry_left_pending_and_unwritten(app, reg, response)
        # The evidence that could not be read is still on disk, untouched.
        assert _audit_path(tmp_path).stat().st_size > 0

        monkeypatch.setattr(Path, "open", real_open)  # restore valid evidence
        await _assert_restored_evidence_yields_one_new_delivery(app, reg)
    finally:
        app.dependency_overrides.pop(require_admin, None)
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", sorted(_BAD_AUDIT_LINES))
async def test_lease_retry_malformed_audit_fails_closed_then_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    from context_intelligence_server.routers.admin import require_admin
    from tests.test_native_recovery import _write_lease

    app, client, reg = await _lease_app_with_one_quarantined(
        tmp_path, monkeypatch, raise_app_exceptions=False
    )
    audit = _audit_path(tmp_path)
    try:
        _write_lease(tmp_path / "lease.json", "lease-retry")
        # A malformed line precedes the genuine evidence the retry archives.
        audit.write_bytes(_BAD_AUDIT_LINES[kind])
        response = await client.post("/admin/recovery/retry-quarantined")
        await _assert_retry_left_pending_and_unwritten(app, reg, response)

        # Restore valid evidence: drop the malformed line, keep the real record.
        lines = audit.read_bytes().splitlines(keepends=True)
        assert lines[0] == _BAD_AUDIT_LINES[kind] and len(lines) == 2
        audit.write_bytes(b"".join(lines[1:]))
        await _assert_restored_evidence_yields_one_new_delivery(app, reg)
    finally:
        app.dependency_overrides.pop(require_admin, None)
        await client.aclose()


def _origin_audit_record(origin: dict[str, Any]) -> bytes:
    import json

    envelope = json.dumps({"_recovery_origin": origin})
    return (json.dumps({"ts": 1.0, "error": "e", "payload": envelope}) + "\n").encode()


@pytest.mark.asyncio
async def test_audit_reader_distinguishes_absent_present_unreadable_malformed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from context_intelligence_server.queue_manager import QueueManager

    queue = QueueManager(tmp_path / "q")
    origin = {"source_handle": "1" * 64, "ordinal": 0, "source_line_sha256": "a" * 64}
    other = {"source_handle": "1" * 64, "ordinal": 1, "source_line_sha256": "b" * 64}
    audit = tmp_path / "q" / f"w{AUDIT_SUFFIX}"

    # Genuinely absent evidence is the only thing that may read as False.
    assert await queue.recovery_origin_has_retry_audit(origin) is False

    audit.write_bytes(_origin_audit_record(other))
    assert await queue.recovery_origin_has_retry_audit(origin) is False  # readable
    audit.write_bytes(_origin_audit_record(origin))
    assert await queue.recovery_origin_has_retry_audit(origin) is True

    for bad in _BAD_AUDIT_LINES.values():
        audit.write_bytes(bad + _origin_audit_record(origin))
        with pytest.raises(ValueError):
            await queue.recovery_origin_has_retry_audit(origin)
        # Unknown must not be answered from a later valid record either way.
        audit.write_bytes(bad)
        with pytest.raises(ValueError):
            await queue.recovery_origin_has_retry_audit(origin)

    audit.write_bytes(_origin_audit_record(origin))
    real_open = Path.open

    def denied(self: Path, *args: Any, **kwargs: Any) -> Any:
        if self.name.endswith(AUDIT_SUFFIX):
            raise PermissionError(13, "Permission denied", str(self))
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", denied)
    with pytest.raises(PermissionError):
        await queue.recovery_origin_has_retry_audit(origin)


# ---------------------------------------------------------------------------
# Concurrent lease-mode retry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_lease_retries_requeue_exactly_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from context_intelligence_server.routers.admin import require_admin
    from tests.test_native_recovery import _write_lease

    app, client, reg = await _lease_app_with_one_quarantined(tmp_path, monkeypatch)
    try:
        _write_lease(tmp_path / "lease.json", "lease-retry")
        first, second = await asyncio.wait_for(
            asyncio.gather(
                client.post("/admin/recovery/retry-quarantined"),
                client.post("/admin/recovery/retry-quarantined"),
            ),
            timeout=10,
        )
        assert sorted([first.json()["retried"], second.json()["retried"]]) == [0, 1]
        assert await app.state.recovery_receipts.status() == {"enqueued": 1}
        assert len((await reg.queue_manager.read_batch(SESSION, 5)).records) == 1

        # Retrying again before the record is written changes nothing.
        third = await client.post("/admin/recovery/retry-quarantined")
        assert third.json() == {"retried": 0}
        assert await app.state.recovery_receipts.status() == {"enqueued": 1}
        assert len((await reg.queue_manager.read_batch(SESSION, 5)).records) == 1

        svc = await _drain_once(reg)
        assert svc.graph.flush.await_count == 1
        assert await app.state.recovery_receipts.status() == {"written": 1}
        assert (await client.post("/admin/recovery/retry-quarantined")).json() == {
            "retried": 0
        }
    finally:
        app.dependency_overrides.pop(require_admin, None)
        await client.aclose()


# ---------------------------------------------------------------------------
# Data-loss invariant: a historical committed queue line must never prove that
# the CURRENT retry was written when its retry proof is absent or invalid.
# ---------------------------------------------------------------------------

_V1_ORIGIN = {
    "source_handle": "1" * 64,
    "ordinal": 3,
    "source_line_sha256": "a" * 64,
}


def _dead_path(tmp_path: Path) -> Path:
    return tmp_path / "recovery" / f"{SESSION}.dead.jsonl"


def _offset_text(tmp_path: Path) -> str | None:
    path = tmp_path / "recovery" / f"{SESSION}.offset"
    return path.read_text() if path.exists() else None


async def _stage_retry_pending(app: Any, reg: Any) -> tuple[dict[str, Any], int]:
    """Do exactly what an operator retry does up to (not including) reconcile."""
    receipts = app.state.recovery_receipts
    receipt = await receipts.next_quarantined()
    origin = {"source_handle": receipt["source_handle"], **receipt["origin"]}
    assert await reg.queue_manager.archive_recovery_dead_letters(origin) == 1
    assert await receipts.requeue_quarantined(
        receipt["source_handle"], receipt["origin"]["ordinal"]
    )
    assert await receipts.status() == {"retry_pending": 1}
    return origin, receipt["origin"]["ordinal"]


def _damage_audit(audit: Path, damage: str) -> None:
    if damage == "missing_file":
        audit.unlink()
    else:
        audit.write_bytes(_BAD_AUDIT_LINES[damage])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "missing_file",
        "payload_empty_object",
        "origin_not_a_dict",
        "origin_missing_fields",
        "binary_payload_only",
        "invalid_json",
    ],
)
async def test_retry_pending_without_valid_audit_is_never_marked_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    from context_intelligence_server.main import _reconcile_native_recovery_outbox
    from context_intelligence_server.queue_manager import RecoveryReceiptError
    from context_intelligence_server.routers.admin import require_admin
    from tests.test_native_recovery import _write_lease

    app, client, reg = await _lease_app_with_one_quarantined(tmp_path, monkeypatch)
    try:
        _write_lease(tmp_path / "lease.json", "lease-retry")
        await _stage_retry_pending(app, reg)
        audit = _audit_path(tmp_path)
        valid_audit = audit.read_bytes()
        offset_before = _offset_text(tmp_path)
        _damage_audit(audit, damage)

        with pytest.raises(RecoveryReceiptError):
            await _reconcile_native_recovery_outbox(app)

        # Left pending: not written, nothing queued, no offset acknowledged,
        # and the receipt is still there to be repaired.
        assert await app.state.recovery_receipts.status() == {"retry_pending": 1}
        assert (await reg.queue_manager.read_batch(SESSION, 5)).records == []
        assert _offset_text(tmp_path) == offset_before
        svc = await _drain_once(reg)
        assert svc.graph.flush.await_count == 0
        assert await app.state.recovery_receipts.status() == {"retry_pending": 1}
        assert _dead_path(tmp_path).read_bytes() == b""  # archived line stays gone

        # Explicit operator repair: restore the valid proof.  One fresh
        # delivery is queued and flushed exactly once.
        audit.write_bytes(valid_audit)
        await _assert_restored_evidence_yields_one_new_delivery(app, reg)
    finally:
        app.dependency_overrides.pop(require_admin, None)
        await client.aclose()


@pytest.mark.asyncio
async def test_corrupt_active_dead_letter_fails_retry_closed_then_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A corrupt active dead letter must not be skipped by the archive step.

    Skipping it leaves no audit, after which the old committed poison line was
    read as the retry's completion: written, never flushed.
    """
    from context_intelligence_server.routers.admin import require_admin
    from tests.test_native_recovery import _write_lease

    app, client, reg = await _lease_app_with_one_quarantined(
        tmp_path, monkeypatch, raise_app_exceptions=False
    )
    dead = _dead_path(tmp_path)
    try:
        _write_lease(tmp_path / "lease.json", "lease-retry")
        valid_dead = dead.read_bytes()
        assert valid_dead.endswith(b"\n")
        offset_before = _offset_text(tmp_path)
        dead.write_bytes(valid_dead[:40] + b"\n")  # terminated but corrupt

        response = await client.post("/admin/recovery/retry-quarantined")

        assert response.status_code == 500, response.text
        assert await app.state.recovery_receipts.status() == {"quarantined": 1}
        assert dead.read_bytes() == valid_dead[:40] + b"\n"  # evidence untouched
        assert not _audit_path(tmp_path).exists()
        assert _offset_text(tmp_path) == offset_before
        assert (await reg.queue_manager.read_batch(SESSION, 5)).records == []
        svc = await _drain_once(reg)
        assert svc.graph.flush.await_count == 0

        dead.write_bytes(valid_dead)  # restore the valid proof
        retried = await client.post("/admin/recovery/retry-quarantined")
        assert retried.json() == {"retried": 1}
        assert await app.state.recovery_receipts.status() == {"enqueued": 1}
        assert len((await reg.queue_manager.read_batch(SESSION, 5)).records) == 1
        svc = await _drain_once(reg)
        assert svc.graph.flush.await_count == 1
        assert await app.state.recovery_receipts.status() == {"written": 1}
    finally:
        app.dependency_overrides.pop(require_admin, None)
        await client.aclose()


def _dead_record(payload: str | None, **extra: Any) -> bytes:
    import json

    record: dict[str, Any] = {"ts": 1.0, "error": "e", **extra}
    if payload is not None:
        record["payload"] = payload
    return (json.dumps(record) + "\n").encode()


def _envelope(origin: dict[str, Any] | None) -> str:
    import json

    body: dict[str, Any] = {"event": "session:start", "data": {"session_id": "s"}}
    if origin is not None:
        body["_recovery_origin"] = origin
    return json.dumps(body)


_VALID_NON_MATCHING_DEAD_LETTERS = {
    "other_valid_origin": _dead_record(
        _envelope({**_V1_ORIGIN, "ordinal": 99, "source_line_sha256": "b" * 64})
    ),
    "non_recovery_event": _dead_record(_envelope(None)),
    "binary_payload": _dead_record(None, payload_b64="AAAA"),
    "blank_line": b"\n",
}

_CORRUPT_DEAD_LETTERS = {
    "invalid_json": b"{not json\n",
    "non_utf8": b"\xff\xfe\n",
    "not_an_object": b"[]\n",
    "no_payload_at_all": b'{"ts": 1.0, "error": "e"}\n',
    "payload_not_a_string": b'{"payload": 7}\n',
    "payload_not_json": b'{"payload": "{broken"}\n',
    "payload_json_not_object": b'{"payload": "7"}\n',
}


@pytest.mark.asyncio
async def test_valid_non_matching_dead_letters_are_not_errors(
    tmp_path: Path,
) -> None:
    """Legitimate dead letters with no matching origin must stay quiet."""
    from context_intelligence_server.queue_manager import QueueManager

    queue = QueueManager(tmp_path / "q")
    dead = tmp_path / "q" / "w.dead.jsonl"
    body = b"".join(_VALID_NON_MATCHING_DEAD_LETTERS.values())
    # An unterminated final fragment is an incomplete append (or an in-flight
    # one) by the queue's framing rule, not a record, and never an error.
    dead.write_bytes(body + b'{"payload": "{part')

    assert await queue.recovery_origin_is_dead(_V1_ORIGIN) is False
    assert await queue.archive_recovery_dead_letters(_V1_ORIGIN) == 0
    assert dead.read_bytes() == body + b'{"payload": "{part'
    assert list((tmp_path / "q").glob(f"*{AUDIT_SUFFIX}")) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", sorted(_CORRUPT_DEAD_LETTERS))
async def test_corrupt_active_dead_letter_is_never_skipped(
    tmp_path: Path, kind: str
) -> None:
    from context_intelligence_server.queue_manager import (
        QueueManager,
        RecoveryReceiptError,
    )

    queue = QueueManager(tmp_path / "q")
    dead = tmp_path / "q" / "w.dead.jsonl"
    matching = _dead_record(_envelope(_V1_ORIGIN))
    dead.write_bytes(_CORRUPT_DEAD_LETTERS[kind] + matching)
    before = dead.read_bytes()

    with pytest.raises(RecoveryReceiptError):
        await queue.recovery_origin_is_dead(_V1_ORIGIN)
    with pytest.raises(RecoveryReceiptError):
        await queue.archive_recovery_dead_letters(_V1_ORIGIN)

    assert dead.read_bytes() == before  # nothing archived, nothing deleted
    assert list((tmp_path / "q").glob(f"*{AUDIT_SUFFIX}")) == []


@pytest.mark.asyncio
async def test_archive_validates_every_dead_file_before_archiving_any(
    tmp_path: Path,
) -> None:
    """A corrupt file later in the scan must not leave a half-done archive."""
    from context_intelligence_server.queue_manager import (
        QueueManager,
        RecoveryReceiptError,
    )

    queue = QueueManager(tmp_path / "q")
    good = tmp_path / "q" / "a.dead.jsonl"
    bad = tmp_path / "q" / "z.dead.jsonl"
    matching = _dead_record(_envelope(_V1_ORIGIN))
    good.write_bytes(matching)
    bad.write_bytes(b"{not json\n")

    with pytest.raises(RecoveryReceiptError):
        await queue.archive_recovery_dead_letters(_V1_ORIGIN)

    assert good.read_bytes() == matching
    assert list((tmp_path / "q").glob(f"*{AUDIT_SUFFIX}")) == []


@pytest.mark.asyncio
async def test_audit_requires_a_canonical_recovery_origin(tmp_path: Path) -> None:
    from context_intelligence_server.queue_manager import QueueManager

    queue = QueueManager(tmp_path / "q")
    audit = tmp_path / "q" / f"w{AUDIT_SUFFIX}"
    legacy = {
        "session_id": "s",
        "ordinal": 0,
        "source_line_sha256": "a" * 64,
        "source_stream_sha256": "b" * 64,
    }
    # Both origin shapes this server writes are accepted as valid evidence.
    for valid in (_V1_ORIGIN, legacy):
        audit.write_bytes(_origin_audit_record(valid))
        assert await queue.recovery_origin_has_retry_audit(valid) is True
        # A valid record for a different origin is readable evidence of nothing.
        elsewhere = {**_V1_ORIGIN, "ordinal": 8}
        assert await queue.recovery_origin_has_retry_audit(elsewhere) is False

    invalid_origins = [
        {**_V1_ORIGIN, "ordinal": -1},
        {**_V1_ORIGIN, "ordinal": True},
        {**_V1_ORIGIN, "ordinal": "3"},
        {**_V1_ORIGIN, "source_line_sha256": "A" * 64},
        {**_V1_ORIGIN, "source_line_sha256": "a" * 63},
        {**_V1_ORIGIN, "source_handle": "x"},
        {k: v for k, v in _V1_ORIGIN.items() if k != "source_line_sha256"},
        {"ordinal": 0, "source_line_sha256": "a" * 64},
        {**legacy, "session_id": " "},
        {**legacy, "source_stream_sha256": "z" * 64},
    ]
    for invalid in invalid_origins:
        audit.write_bytes(_origin_audit_record(invalid))
        with pytest.raises(ValueError):
            await queue.recovery_origin_has_retry_audit(_V1_ORIGIN)

    # An unterminated tail is an incomplete append, not corrupt evidence.
    audit.write_bytes(_origin_audit_record(_V1_ORIGIN) + b'{"payload": "{part')
    assert await queue.recovery_origin_has_retry_audit(_V1_ORIGIN) is True


@pytest.mark.asyncio
async def test_permit_mode_retry_without_audit_fails_closed_and_releases_the_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same invariant through the permit gate, which must not stay locked."""
    from unittest.mock import patch

    from context_intelligence_server.main import (
        _reconcile_native_recovery_outbox,
        app,
        create_asgi_app,
    )
    from context_intelligence_server.queue_manager import RecoveryReceiptError
    from context_intelligence_server.registry import SessionWorker
    from context_intelligence_server.routers.admin import require_admin
    from context_intelligence_server.services import HookStateService

    asgi_app = create_asgi_app(settings=_permit_settings(tmp_path))
    reg = app.state.recovery_registry
    monkeypatch.setattr(reg, "get_or_create", MagicMock())
    app.dependency_overrides[require_admin] = lambda: None
    try:
        assert app.state.recovery_admission_gate is not None  # permit mode
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=asgi_app),
            base_url="http://test",
            headers=_source_headers(),
        ) as client:
            await _admit(client, _recovery_body())
            batch = await reg.queue_manager.read_batch(SESSION, 1)
            worker = SessionWorker(
                session_id=SESSION,
                workspace="one",
                services=HookStateService(workspace="one"),
            )
            with patch(
                "context_intelligence_server.registry.process_event",
                new=AsyncMock(side_effect=ValueError("deterministic poison")),
            ):
                await reg._handle_exhausted_batch(worker, batch, handlers=MagicMock())
            assert await app.state.recovery_receipts.status() == {"quarantined": 1}

            await _stage_retry_pending(app, reg)
            audit = _audit_path(tmp_path)
            valid_audit = audit.read_bytes()
            audit.unlink()

            with pytest.raises(RecoveryReceiptError):
                await asyncio.wait_for(_reconcile_native_recovery_outbox(app), 5)
            assert await app.state.recovery_receipts.status() == {"retry_pending": 1}
            assert (await reg.queue_manager.read_batch(SESSION, 5)).records == []

            # The gate was released by the failure: repair, then it proceeds.
            audit.write_bytes(valid_audit)
            await asyncio.wait_for(_reconcile_native_recovery_outbox(app), 5)
            assert await app.state.recovery_receipts.status() == {"enqueued": 1}
            assert len((await reg.queue_manager.read_batch(SESSION, 5)).records) == 1
            svc = await _drain_once(reg)
            assert svc.graph.flush.await_count == 1
            assert await app.state.recovery_receipts.status() == {"written": 1}
    finally:
        app.dependency_overrides.pop(require_admin, None)
