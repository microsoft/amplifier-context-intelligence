"""Regression tests for PR 126 Copilot recovery review threads.

Covers terminal-record flush eligibility (r4329246576), lease-mode quarantine
retry (r4329246777) and pause/quarantine/resume serialization (r4329246839).
All graph access is mocked; storage is isolated under ``tmp_path``.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import stat
import subprocess
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, Literal, Self, cast
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from context_intelligence_server.config import Settings
from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.recovery import (
    RecoveryReceiptStore,
    _sanitize_stored_payload,
    source_descriptor_digest,
)
from context_intelligence_server.services import GraphState
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
    # Eligibility never discards.  The terminal batch's finalize close is the
    # one recovery ``_safe_close`` and discards its (already flushed, empty)
    # buffer exactly once.
    assert out["discard_calls"] == (1 if event == "session:end" else 0), out
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
    """Swap the pause condition's lock, the quarantine/retry lock and the outbox
    lock for tracing locks.  The recovery registry holds the quarantine/retry
    lock object directly, so it is re-pointed too.
    """
    log: list[tuple[str, str, tuple[str, ...]]] = []
    held: dict = {}
    app.state.recovery_pause_condition = asyncio.Condition(
        _TracingLock("condition", log, held)
    )
    shared = _TracingLock("shared", log, held)
    app.state.recovery_quarantine_retry_lock = shared
    app.state.recovery_registry.quarantine_retry_lock = shared
    app.state.recovery_outbox_lock = _TracingLock("outbox", log, held)
    return log


def _order_violations(
    log: list[tuple[str, str, tuple[str, ...]]],
) -> list[tuple[str, str, tuple[str, ...]]]:
    """Any acquisition of the condition by a task already holding the outbox or
    the quarantine/retry lock."""
    return [
        entry
        for entry in log
        if entry[1] == "condition" and ({"outbox", "shared"} & set(entry[2]))
    ]


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

    log.clear()
    shared = _TracingLock("shared", log, held)
    async with shared, condition:
        pass
    assert _order_violations(log) == [("acquire", "condition", ("shared",))]


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
    # quarantine: condition -> shared -> outbox, in that order
    assert ("acquire", "outbox", ("condition", "shared")) in log, log
    assert ("acquire", "outbox", ()) in log, log  # admission / reconcile
    assert ("acquire", "shared", ("condition",)) in log, log  # quarantine
    assert ("acquire", "shared", ()) in log, log  # reconcile
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


# ---------------------------------------------------------------------------
# Lane B: stored-payload sanitizer, legacy-origin matching, permission-
# preserving receipt rebuilds, and legacy queue-key validation.
# ---------------------------------------------------------------------------

_SAFE_DATA = {"session_id": "s-1", "timestamp": "2026-01-01T00:00:00+00:00"}
_SCRUBBED = {
    "data": _SAFE_DATA,
    "event": "probe:upgrade",
    "idempotency_key": None,
    "workspace": "synthetic",
}


def _canonical_sha(payload: dict[str, Any]) -> str:
    """Hand-derived digest of the API's canonical receipt payload encoding."""
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@pytest.mark.parametrize(
    "payload",
    [
        {**_SCRUBBED, "working_dir": None},
        {**_SCRUBBED, "working_dir": "/home/u/repo"},
        {**_SCRUBBED, "data": {**_SAFE_DATA, "working_dir": "/home/u/repo"}},
        {
            **_SCRUBBED,
            "data": {**_SAFE_DATA, "items": [{"working_dir": "/x", "k": 1}]},
        },
    ],
    ids=["null", "string", "nested-in-data", "nested-in-list"],
)
def test_sanitizer_scrubs_working_dir_without_marking_the_payload_unsafe(
    payload: dict[str, Any],
) -> None:
    """A valid legacy receipt must not be quarantined for carrying a path.

    Breaks if: the "changed" signal is reported as "unsafe for replay".
    """
    sanitized, unsafe = _sanitize_stored_payload(payload)

    assert unsafe is False
    assert "working_dir" not in json.dumps(sanitized)
    assert sanitized["event"] == "probe:upgrade"
    assert sanitized["data"]["session_id"] == "s-1"


@pytest.mark.parametrize(
    "payload",
    [
        {"event": "e"},
        {"event": "e", "data": "text"},
        {"event": "e", "data": {"timestamp": "t"}},
        {"event": "e", "data": {"session_id": "bad\tid"}},
        {"event": "e", "data": {"session_id": "a/b"}},
        {"event": "e", "data": {"session_id": ""}},
        {"event": "e", "data": {"session_id": "bad\nid"}, "working_dir": "/x"},
    ],
    ids=[
        "no-data",
        "data-not-dict",
        "no-session-id",
        "tab-session-id",
        "slash-session-id",
        "empty-session-id",
        "newline-session-id-and-path",
    ],
)
def test_sanitizer_flags_unsafe_payloads_and_scrubs_session_id_and_path(
    payload: dict[str, Any],
) -> None:
    sanitized, unsafe = _sanitize_stored_payload(payload)

    assert unsafe is True
    assert isinstance(sanitized, dict)
    assert "working_dir" not in json.dumps(sanitized)
    data = sanitized.get("data")
    if isinstance(data, dict):
        assert "session_id" not in data


def test_sanitizer_rejects_non_object_payloads_as_unsafe() -> None:
    assert _sanitize_stored_payload(["not", "an", "object"]) == ({}, True)


_DESCRIPTOR = {
    "protocol": "native-recovery-source-v1",
    "session_id": "s-1",
    "source_stream_sha256": "a" * 64,
    "source_sha256": "b" * 64,
    "record_count": 1,
}
_HANDLE = "c" * 64


def _create_current_db(path: Path, rows: list[tuple[dict[str, Any], str]]) -> None:
    db = sqlite3.connect(path)
    try:
        RecoveryReceiptStore._create_current_schema_locked(db)
        db.execute(
            "INSERT INTO recovery_sources VALUES (?, ?, ?)",
            (
                _HANDLE,
                source_descriptor_digest(_DESCRIPTOR),
                json.dumps(_DESCRIPTOR, sort_keys=True, separators=(",", ":")),
            ),
        )
        for ordinal, (payload, state) in enumerate(rows):
            db.execute(
                "INSERT INTO recovery_receipts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    _HANDLE,
                    ordinal,
                    f"{ordinal:064x}",
                    "alice",
                    "synthetic",
                    _canonical_sha(payload),
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                    state,
                ),
            )
        db.commit()
    finally:
        db.close()


def _read_receipts(path: Path) -> list[tuple[Any, ...]]:
    db = sqlite3.connect(path)
    try:
        return db.execute(
            "SELECT ordinal, state, payload_sha256, payload "
            "FROM recovery_receipts ORDER BY ordinal"
        ).fetchall()
    finally:
        db.close()


def test_current_database_detection_flags_scrub_only_rows_and_ignores_clean_ones(
    tmp_path: Path,
) -> None:
    """A valid-but-path-bearing row must still trigger the scrub rebuild.

    Breaks if: detection is narrowed to "unsafe" only, leaving paths in the DB.
    """
    clean, dirty = tmp_path / "clean.sqlite3", tmp_path / "dirty.sqlite3"
    _create_current_db(clean, [(_SCRUBBED, "enqueued")])
    _create_current_db(dirty, [({**_SCRUBBED, "working_dir": "/h/u"}, "enqueued")])

    assert (
        RecoveryReceiptStore(clean)._current_database_needs_sanitization_locked()
        is False
    )
    assert (
        RecoveryReceiptStore(dirty)._current_database_needs_sanitization_locked()
        is True
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state",
    ["pending_enqueue", "enqueued", "retry_pending", "committed_pending", "written"],
)
@pytest.mark.parametrize("working_dir", [None, "/home/u/repo"], ids=["null", "path"])
async def test_current_database_scrub_keeps_valid_receipt_state_and_refreshes_digest(
    tmp_path: Path, state: str, working_dir: str | None
) -> None:
    path = tmp_path / "receipts.sqlite3"
    _create_current_db(path, [({**_SCRUBBED, "working_dir": working_dir}, state)])

    await RecoveryReceiptStore(path).status()

    [(ordinal, final_state, digest, encoded)] = _read_receipts(path)
    assert (ordinal, final_state) == (0, state)
    assert json.loads(encoded) == _SCRUBBED
    assert "working_dir" not in encoded and "/home/u" not in encoded
    assert digest == _canonical_sha(_SCRUBBED)


@pytest.mark.asyncio
async def test_current_database_scrub_never_revives_a_quarantined_receipt(
    tmp_path: Path,
) -> None:
    path = tmp_path / "receipts.sqlite3"
    _create_current_db(
        path, [({**_SCRUBBED, "working_dir": "/home/u/repo"}, "quarantined")]
    )

    await RecoveryReceiptStore(path).status()

    [(_, state, _, encoded)] = _read_receipts(path)
    assert state == "quarantined"
    assert "working_dir" not in encoded


@pytest.mark.asyncio
async def test_current_database_scrub_quarantines_pending_receipt_with_unsafe_session_id(
    tmp_path: Path,
) -> None:
    path = tmp_path / "receipts.sqlite3"
    unsafe = {**_SCRUBBED, "data": {"session_id": "bad\tid"}, "working_dir": "/x"}
    _create_current_db(path, [(unsafe, "pending_enqueue")])

    await RecoveryReceiptStore(path).status()

    [(_, state, _, encoded)] = _read_receipts(path)
    assert state == "quarantined"
    assert "session_id" not in json.loads(encoded)["data"]
    assert "working_dir" not in encoded


# --- SPEC2: legacy-origin matching across every durable-queue consumer -------

_LEGACY_HANDLE_DOMAIN = b"context-intelligence/recovery/legacy-source/v1\x00"
_LEGACY = {
    "session_id": "legacy-s",
    "source_stream_sha256": "b" * 64,
    "ordinal": 3,
    "source_line_sha256": "a" * 64,
}


def _v1_origin_of(legacy: dict[str, Any]) -> dict[str, Any]:
    """The v1 origin a migrated receipt presents for a pre-v1 spool line."""
    descriptor = {
        "protocol": "legacy-recovery-lease-v1",
        "session_id": legacy["session_id"],
        "source_stream_sha256": legacy["source_stream_sha256"],
        "source_sha256": legacy["source_stream_sha256"],
        "record_count": 0,
    }
    encoded = json.dumps(descriptor, sort_keys=True, separators=(",", ":")).encode()
    return {
        "source_handle": hashlib.sha256(_LEGACY_HANDLE_DOMAIN + encoded).hexdigest(),
        "ordinal": legacy["ordinal"],
        "source_line_sha256": legacy["source_line_sha256"],
    }


def _legacy_line(legacy: dict[str, Any]) -> bytes:
    return json.dumps(
        {
            "event": "e",
            "data": {"session_id": "legacy-s"},
            "created_by": "alice",
            "_recovery_origin": legacy,
        },
        separators=(",", ":"),
    ).encode()


@pytest.mark.asyncio
async def test_every_origin_consumer_recognizes_a_legacy_spool_line_for_its_v1_origin(
    tmp_path: Path,
) -> None:
    """Presence, commit state, dead letters, audits and archival share a matcher.

    Breaks if: any consumer compares the v1 origin to a pre-v1 line byte-for-byte.
    """
    queue = QueueManager(tmp_path / "q")
    raw = _legacy_line(_LEGACY)
    v1 = _v1_origin_of(_LEGACY)
    await queue.append("legacy-s", raw)

    assert await queue.contains_recovery_origin(v1) is True
    assert await queue.recovery_origin_commit_state(v1) == "queued"

    batch = await queue.read_batch("legacy-s", 5)
    await queue.commit("legacy-s", batch.end_offset)
    assert await queue.recovery_origin_commit_state(v1) == "committed"

    await queue.dead_letter("legacy-s", raw, "poison")
    assert await queue.recovery_origin_is_dead(v1) is True
    assert await queue.recovery_origin_has_retry_audit(v1) is False
    assert await queue.archive_recovery_dead_letters(v1) == 1
    assert await queue.recovery_origin_has_retry_audit(v1) is True
    assert await queue.recovery_origin_is_dead(v1) is False
    # The old spool bytes keep their identity: nothing rewrote the line.
    assert (tmp_path / "q" / "legacy-s.log").read_bytes() == raw + b"\n"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_stream_sha256", "c" * 64),
        ("session_id", "other-s"),
        ("ordinal", 4),
        ("source_line_sha256", "d" * 64),
    ],
)
async def test_legacy_spool_line_never_matches_a_v1_origin_differing_in_one_field(
    tmp_path: Path, field: str, value: Any
) -> None:
    queue = QueueManager(tmp_path / "q")
    raw = _legacy_line(_LEGACY)
    other = _v1_origin_of({**_LEGACY, field: value})
    await queue.append("legacy-s", raw)
    await queue.dead_letter("legacy-s", raw, "poison")

    assert await queue.contains_recovery_origin(other) is False
    assert await queue.recovery_origin_commit_state(other) is None
    assert await queue.recovery_origin_is_dead(other) is False
    assert await queue.recovery_origin_has_retry_audit(other) is False
    assert await queue.archive_recovery_dead_letters(other) == 0
    # Control: the same line is found by its own origin.
    assert await queue.contains_recovery_origin(_v1_origin_of(_LEGACY)) is True


@pytest.mark.asyncio
async def test_uncommitted_v1_line_wins_over_a_committed_legacy_line_for_one_receipt(
    tmp_path: Path,
) -> None:
    queue = QueueManager(tmp_path / "q")
    v1 = _v1_origin_of(_LEGACY)
    await queue.append("legacy-s", _legacy_line(_LEGACY))
    batch = await queue.read_batch("legacy-s", 5)
    await queue.commit("legacy-s", batch.end_offset)
    replay = json.dumps(
        {"event": "e", "data": {"session_id": "legacy-s"}, "_recovery_origin": v1},
        separators=(",", ":"),
    ).encode()
    await queue.append("legacy-s", replay)

    assert await queue.recovery_origin_commit_state(v1) == "queued"


# --- SPEC3: permission-preserving receipt-database rebuilds ------------------

_LEGACY_TABLE_SQL = """CREATE TABLE recovery_receipts (
    source_session_id TEXT NOT NULL,
    source_stream_sha256 TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    source_line_sha256 TEXT NOT NULL,
    actor TEXT NOT NULL, workspace TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL, payload TEXT NOT NULL,
    state TEXT NOT NULL,
    PRIMARY KEY(source_session_id, source_stream_sha256, ordinal)
)"""


def _create_legacy_db(
    path: Path,
    rows: list[tuple[dict[str, Any], str, str, dict[str, Any] | str, str]],
    leases: tuple[str, ...] = (),
) -> None:
    """Create a base-format receipt DB from SQL mirroring the base-release schema.

    The SQL constructs the schema and rows directly; no endpoint is run.

    A ``str`` payload is stored verbatim, so a test can plant a malformed one.
    """
    db = sqlite3.connect(path)
    try:
        db.execute(_LEGACY_TABLE_SQL)
        db.execute(
            "CREATE TABLE recovery_lease_consumptions (lease_id TEXT PRIMARY KEY)"
        )
        for lease_id in leases:
            db.execute(
                "INSERT INTO recovery_lease_consumptions VALUES (?)", (lease_id,)
            )
        for origin, actor, workspace, payload, state in rows:
            encoded = (
                payload
                if isinstance(payload, str)
                else json.dumps(payload, sort_keys=True, separators=(",", ":"))
            )
            db.execute(
                "INSERT INTO recovery_receipts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    origin["session_id"],
                    origin["source_stream_sha256"],
                    origin["ordinal"],
                    origin["source_line_sha256"],
                    actor,
                    workspace,
                    hashlib.sha256(encoded.encode()).hexdigest(),
                    encoded,
                    state,
                ),
            )
        db.commit()
    finally:
        db.close()


_UPGRADE_SESSION = "upgrade-session"
_UPGRADE_ORIGIN = {
    "session_id": _UPGRADE_SESSION,
    "source_stream_sha256": "a" * 64,
    "ordinal": 0,
    "source_line_sha256": "b" * 64,
}


def _base_payload(variant: str) -> dict[str, Any]:
    """Base-format receipt payload (shape captured from base-release receipts)."""
    payload: dict[str, Any] = {
        "data": {
            "session_id": _UPGRADE_SESSION,
            "timestamp": "2026-01-01T00:00:00+00:00",
        },
        "event": "probe:upgrade",
        "idempotency_key": None,
        "workspace": "synthetic",
    }
    if variant == "null":
        payload["working_dir"] = None
    elif variant == "path":
        payload["working_dir"] = "/home/someone/private/checkout"
    return payload


def _seed_rebuild_database(path: Path, kind: str) -> None:
    if kind == "legacy":
        _create_legacy_db(
            path,
            [
                (
                    _UPGRADE_ORIGIN,
                    "alice",
                    "synthetic",
                    _base_payload("path"),
                    "enqueued",
                )
            ],
        )
    else:
        _create_current_db(path, [({**_SCRUBBED, "working_dir": "/h/u"}, "enqueued")])


_REBUILD_SCRIPT = """
import asyncio, os, stat, sys
from pathlib import Path
os.umask(int(sys.argv[2], 8))
from context_intelligence_server.recovery import RecoveryReceiptStore
path = Path(sys.argv[1])
asyncio.run(RecoveryReceiptStore(path).status())
print(oct(stat.S_IMODE(path.stat().st_mode)))
"""


@pytest.mark.parametrize("kind", ["legacy", "current"])
@pytest.mark.parametrize("mode", [0o600, 0o640, 0o644])
@pytest.mark.parametrize("umask", ["022", "077"])
def test_rebuild_preserves_the_original_database_permission_bits(
    tmp_path: Path, kind: str, mode: int, umask: str
) -> None:
    """A migration must not widen (0600 -> 0644) or narrow the DB's mode.

    The umask is set in an isolated subprocess so no test leaks process state.
    """
    path = tmp_path / "receipts.sqlite3"
    _seed_rebuild_database(path, kind)
    os.chmod(path, mode)
    repo_root = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [sys.executable, "-c", _REBUILD_SCRIPT, str(path), umask],
        cwd=tmp_path,
        env={
            "PATH": "/usr/bin:/bin",
            "HOME": str(tmp_path),
            "PYTHONPATH": str(repo_root),
            "PYTHONDONTWRITEBYTECODE": "1",
        },
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == oct(mode)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["legacy", "current"])
async def test_replacement_file_has_the_original_mode_before_sqlite_opens_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """The mode is fixed before any payload is copied into the replacement."""
    path = tmp_path / "receipts.sqlite3"
    _seed_rebuild_database(path, kind)
    os.chmod(path, 0o640)
    observed: list[int | None] = []
    real_connect = sqlite3.connect

    def spy(database: Any, *args: Any, **kwargs: Any) -> sqlite3.Connection:
        if str(database).endswith(".tmp"):
            try:
                observed.append(stat.S_IMODE(os.stat(database).st_mode))
            except FileNotFoundError:
                observed.append(None)
        return real_connect(database, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", spy)

    await RecoveryReceiptStore(path).status()

    assert observed[:1] == [0o640]


def _assert_original_untouched(tmp_path: Path, path: Path, before: Any) -> None:
    assert stat.S_IMODE(path.stat().st_mode) == 0o640
    assert _read_all(path) == before
    # No replacement temporary and no SQLite sidecar survives the failure.
    assert sorted(entry.name for entry in tmp_path.iterdir()) == [path.name]


def _read_all(path: Path) -> list[tuple[Any, ...]]:
    db = sqlite3.connect(path)
    try:
        return db.execute("SELECT * FROM recovery_receipts ORDER BY 1, 2, 3").fetchall()
    finally:
        db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["legacy", "current"])
async def test_failed_replacement_preparation_leaves_the_original_intact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    path = tmp_path / "receipts.sqlite3"
    _seed_rebuild_database(path, kind)
    os.chmod(path, 0o640)
    before = _read_all(path)

    def boom(self: RecoveryReceiptStore, temporary: Path) -> None:
        raise RuntimeError("prepare boom")

    monkeypatch.setattr(RecoveryReceiptStore, "_prepare_replacement_locked", boom)

    with pytest.raises(RuntimeError, match="prepare boom") as excinfo:
        await RecoveryReceiptStore(path).status()

    # ``excinfo`` keeps the traceback, and so the abandoned rebuild frame and
    # anything it still references, alive while the original is re-read.
    _assert_original_untouched(tmp_path, path, before)
    assert excinfo.traceback


def _legacy_row(
    ordinal: int, payload: dict[str, Any] | str
) -> tuple[dict[str, Any], str, str, dict[str, Any] | str, str]:
    origin = {
        **_UPGRADE_ORIGIN,
        "ordinal": ordinal,
        "source_line_sha256": f"{ordinal:064x}",
    }
    return (origin, "alice", "synthetic", payload, "enqueued")


@pytest.mark.asyncio
async def test_legacy_rebuild_failing_mid_receipt_stream_leaves_original_unlocked(
    tmp_path: Path,
) -> None:
    """A failure while the receipt cursor is half-read must not hold the lock.

    Breaks if: the streamed receipt cursor is left unfinalized, so the legacy
    database stays locked for as long as the exception's traceback lives.  A
    row must still be pending after the bad one: the cursor steps ahead, so a
    failure on the LAST row leaves no open statement and proves nothing.
    """
    path = tmp_path / "receipts.sqlite3"
    _create_legacy_db(
        path,
        [
            _legacy_row(0, _base_payload("path")),
            _legacy_row(1, "{not json"),
            _legacy_row(2, _base_payload("path")),
        ],
    )
    os.chmod(path, 0o640)
    before = _read_all(path)

    with pytest.raises(
        RuntimeError, match="malformed legacy recovery receipt payload"
    ) as excinfo:
        await RecoveryReceiptStore(path).status()

    _assert_original_untouched(tmp_path, path, before)
    assert excinfo.traceback


@pytest.mark.asyncio
async def test_legacy_rebuild_failing_mid_lease_stream_leaves_original_unlocked(
    tmp_path: Path,
) -> None:
    """Same invariant for the lease-consumption cursor, half-read at the failure."""
    path = tmp_path / "receipts.sqlite3"
    _create_legacy_db(path, [_legacy_row(0, _base_payload("path"))], ("", "lease-2"))
    os.chmod(path, 0o640)
    before = _read_all(path)

    with pytest.raises(
        RuntimeError, match="invalid recovery lease consumption"
    ) as excinfo:
        await RecoveryReceiptStore(path).status()

    _assert_original_untouched(tmp_path, path, before)
    assert excinfo.traceback


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["legacy", "current"])
async def test_failure_just_before_replace_leaves_the_original_intact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    path = tmp_path / "receipts.sqlite3"
    _seed_rebuild_database(path, kind)
    os.chmod(path, 0o640)
    before = _read_all(path)
    real_replace = os.replace

    def failing(src: Any, dst: Any, *args: Any, **kwargs: Any) -> None:
        if str(src).endswith(".tmp"):
            raise OSError("replace boom")
        real_replace(src, dst, *args, **kwargs)

    monkeypatch.setattr(os, "replace", failing)

    with pytest.raises(OSError, match="replace boom"):
        await RecoveryReceiptStore(path).status()

    _assert_original_untouched(tmp_path, path, before)


# --- SPEC4: legacy queue keys must not abort startup recovery ----------------


@pytest.mark.asyncio
async def test_recover_skips_legacy_control_character_keys_and_leaves_them_on_disk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    queue_dir = tmp_path / "q"
    queue_dir.mkdir()
    for name in ("\tbad-tab", "bad\nnewline", "aa-valid", "zz-valid"):
        (queue_dir / f"{name}.log").write_bytes(b'{"event":"e"}\n')
    (queue_dir / "\tbad-tab.offset").write_text("0\n", encoding="utf-8")
    before = {entry.name: entry.read_bytes() for entry in queue_dir.iterdir()}
    queue = QueueManager(queue_dir)
    offset_reads: list[str] = []
    real_read = QueueManager._read_committed_offset

    def spy(self: QueueManager, session_id: str) -> int:
        offset_reads.append(session_id)
        return real_read(self, session_id)

    monkeypatch.setattr(QueueManager, "_read_committed_offset", spy)

    with caplog.at_level(logging.ERROR):
        recovered = await queue.recover()

    assert recovered == ["aa-valid", "zz-valid"]
    # The key is validated BEFORE any offset or log state is read for it.
    assert offset_reads == ["aa-valid", "zz-valid"]
    assert {entry.name: entry.read_bytes() for entry in queue_dir.iterdir()} == before
    assert "\\tbad-tab" in caplog.text and "bad\\nnewline" in caplog.text
    assert "\tbad-tab" not in caplog.text and "bad\nnewline" not in caplog.text


@pytest.mark.asyncio
async def test_invalid_legacy_keys_cannot_consume_the_startup_respawn_cap(
    tmp_path: Path,
) -> None:
    queue_dir = tmp_path / "q"
    queue_dir.mkdir()
    for name in ("\t1", "\t2", "\t3", "aa", "bb", "cc"):
        (queue_dir / f"{name}.log").write_bytes(b'{"event":"e"}\n')

    recovered = await QueueManager(queue_dir).recover()

    # Invalid keys sort first; a finite cap of 2 must still reach healthy keys.
    assert recovered == ["aa", "bb", "cc"]
    assert recovered[:2] == ["aa", "bb"]


@pytest.mark.asyncio
async def test_reclaim_orphans_escapes_invalid_legacy_keys_in_its_failure_log(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A control-character key must not forge or split a log line.

    Breaks if: the failure log interpolates the raw key (``%s``) instead of an
    escaped one, or the sweep touches the invalid file.
    """
    queue_dir = tmp_path / "q"
    queue_dir.mkdir()
    (queue_dir / "bad\tkey.log").write_bytes(b'{"event":"e"}\n')
    queue = QueueManager(queue_dir)

    with caplog.at_level(logging.WARNING):
        await queue.reclaim_drained_orphans()

    assert "reclaim_drained_orphans_key_failed key='bad\\tkey'" in caplog.text
    assert "bad\tkey" not in caplog.text
    assert (queue_dir / "bad\tkey.log").read_bytes() == b'{"event":"e"}\n'


# --- SPEC1+2 end to end: legacy receipts across two head restarts ------------


class _RecordingGraph(GraphState):
    """In-memory graph that records the Event node ids present at each flush."""

    def __init__(self, workspace: str) -> None:
        super().__init__(workspace)
        self.flushes: list[list[str]] = []

    async def flush(self) -> None:
        self.flushes.append(
            sorted(
                key
                for key, data in self._nodes.items()
                if "Event" in data.get("labels", [])
            )
        )


async def _seed_base_spool(
    tmp_path: Path, variant: str, *, committed: bool = False, dead: bool = False
) -> tuple[str, bytes]:
    """Write an old-format spool line shaped like the base release's appends."""
    from context_intelligence_server.main import _recovery_worker_key

    queue = QueueManager(tmp_path / "recovery")
    key = _recovery_worker_key(_UPGRADE_SESSION)
    line = json.dumps(
        {
            **_base_payload(variant),
            "created_by": "alice",
            "_recovery_origin": _UPGRADE_ORIGIN,
        },
        separators=(",", ":"),
    ).encode()
    await queue.append(key, line)
    if committed or dead:
        batch = await queue.read_batch(key, 5)
        if dead:
            await queue.dead_letter(key, line, "poison")
        await queue.commit(key, batch.end_offset)
    return key, line + b"\n"


async def _head_restart(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    from context_intelligence_server.main import (
        _resume_native_recovery,
        app,
        create_asgi_app,
    )

    create_asgi_app(settings=_permit_settings(tmp_path))
    registry = app.state.recovery_registry
    monkeypatch.setattr(registry, "get_or_create", MagicMock())
    await _resume_native_recovery(app)
    return app


async def _drain_recovery(app: Any) -> list[list[str]]:
    from context_intelligence_server.main import _recovery_worker_key
    from context_intelligence_server.pipeline import setup_handlers
    from context_intelligence_server.registry import SessionWorker
    from context_intelligence_server.services import HookStateService

    services = HookStateService(workspace="synthetic")
    graph = _RecordingGraph("synthetic")
    services.graph = graph
    worker = SessionWorker(
        session_id=_recovery_worker_key(_UPGRADE_SESSION),
        workspace="synthetic",
        services=services,
    )
    registry = app.state.recovery_registry
    assert await registry._drain_to_eof(worker, setup_handlers(services))
    return graph.flushes


def _spool_lines(tmp_path: Path) -> list[bytes]:
    lines: list[bytes] = []
    for log in sorted((tmp_path / "recovery").glob("*.log")):
        lines += log.read_bytes().splitlines(keepends=True)
    return lines


_SCENARIOS = {
    # scenario: (receipt state, spool kind, flushes the drain must perform)
    "pending_enqueue_nospool": ("pending_enqueue", None, 1),
    "pending_enqueue_oldspool": ("pending_enqueue", "uncommitted", 1),
    "enqueued_uncommitted": ("enqueued", "uncommitted", 1),
    "enqueued_committed": ("enqueued", "committed", 0),
    "committed_pending": ("committed_pending", "committed", 0),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("variant", ["absent", "null", "path"])
@pytest.mark.parametrize("scenario", list(_SCENARIOS))
async def test_legacy_receipt_upgrade_finishes_once_across_two_head_restarts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scenario: str, variant: str
) -> None:
    """A base-format receipt fixture is neither quarantined nor duplicated.

    Breaks if: a path-only scrub quarantines a valid receipt, or a legacy spool
    line is invisible to reconciliation and so appended a second time.
    """
    from context_intelligence_server.main import _retry_quarantined_native_recovery

    state, spool, expected_flushes = _SCENARIOS[scenario]
    receipts = tmp_path / "receipts.sqlite3"
    _create_legacy_db(
        receipts,
        [(_UPGRADE_ORIGIN, "alice", "synthetic", _base_payload(variant), state)],
    )
    old_line = None
    if spool is not None:
        _, old_line = await _seed_base_spool(
            tmp_path, variant, committed=spool == "committed"
        )

    app = await _head_restart(tmp_path, monkeypatch)
    flushes = await _drain_recovery(app)

    assert await app.state.recovery_receipts.status() == {"written": 1}
    assert len(flushes) == expected_flushes
    lines = _spool_lines(tmp_path)
    if old_line is None:
        assert len(lines) == 1  # exactly one fresh append for the lost spool line
    else:
        assert lines == [old_line]  # no duplicate; old bytes keep their identity
    flushed_event_ids = {event_id for flush in flushes for event_id in flush}
    assert len(flushed_event_ids) == expected_flushes

    # Second head restart: nothing further is appended, flushed, or retried.
    lines_before_restart = _spool_lines(tmp_path)
    app = await _head_restart(tmp_path, monkeypatch)
    assert await _drain_recovery(app) == []
    assert await app.state.recovery_receipts.status() == {"written": 1}
    assert _spool_lines(tmp_path) == lines_before_restart
    assert not await _retry_quarantined_native_recovery(app)
    assert _spool_lines(tmp_path) == lines_before_restart


@pytest.mark.asyncio
@pytest.mark.parametrize("variant", ["absent", "null", "path"])
async def test_legacy_dead_lettered_line_stays_quarantined_and_is_not_requeued(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, variant: str
) -> None:
    """Poison evidence written under the legacy origin still fences the receipt."""
    receipts = tmp_path / "receipts.sqlite3"
    _create_legacy_db(
        receipts,
        [(_UPGRADE_ORIGIN, "alice", "synthetic", _base_payload(variant), "enqueued")],
    )
    _, old_line = await _seed_base_spool(tmp_path, variant, dead=True)

    for _ in range(2):  # two head restarts
        app = await _head_restart(tmp_path, monkeypatch)
        assert await _drain_recovery(app) == []
        assert await app.state.recovery_receipts.status() == {"quarantined": 1}
        assert _spool_lines(tmp_path) == [old_line]


# --- Origin shapes are matched exactly -------------------------------------------

_LEG1 = {**_LEGACY, "ordinal": 1}
_WANT1 = _v1_origin_of(_LEG1)


def _without(origin: dict[str, Any], key: str) -> dict[str, Any]:
    return {k: v for k, v in origin.items() if k != key}


# Every shape reuses the handle, ordinal and line digest of ``_WANT1`` so that a
# matcher which looks only at those fields would wrongly accept it.
_MALFORMED_ORIGINS: dict[str, Any] = {
    "origin-null": None,
    "origin-text": "not-an-origin",
    "hybrid-conflicting-legacy-fields": {
        **_WANT1,
        "session_id": "other-s",
        "source_stream_sha256": "e" * 64,
    },
    "hybrid-consistent-legacy-fields": {
        **_WANT1,
        "session_id": _LEG1["session_id"],
        "source_stream_sha256": _LEG1["source_stream_sha256"],
    },
    "v1-unknown-extra-field": {**_WANT1, "unrelated": "x"},
    "legacy-unknown-extra-field": {**_LEG1, "unrelated": "x"},
    "legacy-carrying-a-handle": {**_LEG1, "source_handle": _WANT1["source_handle"]},
    "v1-missing-ordinal": _without(_WANT1, "ordinal"),
    "v1-missing-line-sha": _without(_WANT1, "source_line_sha256"),
    "legacy-missing-stream-sha": _without(_LEG1, "source_stream_sha256"),
    "v1-ordinal-as-string": {**_WANT1, "ordinal": "1"},
    "v1-ordinal-as-bool": {**_WANT1, "ordinal": True},
    "legacy-ordinal-as-bool": {**_LEG1, "ordinal": True},
    "v1-line-sha-malformed": {**_WANT1, "source_line_sha256": "A" * 64},
}


def _snapshot(directory: Path) -> dict[str, bytes]:
    return {
        entry.name: entry.read_bytes()
        for entry in sorted(directory.iterdir())
        if entry.is_file()
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", list(_MALFORMED_ORIGINS))
async def test_malformed_origin_shapes_never_match_queue_lines(
    tmp_path: Path, shape: str
) -> None:
    """A spool line with a malformed origin is simply not a match.

    Breaks if: the validator accepts a hybrid or extra-key origin, so a
    record whose legacy fields conflict with the receipt is matched.
    """
    queue_dir = tmp_path / "q"
    queue = QueueManager(queue_dir)
    raw = json.dumps(
        {
            "event": "e",
            "data": {"session_id": "legacy-s"},
            "_recovery_origin": _MALFORMED_ORIGINS[shape],
        },
        separators=(",", ":"),
    ).encode()
    await queue.append("legacy-s", raw)
    before = _snapshot(queue_dir)

    assert await queue.contains_recovery_origin(_WANT1) is False
    assert await queue.recovery_origin_commit_state(_WANT1) is None
    assert await queue.recovery_origin_has_retry_audit(_WANT1) is False
    assert _snapshot(queue_dir) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", list(_MALFORMED_ORIGINS))
async def test_malformed_origin_in_an_active_dead_letter_fails_closed(
    tmp_path: Path, shape: str
) -> None:
    """A dead letter whose origin key is present but malformed is not evidence.

    It may be the very record that proves a receipt was quarantined, so reading
    it as "absent" would let a committed line be marked written. Both the lookup
    and archival raise, and no file is mutated.
    """
    from context_intelligence_server.queue_manager import RecoveryReceiptError

    queue_dir = tmp_path / "q"
    queue = QueueManager(queue_dir)
    raw = json.dumps(
        {
            "event": "e",
            "data": {"session_id": "legacy-s"},
            "_recovery_origin": _MALFORMED_ORIGINS[shape],
        },
        separators=(",", ":"),
    ).encode()
    await queue.dead_letter("legacy-s", raw, "poison")
    before = _snapshot(queue_dir)

    with pytest.raises(RecoveryReceiptError):
        await queue.recovery_origin_is_dead(_WANT1)
    with pytest.raises(RecoveryReceiptError):
        await queue.archive_recovery_dead_letters(_WANT1)

    assert _snapshot(queue_dir) == before


@pytest.mark.asyncio
async def test_dead_letter_without_an_origin_key_is_still_not_an_error(
    tmp_path: Path,
) -> None:
    """A dead-lettered non-recovery event legitimately carries no origin."""
    queue_dir = tmp_path / "q"
    queue = QueueManager(queue_dir)
    raw = json.dumps({"event": "e", "data": {"session_id": "legacy-s"}}).encode()
    await queue.dead_letter("legacy-s", raw, "poison")
    before = _snapshot(queue_dir)

    assert await queue.recovery_origin_is_dead(_WANT1) is False
    assert await queue.archive_recovery_dead_letters(_WANT1) == 0
    assert _snapshot(queue_dir) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", list(_MALFORMED_ORIGINS))
async def test_malformed_origin_shapes_fail_closed_in_the_retry_audit(
    tmp_path: Path, shape: str
) -> None:
    queue_dir = tmp_path / "q"
    queue = QueueManager(queue_dir)
    audit = queue_dir / f"legacy-s{AUDIT_SUFFIX}"
    audit.write_bytes(_origin_audit_record(_MALFORMED_ORIGINS[shape]))
    before = _snapshot(queue_dir)

    with pytest.raises(ValueError):
        await queue.recovery_origin_has_retry_audit(_WANT1)

    assert _snapshot(queue_dir) == before


# --- Replay-envelope validation ----------------------------------------------


def _valid_envelope() -> dict[str, Any]:
    return {
        "event": "e",
        "workspace": "synthetic",
        "data": {"session_id": "s-1", "timestamp": "2026-01-01T00:00:00+00:00"},
        "idempotency_key": None,
    }


def _with_data(payload: dict[str, Any], **changes: Any) -> dict[str, Any]:
    return {**payload, "data": {**payload["data"], **changes}}


_BROKEN_ENVELOPES: dict[str, Any] = {
    "no-event": lambda p: _without(p, "event"),
    "event-not-string": lambda p: {**p, "event": 5},
    "no-workspace": lambda p: _without(p, "workspace"),
    "workspace-blank": lambda p: {**p, "workspace": "   "},
    "workspace-not-string": lambda p: {**p, "workspace": 7},
    "data-not-object": lambda p: {**p, "data": ["s-1"]},
    "no-timestamp": lambda p: {**p, "data": {"session_id": "s-1"}},
    "timestamp-blank": lambda p: _with_data(p, timestamp="  "),
    "timestamp-not-string": lambda p: _with_data(p, timestamp=12345),
    "timestamp-not-iso": lambda p: _with_data(p, timestamp="yesterday"),
    "idempotency-key-int": lambda p: {**p, "idempotency_key": 7},
    "idempotency-key-list": lambda p: {**p, "idempotency_key": []},
}


@pytest.mark.parametrize("broken", list(_BROKEN_ENVELOPES))
def test_sanitizer_flags_a_missing_or_mistyped_envelope_field_and_scrubs_the_path(
    broken: str,
) -> None:
    """Each required field is checked on its own, with a path alongside it."""
    payload = {
        **_BROKEN_ENVELOPES[broken](_valid_envelope()),
        "working_dir": "/home/u/secret",
    }

    sanitized, unsafe = _sanitize_stored_payload(payload)

    assert unsafe is True
    assert "working_dir" not in sanitized
    assert "/home/u/secret" not in json.dumps(sanitized)


def test_sanitizer_flags_a_payload_lacking_event_workspace_and_timestamp() -> None:
    payload = {"data": {"session_id": "safe"}, "working_dir": "/p"}

    sanitized, unsafe = _sanitize_stored_payload(payload)

    assert unsafe is True
    assert sanitized == {"data": {"session_id": "safe"}}  # nothing invented


_VALID_ENVELOPES = [
    {**_valid_envelope(), "event": ""},
    _without(_valid_envelope(), "idempotency_key"),
    {**_valid_envelope(), "idempotency_key": "k-1"},
    {**_valid_envelope(), "created_by": "alice", "future_field": {"a": [1, 2]}},
    _with_data(_valid_envelope(), timestamp="2026-01-01T00:00:00Z"),
    _with_data(_valid_envelope(), timestamp="2026-01-01T00:00:00"),
]


@pytest.mark.parametrize(
    "envelope",
    _VALID_ENVELOPES,
    ids=[
        "empty-event",
        "no-idempotency-key",
        "string-idempotency-key",
        "extra-compat-fields",
        "z-timestamp",
        "naive-timestamp",
    ],
)
def test_sanitizer_accepts_valid_envelopes_and_preserves_extra_fields(
    envelope: dict[str, Any],
) -> None:
    sanitized, unsafe = _sanitize_stored_payload({**envelope, "working_dir": None})

    assert unsafe is False
    assert sanitized == envelope  # no defaults added, no coercion, extras kept


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["legacy", "current"])
@pytest.mark.parametrize("broken", ["no-workspace", "no-event", "no-timestamp"])
async def test_scrub_quarantines_pending_broken_envelopes_and_drops_path_bytes(
    tmp_path: Path, kind: str, broken: str
) -> None:
    path = tmp_path / "receipts.sqlite3"
    secret = "/home/someone/private/checkout"
    payload = {**_BROKEN_ENVELOPES[broken](_valid_envelope()), "working_dir": secret}
    if kind == "legacy":
        _create_legacy_db(path, [_legacy_row(0, payload)])
    else:
        _create_current_db(path, [(payload, "enqueued")])
    assert secret.encode() in path.read_bytes()  # control: the path was planted

    await RecoveryReceiptStore(path).status()

    assert secret.encode() not in path.read_bytes()
    [(_, state, _, encoded)] = _read_receipts(path)
    assert state == "quarantined"
    assert "working_dir" not in encoded


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["legacy", "current"])
async def test_scrub_leaves_a_written_receipt_with_a_broken_envelope_written(
    tmp_path: Path, kind: str
) -> None:
    path = tmp_path / "receipts.sqlite3"
    payload = {**_without(_valid_envelope(), "workspace"), "working_dir": "/h/u"}
    if kind == "legacy":
        _create_legacy_db(
            path, [({**_UPGRADE_ORIGIN}, "alice", "synthetic", payload, "written")]
        )
    else:
        _create_current_db(path, [(payload, "written")])

    await RecoveryReceiptStore(path).status()

    [(_, state, _, encoded)] = _read_receipts(path)
    assert state == "written"
    assert "working_dir" not in encoded


@pytest.mark.asyncio
async def test_next_quarantined_skips_a_broken_envelope_and_returns_a_later_valid_one(
    tmp_path: Path,
) -> None:
    path = tmp_path / "receipts.sqlite3"
    broken = _without(_valid_envelope(), "workspace")
    valid = _valid_envelope()
    _create_current_db(path, [(broken, "quarantined"), (valid, "quarantined")])
    store = RecoveryReceiptStore(path)

    found = await store.next_quarantined()

    assert found is not None
    assert found["origin"]["ordinal"] == 1
    assert found["payload"] == valid
    # Neither receipt was revived by looking at it.
    assert [row[1] for row in _read_receipts(path)] == ["quarantined", "quarantined"]


@pytest.mark.asyncio
async def test_next_quarantined_returns_nothing_when_only_broken_receipts_remain(
    tmp_path: Path,
) -> None:
    path = tmp_path / "receipts.sqlite3"
    _create_current_db(
        path, [(_without(_valid_envelope(), "workspace"), "quarantined")]
    )

    assert await RecoveryReceiptStore(path).next_quarantined() is None


# --- Unsafe legacy keys are rejected before any key-derived I/O ---------------


def test_component_check_has_no_length_limit_but_session_id_keeps_its_budget() -> None:
    from context_intelligence_server.queue_manager import (
        MAX_SESSION_ID_BYTES,
        _is_safe_session_component,
        is_safe_session_id,
    )

    over_budget = "x" * (MAX_SESSION_ID_BYTES + 1)
    assert _is_safe_session_component(over_budget) is True
    assert _is_safe_session_component("é" * 200) is True
    assert is_safe_session_id(over_budget) is False
    assert is_safe_session_id("x" * MAX_SESSION_ID_BYTES) is True
    for unsafe in ("", ".", "..", "a/b", "a\\b", "a\tb", "a\nb", "a\x85b", "a\x7fb"):
        assert _is_safe_session_component(unsafe) is False
        assert is_safe_session_id(unsafe) is False
    for not_text in ("a\ud800b", 5, None, b"x"):
        assert _is_safe_session_component(not_text) is False
        assert is_safe_session_id(not_text) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("boot_pass", ["migrate", "reconcile", "seed"])
async def test_unsafe_legacy_keys_are_rejected_before_any_key_derived_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    boot_pass: str,
) -> None:
    """Each boot pass skips tab/newline/C1/overlong-unsafe keys untouched.

    Each pass runs alone so its own guard is what is tested.

    Breaks if: a pass derives a path from a key before checking it, so an
    ``.offset`` symlink is followed, a dead-letter match advances an offset, or an
    unsafe over-long key is renamed.
    """
    from context_intelligence_server.queue_manager import fold_worker_key

    queue_dir = tmp_path / "q"
    queue_dir.mkdir()
    line_1, line_2 = b'{"event":"e","n":1}\n', b'{"event":"e","n":2}\n'
    dead_record = (
        json.dumps({"ts": 1.0, "error": "e", "payload": line_1.decode().rstrip("\n")})
        + "\n"
    )
    sentinel = tmp_path / "sentinel.txt"
    sentinel.write_bytes(b"keep")
    unsafe = ["\tbad-tab", "bad\nnewline", "c1\x85key", "\t" + "y" * 220]
    for key in unsafe:
        (queue_dir / f"{key}.log").write_bytes(line_1 + line_2)
        (queue_dir / f"{key}.dead.jsonl").write_text(dead_record, encoding="utf-8")
        os.symlink(sentinel, queue_dir / f"{key}.offset")
    for key in ("aa-valid", "zz-valid"):
        (queue_dir / f"{key}.log").write_bytes(line_1 + line_2)
        (queue_dir / f"{key}.dead.jsonl").write_text(dead_record, encoding="utf-8")
    safe_overlong = ["x" * 230, "é" * 110]
    for key in safe_overlong:
        (queue_dir / f"{key}.log").write_bytes(line_1)
    before = {
        entry.name: entry.read_bytes()
        for entry in queue_dir.iterdir()
        if entry.is_file() and not entry.is_symlink()
    }

    touched: list[str] = []

    def record_keys(name: str) -> None:
        real = getattr(QueueManager, name)

        def spy(self: QueueManager, key: str) -> Path:
            touched.append(key)
            return real(self, key)

        monkeypatch.setattr(QueueManager, name, spy)

    for builder in ("_log_path", "_offset_path", "_dead_path"):
        record_keys(builder)
    queue = QueueManager(queue_dir)
    migrated = skipped_lines = seeds = None

    with caplog.at_level(logging.ERROR):
        if boot_pass == "migrate":
            migrated = await queue.migrate_overlong_keys()
        elif boot_pass == "reconcile":
            skipped_lines = await queue.recovery_reconcile_dead()
        else:
            seeds = await queue.recovery_seed_counts()

    assert set(touched).isdisjoint(unsafe)
    # Healthy and safe-over-long keys behave exactly as before.
    if boot_pass == "migrate":
        # Safe over-long keys (ASCII and multibyte) still fold; unsafe ones do not.
        assert migrated == (2, 0, 0)
        for key in safe_overlong:
            folded = fold_worker_key(key)
            assert len(folded.encode("utf-8")) <= 211
            assert (queue_dir / f"{key}.log").exists() is False
            assert (queue_dir / f"{folded}.log").read_bytes() == line_1
        assert await queue.migrate_overlong_keys() == (0, 0, 0)  # idempotent
    elif boot_pass == "reconcile":
        assert skipped_lines == 2
        assert (queue_dir / "aa-valid.offset").read_text() == str(len(line_1))
        assert (queue_dir / "zz-valid.offset").read_text() == str(len(line_1))
    else:
        # Two healthy keys: 0 written + 2 pending + 1 dead each; two safe
        # over-long keys: 1 pending each. The unsafe keys add nothing.
        assert seeds == (8, 0)
    # Unsafe keys: bytes, symlinks and the symlink target are exactly as left.
    for key in unsafe:
        assert (queue_dir / f"{key}.log").read_bytes() == before[f"{key}.log"]
        assert (queue_dir / f"{key}.dead.jsonl").read_bytes() == before[
            f"{key}.dead.jsonl"
        ]
        assert os.readlink(queue_dir / f"{key}.offset") == str(sentinel)
    assert sentinel.read_bytes() == b"keep"
    assert [p.name for p in queue_dir.iterdir() if p.name.endswith(".tmp")] == []
    for key in unsafe:
        assert ascii(key) in caplog.text
        assert key not in caplog.text


# --- A failing permission step is contained ----------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["legacy", "current"])
async def test_fchmod_failure_after_creation_closes_the_fd_and_leaves_the_original(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """The helper's own failure branch: the temp exists, then fchmod is denied."""
    path = tmp_path / "receipts.sqlite3"
    _seed_rebuild_database(path, kind)
    os.chmod(path, 0o640)
    before = _read_all(path)
    opened: list[int] = []
    closed: list[int] = []
    real_open, real_close = os.open, os.close

    def spy_open(file: Any, *args: Any, **kwargs: Any) -> int:
        fd = real_open(file, *args, **kwargs)
        if str(file).endswith(".tmp"):
            opened.append(fd)
        return fd

    def spy_close(fd: int) -> None:
        closed.append(fd)
        real_close(fd)

    def denied(fd: int, mode: int) -> None:
        raise PermissionError("fchmod denied")

    monkeypatch.setattr(os, "open", spy_open)
    monkeypatch.setattr(os, "close", spy_close)
    monkeypatch.setattr(os, "fchmod", denied)

    with pytest.raises(PermissionError, match="fchmod denied") as excinfo:
        await RecoveryReceiptStore(path).status()

    assert len(opened) == 1 and opened[0] in closed
    _assert_original_untouched(tmp_path, path, before)
    assert excinfo.traceback


# --- Reconcile never reads a malformed dead letter as "no evidence" -----------


@pytest.mark.asyncio
@pytest.mark.parametrize("dead_origin", ["hybrid", "absent"])
async def test_reconcile_never_marks_a_committed_line_written_over_a_malformed_dead_letter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, dead_origin: str
) -> None:
    """A committed legacy line plus an untrusted dead letter must not read as written.

    Breaks if: a dead letter whose origin is malformed (here a hybrid with
    conflicting legacy fields) is read as "no dead letter", so reconciliation
    marks the receipt written although the line may have been quarantined.
    The ``absent`` control has no origin key and is legitimately no evidence;
    it reaches ``written`` through the same flow.
    """
    from context_intelligence_server.main import (
        _reconcile_native_recovery_outbox,
        app,
        create_asgi_app,
    )
    from context_intelligence_server.queue_manager import RecoveryReceiptError

    receipts = tmp_path / "receipts.sqlite3"
    _create_legacy_db(
        receipts,
        [(_UPGRADE_ORIGIN, "alice", "synthetic", _base_payload("absent"), "enqueued")],
    )
    key, old_line = await _seed_base_spool(tmp_path, "absent", committed=True)
    envelope: dict[str, Any] = {**_base_payload("absent"), "created_by": "alice"}
    if dead_origin == "hybrid":
        envelope["_recovery_origin"] = {
            **_v1_origin_of(_UPGRADE_ORIGIN),
            "session_id": "other-s",
            "source_stream_sha256": "e" * 64,
        }
    await QueueManager(tmp_path / "recovery").dead_letter(
        key, json.dumps(envelope, separators=(",", ":")).encode(), "poison"
    )
    dead_file = tmp_path / "recovery" / f"{key}.dead.jsonl"
    dead_before = dead_file.read_bytes()

    create_asgi_app(settings=_permit_settings(tmp_path))
    monkeypatch.setattr(app.state.recovery_registry, "get_or_create", MagicMock())
    await app.state.recovery_receipts.status()  # migrate the legacy rows first
    rows_before = _read_receipts(receipts)
    assert [row[1] for row in rows_before] == ["enqueued"]

    if dead_origin == "hybrid":
        with pytest.raises(RecoveryReceiptError):
            await _reconcile_native_recovery_outbox(app)
        assert _read_receipts(receipts) == rows_before  # still enqueued
        assert dead_file.read_bytes() == dead_before
        assert _spool_lines(tmp_path) == [old_line]
    else:
        await _reconcile_native_recovery_outbox(app)
        assert await app.state.recovery_receipts.status() == {"written": 1}


# ---------------------------------------------------------------------------
# PR 126 r4349141396: an abandoned recovery drainer must not flush on close.
#
# Real SessionRegistry / queue / receipts / pause condition / Neo4jGraphStore
# buffering.  The only fake is the external Neo4j driver, injected through the
# store's public ``driver=`` parameter.  Nothing touches a network.
# ---------------------------------------------------------------------------


class _RecordingDriver:
    """Fake external Neo4j driver: records ``execute_write`` chunks only."""

    def __init__(self) -> None:
        self.writes: list[list[str]] = []
        self.wrote = asyncio.Event()
        self.fail = False
        self.on_fail: Any = None
        self.closed = 0

    def session(self, *a: Any, **k: Any) -> _RecordingDriver:
        return self

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *a: object) -> None:
        return None

    async def execute_write(self, fn: Any, nodes: Any, *a: Any, **k: Any) -> None:
        from neo4j.exceptions import ServiceUnavailable

        if self.fail:
            if self.on_fail is not None:
                self.on_fail()
            raise ServiceUnavailable("fake driver unavailable")
        self.writes.append(sorted(nodes))
        self.wrote.set()

    async def execute_query(self, *a: Any, **k: Any) -> Any:
        return type("R", (), {"records": []})()

    async def close(self) -> None:
        self.closed += 1


def _real_store_worker() -> tuple[Any, Any, _RecordingDriver]:
    from context_intelligence_server.neo4j_store import (
        Neo4jGraphStore,
        mark_schema_ready,
    )
    from context_intelligence_server.registry import SessionWorker
    from context_intelligence_server.services import HookStateService

    mark_schema_ready()
    driver = _RecordingDriver()
    store = Neo4jGraphStore(
        uri="bolt://fake.invalid:7687", auth=("u", "p"), driver=driver
    )
    svc = HookStateService(workspace="one", graph_store=store)
    return (
        SessionWorker(session_id=SESSION, workspace="one", services=svc),
        store,
        driver,
    )


class _Cancel:
    """Handles for one real recovery app with a real-store worker registered."""

    app: Any
    reg: Any
    client: httpx.AsyncClient
    worker: Any
    store: Any
    driver: _RecordingDriver
    origin: dict[str, Any]
    close_entries: list[list[str]]


@pytest.fixture
def _cancel_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    import contextlib

    from context_intelligence_server.main import app, create_asgi_app
    from context_intelligence_server.routers.admin import require_admin

    @contextlib.asynccontextmanager
    async def build(
        event: str = "session:start", admit: bool = True
    ) -> AsyncIterator[_Cancel]:
        asgi = create_asgi_app(settings=_permit_settings(tmp_path))
        reg = app.state.recovery_registry
        monkeypatch.setattr(reg, "get_or_create", MagicMock())
        app.dependency_overrides[require_admin] = lambda: None
        h = _Cancel()
        h.app, h.reg, h.origin, h.close_entries = app, reg, {}, []
        h.worker, h.store, h.driver = _real_store_worker()
        reg._register_for_test(h.worker)
        real_close = reg._safe_close

        async def traced(worker: Any) -> None:
            h.close_entries.append(sorted(h.store._node_buffer))
            await real_close(worker)

        monkeypatch.setattr(reg, "_safe_close", traced)
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=asgi),
                base_url="http://test",
                headers=_source_headers(),
            ) as client:
                h.client = client
                if admit:
                    body = _recovery_body()
                    body["event"] = event
                    await _admit(client, body)
                    batch = await reg.queue_manager.read_batch(SESSION, max_items=1)
                    h.origin = json.loads(batch.records[0].raw)["_recovery_origin"]
                yield h
        finally:
            task = h.worker.task
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            app.dependency_overrides.pop(require_admin, None)

    return build


async def _cancel_pause(h: _Cancel) -> None:
    assert (await h.client.post("/admin/recovery/pause")).json() == {"paused": True}


async def _cancel_resume(h: _Cancel) -> None:
    assert (await h.client.post("/admin/recovery/resume")).json() == {"paused": False}


def _watch_pre_flush(h: _Cancel, *, pause_after: bool) -> asyncio.Event:
    """Wrap the REAL pre_flush hook; the event fires with the buffer resident."""
    entered = asyncio.Event()
    real = h.reg.pre_flush
    assert real is not None

    async def after() -> None:
        await real()
        await _cancel_pause(h)  # pause lands after pre_flush, before the guard
        entered.set()

    async def waiting() -> None:
        entered.set()
        await real()

    h.reg.pre_flush = after if pause_after else waiting
    return entered


async def _cancel_via_shutdown(h: _Cancel) -> None:
    await asyncio.wait_for(h.reg.shutdown_workers(), timeout=5)
    assert h.worker.task is not None and h.worker.task.done()


@pytest.mark.parametrize("trigger", ["pre_flush_wait", "write_guard_wait"])
async def test_cancel_after_quarantine_discards_buffer_and_writes_nothing(
    _cancel_env: Any, trigger: str
) -> None:
    async with _cancel_env() as h:
        if trigger == "pre_flush_wait":
            await _cancel_pause(h)
        entered = _watch_pre_flush(h, pause_after=trigger == "write_guard_wait")
        h.reg.start_drain(h.worker)
        await asyncio.wait_for(entered.wait(), timeout=3)
        assert h.store._node_buffer, "buffer must hold the recovery record"
        r = await h.client.post(
            "/admin/recovery/quarantine-pending",
            json={"source_handle": _source_handle(), "ordinal": 0},
        )
        assert r.json() == {"quarantined": 1}
        assert await h.reg.queue_manager.recovery_origin_commit_state(h.origin) == (
            "committed"
        )
        offset_before = h.reg.queue_manager._offset_path(SESSION).read_text()

        await _cancel_via_shutdown(h)

        assert h.close_entries, "cancel must converge on _safe_close"
        assert h.driver.writes == []
        assert h.store._node_buffer == {}
        assert h.reg.queue_manager._offset_path(SESSION).read_text() == offset_before
        assert await h.reg.queue_manager.recovery_origin_commit_state(h.origin) == (
            "committed"
        )


async def test_cancel_of_eligible_paused_record_writes_nothing_then_replays_once(
    _cancel_env: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with _cancel_env() as h:
        await _cancel_pause(h)
        entered = _watch_pre_flush(h, pause_after=False)
        h.reg.start_drain(h.worker)
        await asyncio.wait_for(entered.wait(), timeout=3)
        assert h.store._node_buffer

        await _cancel_via_shutdown(h)

        assert h.driver.writes == []
        qm = h.reg.queue_manager
        assert await qm.recovery_origin_commit_state(h.origin) == "queued"

        w2, _store2, driver2 = _real_store_worker()
        h.reg._register_for_test(w2)

        # The replay's observable completion is NOT the driver's ``wrote``
        # signal: the registry runs flush -> receipt write (on_batch_flushed)
        # -> queue commit, so ``wrote`` fires while the receipt is still
        # ``enqueued``.  Observe each later stage through its own Event.
        receipt_written = asyncio.Event()
        queue_committed = asyncio.Event()
        commits: list[tuple[str, int]] = []
        real_flushed = h.reg.on_batch_flushed
        real_commit = qm.commit
        assert real_flushed is not None
        flush_stamp = w2.last_successful_flush

        async def flushed(worker: Any, records: list[Any]) -> None:
            await real_flushed(worker, records)
            if worker is w2:
                receipt_written.set()

        async def commit(session_id: str, new_offset: int) -> None:
            await real_commit(session_id, new_offset)
            commits.append((session_id, new_offset))
            if receipt_written.is_set():
                queue_committed.set()

        monkeypatch.setattr(h.reg, "on_batch_flushed", flushed)
        monkeypatch.setattr(qm, "commit", commit)

        await _cancel_resume(h)
        h.reg.pre_flush = None
        h.reg.start_drain(w2)
        try:
            try:
                await asyncio.wait_for(
                    asyncio.gather(
                        driver2.wrote.wait(),
                        receipt_written.wait(),
                        queue_committed.wait(),
                    ),
                    timeout=3,
                )
            except TimeoutError:
                receipts = h.app.state.recovery_receipts
                pytest.fail(
                    "replay did not complete: "
                    f"driver_wrote={driver2.wrote.is_set()} "
                    f"receipt_written={receipt_written.is_set()} "
                    f"queue_committed={queue_committed.is_set()} "
                    f"driver_writes={driver2.writes} commits={commits} "
                    f"receipts={await receipts.status() if receipts else None} "
                    f"origin_state={await qm.recovery_origin_commit_state(h.origin)} "
                    f"task_done={w2.task.done()}"
                )
            assert w2.last_successful_flush != flush_stamp
            assert await h.app.state.recovery_receipts.status() == {"written": 1}
            assert await qm.recovery_origin_commit_state(h.origin) == "committed"
        finally:
            w2.task.cancel()
            await asyncio.gather(w2.task, return_exceptions=True)
        assert [w for w in driver2.writes if w] == driver2.writes[:1]
        assert len([w for w in driver2.writes if w]) == 1
        assert h.driver.writes == []


async def test_cancel_during_transient_retry_backoff_discards_restored_buffer(
    _cancel_env: Any,
) -> None:
    async with _cancel_env() as h:
        h.reg.pre_flush = None
        h.driver.fail = True
        loop = asyncio.get_running_loop()

        def recover_then_cancel() -> None:
            h.driver.on_fail = None
            h.driver.fail = False  # driver would now accept a close-time flush
            loop.call_soon(h.worker.task.cancel)

        h.driver.on_fail = recover_then_cancel
        h.reg.start_drain(h.worker)
        await asyncio.wait_for(asyncio.shield(_await_task(h.worker)), timeout=5)

        assert h.close_entries
        assert h.driver.writes == []
        assert h.store._node_buffer == {}
        assert await h.reg.queue_manager.recovery_origin_commit_state(h.origin) == (
            "queued"
        )


async def _await_task(worker: Any) -> None:
    while worker.task is None:
        await asyncio.sleep(0)
    await asyncio.gather(worker.task, return_exceptions=True)


async def test_died_drain_task_close_discards_recovery_buffer(
    _cancel_env: Any,
) -> None:
    async with _cancel_env(admit=False) as h:
        await h.store.upsert_node("resident", {"labels": ["Session"]})
        h.reg.pre_flush = None

        async def boom(worker: Any) -> None:
            raise RuntimeError("drainer died")

        h.worker.task = asyncio.create_task(boom(h.worker))
        await asyncio.gather(h.worker.task, return_exceptions=True)
        h.reg._on_drain_done(h.worker, h.worker.task)
        await asyncio.gather(*h.reg._close_tasks)

        assert h.close_entries == [["resident"]]
        assert h.driver.writes == []
        assert h.store._node_buffer == {}
        assert h.worker.store_closed is True
        assert h.reg._workers.get(SESSION) is not h.worker


async def test_normal_terminal_finalize_closes_empty_buffer_and_deregisters(
    _cancel_env: Any,
) -> None:
    async with _cancel_env(event="session:end") as h:
        h.reg.start_drain(h.worker)
        await asyncio.wait_for(h.driver.wrote.wait(), timeout=5)
        await asyncio.wait_for(asyncio.shield(_await_task(h.worker)), timeout=5)

        assert h.close_entries == [[]]
        assert h.store._node_buffer == {}
        assert await h.app.state.recovery_receipts.status() == {"written": 1}
        assert h.reg._workers.get(SESSION) is not h.worker


async def test_live_registry_close_still_flushes_resident_buffer() -> None:
    from context_intelligence_server.registry import SessionRegistry

    worker, store, driver = _real_store_worker()
    reg = SessionRegistry()
    assert reg.is_recovery is False
    await store.upsert_node("live-node", {"labels": ["Session"]})

    await reg._safe_close(worker)

    assert len(driver.writes) == 1 and "live-node" in driver.writes[0]
    assert worker.store_closed is True


# --- r4357836422: migrated origins share quarantine and pre-flush eligibility --


async def _migrated_review_app(
    tmp_path: Path,
    spelling: str,
    event: str = "session:start",
    *,
    queued_origin: Any = ...,
    raw_override: bytes | None = None,
) -> tuple[Any, Any, dict[str, Any], bytes]:
    """Use a real old-schema row, migration and spool, never a receipt mock."""
    from context_intelligence_server.main import app, create_asgi_app

    payload = {**_base_payload("absent"), "event": event}
    _create_legacy_db(
        tmp_path / "receipts.sqlite3",
        [(_UPGRADE_ORIGIN, "alice", "synthetic", payload, "enqueued")],
    )
    asgi = create_asgi_app(settings=_permit_settings(tmp_path))
    reg = app.state.recovery_registry
    v1 = _v1_origin_of(_UPGRADE_ORIGIN)
    if queued_origin is ...:
        queued_origin = _UPGRADE_ORIGIN if spelling == "legacy" else v1
    raw = raw_override
    if raw is None:
        raw = json.dumps(
            {**payload, "created_by": "alice", "_recovery_origin": queued_origin},
            separators=(",", ":"),
        ).encode("utf-8")
    await reg.queue_manager.append(_UPGRADE_SESSION, raw)
    assert await app.state.recovery_receipts.status() == {"enqueued": 1}
    assert await app.state.recovery_receipts.pending_receipts() == [
        {"source_handle": v1["source_handle"], "ordinal": 0, "state": "enqueued"}
    ]
    return asgi, reg, v1, raw


async def _migrated_selection_scenario(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    spelling: str,
    event: str,
    *,
    quarantine: bool,
) -> dict[str, Any]:
    """Real drainer and graph buffer, paused after dispatch, before flush."""
    from context_intelligence_server.main import app
    from context_intelligence_server.routers.admin import require_admin

    asgi, reg, v1, raw = await _migrated_review_app(tmp_path, spelling, event)
    queue = reg.queue_manager
    worker, store, driver = _real_store_worker()
    worker.session_id, worker.workspace = _UPGRADE_SESSION, "synthetic"
    worker.services.workspace = "synthetic"
    parked, release, discarded = asyncio.Event(), asyncio.Event(), asyncio.Event()
    flushed = asyncio.Event()
    real_discard, real_flushed = store.discard_buffer, reg.on_batch_flushed
    flush_spy = AsyncMock(wraps=store.flush)
    monkeypatch.setattr(store, "flush", flush_spy)

    async def dispatch(
        selected: Any, selected_event: str, data: dict[str, Any], *a: Any, **k: Any
    ) -> None:
        assert selected is worker and selected_event == event
        assert data["session_id"] == _UPGRADE_SESSION
        await store.upsert_node("selected-record", {"labels": ["Session"]})

    async def hold() -> None:
        parked.set()
        async with asyncio.timeout(5):
            await release.wait()

    def discard() -> None:
        real_discard()
        discarded.set()

    async def after_flush(selected: Any, records: list[Any]) -> None:
        await real_flushed(selected, records)
        flushed.set()

    # Retain bytes for comparison; dispatch, commits and receipt hooks stay real.
    monkeypatch.setattr(queue, "delete_drained", AsyncMock(return_value=True))
    monkeypatch.setattr(reg, "_process_one", dispatch)
    monkeypatch.setattr(reg, "pre_flush", hold)
    monkeypatch.setattr(reg, "on_batch_flushed", after_flush)
    monkeypatch.setattr(store, "discard_buffer", discard)
    spool_before = queue._log_path(_UPGRADE_SESSION).read_bytes()
    app.dependency_overrides[require_admin] = lambda: None
    drain: asyncio.Task[None] | None = None
    out: dict[str, Any] = {}
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=asgi), base_url="http://test"
        ) as client:
            drain = asyncio.create_task(reg.drain_worker(worker, flush_timeout=0.2))
            await asyncio.wait_for(parked.wait(), timeout=3)
            assert store._node_buffer, "selection must actually buffer graph work"
            assert (await client.post("/admin/recovery/pause")).json() == {
                "paused": True
            }
            if quarantine:
                result = await client.post(
                    "/admin/recovery/quarantine-pending",
                    json={"source_handle": v1["source_handle"], "ordinal": 0},
                )
                assert result.status_code == 200
                assert result.json() == {"quarantined": 1}
                out["quarantine_offset"] = int(
                    queue._offset_path(_UPGRADE_SESSION).read_text(encoding="utf-8")
                )
                assert await app.state.recovery_receipts.status() == {"quarantined": 1}
            assert (await client.post("/admin/recovery/resume")).json() == {
                "paused": False
            }
            release.set()
            await asyncio.wait_for(
                (discarded if quarantine else flushed).wait(), timeout=3
            )
            # close() may call flush on an empty buffer; count the write boundary
            # before cancellation/close, then inspect driver writes after cleanup.
            out["flushes"] = flush_spy.await_count
    finally:
        release.set()
        async with app.state.recovery_pause_condition:
            app.state.recovery_paused = False
            app.state.recovery_pause_condition.notify_all()
        try:
            if drain is not None:
                drain.cancel()
                await asyncio.wait_for(
                    asyncio.gather(drain, return_exceptions=True), timeout=5
                )
        finally:
            try:
                # Cleanup itself must not manufacture a successful graph write.
                await reg._safe_close(worker)
            finally:
                app.dependency_overrides.pop(require_admin, None)
    out.update(
        writes=driver.writes,
        discarded=discarded.is_set(),
        buffer=dict(store._node_buffer),
        receipts=await app.state.recovery_receipts.status(),
        offset=int(queue._offset_path(_UPGRADE_SESSION).read_text(encoding="utf-8")),
    )
    assert queue._log_path(_UPGRADE_SESSION).read_bytes() == spool_before == raw + b"\n"
    return out


@pytest.mark.parametrize("spelling", ["legacy", "v1"])
@pytest.mark.parametrize("event", ["session:start", "session:end"])
async def test_migrated_origin_can_be_quarantined_after_selection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, spelling: str, event: str
) -> None:
    """Breaks if literal origin comparison prevents quarantining an old spool."""
    out = await _migrated_selection_scenario(
        tmp_path, monkeypatch, spelling, event, quarantine=True
    )
    assert out["flushes"] == 0 and out["writes"] == []
    assert out["discarded"] and out["buffer"] == {}
    assert out["receipts"] == {"quarantined": 1}
    assert out["offset"] >= out["quarantine_offset"]
    [evidence] = [
        json.loads(line)
        for line in (tmp_path / "recovery" / f"{_UPGRADE_SESSION}.dead.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert (
        evidence["payload"].encode("utf-8") + b"\n"
        == (tmp_path / "recovery" / f"{_UPGRADE_SESSION}.log").read_bytes()
    )


@pytest.mark.parametrize("spelling", ["legacy", "v1"])
@pytest.mark.parametrize("event", ["session:start", "session:end"])
async def test_migrated_origin_flushes_without_quarantine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, spelling: str, event: str
) -> None:
    """Positive control: the same selected record really flushes when eligible."""
    out = await _migrated_selection_scenario(
        tmp_path, monkeypatch, spelling, event, quarantine=False
    )
    assert out["flushes"] >= 1
    assert out["writes"] and "selected-record" in out["writes"][0]
    assert out["receipts"] == {"written": 1}


@pytest.mark.parametrize("spelling", ["legacy", "v1"])
async def test_legacy_flush_fence_rejects_terminalized_captured_record(
    tmp_path: Path, spelling: str
) -> None:
    """Breaks if legacy origins bypass the independently installed flush fence."""
    from context_intelligence_server.main import app

    _, reg, v1, _ = await _migrated_review_app(tmp_path, spelling)
    batch = await reg.queue_manager.read_batch(_UPGRADE_SESSION, max_items=1)
    worker, store, driver = _real_store_worker()
    await store.upsert_node("selected-record", {"labels": ["Session"]})
    try:
        # Deliberately bypass the quarantine helper: queue state stays queued.
        await app.state.recovery_receipts.quarantine_pending(v1["source_handle"], 0)
        assert await reg.queue_manager.recovery_origin_commit_state(v1) == "queued"
        with pytest.raises(
            RuntimeError, match="recovery record was terminalized before graph flush"
        ):
            await reg.before_recovery_flush(worker, batch.records)
        assert store._node_buffer == {}
        assert driver.writes == []
        assert await app.state.recovery_receipts.status() == {"quarantined": 1}
    finally:
        await reg._safe_close(worker)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("session_id", "different-session"),
        ("source_stream_sha256", "c" * 64),
        ("ordinal", 1),
        ("source_line_sha256", "d" * 64),
    ],
)
async def test_pending_quarantine_does_not_match_a_different_normalized_origin(
    tmp_path: Path, field: str, value: Any
) -> None:
    """Every identity component participates; mismatches leave storage untouched."""
    from context_intelligence_server.main import (
        _quarantine_pending_native_recovery,
        app,
    )

    _, reg, v1, _ = await _migrated_review_app(
        tmp_path, "legacy", queued_origin={**_UPGRADE_ORIGIN, field: value}
    )
    queue_dir = tmp_path / "recovery"
    before = {p.name: p.read_bytes() for p in queue_dir.iterdir()}
    rows_before = _read_all(tmp_path / "receipts.sqlite3")
    app.state.recovery_paused = True

    assert await _quarantine_pending_native_recovery(app, v1["source_handle"], 0) == (
        "none"
    )
    assert _read_all(tmp_path / "receipts.sqlite3") == rows_before
    assert {p.name: p.read_bytes() for p in queue_dir.iterdir()} == before
    assert (
        await reg.queue_manager.recovery_origin_commit_state(
            _v1_origin_of({**_UPGRADE_ORIGIN, field: value})
        )
        == "queued"
    )


_INVALID_REVIEW_ORIGINS = [
    None,
    [],
    {},
    "private-payload-marker",
    {**_UPGRADE_ORIGIN, "source_handle": "c" * 64},
    {**_UPGRADE_ORIGIN, "extra": True},
    {k: v for k, v in _UPGRADE_ORIGIN.items() if k != "source_line_sha256"},
    {**_UPGRADE_ORIGIN, "session_id": ""},
    {**_UPGRADE_ORIGIN, "source_stream_sha256": "not-a-digest"},
    {**_UPGRADE_ORIGIN, "source_line_sha256": "A" * 64},
    *[
        {**origin, "ordinal": ordinal}
        for origin in (_UPGRADE_ORIGIN, _v1_origin_of(_UPGRADE_ORIGIN))
        for ordinal in (True, -1, "0", 1 << 63)
    ],
    {**_v1_origin_of(_UPGRADE_ORIGIN), "source_handle": "invalid"},
]
_INVALID_REVIEW_LINES = [
    b"[]",
    b"null",
    b"{not-json-private-payload-marker",
    b'{"event":"session:start"}',
    *[
        json.dumps({"_recovery_origin": origin}).encode("utf-8")
        for origin in _INVALID_REVIEW_ORIGINS
    ],
]


@pytest.mark.parametrize("raw", _INVALID_REVIEW_LINES)
async def test_pending_quarantine_refuses_malformed_envelope_without_mutation(
    tmp_path: Path, raw: bytes
) -> None:
    from context_intelligence_server.main import (
        _quarantine_pending_native_recovery,
        app,
    )

    await _migrated_review_app(tmp_path, "legacy", raw_override=raw)
    v1 = _v1_origin_of(_UPGRADE_ORIGIN)
    before = {p.name: p.read_bytes() for p in (tmp_path / "recovery").iterdir()}
    rows_before = _read_all(tmp_path / "receipts.sqlite3")
    app.state.recovery_paused = True

    assert await _quarantine_pending_native_recovery(app, v1["source_handle"], 0) == (
        "none"
    )
    assert _read_all(tmp_path / "receipts.sqlite3") == rows_before
    assert {p.name: p.read_bytes() for p in (tmp_path / "recovery").iterdir()} == before


@pytest.mark.parametrize("raw", _INVALID_REVIEW_LINES)
async def test_flush_fence_refuses_malformed_origin_before_receipt_io(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: Any, raw: bytes
) -> None:
    """Breaks if absent/invalid/overflow origins skip the fence or reach SQLite."""
    from context_intelligence_server.main import app

    _, reg, _, _ = await _migrated_review_app(tmp_path, "legacy", raw_override=raw)
    batch = await reg.queue_manager.read_batch(_UPGRADE_SESSION, max_items=1)
    io_calls: list[str] = []

    async def forbidden_io(*a: Any, **k: Any) -> Any:
        io_calls.append("invalid origin reached storage")
        raise AssertionError("malformed origin must be rejected before storage I/O")

    monkeypatch.setattr(reg.queue_manager, "recovery_origin_commit_state", forbidden_io)
    monkeypatch.setattr(app.state.recovery_receipts, "is_flushable", forbidden_io)
    worker, store, driver = _real_store_worker()
    await store.upsert_node("selected-record", {"labels": ["Session"]})
    try:
        with pytest.raises(
            RuntimeError, match="recovery record was terminalized before graph flush"
        ) as excinfo:
            await reg.before_recovery_flush(worker, batch.records)
        assert io_calls == []
        assert store._node_buffer == {} and driver.writes == []
        assert "private-payload-marker" not in str(excinfo.value) + caplog.text
    finally:
        await reg._safe_close(worker)


def test_public_recovery_origin_normalizer_matches_legacy_and_v1() -> None:
    """The public boundary preserves exact identity and rejects unsafe origins."""
    normalize = getattr(QueueManager, "normalized_recovery_origin", None)
    assert callable(normalize), (
        "recovery consumers need the canonical public normalizer"
    )
    v1 = _v1_origin_of(_UPGRADE_ORIGIN)
    expected = (v1["source_handle"], 0, "b" * 64)
    assert normalize(_UPGRADE_ORIGIN) == normalize(v1) == expected
    maximum = (1 << 63) - 1
    assert normalize({**_UPGRADE_ORIGIN, "ordinal": maximum}) == (
        v1["source_handle"],
        maximum,
        "b" * 64,
    )
    for invalid in _INVALID_REVIEW_ORIGINS:
        assert normalize(invalid) is None
