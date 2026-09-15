"""Focused tests for native recovery persistence and scoped authorization."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi import FastAPI, HTTPException, Request

from context_intelligence_server.authz import (
    require_arbitrary_data_read,
    require_claimed_session_access,
    require_read,
    require_workspace_access,
)
from context_intelligence_server.config import Settings
from context_intelligence_server.recovery import RecoveryReceiptStore, lease_allows
from context_intelligence_server.session_claims import SessionClaimStore


def _write_lease(path: object, lease_id: str = "lease-1") -> None:
    path.write_text(  # type: ignore[union-attr]
        json.dumps(
            {
                "allow": True,
                "expires_at": time.time() + 10,
                "lease_id": lease_id,
            }
        )
    )


def _request(contributor: str, grant: object) -> Request:
    return cast(
        Request,
        SimpleNamespace(
            scope={"state": {"contributor_id": contributor}},
            app=SimpleNamespace(
                state=SimpleNamespace(
                    access_control_mode="scoped",
                    contributor_grants={contributor: grant},
                )
            ),
        ),
    )


def test_scoped_grant_separates_read_and_workspace() -> None:
    settings = Settings(
        access_control_mode="scoped",
        contributor_grants=cast(
            Any,
            {
                "writer": {
                    "workspaces": ["one"],
                    "capabilities": ["live:write"],
                }
            },
        ),
    )
    request = _request("writer", settings.contributor_grants["writer"])
    require_workspace_access(request, "one", "live:write")
    with pytest.raises(HTTPException) as denied:
        require_read(request)
    assert denied.value.status_code == 403
    with pytest.raises(HTTPException) as wrong_workspace:
        require_workspace_access(request, "two", "live:write")
    assert wrong_workspace.value.status_code == 403


def test_workspace_limited_reader_cannot_use_arbitrary_cypher() -> None:
    settings = Settings(
        access_control_mode="scoped",
        contributor_grants=cast(
            Any, {"reader": {"workspaces": ["one"], "capabilities": ["data:read"]}}
        ),
    )
    with pytest.raises(HTTPException) as denied:
        require_arbitrary_data_read(
            _request("reader", settings.contributor_grants["reader"])
        )
    assert denied.value.status_code == 403


def test_all_workspace_reader_can_use_arbitrary_cypher() -> None:
    settings = Settings(
        access_control_mode="scoped",
        contributor_grants=cast(
            Any, {"reader": {"all_workspaces": True, "capabilities": ["data:read"]}}
        ),
    )
    require_arbitrary_data_read(
        _request("reader", settings.contributor_grants["reader"])
    )


def test_recovery_is_disabled_and_uses_a_distinct_queue_by_default(
    tmp_path: object,
) -> None:
    from context_intelligence_server.main import app, create_asgi_app

    settings = Settings()
    assert not settings.recovery.enabled
    recovery_queue, _receipt_store, _claims_store, _lease = settings.recovery_paths()
    assert recovery_queue != settings.queues_path

    recovery_root = tmp_path / "recovery"  # type: ignore[operator]
    settings = Settings(
        allow_unauthenticated=True,
        recovery=cast(
            Any,
            {
                "enabled": False,
                "claims_store_path": str(recovery_root / "claims.sqlite3"),
                "receipt_store_path": str(recovery_root / "receipts.sqlite3"),
                "queues_path": str(recovery_root / "queues"),
            },
        ),
    )
    create_asgi_app(settings=settings)

    assert app.state.session_claims is None
    assert app.state.recovery_receipts is None
    assert app.state.recovery_registry is None
    assert not recovery_root.exists()


def test_recovery_defaults_derive_from_the_configured_queue_root(
    tmp_path: object,
) -> None:
    settings = Settings(queues_path=str(tmp_path / "live"))  # type: ignore[operator]
    recovery_queue, receipt_store, claims_store, lease = settings.recovery_paths()
    assert recovery_queue == tmp_path / "recovery-queues"  # type: ignore[operator]
    assert receipt_store == tmp_path / "recovery-receipts.sqlite3"  # type: ignore[operator]
    assert claims_store == tmp_path / "session-claims.sqlite3"  # type: ignore[operator]
    assert lease == tmp_path / "recovery-guard-lease.json"  # type: ignore[operator]


@pytest.mark.asyncio
async def test_receipt_store_initializes_lazily_off_the_constructor_path(
    tmp_path: object,
) -> None:
    path = tmp_path / "recovery" / "receipts.sqlite3"  # type: ignore[operator]
    store = RecoveryReceiptStore(path)
    assert not path.parent.exists()
    assert await store.status() == {}
    assert path.exists()


@pytest.mark.asyncio
async def test_session_claim_is_durable_and_non_disclosing(tmp_path: object) -> None:
    store = SessionClaimStore(str(tmp_path / "claims.sqlite3"))  # type: ignore[operator]
    assert await store.claim("s", "alice", "one")
    assert await store.claim("s", "alice", "one")
    assert not await store.claim("s", "bob", "one")
    assert not await store.claim("s", "alice", "two")
    assert (
        await store.reserve_recovery("s", "alice", "one", "reservation") == "committed"
    )


@pytest.mark.asyncio
async def test_recovery_reservation_blocks_live_claim_until_exactly_released(
    tmp_path: object,
) -> None:
    store = SessionClaimStore(tmp_path / "claims.sqlite3")  # type: ignore[operator]
    assert (
        await store.reserve_recovery("s", "alice", "one", "reservation") == "reserved"
    )
    assert not await store.claim("s", "alice", "one")
    assert await store.get_workspace("s") is None
    await store.release_recovery("s", "alice", "one", "wrong-reservation")
    assert not await store.claim("s", "alice", "one")
    await store.release_recovery("s", "alice", "one", "reservation")
    assert await store.claim("s", "alice", "one")


@pytest.mark.asyncio
async def test_existing_claim_database_migrates_to_committed_claims(
    tmp_path: object,
) -> None:
    path = tmp_path / "claims.sqlite3"  # type: ignore[operator]
    with sqlite3.connect(path) as db:
        db.execute(
            """CREATE TABLE session_claims (
                session_id TEXT PRIMARY KEY, contributor_id TEXT NOT NULL,
                workspace TEXT NOT NULL
            )"""
        )
        db.execute("INSERT INTO session_claims VALUES ('s', 'alice', 'one')")

    store = SessionClaimStore(path)
    assert await store.claim("s", "alice", "one")
    with sqlite3.connect(path) as db:
        assert db.execute(
            "SELECT reservation_token FROM session_claims WHERE session_id='s'"
        ).fetchone() == (None,)


@pytest.mark.asyncio
async def test_claimed_session_access_fails_closed_without_a_claim(
    tmp_path: object,
) -> None:
    settings = Settings(
        access_control_mode="scoped",
        contributor_grants=cast(
            Any, {"reader": {"workspaces": ["one"], "capabilities": ["data:read"]}}
        ),
    )
    request = _request("reader", settings.contributor_grants["reader"])
    request.app.state.session_claims = SessionClaimStore(
        tmp_path / "claims.sqlite3"  # type: ignore[operator]
    )
    with pytest.raises(HTTPException) as denied:
        await require_claimed_session_access(request, "unknown", "data:read")
    assert denied.value.status_code == 403
    assert await request.app.state.session_claims.claim("known", "writer", "one")
    await require_claimed_session_access(request, "known", "data:read")


@pytest.mark.asyncio
async def test_receipts_are_idempotent_conflict_safe_and_single_pending(
    tmp_path: object,
) -> None:
    store = RecoveryReceiptStore(str(tmp_path / "receipts.sqlite3"))  # type: ignore[operator]
    lease = tmp_path / "lease.json"  # type: ignore[operator]
    _write_lease(lease)
    origin = {
        "session_id": "s",
        "source_stream_sha256": "a" * 64,
        "source_line_sha256": "b" * 64,
        "ordinal": 0,
    }
    payload = {"event": "session:start", "data": {"session_id": "s"}}
    assert await store.admit(origin, "alice", "one", payload, lease) == "accepted"
    assert await store.admit(origin, "alice", "one", payload, lease) == "duplicate"
    assert (
        await store.admit(origin, "alice", "one", {"event": "changed"}, lease)
        == "conflict"
    )
    second = {**origin, "ordinal": 1, "source_line_sha256": "c" * 64}
    assert await store.admit(second, "alice", "one", payload, lease) == "busy"
    await store.mark(origin, "written")
    await store.mark(origin, "enqueued")
    assert (await store.status()) == {"written": 1}
    with sqlite3.connect(store.path) as db:
        state, stored_payload = db.execute(
            "SELECT state, payload FROM recovery_receipts"
        ).fetchone()
    assert (state, stored_payload) == ("written", "")
    _write_lease(lease, "lease-2")
    assert (
        await store.admit(origin, "alice", "one", {"event": "changed"}, lease)
        == "conflict"
    )
    assert await store.admit(second, "alice", "one", payload, lease) == "accepted"


@pytest.mark.asyncio
async def test_identical_line_hash_is_valid_at_a_distinct_ordinal(
    tmp_path: object,
) -> None:
    store = RecoveryReceiptStore(str(tmp_path / "receipts.sqlite3"))  # type: ignore[operator]
    lease = tmp_path / "lease.json"  # type: ignore[operator]
    _write_lease(lease)
    origin = {
        "session_id": "s",
        "source_stream_sha256": "a" * 64,
        "source_line_sha256": "b" * 64,
        "ordinal": 0,
    }
    payload = {"event": "session:start", "data": {"session_id": "s"}}
    assert await store.admit(origin, "alice", "one", payload, lease) == "accepted"
    await store.mark(origin, "written")
    _write_lease(lease, "lease-2")
    assert (
        await store.admit({**origin, "ordinal": 1}, "alice", "one", payload, lease)
        == "accepted"
    )


@pytest.mark.asyncio
async def test_lease_consumption_is_durable_across_store_restart(
    tmp_path: object,
) -> None:
    path = tmp_path / "receipts.sqlite3"  # type: ignore[operator]
    lease = tmp_path / "lease.json"  # type: ignore[operator]
    origin = {
        "session_id": "s",
        "source_stream_sha256": "a" * 64,
        "source_line_sha256": "b" * 64,
        "ordinal": 0,
    }
    payload = {"event": "session:start", "data": {"session_id": "s"}}
    _write_lease(lease)
    store = RecoveryReceiptStore(path)
    assert await store.admit(origin, "alice", "one", payload, lease) == "accepted"
    await store.mark(origin, "written")

    with sqlite3.connect(path) as db:
        assert db.execute(
            "SELECT lease_id FROM recovery_lease_consumptions"
        ).fetchall() == [("lease-1",)]

    restarted_store = RecoveryReceiptStore(path)
    second = {**origin, "ordinal": 1, "source_line_sha256": "c" * 64}
    assert await restarted_store.admit(second, "alice", "one", payload, lease) == "busy"
    _write_lease(lease, "lease-2")
    assert (
        await restarted_store.admit(second, "alice", "one", payload, lease)
        == "accepted"
    )


@pytest.mark.asyncio
async def test_receipt_store_migrates_invalid_line_uniqueness(tmp_path: object) -> None:
    path = tmp_path / "receipts.sqlite3"  # type: ignore[operator]
    with sqlite3.connect(path) as db:
        db.execute(
            """CREATE TABLE recovery_receipts (
                source_session_id TEXT NOT NULL, source_stream_sha256 TEXT NOT NULL,
                ordinal INTEGER NOT NULL, source_line_sha256 TEXT NOT NULL,
                actor TEXT NOT NULL, workspace TEXT NOT NULL, payload_sha256 TEXT NOT NULL,
                payload TEXT NOT NULL, state TEXT NOT NULL,
                PRIMARY KEY(source_session_id, source_stream_sha256, ordinal),
                UNIQUE(source_session_id, source_stream_sha256, source_line_sha256)
            )"""
        )
    store = RecoveryReceiptStore(path)
    await store.status()  # initializes and migrates off the event loop
    with sqlite3.connect(path) as db:
        unique_indexes = [
            index
            for index in db.execute("PRAGMA index_list(recovery_receipts)").fetchall()
            if index[2]
        ]
        unique_columns = [
            [
                row[2]
                for row in db.execute(f"PRAGMA index_info({index[1]!r})").fetchall()
            ]
            for index in unique_indexes
        ]
        assert [
            "source_session_id",
            "source_stream_sha256",
            "source_line_sha256",
        ] not in unique_columns


@pytest.mark.asyncio
async def test_outbox_reconciliation_appends_once_after_admission(
    tmp_path: object,
) -> None:
    """Models a crash after receipt admission and before queue append."""
    from context_intelligence_server.main import _reconcile_native_recovery_outbox
    from context_intelligence_server.queue_manager import QueueManager

    origin = {
        "session_id": "s",
        "source_stream_sha256": "a" * 64,
        "source_line_sha256": "b" * 64,
        "ordinal": 0,
    }
    receipts = RecoveryReceiptStore(tmp_path / "receipts.sqlite3")  # type: ignore[operator]
    lease = tmp_path / "lease.json"  # type: ignore[operator]
    _write_lease(lease)
    assert (
        await receipts.admit(
            origin,
            "alice",
            "one",
            {
                "event": "session:start",
                "workspace": "one",
                "data": {"session_id": "s", "timestamp": "2026-01-01T00:00:00"},
            },
            lease,
        )
        == "accepted"
    )
    registry = SimpleNamespace(
        queue_manager=QueueManager(tmp_path / "queue"),  # type: ignore[operator]
        get_or_create=MagicMock(),
    )
    app = SimpleNamespace(
        state=SimpleNamespace(recovery_receipts=receipts, recovery_registry=registry)
    )

    await _reconcile_native_recovery_outbox(cast(FastAPI, app))
    await _reconcile_native_recovery_outbox(cast(FastAPI, app))

    batch = await registry.queue_manager.read_batch("s", max_items=10)
    assert len(batch.records) == 1
    assert await receipts.status() == {"enqueued": 1}


@pytest.mark.asyncio
async def test_reconciliation_finalizes_crash_shaped_recovery_reservation(
    tmp_path: object,
) -> None:
    from context_intelligence_server.main import (
        _reconcile_native_recovery_outbox,
        _recovery_reservation_token,
    )
    from context_intelligence_server.queue_manager import QueueManager

    origin = {
        "session_id": "s",
        "source_stream_sha256": "a" * 64,
        "source_line_sha256": "b" * 64,
        "ordinal": 0,
    }
    receipt = {
        "origin": origin,
        "actor": "alice",
        "workspace": "one",
        "payload": {
            "event": "session:start",
            "workspace": "one",
            "data": {"session_id": "s", "timestamp": "2026-01-01T00:00:00"},
        },
    }
    claims = SessionClaimStore(tmp_path / "claims.sqlite3")  # type: ignore[operator]
    assert (
        await claims.reserve_recovery(
            "s", "alice", "one", _recovery_reservation_token(origin)
        )
        == "reserved"
    )
    receipts = RecoveryReceiptStore(tmp_path / "receipts.sqlite3")  # type: ignore[operator]
    lease = tmp_path / "lease.json"  # type: ignore[operator]
    _write_lease(lease)
    assert (
        await receipts.admit(origin, "alice", "one", receipt["payload"], lease)
        == "accepted"
    )
    registry = SimpleNamespace(
        queue_manager=QueueManager(tmp_path / "queue"),  # type: ignore[operator]
        get_or_create=MagicMock(),
    )
    app = SimpleNamespace(
        state=SimpleNamespace(
            access_control_mode="scoped",
            session_claims=claims,
            recovery_receipts=receipts,
            recovery_registry=registry,
        )
    )

    await _reconcile_native_recovery_outbox(cast(FastAPI, app))

    with sqlite3.connect(claims.path) as db:
        assert db.execute(
            "SELECT reservation_token FROM session_claims WHERE session_id='s'"
        ).fetchone() == (None,)
    assert await claims.get_workspace("s") == "one"
    batch = await registry.queue_manager.read_batch("s", max_items=10)
    assert len(batch.records) == 1


@pytest.mark.asyncio
async def test_recovery_route_rolls_back_rejected_reservations_and_consumes_each_lease(
    tmp_path: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    from context_intelligence_server.main import app, create_asgi_app

    lease = tmp_path / "lease.json"  # type: ignore[operator]
    settings = Settings(
        api_key="recovery-test-key",
        api_keys_store_path=str(tmp_path / "api-keys.json"),  # type: ignore[operator]
        access_control_mode="scoped",
        contributor_grants=cast(
            Any,
            {
                "owner": {
                    "workspaces": ["one"],
                    "capabilities": ["recovery:write"],
                }
            },
        ),
        queues_path=str(tmp_path / "live"),  # type: ignore[operator]
        recovery={
            "enabled": True,
            "guard_lease_path": str(lease),
            "receipt_store_path": str(tmp_path / "receipts.sqlite3"),  # type: ignore[operator]
            "queues_path": str(tmp_path / "recovery-queue"),  # type: ignore[operator]
        },
    )
    asgi_app = create_asgi_app(settings=settings)
    monkeypatch.setattr(app.state.recovery_registry, "get_or_create", MagicMock())
    body = {
        "event": "session:start",
        "workspace": "one",
        "data": {"session_id": "s", "timestamp": "2026-01-01T00:00:00"},
        "origin": {
            "session_id": "s",
            "ordinal": 0,
            "source_line_sha256": "a" * 64,
            "source_stream_sha256": "b" * 64,
        },
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app),
        base_url="http://test",
        headers={"Authorization": "Bearer recovery-test-key"},
    ) as client:
        assert (await client.post("/recovery/events", json=body)).status_code == 429
        claims = app.state.session_claims
        assert claims is not None
        assert await claims.get_workspace("s") is None
        lease.write_text(  # type: ignore[union-attr]
            '{"allow": true, "expires_at": %s}' % (time.time() + 10)
        )
        assert (await client.post("/recovery/events", json=body)).status_code == 429
        assert await claims.get_workspace("s") is None
        assert await claims.claim("s", "live-writer", "one")
        _write_lease(lease, "conflict-lease")
        assert (await client.post("/recovery/events", json=body)).status_code == 409
        with sqlite3.connect(app.state.recovery_receipts.path) as db:
            assert db.execute(
                "SELECT count(*) FROM recovery_lease_consumptions"
            ).fetchone() == (0,)
        body = {
            **body,
            "data": {
                **body["data"],
                "session_id": "recovery-session",
            },
            "origin": {
                **body["origin"],
                "session_id": "recovery-session",
            },
        }
        _write_lease(lease)
        accepted = await client.post("/recovery/events", json=body)
        assert accepted.status_code == 202
        assert accepted.json()["status"] == "queued"
        assert await claims.get_workspace("recovery-session") == "one"
        assert not await claims.claim("recovery-session", "live-writer", "one")
        _write_lease(lease, "lease-2")
        duplicate = await client.post("/recovery/events", json=body)
        assert duplicate.status_code == 202
        assert duplicate.json()["status"] == "duplicate"
        await app.state.recovery_receipts.mark(body["origin"], "written")
        second = {
            **body,
            "origin": {
                **body["origin"],
                "ordinal": 1,
                "source_line_sha256": "c" * 64,
            },
        }
        _write_lease(lease, "lease-1")
        assert (await client.post("/recovery/events", json=second)).status_code == 429
        _write_lease(lease, "lease-2")
        second_accepted = await client.post("/recovery/events", json=second)
        assert second_accepted.status_code == 202
        assert second_accepted.json()["status"] == "queued"
    batch = await app.state.recovery_registry.queue_manager.read_batch(
        "recovery-session", max_items=10
    )
    assert len(batch.records) == 2


@pytest.mark.asyncio
async def test_live_first_gate_prioritizes_waiting_live_flushes() -> None:
    from context_intelligence_server.registry import LiveFirstFlushGate

    gate = LiveFirstFlushGate(1)
    order: list[str] = []
    release_recovery = asyncio.Event()
    live_ready = asyncio.Event()

    async def recovery() -> None:
        async with gate.acquire(recovery=True):
            order.append("recovery-1")
            await release_recovery.wait()
        async with gate.acquire(recovery=True):
            order.append("recovery-2")

    async def live() -> None:
        live_ready.set()
        async with gate.acquire(recovery=False):
            order.append("live")

    recovery_task = asyncio.create_task(recovery())
    while order != ["recovery-1"]:
        await asyncio.sleep(0)
    live_task = asyncio.create_task(live())
    await live_ready.wait()
    await asyncio.sleep(0)
    release_recovery.set()
    await asyncio.gather(recovery_task, live_task)
    assert order == ["recovery-1", "live", "recovery-2"]


@pytest.mark.asyncio
async def test_startup_resumes_queued_native_recovery_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from context_intelligence_server.main import _resume_native_recovery, app

    worker_registry = SimpleNamespace(
        queue_manager=SimpleNamespace(
            recover=AsyncMock(return_value=["s"]),
            read_batch=AsyncMock(
                return_value=SimpleNamespace(
                    lines=[b'{"workspace":"one","created_by":"alice"}']
                )
            ),
        ),
        get_or_create=MagicMock(),
    )
    monkeypatch.setattr(app.state, "recovery_registry", worker_registry)
    monkeypatch.setattr(app.state, "recovery_receipts", None)

    await _resume_native_recovery(app)

    worker_registry.get_or_create.assert_called_once_with(
        "s", "one", created_by="alice"
    )


def test_lease_requires_explicit_unexpired_allow(tmp_path: object) -> None:
    path = tmp_path / "lease.json"  # type: ignore[operator]
    assert not lease_allows(str(path))
    path.write_text('{"allow": true, "expires_at": 0}')  # type: ignore[union-attr]
    assert not lease_allows(str(path))
    path.write_text(  # type: ignore[union-attr]
        '{"allow": true, "expires_at": %s}' % (time.time() + 10)
    )
    assert not lease_allows(str(path))
    _write_lease(path)
    assert lease_allows(str(path))
