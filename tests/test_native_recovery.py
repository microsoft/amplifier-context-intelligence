"""Focused tests for native recovery persistence and scoped authorization."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

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
from context_intelligence_server.handlers.data_layer_1.default import DefaultHandler
from context_intelligence_server.pipeline import PipelineHandlers, process_event
from context_intelligence_server.recovery import (
    _PermitBinding,
    RecoveryAdmissionGate,
    RecoveryReceiptStore,
    lease_allows,
    recovery_event_identity,
    source_descriptor_digest,
    source_handle_from_capability,
)
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.session_claims import SessionClaimStore
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import SPOOL_EVENT_IDENTITY, make_node_id


class _RecordingBlobStore:
    def __init__(self) -> None:
        self.keys: list[str] = []

    async def write(self, session_id: str, key: str, _value: object) -> str:
        self.keys.append(key)
        return f"ci-blob://{session_id}/{key}"


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
@pytest.mark.parametrize("failure_stage", ("receipt_marker", "queue_commit"))
async def test_recovery_deadletter_boundaries_never_advance_past_an_unmarked_record(
    tmp_path: Path, failure_stage: str
) -> None:
    """A source may advance only after its original 202; recovery failures retain its queue.

    The two injected boundaries are after durable dead-letter evidence: first,
    the terminal receipt marker; second, the recovery queue offset commit.  In
    either interruption shape the admitted record and its queued successor
    remain replayable, and startup reconciliation must classify the original
    receipt as quarantined, never written.
    """
    from context_intelligence_server.main import _reconcile_native_recovery_outbox
    from context_intelligence_server.queue_manager import QueueManager

    body = _recovery_body(session_id="deadletter-boundary")
    payload = _without_recovery_source_paths(
        {key: value for key, value in body.items() if key not in {"origin", "source"}}
    )
    source_handle = _source_handle()
    receipts = RecoveryReceiptStore(tmp_path / "receipts.sqlite3")
    # This accepted result is the server-side durable condition that produces
    # the source client's original 202; no retry acknowledgement is involved.
    assert (
        await receipts.admit_source(
            source_handle,
            body["source"],
            body["origin"],
            "source-client",
            body["workspace"],
            payload,
            require_lease=False,
        )
        == "accepted"
    )
    await receipts.mark(body["origin"], "enqueued", source_handle=source_handle)

    queue_manager = QueueManager(tmp_path / "recovery-queue")
    origin = {"source_handle": source_handle, **body["origin"]}
    raw = json.dumps(
        {
            **payload,
            "created_by": "source-client",
            "_recovery_origin": origin,
        },
        separators=(",", ":"),
    ).encode()
    successor = b'{"event":"successor","data":{"session_id":"deadletter-boundary"}}'
    await queue_manager.append("deadletter-boundary", raw)
    await queue_manager.append("deadletter-boundary", successor)

    recovery_registry = SessionRegistry()
    recovery_registry.is_recovery = True
    recovery_registry._queue_manager = queue_manager
    recovery_registry._write_semaphore = asyncio.Semaphore(1)
    recovery_registry.on_record_quarantined = lambda _worker, _record: receipts.mark(
        body["origin"], "quarantined", source_handle=source_handle
    )
    worker = SessionWorker(
        session_id="deadletter-boundary",
        workspace=body["workspace"],
        services=HookStateService(workspace=body["workspace"]),
    )
    original_mark = receipts.mark
    original_commit = queue_manager.commit

    if failure_stage == "receipt_marker":

        async def fail_marker(*_args: Any, **_kwargs: Any) -> None:
            raise OSError("injected receipt marker failure")

        recovery_registry.on_record_quarantined = lambda _worker, _record: fail_marker()
    else:

        async def fail_commit(*_args: Any, **_kwargs: Any) -> None:
            raise OSError("injected recovery queue commit failure")

        queue_manager.commit = fail_commit  # type: ignore[method-assign]

    batch = await queue_manager.read_batch("deadletter-boundary", max_items=1)
    with patch(
        "context_intelligence_server.registry.process_event",
        new=AsyncMock(side_effect=ValueError("deterministic poison")),
    ):
        with pytest.raises(OSError, match="injected"):
            await recovery_registry._handle_exhausted_batch(
                worker, batch, handlers=MagicMock()
            )

    # The evidence is durable, but neither error path may acknowledge the
    # original record or silently skip the following source record.
    assert len(await queue_manager.read_dead_letters("deadletter-boundary")) == 1
    replay = await queue_manager.read_batch("deadletter-boundary", max_items=10)
    assert [record.raw for record in replay.records] == [raw, successor]
    if failure_stage == "receipt_marker":
        assert await receipts.status() == {"enqueued": 1}
        recovery_registry.on_record_quarantined = lambda _worker, _record: (
            original_mark(body["origin"], "quarantined", source_handle=source_handle)
        )
    else:
        assert await receipts.status() == {"quarantined": 1}
        queue_manager.commit = original_commit  # type: ignore[method-assign]

    app = SimpleNamespace(
        state=SimpleNamespace(
            recovery_receipts=receipts,
            recovery_registry=SimpleNamespace(
                queue_manager=queue_manager, get_or_create=MagicMock()
            ),
        )
    )
    await _reconcile_native_recovery_outbox(cast(FastAPI, app))

    # Historic commit-first state and a marker failure both converge on the
    # dead-letter evidence.  Only an explicit quarantine recovery workflow may
    # change this state; reconciliation can never call it a graph write.
    assert await receipts.status() == {"quarantined": 1}
    replay = await queue_manager.read_batch("deadletter-boundary", max_items=10)
    assert [record.raw for record in replay.records] == [raw, successor]


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
            "permit_required": False,
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
        assert (await client.post("/recovery/events", json=body)).status_code == 429
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


def _recovery_body(
    *, session_id: str = "recovery-session", workspace: str = "one"
) -> dict[str, Any]:
    return {
        "event": "session:start",
        "workspace": workspace,
        "data": {
            "session_id": session_id,
            "timestamp": "2026-01-01T00:00:00",
            "label": "café",
        },
        "source": {
            "protocol": "native-recovery-source-v1",
            "session_id": session_id,
            "source_stream_sha256": "b" * 64,
            "source_sha256": "c" * 64,
            "record_count": 1,
        },
        "origin": {
            "ordinal": 0,
            "source_line_sha256": "a" * 64,
        },
    }


def _admission_body(body: dict[str, Any]) -> dict[str, Any]:
    payload = _without_recovery_source_paths(
        {key: value for key, value in body.items() if key not in {"origin", "source"}}
    )
    return {
        "workspace": body["workspace"],
        "source": body["source"],
        "origin": body["origin"],
        # This is the bundle recovery client's semantic canonicalization,
        # not a call to the server helper under test.
        "payload_sha256": hashlib.sha256(
            json.dumps(
                payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ).encode("utf-8")
        ).hexdigest(),
    }


_SOURCE_CAPABILITY = base64.urlsafe_b64encode(b"\x01" * 32).decode().rstrip("=")
_SECOND_SOURCE_CAPABILITY = base64.urlsafe_b64encode(b"\x02" * 32).decode().rstrip("=")
_THIRD_SOURCE_CAPABILITY = base64.urlsafe_b64encode(b"\x03" * 32).decode().rstrip("=")


def _source_headers(
    *, source_capability: str = _SOURCE_CAPABILITY, **headers: str
) -> dict[str, str]:
    return {"X-Recovery-Source": source_capability, **headers}


def _source_handle(capability: str = _SOURCE_CAPABILITY) -> str:
    handle = source_handle_from_capability(capability)
    assert handle
    return handle


def _source_capability(byte: int) -> str:
    return base64.urlsafe_b64encode(bytes([byte]) * 32).decode().rstrip("=")


def _permit_binding(
    gate: RecoveryAdmissionGate,
    body: dict[str, Any],
    *,
    actor: str = "",
    source_capability: str = _SOURCE_CAPABILITY,
) -> _PermitBinding:
    return gate.binding(
        _source_handle(source_capability),
        source_descriptor_digest(body["source"]),
        body["origin"],
        actor,
        body["workspace"],
        _admission_body(body)["payload_sha256"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ("/recovery/admissions", "/recovery/events"))
async def test_recovery_body_limit_rejects_declared_oversize_before_receive(
    path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import context_intelligence_server.main as main_module

    monkeypatch.setattr(main_module, "_MAX_RECOVERY_BODY_BYTES", 512)
    downstream = AsyncMock()
    receive = AsyncMock()
    sent: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await main_module.RecoveryBodyLimitMiddleware(downstream)(
        {
            "type": "http",
            "method": "POST",
            "path": path,
            "headers": [(b"content-length", b"513")],
        },
        receive,
        send,
    )

    receive.assert_not_awaited()
    downstream.assert_not_awaited()
    assert sent[0]["status"] == 413


def _without_recovery_source_paths(value: Any) -> Any:
    """Mirror the documented client-side canonicalization without server reuse."""
    if isinstance(value, dict):
        return {
            key: _without_recovery_source_paths(child)
            for key, child in value.items()
            if key != "working_dir"
        }
    if isinstance(value, list):
        return [_without_recovery_source_paths(child) for child in value]
    return value


@pytest.mark.asyncio
async def test_recovery_strips_source_paths_before_permit_and_delivery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Native recovery accepts legacy paths but never retains them at any depth."""
    from context_intelligence_server.main import app, create_asgi_app

    settings = Settings(
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
    asgi_app = create_asgi_app(settings=settings)
    monkeypatch.setattr(app.state.recovery_registry, "get_or_create", MagicMock())
    body = _recovery_body()
    body["working_dir"] = "/private/top-level"
    body["data"]["working_dir"] = "/private/data"
    body["data"]["nested"] = {
        "working_dir": "/private/nested",
        "items": [{"working_dir": "/private/list"}],
    }

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app),
        base_url="http://test",
        headers=_source_headers(),
    ) as client:
        admission = await client.post(
            "/recovery/admissions",
            json={**_admission_body(body), "working_dir": "/private/admission"},
        )
        assert admission.status_code == 201
        queued = await client.post(
            "/recovery/events",
            json=body,
            headers={"X-Recovery-Permit": admission.json()["permit"]},
        )
        assert queued.status_code == 202
        duplicate = await client.post("/recovery/events", json=body)
        assert duplicate.status_code == 202
        assert duplicate.json()["status"] == "duplicate"

    receipts = await app.state.recovery_receipts.pending_outbox()
    batch = await app.state.recovery_registry.queue_manager.read_batch(
        "recovery-session", max_items=1
    )
    persisted = json.dumps(
        {
            "receipts": receipts,
            "spool": [record.raw.decode() for record in batch.records],
        }
    )
    assert "working_dir" not in persisted
    assert "/private/" not in persisted


@pytest.mark.integration
async def test_native_recovery_assigns_stable_identity_to_same_timestamp_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two source records survive a collision; replaying one remains idempotent."""
    from context_intelligence_server.main import app, create_asgi_app

    settings = Settings(
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
    asgi_app = create_asgi_app(settings=settings)
    monkeypatch.setattr(app.state.recovery_registry, "get_or_create", MagicMock())
    timestamp = "2026-01-01T00:00:00+00:00"
    first = _recovery_body()
    first.update(event="custom:marker")
    first["source"]["record_count"] = 2
    first["data"].update(timestamp=timestamp, marker="first", debug={"record": 1})
    second = {
        **first,
        "data": {**first["data"], "marker": "second", "debug": {"record": 2}},
        "origin": {
            "ordinal": 1,
            "source_line_sha256": "d" * 64,
        },
    }

    async def admit_and_queue(body: dict[str, Any]) -> tuple[bytes, int]:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=asgi_app),
            base_url="http://test",
            headers=_source_headers(),
        ) as client:
            admission = await client.post(
                "/recovery/admissions", json=_admission_body(body)
            )
            assert admission.status_code == 201
            queued = await client.post(
                "/recovery/events",
                json=body,
                headers={"X-Recovery-Permit": admission.json()["permit"]},
            )
            assert queued.status_code == 202
        batch = await app.state.recovery_registry.queue_manager.read_batch(
            body["data"]["session_id"], max_items=1
        )
        assert len(batch.records) == 1
        return batch.records[0].raw, batch.end_offset

    blob_store = _RecordingBlobStore()
    services = HookStateService(workspace="one", blob_store=blob_store)
    worker = SessionWorker(
        session_id="recovery-session", workspace="one", services=services
    )
    handlers = PipelineHandlers(default=DefaultHandler(services), enrichers=[])

    first_raw, first_end = await admit_and_queue(first)
    first_identity = recovery_event_identity(_source_handle(), 0)
    assert json.loads(first_raw)[SPOOL_EVENT_IDENTITY] == first_identity
    event, _workspace, _working_dir, data, identity = SessionRegistry._parse_line(
        first_raw
    )
    await process_event(worker, event, data, handlers, event_identity=identity)
    await app.state.recovery_receipts.mark(
        first["origin"], "written", source_handle=_source_handle()
    )
    await app.state.recovery_registry.queue_manager.commit(
        "recovery-session", first_end
    )
    first_event_id = f"{make_node_id('recovery-session', 'custom:marker', timestamp)}__{first_identity}"

    second_raw, second_end = await admit_and_queue(second)
    second_identity = recovery_event_identity(_source_handle(), 1)
    assert json.loads(second_raw)[SPOOL_EVENT_IDENTITY] == second_identity
    event, _workspace, _working_dir, data, identity = SessionRegistry._parse_line(
        second_raw
    )
    await process_event(worker, event, data, handlers, event_identity=identity)
    await app.state.recovery_receipts.mark(
        second["origin"], "written", source_handle=_source_handle()
    )
    await app.state.recovery_registry.queue_manager.commit(
        "recovery-session", second_end
    )
    second_event_id = f"{make_node_id('recovery-session', 'custom:marker', timestamp)}__{second_identity}"

    # Replay exactly the first native source record. Its durable identity must
    # target the first Event/blob, never overwrite the second or make a third.
    event, _workspace, _working_dir, data, identity = SessionRegistry._parse_line(
        first_raw
    )
    await process_event(worker, event, data, handlers, event_identity=identity)

    assert first_event_id != second_event_id
    first_node = await services.graph.get_node(first_event_id)
    second_node = await services.graph.get_node(second_event_id)
    assert first_node is not None and second_node is not None
    assert json.loads(first_node["data"])["marker"] == "first"
    assert json.loads(second_node["data"])["marker"] == "second"
    event_nodes = {
        node_id
        for node_id, props in services.graph._nodes.items()  # type: ignore[attr-defined]
        if props.get("event_name") == "custom:marker"
    }
    assert event_nodes == {first_event_id, second_event_id}
    assert set(blob_store.keys) == {
        f"{first_event_id}__debug",
        f"{second_event_id}__debug",
    }


@pytest.mark.asyncio
async def test_recovery_flush_marks_the_source_receipt_written(tmp_path: Path) -> None:
    """The terminal receipt transition must precede the queue acknowledgement."""
    from context_intelligence_server.main import app, create_asgi_app

    settings = Settings(
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
    create_asgi_app(settings=settings)
    body = _recovery_body()
    source_handle = _source_handle()
    payload = {
        key: value for key, value in body.items() if key not in {"origin", "source"}
    }
    assert (
        await app.state.recovery_receipts.admit_source(
            source_handle,
            body["source"],
            body["origin"],
            "",
            body["workspace"],
            payload,
            require_lease=False,
        )
        == "accepted"
    )

    callback = app.state.recovery_registry.on_batch_flushed
    await callback(
        None,
        [
            SimpleNamespace(
                raw=json.dumps(
                    {
                        "_recovery_origin": {
                            "source_handle": source_handle,
                            "ordinal": body["origin"]["ordinal"],
                        }
                    }
                ).encode()
            )
        ],
    )

    assert await app.state.recovery_receipts.status() == {"written": 1}


@pytest.mark.asyncio
async def test_recovery_admission_reads_the_supplied_app_registry() -> None:
    """An app-specific admission check must not consult the module singleton."""
    from context_intelligence_server.main import _native_recovery_admission_available

    queue_manager = SimpleNamespace(has_pending_records=AsyncMock(return_value=False))
    receipts = SimpleNamespace(has_pending=AsyncMock(return_value=False))
    app_instance = SimpleNamespace(
        state=SimpleNamespace(
            recovery_receipts=receipts,
            recovery_paused=False,
            registry=SimpleNamespace(queue_manager=queue_manager),
        )
    )

    assert await _native_recovery_admission_available(cast(FastAPI, app_instance))
    receipts.has_pending.assert_awaited_once()
    queue_manager.has_pending_records.assert_awaited_once()


@pytest.mark.asyncio
async def test_permit_denials_scan_live_queue_for_existing_and_new_origins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Foreign receipt denials perform the same bounded live gate work as new ones."""
    from context_intelligence_server.main import app, create_asgi_app

    settings = Settings(
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
    asgi_app = create_asgi_app(settings=settings)
    body = _recovery_body()
    payload = _without_recovery_source_paths(
        {key: value for key, value in body.items() if key not in {"origin", "source"}}
    )
    assert (
        await app.state.recovery_receipts.admit_source(
            _source_handle(),
            body["source"],
            body["origin"],
            "other",
            body["workspace"],
            payload,
            require_lease=False,
        )
        == "accepted"
    )
    await app.state.recovery_receipts.mark(
        body["origin"], "written", source_handle=_source_handle()
    )

    scan_results = iter((False, False, True))
    scans = 0

    async def delayed_live_scan() -> bool:
        nonlocal scans
        scans += 1
        await asyncio.sleep(0)
        return next(scan_results)

    monkeypatch.setattr(
        app.state.registry.queue_manager, "has_pending_records", delayed_live_scan
    )
    gate = app.state.recovery_admission_gate
    foreign_permit = "test-valid-foreign-permit"
    gate._permits[foreign_permit] = (
        _permit_binding(gate, body),
        time.monotonic() + 10,
    )
    unavailable_body = {
        **body,
        "data": {**body["data"], "session_id": "new-session"},
        "source": {**body["source"], "session_id": "new-session"},
        "origin": {
            **body["origin"],
            "ordinal": 1,
            "source_line_sha256": "c" * 64,
        },
    }
    unavailable_permit = "test-valid-unavailable-permit"
    gate._permits[unavailable_permit] = (
        _permit_binding(
            gate,
            unavailable_body,
            source_capability=_SECOND_SOURCE_CAPABILITY,
        ),
        time.monotonic() + 10,
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app),
        base_url="http://test",
        headers=_source_headers(),
    ) as client:
        issuance = await client.post("/recovery/admissions", json=_admission_body(body))
        assert issuance.status_code == 429
        assert scans == 1
        foreign = await client.post(
            "/recovery/events",
            json=body,
            headers={"X-Recovery-Permit": foreign_permit},
        )
        assert scans == 2
        unavailable = await client.post(
            "/recovery/events",
            json=unavailable_body,
            headers={
                "X-Recovery-Source": _SECOND_SOURCE_CAPABILITY,
                "X-Recovery-Permit": unavailable_permit,
            },
        )
        assert scans == 3

    assert foreign.status_code == unavailable.status_code == 429
    assert foreign.json() == unavailable.json() == {"detail": "Recovery unavailable"}
    assert foreign.headers["Retry-After"] == unavailable.headers["Retry-After"]


@pytest.mark.asyncio
async def test_recovery_events_reconcile_once_before_every_permit_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """All valid recovery event outcomes perform one pre-classification repair."""
    import context_intelligence_server.main as main_module
    from context_intelligence_server.main import app, create_asgi_app

    settings = Settings(
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
    asgi_app = create_asgi_app(settings=settings)
    reconciled = AsyncMock()
    monkeypatch.setattr(main_module, "_reconcile_native_recovery_outbox", reconciled)
    gate = app.state.recovery_admission_gate

    def body_for(session_id: str, ordinal: int, source_line: str) -> dict[str, Any]:
        body = _recovery_body(session_id=session_id)
        body["origin"]["ordinal"] = ordinal
        body["origin"]["source_line_sha256"] = source_line * 64
        return body

    def add_permit(
        token: str, body: dict[str, Any], *, expires_at: float | None = None
    ) -> None:
        gate._permits[token] = (
            _permit_binding(gate, body),
            time.monotonic() + 10 if expires_at is None else expires_at,
        )

    async def seed_receipt(body: dict[str, Any], actor: str) -> None:
        payload = _without_recovery_source_paths(
            {key: value for key, value in body.items() if key != "origin"}
        )
        assert (
            await app.state.recovery_receipts.admit(
                body["origin"],
                actor,
                body["workspace"],
                payload,
                require_lease=False,
            )
            == "accepted"
        )
        await app.state.recovery_receipts.mark(body["origin"], "written")

    async def seed_source_receipt(body: dict[str, Any]) -> None:
        payload = _without_recovery_source_paths(
            {
                key: value
                for key, value in body.items()
                if key not in {"origin", "source"}
            }
        )
        source_handle = _source_handle()
        assert (
            await app.state.recovery_receipts.admit_source(
                source_handle,
                body["source"],
                body["origin"],
                "",
                body["workspace"],
                payload,
                require_lease=False,
            )
            == "accepted"
        )
        await app.state.recovery_receipts.mark(
            body["origin"], "written", source_handle=source_handle
        )

    async def request_once(
        client: httpx.AsyncClient,
        body: dict[str, Any],
        token: str | None = None,
        source_capability: str = _SOURCE_CAPABILITY,
    ) -> httpx.Response:
        reconciled.reset_mock()
        headers = _source_headers(source_capability=source_capability)
        if token is not None:
            headers["X-Recovery-Permit"] = token
        response = await client.post("/recovery/events", json=body, headers=headers)
        reconciled.assert_awaited_once_with(app)
        return response

    duplicate_body = body_for("duplicate", 0, "a")
    foreign_body = body_for("foreign", 1, "b")
    await seed_source_receipt(duplicate_body)
    await seed_receipt(foreign_body, "other")
    add_permit("foreign-permit", foreign_body)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app),
        base_url="http://test",
        headers=_source_headers(),
    ) as client:
        duplicate = await request_once(client, duplicate_body)
        assert duplicate.status_code == 202
        assert duplicate.json()["status"] == "duplicate"

        foreign = await request_once(
            client, foreign_body, "foreign-permit", _SECOND_SOURCE_CAPABILITY
        )
        assert foreign.status_code == 429

        missing = await request_once(
            client,
            body_for("missing", 2, "c"),
            source_capability=_THIRD_SOURCE_CAPABILITY,
        )
        assert missing.status_code == 429

        expired_body = body_for("expired", 3, "d")
        add_permit("expired-permit", expired_body, expires_at=0)
        expired = await request_once(
            client, expired_body, "expired-permit", _source_capability(4)
        )
        assert expired.status_code == 429

        mismatch_body = body_for("mismatch", 4, "e")
        add_permit("mismatch-permit", body_for("other", 5, "f"))
        mismatch = await request_once(
            client, mismatch_body, "mismatch-permit", _source_capability(5)
        )
        assert mismatch.status_code == 429

        unavailable_body = body_for("unavailable", 6, "0")
        add_permit("unavailable-permit", unavailable_body)
        monkeypatch.setattr(app.state, "recovery_paused", True)
        unavailable = await request_once(
            client, unavailable_body, "unavailable-permit", _source_capability(6)
        )
        assert unavailable.status_code == 429


@pytest.mark.asyncio
async def test_recovery_permit_endpoint_binds_identity_and_keeps_duplicates_retryable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only the exact issuer/workspace/origin/payload may consume a permit."""
    from context_intelligence_server.main import app, create_asgi_app

    def key(token: str) -> str:
        return hashlib.sha256(token.encode()).hexdigest()

    settings = Settings(
        api_keys={
            key("alice-token"): {"id": "alice"},
            key("bob-token"): {"id": "bob"},
        },
        api_keys_store_path=str(tmp_path / "keys.json"),
        access_control_mode="scoped",
        contributor_grants=cast(
            Any,
            {
                "alice": {
                    "workspaces": ["one", "two"],
                    "capabilities": ["live:write", "recovery:write"],
                },
                "bob": {
                    "workspaces": ["one"],
                    "capabilities": ["live:write", "recovery:write"],
                },
            },
        ),
        queues_path=str(tmp_path / "live"),
        recovery=cast(
            Any,
            {
                "enabled": True,
                "permit_ttl_seconds": 10,
                "receipt_store_path": str(tmp_path / "receipts.sqlite3"),
                "queues_path": str(tmp_path / "recovery"),
            },
        ),
    )
    asgi_app = create_asgi_app(settings=settings)
    monkeypatch.setattr(app.state.recovery_registry, "get_or_create", MagicMock())
    body = _recovery_body()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app),
        base_url="http://test",
        headers=_source_headers(Authorization="Bearer alice-token"),
    ) as alice:
        assert (
            "native-recovery-source-v1"
            in (await alice.get("/version")).json()["capabilities"]
        )
        missing = await alice.post("/recovery/events", json=body)
        assert missing.status_code == 429
        assert await app.state.recovery_receipts.status() == {}

        issued = await alice.post("/recovery/admissions", json=_admission_body(body))
        assert issued.status_code == 201
        permit = issued.json()["permit"]
        assert issued.json()["expires_at"] > time.time()
        assert (
            await alice.post("/recovery/admissions", json=_admission_body(body))
        ).status_code == 429

        wrong_origin = {
            **body,
            "origin": {**body["origin"], "ordinal": 1, "source_line_sha256": "c" * 64},
        }
        wrong_origin_response = await alice.post(
            "/recovery/events",
            json=wrong_origin,
            headers={"X-Recovery-Permit": permit},
        )
        assert wrong_origin_response.status_code == 429
        wrong_payload = {**body, "event": "tool_use"}
        wrong_payload_response = await alice.post(
            "/recovery/events",
            json=wrong_payload,
            headers={"X-Recovery-Permit": permit},
        )
        assert wrong_payload_response.status_code == 429
        wrong_workspace = {**body, "workspace": "two"}
        wrong_workspace_response = await alice.post(
            "/recovery/events",
            json=wrong_workspace,
            headers={"X-Recovery-Permit": permit},
        )
        assert wrong_workspace_response.status_code == 429

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=asgi_app),
            base_url="http://test",
            headers=_source_headers(Authorization="Bearer bob-token"),
        ) as bob:
            bob_mismatch = await bob.post(
                "/recovery/events", json=body, headers={"X-Recovery-Permit": permit}
            )
            assert bob_mismatch.status_code == 429
        assert await app.state.recovery_receipts.status() == {}

        accepted = await alice.post(
            "/recovery/events", json=body, headers={"X-Recovery-Permit": permit}
        )
        assert accepted.status_code == 202
        assert accepted.json()["status"] == "queued"
        # A lost 202 must not force the native cursor to request a new permit.
        # The durable receipt is an acknowledgement only, never another write.
        acknowledged = await alice.post(
            "/recovery/admissions", json=_admission_body(body)
        )
        assert acknowledged.status_code == 200
        assert acknowledged.json() == {"status": "duplicate"}
        assert "permit" not in acknowledged.json()
        changed_descriptor = await alice.post(
            "/recovery/admissions",
            json={
                **_admission_body(body),
                "source": {**body["source"], "record_count": 2},
            },
        )
        assert changed_descriptor.status_code == 409
        changed_payload = await alice.post(
            "/recovery/admissions",
            json={**_admission_body(body), "payload_sha256": "f" * 64},
        )
        assert changed_payload.status_code == 409
        await app.state.recovery_receipts.mark(
            body["origin"], "written", source_handle=_source_handle()
        )

        # Model the narrow crash/upgrade TOCTOU window: Bob's permit was issued
        # while this origin was clear, then a foreign durable receipt appeared
        # before it was consumed. The request must neither spend Bob's permit
        # nor disclose that durable receipt.
        foreign_body = {
            **body,
            "data": {**body["data"], "session_id": "foreign-session"},
            "source": {**body["source"], "session_id": "foreign-session"},
            "origin": {
                **body["origin"],
                "ordinal": 2,
                "source_line_sha256": "e" * 64,
            },
        }
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=asgi_app),
            base_url="http://test",
            headers=_source_headers(
                source_capability=_SECOND_SOURCE_CAPABILITY,
                Authorization="Bearer bob-token",
            ),
        ) as bob:
            foreign_acknowledgement = await bob.post(
                "/recovery/admissions",
                json=_admission_body(body),
                headers=_source_headers(Authorization="Bearer bob-token"),
            )
            assert foreign_acknowledgement.status_code == 429
            foreign_admission = await bob.post(
                "/recovery/admissions", json=_admission_body(foreign_body)
            )
            assert foreign_admission.status_code == 201
            foreign_permit = foreign_admission.json()["permit"]
            foreign_payload = _without_recovery_source_paths(
                {
                    key: value
                    for key, value in foreign_body.items()
                    if key not in {"origin", "source"}
                }
            )
            assert (
                await app.state.recovery_receipts.admit_source(
                    _source_handle(_SECOND_SOURCE_CAPABILITY),
                    foreign_body["source"],
                    foreign_body["origin"],
                    "alice",
                    "one",
                    foreign_payload,
                    require_lease=False,
                )
                == "accepted"
            )
            await app.state.recovery_receipts.mark(
                foreign_body["origin"],
                "written",
                source_handle=_source_handle(_SECOND_SOURCE_CAPABILITY),
            )
            receipt_state_before = await app.state.recovery_receipts.status()
            foreign_existing = await bob.post(
                "/recovery/events",
                json=foreign_body,
                headers={"X-Recovery-Permit": foreign_permit},
            )
            assert foreign_permit in app.state.recovery_admission_gate._permits
            assert await app.state.recovery_receipts.status() == receipt_state_before

            # The public protocol permits only one outstanding token. Inject a
            # second independently issued binding to compare the same HTTP
            # denial against a valid permit blocked by current live work.
            unavailable_body = {
                **body,
                "data": {**body["data"], "session_id": "unavailable-session"},
                "source": {**body["source"], "session_id": "unavailable-session"},
                "origin": {
                    **body["origin"],
                    "ordinal": 3,
                    "source_line_sha256": "f" * 64,
                },
            }
            gate = app.state.recovery_admission_gate
            unavailable_permit = "test-valid-currently-unavailable"
            gate._permits[unavailable_permit] = (
                _permit_binding(
                    gate,
                    unavailable_body,
                    actor="bob",
                    source_capability=_THIRD_SOURCE_CAPABILITY,
                ),
                time.monotonic() + 10,
            )
            await app.state.registry.queue_manager.append("live-blocker", b"{}")
            unavailable = await bob.post(
                "/recovery/events",
                json=unavailable_body,
                headers={
                    "X-Recovery-Permit": unavailable_permit,
                    "X-Recovery-Source": _THIRD_SOURCE_CAPABILITY,
                },
            )
        assert foreign_existing.status_code == unavailable.status_code == 429
        assert (
            foreign_existing.json()
            == unavailable.json()
            == {"detail": "Recovery unavailable"}
        )
        assert (
            foreign_existing.headers["Retry-After"]
            == unavailable.headers["Retry-After"]
        )
        spent = {
            **body,
            "data": {**body["data"], "session_id": "second"},
            "source": {**body["source"], "session_id": "second"},
            "origin": {
                **body["origin"],
                "ordinal": 1,
                "source_line_sha256": "d" * 64,
            },
        }
        assert (
            await alice.post(
                "/recovery/events",
                json=spent,
                headers={
                    "X-Recovery-Permit": permit,
                    "X-Recovery-Source": _source_capability(7),
                },
            )
        ).status_code == 429
        # An exact durable duplicate is safe to retry without another permit.
        duplicate = await alice.post("/recovery/events", json=body)
        assert duplicate.status_code == 202
        assert duplicate.json()["status"] == "duplicate"


@pytest.mark.asyncio
async def test_recovery_permits_expire_and_are_invalid_after_restart() -> None:
    async def available() -> bool:
        return True

    body = _recovery_body(session_id="s")
    body["origin"]["source_line_sha256"] = "b" * 64
    gate = RecoveryAdmissionGate(1)
    binding = _permit_binding(gate, body, actor="alice")
    issued = await gate.issue(binding, available)
    assert issued is not None
    permit, _expiry = issued
    # A new gate is a new server generation and has no predecessor permits.
    async with RecoveryAdmissionGate(1).consume(permit, binding, available) as result:
        assert result == "invalid"

    expired = RecoveryAdmissionGate(1)
    expired._permits["expired"] = (binding, 0)
    async with expired.consume("expired", binding, available) as result:
        assert result == "invalid"


@pytest.mark.asyncio
async def test_permit_expiry_uses_monotonic_time_when_wall_clock_moves_backward(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A wall-clock rollback must not revive a monotonic-expired permit."""
    import context_intelligence_server.recovery as recovery_module

    async def available() -> bool:
        return True

    body = _recovery_body(session_id="s")
    body["origin"]["source_line_sha256"] = "b" * 64
    gate = RecoveryAdmissionGate(10)
    binding = _permit_binding(gate, body, actor="alice")
    gate._permits["permit"] = (binding, 100.0)
    monkeypatch.setattr(recovery_module.time, "time", lambda: 1.0)
    monkeypatch.setattr(recovery_module.time, "monotonic", lambda: 101.0)

    async with gate.consume("permit", binding, available) as result:
        assert result == "invalid"


@pytest.mark.asyncio
async def test_live_enqueue_and_recovery_consume_are_serialized() -> None:
    """A live append that wins the shared gate makes recovery unavailable."""
    live_present = False

    async def available() -> bool:
        return not live_present

    body = _recovery_body(session_id="s")
    body["origin"]["source_line_sha256"] = "b" * 64
    gate = RecoveryAdmissionGate(10)
    binding = _permit_binding(gate, body, actor="alice")
    issued = await gate.issue(binding, available)
    assert issued is not None
    permit, _expiry = issued
    live_entered = asyncio.Event()
    release_live = asyncio.Event()

    async def live_enqueue() -> None:
        nonlocal live_present
        async with gate.live_enqueue():
            live_present = True
            live_entered.set()
            await release_live.wait()

    live_task = asyncio.create_task(live_enqueue())
    await live_entered.wait()

    async def consume() -> str:
        async with gate.consume(permit, binding, available) as result:
            return result

    consume_task = asyncio.create_task(consume())
    await asyncio.sleep(0)
    release_live.set()
    assert await consume_task == "unavailable"
    await live_task


@pytest.mark.asyncio
async def test_paused_recovery_queue_waits_before_graph_flush_and_commits_once_after_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pause atomically fences an admitted recovery record's flush and commit."""
    from context_intelligence_server.main import app, create_asgi_app
    from context_intelligence_server.registry import SessionWorker
    from context_intelligence_server.routers.admin import require_admin
    from context_intelligence_server.services import HookStateService

    settings = Settings(
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
    asgi_app = create_asgi_app(settings=settings)
    recovery_registry = app.state.recovery_registry
    monkeypatch.setattr(recovery_registry, "get_or_create", MagicMock())
    app.dependency_overrides[require_admin] = lambda: None
    body = _recovery_body()
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=asgi_app), base_url="http://test"
        ) as client:
            permit_response = await client.post(
                "/recovery/admissions",
                json=_admission_body(body),
                headers=_source_headers(),
            )
            assert permit_response.status_code == 201
            accepted = await client.post(
                "/recovery/events",
                json=body,
                headers={
                    **_source_headers(),
                    "X-Recovery-Permit": permit_response.json()["permit"],
                },
            )
            assert accepted.status_code == 202
            assert (await client.post("/admin/recovery/pause")).json() == {
                "paused": True
            }

            pre_flush_entered = asyncio.Event()
            real_pre_flush = recovery_registry.pre_flush

            async def observe_pre_flush() -> None:
                pre_flush_entered.set()
                assert real_pre_flush is not None
                await real_pre_flush()

            recovery_registry.pre_flush = AsyncMock(side_effect=observe_pre_flush)
            recovery_registry._process_batch = AsyncMock(return_value=(1, None))
            worker = SessionWorker(
                session_id=body["data"]["session_id"],
                workspace="one",
                services=HookStateService(workspace="one"),
            )
            worker.services.graph = AsyncMock()  # type: ignore[assignment]
            queue = recovery_registry.queue_manager
            real_commit = queue.commit
            queue.commit = AsyncMock(wraps=real_commit)  # type: ignore[method-assign]
            drain = asyncio.create_task(
                recovery_registry._drain_to_eof(worker, MagicMock())
            )
            await asyncio.wait_for(pre_flush_entered.wait(), timeout=1)
            await asyncio.sleep(0)

            recovery_registry.pre_flush.assert_awaited_once()
            worker.services.graph.flush.assert_not_awaited()
            queue.commit.assert_not_awaited()
            assert (
                len(
                    (
                        await queue.read_batch(body["data"]["session_id"], max_items=1)
                    ).records
                )
                == 1
            )

            assert (await client.post("/admin/recovery/resume")).json() == {
                "paused": False
            }
            assert await asyncio.wait_for(drain, timeout=1)

        worker.services.graph.flush.assert_awaited_once()
        queue.commit.assert_awaited_once()
        recovery_registry._process_batch.assert_awaited_once()
        assert (
            await queue.read_batch(body["data"]["session_id"], max_items=1)
        ).records == []
    finally:
        app.dependency_overrides.pop(require_admin, None)


@pytest.mark.asyncio
async def test_permit_issue_blocks_on_live_queue_until_its_record_drains(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Permit availability is derived from live durable records, not /status."""
    from context_intelligence_server.main import create_asgi_app, registry

    settings = Settings(
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
    asgi_app = create_asgi_app(settings=settings)
    monkeypatch.setattr(registry, "get_or_create", MagicMock())
    body = _recovery_body()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app),
        base_url="http://test",
        headers=_source_headers(),
    ) as client:
        live = await client.post(
            "/events",
            json={
                "event": "tool_use",
                "workspace": "one",
                "data": {"session_id": "live", "timestamp": "2026-01-01T00:00:00"},
            },
        )
        assert live.status_code == 202
        blocked = await client.post("/recovery/admissions", json=_admission_body(body))
        assert blocked.status_code == 429
        assert blocked.headers["Retry-After"] == "2"

        batch = await registry.queue_manager.read_batch("live", max_items=1)
        await registry.queue_manager.commit("live", batch.end_offset)
        assert (
            await client.post("/recovery/admissions", json=_admission_body(body))
        ).status_code == 201
