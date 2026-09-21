"""Security-focused coverage for native-recovery-source-v1."""

from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from context_intelligence_server.config import Settings
from context_intelligence_server.queue_manager import MAX_SESSION_ID_BYTES, QueueManager
from context_intelligence_server.recovery import (
    RecoveryAdmissionGate,
    RecoveryReceiptStore,
    source_descriptor_digest,
    source_handle_from_capability,
)


def _capability(byte: int) -> str:
    return base64.urlsafe_b64encode(bytes([byte]) * 32).decode().rstrip("=")


def _source(session_id: str = "capture-session") -> dict[str, Any]:
    return {
        "protocol": "native-recovery-source-v1",
        "session_id": session_id,
        "source_stream_sha256": "a" * 64,
        "source_sha256": "b" * 64,
        "record_count": 1,
    }


def _origin(ordinal: int = 0, line: str = "c") -> dict[str, Any]:
    return {"ordinal": ordinal, "source_line_sha256": line * 64}


def _payload(session_id: str = "capture-session") -> dict[str, Any]:
    return {
        "event": "session:start",
        "workspace": "one",
        "data": {"session_id": session_id, "timestamp": "2026-01-01T00:00:00"},
    }


def _payload_digest(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
    ).hexdigest()


def _raw_ascii_json(value: dict[str, Any]) -> bytes:
    """Encode a request exactly as an escaped-surrogate client would send it."""
    body = json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    assert b"\\ud800" in body
    return body


def _session_id_at_queue_filename_limit() -> str:
    return "x" * MAX_SESSION_ID_BYTES


def test_source_capability_requires_exact_unpadded_base64url_32_bytes() -> None:
    valid = _capability(0)
    assert source_handle_from_capability(valid)
    assert source_handle_from_capability(None) is None
    assert source_handle_from_capability(valid + "=") is None
    assert source_handle_from_capability(valid[:-1]) is None
    assert source_handle_from_capability(valid[:-1] + "+") is None


@pytest.mark.asyncio
async def test_capability_namespaces_prevent_public_origin_receipt_oracles(
    tmp_path: Path,
) -> None:
    """Same public source values under another capability are an unrelated source."""
    store = RecoveryReceiptStore(tmp_path / "receipts.sqlite3")
    source = _source()
    origin = _origin()
    payload = _payload()
    first = source_handle_from_capability(_capability(1))
    second = source_handle_from_capability(_capability(2))
    assert first and second and first != second

    assert (
        await store.admit_source(
            first, source, origin, "alice", "one", payload, require_lease=False
        )
        == "accepted"
    )
    await store.mark(origin, "written", source_handle=first)
    assert (
        await store.classify_source(
            first,
            source,
            origin,
            "alice",
            "one",
            hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        )
        == "duplicate"
    )
    # A second capability holder cannot learn another actor's receipt state.
    assert (
        await store.classify_source(
            first,
            source,
            origin,
            "bob",
            "one",
            hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        )
        == "foreign"
    )
    # A guessed public descriptor/origin under another capability cannot look up
    # Alice's receipt: it has its own source_handle namespace.
    assert (
        await store.classify_source(
            second,
            source,
            origin,
            "bob",
            "one",
            hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        )
        == "new"
    )


@pytest.mark.asyncio
async def test_source_conflicts_are_hash_only_durable_and_restart_safe(
    tmp_path: Path,
) -> None:
    path = tmp_path / "receipts.sqlite3"
    store = RecoveryReceiptStore(path)
    handle = source_handle_from_capability(_capability(3))
    assert handle
    source = _source()
    origin = _origin()
    payload = _payload()
    assert (
        await store.admit_source(
            handle, source, origin, "alice", "one", payload, require_lease=False
        )
        == "accepted"
    )
    await store.mark(origin, "written", source_handle=handle)

    changed_descriptor = {**source, "record_count": 2}
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert (
        await store.classify_source(
            handle, changed_descriptor, origin, "alice", "one", digest
        )
        == "conflict"
    )
    assert (
        await store.classify_source(
            handle, source, _origin(line="d"), "alice", "one", digest
        )
        == "conflict"
    )
    assert (
        await store.classify_source(handle, source, origin, "alice", "one", "e" * 64)
        == "conflict"
    )
    assert await store.pending_outbox() == []
    # Registration and immutable descriptor survive a process restart.
    restarted = RecoveryReceiptStore(path)
    assert (
        await restarted.classify_source(handle, source, origin, "alice", "one", digest)
        == "duplicate"
    )
    with sqlite3.connect(path) as db:
        stored = "\n".join(
            row[0]
            for row in db.execute(
                "SELECT descriptor FROM recovery_sources UNION ALL "
                "SELECT printf('%s|%s|%s', coalesce(stored_descriptor_sha256,''), "
                "coalesce(stored_line_sha256,''), coalesce(stored_payload_sha256,'')) "
                "FROM recovery_source_conflicts"
            )
        )
    assert _capability(3) not in stored
    assert "/private/" not in stored
    assert source_descriptor_digest(source) in stored


@pytest.mark.asyncio
async def test_legacy_receipt_migration_is_transactional_and_strips_paths(
    tmp_path: Path,
) -> None:
    path = tmp_path / "receipts.sqlite3"
    historic_path = b"/synthetic/legacy-working-dir-must-not-survive"
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        db.execute(
            """CREATE TABLE recovery_receipts (
                source_session_id TEXT NOT NULL, source_stream_sha256 TEXT NOT NULL,
                ordinal INTEGER NOT NULL, source_line_sha256 TEXT NOT NULL,
                actor TEXT NOT NULL, workspace TEXT NOT NULL, payload_sha256 TEXT NOT NULL,
                payload TEXT NOT NULL, state TEXT NOT NULL,
                PRIMARY KEY(source_session_id, source_stream_sha256, ordinal)
            )"""
        )
        db.execute(
            "INSERT INTO recovery_receipts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "legacy-session",
                "a" * 64,
                0,
                "b" * 64,
                "alice",
                "one",
                "c" * 64,
                json.dumps(
                    {
                        "event": "session:start",
                        "data": {
                            "session_id": "legacy-session",
                            "working_dir": historic_path.decode(),
                        },
                    }
                ),
                "written",
            ),
        )
        db.execute(
            """CREATE TABLE recovery_lease_consumptions (
                lease_id TEXT PRIMARY KEY
            )"""
        )
        db.execute("INSERT INTO recovery_lease_consumptions VALUES ('legacy-lease')")
    assert any(
        historic_path in artifact.read_bytes()
        for artifact in (
            path,
            path.with_name(f"{path.name}-wal"),
        )
        if artifact.exists()
    )
    store = RecoveryReceiptStore(path)
    assert await store.status() == {"written": 1}
    with sqlite3.connect(path) as db:
        columns = {row[1] for row in db.execute("PRAGMA table_info(recovery_receipts)")}
        persisted = "\n".join(
            row[0] for row in db.execute("SELECT descriptor FROM recovery_sources")
        )
        persisted += "\n" + "\n".join(
            row[0] for row in db.execute("SELECT payload FROM recovery_receipts")
        )
        assert db.execute(
            "SELECT lease_id FROM recovery_lease_consumptions"
        ).fetchall() == [("legacy-lease",)]
    assert columns >= {"source_handle", "ordinal", "source_line_sha256"}
    assert "source_session_id" not in columns
    assert historic_path.decode() not in persisted
    # The final database and every SQLite sidecar must be free of the old
    # payload bytes, including bytes that previously lived in a legacy WAL.
    for artifact in (
        path,
        path.with_name(f"{path.name}-wal"),
        path.with_name(f"{path.name}-shm"),
        path.with_name(f"{path.name}-journal"),
    ):
        if artifact.exists():
            assert historic_path not in artifact.read_bytes()


@pytest.mark.asyncio
async def test_permits_bind_source_actor_descriptor_ordinal_and_payload() -> None:
    async def available() -> bool:
        return True

    gate = RecoveryAdmissionGate(10)
    source_handle = source_handle_from_capability(_capability(4))
    other_handle = source_handle_from_capability(_capability(5))
    assert source_handle and other_handle
    binding = gate.binding(
        source_handle,
        source_descriptor_digest(_source()),
        _origin(),
        "alice",
        "one",
        "e" * 64,
    )
    issued = await gate.issue(binding, available)
    assert issued is not None
    permit, _ = issued
    wrong = gate.binding(
        other_handle,
        source_descriptor_digest(_source()),
        _origin(),
        "bob",
        "one",
        "e" * 64,
    )
    async with gate.consume(permit, wrong, available) as result:
        assert result == "mismatch"
    async with gate.consume(permit, binding, available) as result:
        assert result == "accepted"
    async with gate.consume(permit, binding, available) as result:
        assert result == "invalid"


@pytest.mark.asyncio
async def test_default_http_protocol_requires_capability_and_keeps_it_out_of_storage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    source = _source()
    origin = _origin()
    payload = _payload()
    body = {**payload, "source": source, "origin": origin}
    body["working_dir"] = "/private/envelope"
    body["data"] = {**body["data"], "working_dir": "/private/data"}
    admission = {
        "workspace": "one",
        "source": source,
        "origin": origin,
        "payload_sha256": hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }
    raw_capability = _capability(6)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app), base_url="http://test"
    ) as client:
        missing = await client.post("/recovery/admissions", json=admission)
        malformed = await client.post(
            "/recovery/admissions", json=admission, headers={"X-Recovery-Source": "bad"}
        )
        assert missing.status_code == malformed.status_code == 429
        assert missing.json() == malformed.json() == {"detail": "Recovery unavailable"}
        granted = await client.post(
            "/recovery/admissions",
            json=admission,
            headers={"X-Recovery-Source": raw_capability},
        )
        assert granted.status_code == 201
        queued = await client.post(
            "/recovery/events",
            json=body,
            headers={
                "X-Recovery-Source": raw_capability,
                "X-Recovery-Permit": granted.json()["permit"],
            },
        )
        assert queued.status_code == 202
        assert raw_capability not in json.dumps(queued.json())
        holder_conflict = await client.post(
            "/recovery/events",
            json={**body, "origin": _origin(line="d")},
            headers={"X-Recovery-Source": raw_capability},
        )
        unrelated = await client.post(
            "/recovery/events",
            json={**body, "origin": _origin(line="d")},
            headers={"X-Recovery-Source": _capability(7)},
        )
        assert holder_conflict.status_code == 409
        assert unrelated.status_code == 429
    persisted = json.dumps(await app.state.recovery_receipts.pending_outbox())
    batch = await app.state.recovery_registry.queue_manager.read_batch(
        "capture-session", max_items=1
    )
    persisted += "\n" + "\n".join(record.raw.decode() for record in batch.records)
    assert raw_capability not in persisted
    assert "/private/" not in persisted


@pytest.mark.asyncio
async def test_v1_recovery_requires_both_recovery_and_live_workspace_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A source capability namespaces a capture but never grants event authority."""
    from context_intelligence_server.main import app, create_asgi_app

    def key(token: str) -> str:
        return hashlib.sha256(token.encode()).hexdigest()

    recovery_only = "recovery-only-token"
    both_writes = "both-writes-token"
    settings = Settings(
        api_keys={
            key(recovery_only): {"id": "recovery-only"},
            key(both_writes): {"id": "both-writes"},
        },
        api_keys_store_path=str(tmp_path / "keys.json"),
        access_control_mode="scoped",
        contributor_grants=cast(
            Any,
            {
                "recovery-only": {
                    "workspaces": ["one"],
                    "capabilities": ["recovery:write"],
                },
                "both-writes": {
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
                "receipt_store_path": str(tmp_path / "receipts.sqlite3"),
                "queues_path": str(tmp_path / "recovery"),
            },
        ),
    )
    asgi_app = create_asgi_app(settings=settings)
    monkeypatch.setattr(app.state.recovery_registry, "get_or_create", MagicMock())
    source = _source()
    origin = _origin()
    live_body = _payload()
    recovery_body = {**live_body, "source": source, "origin": origin}
    admission = {
        "workspace": "one",
        "source": source,
        "origin": origin,
        "payload_sha256": hashlib.sha256(
            json.dumps(live_body, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }
    capability = _capability(8)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app),
        base_url="http://test",
        headers={
            "Authorization": f"Bearer {recovery_only}",
            "X-Recovery-Source": capability,
        },
    ) as denied:
        ordinary_live = await denied.post("/events", json=live_body)
        denied_admission = await denied.post("/recovery/admissions", json=admission)
        denied_event = await denied.post("/recovery/events", json=recovery_body)

    assert (
        ordinary_live.status_code
        == denied_admission.status_code
        == denied_event.status_code
        == 403
    )
    assert ordinary_live.json() == denied_admission.json() == denied_event.json()
    assert await app.state.recovery_receipts.status() == {}
    with sqlite3.connect(tmp_path / "receipts.sqlite3") as db:
        assert db.execute("SELECT count(*) FROM recovery_sources").fetchone() == (0,)
        assert db.execute("SELECT count(*) FROM recovery_receipts").fetchone() == (0,)
    assert not await app.state.recovery_registry.queue_manager.has_pending_records()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app),
        base_url="http://test",
        headers={
            "Authorization": f"Bearer {both_writes}",
            "X-Recovery-Source": capability,
        },
    ) as allowed:
        issued = await allowed.post("/recovery/admissions", json=admission)
        assert issued.status_code == 201
        queued = await allowed.post(
            "/recovery/events",
            json=recovery_body,
            headers={"X-Recovery-Permit": issued.json()["permit"]},
        )
    assert queued.status_code == 202


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_session_id",
    (
        "",
        ".",
        "..",
        "capture/escape",
        r"capture\\escape",
        "capture\0id",
        "bad\nid",
        "x" * (MAX_SESSION_ID_BYTES + 1),
    ),
)
async def test_v1_rejects_unsafe_session_ids_without_registering_sources_or_receipts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, invalid_session_id: str
) -> None:
    """Unsafe queue stems fail before permits, durable source registration, or spool writes."""
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
    body = {
        **_payload(invalid_session_id),
        "source": _source(invalid_session_id),
        "origin": _origin(),
    }
    admission = {
        "workspace": "one",
        "source": body["source"],
        "origin": body["origin"],
        "payload_sha256": hashlib.sha256(
            json.dumps(
                _payload(invalid_session_id), sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest(),
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app),
        base_url="http://test",
        headers={"X-Recovery-Source": _capability(9)},
    ) as client:
        admission_response = await client.post("/recovery/admissions", json=admission)
        event_response = await client.post("/recovery/events", json=body)
    assert admission_response.status_code == event_response.status_code == 400
    if invalid_session_id:
        assert invalid_session_id not in admission_response.text
        assert invalid_session_id not in event_response.text
    assert await app.state.recovery_receipts.status() == {}
    with sqlite3.connect(tmp_path / "receipts.sqlite3") as db:
        assert db.execute("SELECT count(*) FROM recovery_sources").fetchone() == (0,)
        assert db.execute("SELECT count(*) FROM recovery_receipts").fetchone() == (0,)
    assert not await app.state.recovery_registry.queue_manager.has_pending_records()


@pytest.mark.asyncio
async def test_queue_session_id_limit_leaves_room_for_append_and_commit_temp_file(
    tmp_path: Path,
) -> None:
    """The maximum accepted UTF-8 stem fits every queue filename we create."""
    queue = QueueManager(tmp_path / "queues")
    session_id = _session_id_at_queue_filename_limit()

    await queue.append(session_id, b'{"event":"test"}')
    batch = await queue.read_batch(session_id, max_items=1)
    assert len(batch.records) == 1
    await queue.commit(session_id, batch.end_offset)

    assert (await queue.read_batch(session_id, max_items=1)).records == []
    # Still a ValueError (never an OSError -- registry._is_transient_infra_error
    # allow-lists bare OSError as retry-forever). The message is now the
    # specific over-budget diagnostic rather than the generic one, because
    # _validate_session_id checks length before is_safe_session_id: both
    # reject an over-budget stem, but only one tells the caller to fold.
    with pytest.raises(ValueError, match="session_id too long"):
        await queue.append(session_id + "x", b'{"event":"test"}')


@pytest.mark.asyncio
async def test_v1_rejects_overlong_source_and_data_ids_before_receipt_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """V1 validates each queue filename stem before permit receipt mutations."""
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
    largest = _session_id_at_queue_filename_limit()
    too_long = largest + "x"
    payload = _payload(largest)
    source = _source(largest)
    origin = _origin()
    admission = {
        "workspace": "one",
        "source": source,
        "origin": origin,
        "payload_sha256": _payload_digest(payload),
    }

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app), base_url="http://test"
    ) as client:
        accepted = await client.post(
            "/recovery/admissions",
            json=admission,
            headers={"X-Recovery-Source": _capability(17)},
        )
        assert accepted.status_code == 201
        queued = await client.post(
            "/recovery/events",
            json={**payload, "source": source, "origin": origin},
            headers={
                "X-Recovery-Source": _capability(17),
                "X-Recovery-Permit": accepted.json()["permit"],
            },
        )
        assert queued.status_code == 202

    accepted_handle = source_handle_from_capability(_capability(17))
    assert accepted_handle
    await app.state.recovery_receipts.mark(
        origin, "written", source_handle=accepted_handle
    )

    # An overlong source identifier fails while parsing the admission request,
    # before source registration, receipt creation, or permit issuance.
    fresh_asgi_app = create_asgi_app(settings=settings)
    monkeypatch.setattr(app.state.recovery_registry, "get_or_create", MagicMock())
    oversized_source = _source(too_long)
    oversized_admission = {
        "workspace": "one",
        "source": oversized_source,
        "origin": _origin(ordinal=1, line="d"),
        "payload_sha256": _payload_digest(_payload(too_long)),
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=fresh_asgi_app), base_url="http://test"
    ) as client:
        source_rejected = await client.post(
            "/recovery/admissions",
            json=oversized_admission,
            headers={"X-Recovery-Source": _capability(18)},
        )
    assert source_rejected.status_code == 400
    assert too_long not in source_rejected.text
    assert not app.state.recovery_admission_gate._permits

    # The source descriptor alone cannot validate a later data session ID. The
    # event boundary therefore rejects the too-long data ID before it can add a
    # receipt or source row.
    data_source = _source("safe-source")
    data_origin = _origin(ordinal=2, line="e")
    data_payload = _payload(too_long)
    data_admission = {
        "workspace": "one",
        "source": data_source,
        "origin": data_origin,
        "payload_sha256": _payload_digest(data_payload),
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=fresh_asgi_app), base_url="http://test"
    ) as client:
        data_permit = await client.post(
            "/recovery/admissions",
            json=data_admission,
            headers={"X-Recovery-Source": _capability(19)},
        )
        assert data_permit.status_code == 201
        data_rejected = await client.post(
            "/recovery/events",
            json={**data_payload, "source": data_source, "origin": data_origin},
            headers={
                "X-Recovery-Source": _capability(19),
                "X-Recovery-Permit": data_permit.json()["permit"],
            },
        )
    assert data_rejected.status_code == 400
    assert data_rejected.json() == {"detail": "Invalid recovery session identity"}
    assert too_long not in data_rejected.text
    assert await app.state.recovery_receipts.status() == {"written": 1}
    with sqlite3.connect(tmp_path / "receipts.sqlite3") as db:
        assert db.execute("SELECT count(*) FROM recovery_sources").fetchone() == (1,)
        assert db.execute("SELECT count(*) FROM recovery_receipts").fetchone() == (1,)


@pytest.mark.asyncio
async def test_v1_requires_source_and_data_session_ids_to_match_before_receipt_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    source = _source("source-session")
    payload = _payload("different-session")
    body = {**payload, "source": source, "origin": _origin()}
    admission = {
        "workspace": "one",
        "source": source,
        "origin": body["origin"],
        "payload_sha256": hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app),
        base_url="http://test",
        headers={"X-Recovery-Source": _capability(12)},
    ) as client:
        issued = await client.post("/recovery/admissions", json=admission)
        assert issued.status_code == 201
        rejected = await client.post(
            "/recovery/events",
            json=body,
            headers={"X-Recovery-Permit": issued.json()["permit"]},
        )
    assert rejected.status_code == 400
    assert rejected.json() == {"detail": "Invalid recovery session identity"}
    assert await app.state.recovery_receipts.status() == {}
    with sqlite3.connect(tmp_path / "receipts.sqlite3") as db:
        assert db.execute("SELECT count(*) FROM recovery_sources").fetchone() == (0,)
        assert db.execute("SELECT count(*) FROM recovery_receipts").fetchone() == (0,)


@pytest.mark.asyncio
async def test_persisted_invalid_pending_receipt_is_scrubbed_quarantined_and_does_not_block_valid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Startup reconciliation must terminally isolate old unsafe rows before enqueue."""
    from context_intelligence_server.main import (
        _reconcile_native_recovery_outbox,
        app,
        create_asgi_app,
    )

    receipt_path = tmp_path / "receipts.sqlite3"
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
                "receipt_store_path": str(receipt_path),
                "queues_path": str(tmp_path / "recovery"),
            },
        ),
    )
    create_asgi_app(settings=settings)
    monkeypatch.setattr(app.state.recovery_registry, "get_or_create", MagicMock())
    unsafe = "/old/source-path-must-be-scrubbed"
    unsafe_session_id = _session_id_at_queue_filename_limit() + "x"
    old_handle = source_handle_from_capability(_capability(10))
    assert old_handle
    seeded = RecoveryReceiptStore(receipt_path)
    assert (
        await seeded.admit_source(
            old_handle,
            _source("old-safe-session"),
            _origin(),
            "",
            "one",
            _payload("old-safe-session"),
            require_lease=False,
        )
        == "accepted"
    )
    with sqlite3.connect(receipt_path) as db:
        db.execute(
            "UPDATE recovery_sources SET descriptor=? WHERE source_handle=?",
            (json.dumps(_source(unsafe_session_id)), old_handle),
        )
        db.execute(
            "UPDATE recovery_receipts SET payload=? WHERE source_handle=?",
            (
                json.dumps(
                    {
                        **_payload(unsafe_session_id),
                        "data": {
                            **_payload(unsafe_session_id)["data"],
                            "working_dir": unsafe,
                        },
                    }
                ),
                old_handle,
            ),
        )
    # A newly constructed store models startup and causes the fresh physical
    # scrub before reconciliation ever attempts QueueManager.append.
    app.state.recovery_receipts = RecoveryReceiptStore(receipt_path)
    await _reconcile_native_recovery_outbox(app)
    assert await app.state.recovery_receipts.status() == {"quarantined": 1}
    assert await app.state.recovery_receipts.pending_outbox() == []
    for artifact in (
        receipt_path,
        receipt_path.with_name(f"{receipt_path.name}-wal"),
        receipt_path.with_name(f"{receipt_path.name}-shm"),
        receipt_path.with_name(f"{receipt_path.name}-journal"),
    ):
        if artifact.exists():
            assert unsafe.encode() not in artifact.read_bytes()
            assert unsafe_session_id.encode() not in artifact.read_bytes()

    valid_handle = source_handle_from_capability(_capability(11))
    assert valid_handle
    assert (
        await app.state.recovery_receipts.admit_source(
            valid_handle,
            _source("new-safe-session"),
            _origin(),
            "",
            "one",
            _payload("new-safe-session"),
            require_lease=False,
        )
        == "accepted"
    )
    assert len(await app.state.recovery_receipts.pending_outbox()) == 1


@pytest.mark.asyncio
async def test_live_enqueue_wins_and_only_one_pending_recovery_holds() -> None:
    live_pending = False

    async def available() -> bool:
        return not live_pending

    gate = RecoveryAdmissionGate(10)
    handle = source_handle_from_capability(_capability(7))
    assert handle
    binding = gate.binding(
        handle, source_descriptor_digest(_source()), _origin(), "alice", "one", "f" * 64
    )
    issued = await gate.issue(binding, available)
    assert issued is not None
    permit, _ = issued
    async with gate.live_enqueue():
        live_pending = True
    async with gate.consume(permit, binding, available) as result:
        assert result == "unavailable"
    assert len(gate._permits) == 1  # unavailable work does not spend the permit
    # A new gate models restart and invalidates the outstanding runtime permit.
    async with RecoveryAdmissionGate(10).consume(permit, binding, available) as result:
        assert result == "invalid"


@pytest.mark.asyncio
async def test_recovery_receipt_terminalization_precedes_its_queue_commit(
    tmp_path: Path,
) -> None:
    """A crash after terminalization leaves a replayable, already-terminal record."""
    from context_intelligence_server.queue_manager import QueueManager
    from context_intelligence_server.registry import SessionRegistry

    handle = source_handle_from_capability(_capability(13))
    assert handle
    source = _source()
    origin = _origin()
    payload = _payload()
    receipts = RecoveryReceiptStore(tmp_path / "receipts.sqlite3")
    assert (
        await receipts.admit_source(
            handle, source, origin, "alice", "one", payload, require_lease=False
        )
        == "accepted"
    )
    queue = QueueManager(tmp_path / "recovery")
    await queue.append(
        "capture-session",
        json.dumps(
            {
                **payload,
                "created_by": "alice",
                "_recovery_origin": {"source_handle": handle, **origin},
            },
            separators=(",", ":"),
        ).encode("utf-8"),
    )

    registry = SessionRegistry()
    registry._queue_manager = queue
    registry.drain_batch_size = 1
    registry._process_batch = AsyncMock()
    registry._flush_barrier = AsyncMock()

    async def persist_terminal(_worker: object, _records: list[object]) -> None:
        await receipts.mark(origin, "written", source_handle=handle)

    real_commit = queue.commit

    async def assert_terminal_then_commit(session_id: str, offset: int) -> None:
        assert await receipts.status() == {"written": 1}
        await real_commit(session_id, offset)

    registry.on_batch_flushed = persist_terminal
    queue.commit = assert_terminal_then_commit  # type: ignore[method-assign]
    await registry._drain_to_eof(
        SimpleNamespace(session_id="capture-session"), handlers=MagicMock()
    )

    assert (await queue.read_batch("capture-session", max_items=1)).records == []
    assert await receipts.status() == {"written": 1}


@pytest.mark.asyncio
async def test_restart_repairs_old_post_commit_receipt_gap_and_admits_successor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An old commit-then-marker crash is repaired by explicit queue proof only."""
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
    source = {**_source(), "record_count": 2}
    origin = _origin()
    payload = _payload()
    handle = source_handle_from_capability(_capability(14))
    assert handle
    receipts = app.state.recovery_receipts
    assert (
        await receipts.admit_source(
            handle, source, origin, "", "one", payload, require_lease=False
        )
        == "accepted"
    )
    queue = app.state.recovery_registry.queue_manager
    await queue.append(
        "capture-session",
        json.dumps(
            {
                **payload,
                "_recovery_origin": {"source_handle": handle, **origin},
            },
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    await receipts.mark(origin, "enqueued", source_handle=handle)
    await queue.commit(
        "capture-session",
        (await queue.read_batch("capture-session", max_items=1)).end_offset,
    )

    # Fresh app state models the process restart after the old post-commit
    # callback was skipped. The committed queue offset is the required durable
    # proof; no enqueued receipt is terminalized merely because it is enqueued.
    asgi_app = create_asgi_app(settings=settings)
    monkeypatch.setattr(app.state.recovery_registry, "get_or_create", MagicMock())
    successor_origin = _origin(ordinal=1, line="d")
    successor_payload = _payload()
    successor_admission = {
        "workspace": "one",
        "source": source,
        "origin": successor_origin,
        "payload_sha256": _payload_digest(successor_payload),
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app), base_url="http://test"
    ) as client:
        successor = await client.post(
            "/recovery/admissions",
            json=successor_admission,
            headers={"X-Recovery-Source": _capability(14)},
        )
    assert successor.status_code == 201
    assert await app.state.recovery_receipts.status() == {"written": 1}


@pytest.mark.asyncio
async def test_admission_reconciles_pending_append_before_duplicate_acknowledgement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed append remains a generic retry; its repaired retry gets a 200."""
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
    source = {**_source(), "record_count": 2}
    origin = _origin()
    payload = _payload()
    body = {**payload, "source": source, "origin": origin}
    admission = {
        "workspace": "one",
        "source": source,
        "origin": origin,
        "payload_sha256": _payload_digest(payload),
    }
    queue = app.state.recovery_registry.queue_manager
    real_append = queue.append
    append_fails = True

    async def flaky_append(session_id: str, raw: bytes) -> None:
        if append_fails:
            raise OSError("simulated recovery spool failure")
        await real_append(session_id, raw)

    monkeypatch.setattr(queue, "append", flaky_append)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app, raise_app_exceptions=False),
        base_url="http://test",
        headers={"X-Recovery-Source": _capability(15)},
    ) as client:
        permit_response = await client.post("/recovery/admissions", json=admission)
        assert permit_response.status_code == 201
        first_event = await client.post(
            "/recovery/events",
            json=body,
            headers={"X-Recovery-Permit": permit_response.json()["permit"]},
        )
        assert first_event.status_code == 500

        pending_retry = await client.post("/recovery/admissions", json=admission)
        assert pending_retry.status_code == 429
        assert pending_retry.json() == {"detail": "Recovery unavailable"}

        append_fails = False
        duplicate = await client.post("/recovery/admissions", json=admission)
        assert duplicate.status_code == 200
        assert duplicate.json() == {"status": "duplicate"}

        handle = source_handle_from_capability(_capability(15))
        assert handle
        await app.state.recovery_receipts.mark(
            origin,
            "written",
            source_handle=handle,
        )
        successor = await client.post(
            "/recovery/admissions",
            json={
                "workspace": "one",
                "source": source,
                "origin": _origin(ordinal=1, line="d"),
                "payload_sha256": _payload_digest(payload),
            },
        )
    assert successor.status_code == 201


@pytest.mark.asyncio
async def test_v1_rejects_escaped_lone_surrogates_before_recovery_state_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Invalid Unicode never issues or consumes a permit, receipt, or queue record."""
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
    source = _source()
    origin = _origin()
    valid_payload = {
        **_payload(),
        "data": {**_payload()["data"], "nested": {"label": "café"}},
    }
    admission = {
        "workspace": "one",
        "source": source,
        "origin": origin,
        "payload_sha256": _payload_digest(valid_payload),
    }

    invalid_admissions = (
        {**admission, "workspace": "\ud800"},
        {**admission, "source": {**source, "session_id": "\ud800"}},
        {
            **admission,
            "origin": {**origin, "source_line_sha256": "\ud800"},
        },
    )
    invalid_events = (
        {**valid_payload, "event": "\ud800", "source": source, "origin": origin},
        {**valid_payload, "workspace": "\ud800", "source": source, "origin": origin},
        {
            **valid_payload,
            "data": {**valid_payload["data"], "nested": {"label": "\ud800"}},
            "source": source,
            "origin": origin,
        },
        {
            **valid_payload,
            "source": {**source, "session_id": "\ud800"},
            "origin": origin,
        },
        {
            **valid_payload,
            "source": source,
            "origin": {**origin, "source_line_sha256": "\ud800"},
        },
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app), base_url="http://test"
    ) as client:
        for invalid_admission in invalid_admissions:
            rejected = await client.post(
                "/recovery/admissions",
                content=_raw_ascii_json(invalid_admission),
                headers={
                    "Content-Type": "application/json",
                    "X-Recovery-Source": _capability(20),
                },
            )
            assert rejected.status_code == 400
            assert rejected.json() == {"detail": "Invalid recovery event"}
        assert not app.state.recovery_admission_gate._permits
        assert await app.state.recovery_receipts.status() == {}
        assert not await app.state.recovery_registry.queue_manager.has_pending_records()

        issued = await client.post(
            "/recovery/admissions",
            json=admission,
            headers={"X-Recovery-Source": _capability(20)},
        )
        assert issued.status_code == 201
        permit = issued.json()["permit"]

        for invalid_event in invalid_events:
            rejected = await client.post(
                "/recovery/events",
                content=_raw_ascii_json(invalid_event),
                headers={
                    "Content-Type": "application/json",
                    "X-Recovery-Source": _capability(20),
                    "X-Recovery-Permit": permit,
                },
            )
            assert rejected.status_code == 400
            assert rejected.json() == {"detail": "Invalid recovery event"}
        assert set(app.state.recovery_admission_gate._permits) == {permit}
        assert await app.state.recovery_receipts.status() == {}
        assert not await app.state.recovery_registry.queue_manager.has_pending_records()

        valid = await client.post(
            "/recovery/events",
            content=json.dumps(
                {**valid_payload, "source": source, "origin": origin},
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "X-Recovery-Source": _capability(20),
                "X-Recovery-Permit": permit,
            },
        )
    assert valid.status_code == 202


@pytest.mark.asyncio
async def test_source_capability_admission_and_event_share_utf8_canonical_payload_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unicode payloads use the native client's unescaped UTF-8 canonical bytes."""
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
    payload = {
        **_payload(),
        "data": {
            **_payload()["data"],
            "nested": {"label": "café"},
        },
    }
    source = _source()
    origin = _origin()
    admission = {
        "workspace": "one",
        "source": source,
        "origin": origin,
        "payload_sha256": _payload_digest(payload),
    }
    body = {**payload, "source": source, "origin": origin}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app), base_url="http://test"
    ) as client:
        permit = await client.post(
            "/recovery/admissions",
            json=admission,
            headers={"X-Recovery-Source": _capability(16)},
        )
        assert permit.status_code == 201
        event = await client.post(
            "/recovery/events",
            json=body,
            headers={
                "X-Recovery-Source": _capability(16),
                "X-Recovery-Permit": permit.json()["permit"],
            },
        )
    assert event.status_code == 202
