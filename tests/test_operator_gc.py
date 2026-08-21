"""Operator-triggerable GC (preview -> apply), RED-first test plan.

Covers R1-R9 (authoritative).
Router-level tests use the `client`/`auth_client` fixtures and the
`_point_registry_at(tmp_path)` helper pattern from `tests/routers/test_queues.py`.

Every test is RED against the pre-fix tree for the RIGHT reason (route absent
-> 404, or `scan_gc_candidates`/`require_logless` absent -> AttributeError/
TypeError) and GREEN after the implementation lands.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError

from context_intelligence_server.config import get_settings
from context_intelligence_server.main import registry
from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.registry import SessionWorker
from context_intelligence_server.services import HookStateService


def _point_registry_at(tmp_path: Path) -> QueueManager:
    """Point the shared registry's durable infra at a tmp_path queues dir.

    Mirrors `tests/routers/test_queues.py::_point_registry_at`.
    """
    qm = QueueManager(queues_dir=tmp_path / "queues")
    registry._queue_manager = qm
    registry._write_semaphore = asyncio.Semaphore(2)
    registry._max_delivery_attempts = 5
    return qm


def _configure_gc(
    monkeypatch: pytest.MonkeyPatch,
    *,
    queue_ttl: float = 100.0,
    dead_letter_retention: float = 100.0,
    max_delete: int = 1000,
) -> None:
    """Monkeypatch the REAL (lru_cached) Settings singleton's GC knobs.

    `get_settings()` is `@lru_cache`'d with no args, so every module that
    calls the plain function (queue_manager.py, registry.py, and our new
    routers/queues.py code) gets back the SAME Settings instance -- mutating
    attributes on that instance (not replacing the function) is what
    `tests/test_steady_state_reclaim.py` already does via
    `monkeypatch.setattr(main_module._settings, ...)`. We do the same here.
    """
    settings = get_settings()
    monkeypatch.setattr(settings, "gc_queue_ttl_seconds", queue_ttl)
    monkeypatch.setattr(
        settings, "dead_letter_retention_seconds", dead_letter_retention
    )
    monkeypatch.setattr(settings, "gc_max_delete_per_pass", max_delete)


def _write_log(qm: QueueManager, key: str, records: list[bytes]) -> Path:
    path = qm.queues_dir / f"{key}.log"
    path.write_bytes(b"".join(r + b"\n" for r in records))
    return path


def _write_offset(qm: QueueManager, key: str, value: int) -> Path:
    path = qm.queues_dir / f"{key}.offset"
    path.write_text(str(value), encoding="utf-8")
    return path


def _write_dead(qm: QueueManager, key: str, n: int) -> Path:
    path = qm.queues_dir / f"{key}.dead.jsonl"
    line = b'{"ts": 1.0, "error": "e", "payload": "x"}\n'
    path.write_bytes(line * n)
    return path


def _backdate(path: Path, age_seconds: float) -> None:
    t = time.time() - age_seconds
    os.utime(path, (t, t))


def _register_worker(key: str) -> None:
    registry._register_for_test(
        SessionWorker(
            session_id=key,
            workspace="ws",
            services=HookStateService(workspace="ws"),
        )
    )


class TestPreviewAndApplyFullyDrainedLog:
    """T1: a fully-drained, backdated log is listed by preview and deleted by apply."""

    @pytest.mark.anyio
    async def test_fully_drained_safe_log_listed_and_deleted(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch, queue_ttl=100.0)
        log = _write_log(qm, "k1", [b'{"a":1}', b'{"a":2}'])
        size = log.stat().st_size
        offset_path = _write_offset(qm, "k1", size)
        _backdate(log, 200.0)

        resp = await client.get("/queues/gc")
        assert resp.status_code == 200
        data = resp.json()
        cands = {c["key"]: c for c in data["candidates"]}
        assert "k1" in cands
        c = cands["k1"]
        assert c["class"] == "queue_log"
        assert c["reason"] == "fully_drained"
        assert c["bytes"] == size + offset_path.stat().st_size
        assert c["age_seconds"] >= 100.0
        assert c["action"] == "preview"

        resp = await client.post("/queues/gc/apply")
        assert resp.status_code == 200
        adata = resp.json()
        entry = next(x for x in adata["candidates"] if x["key"] == "k1")
        assert entry["action"] == "deleted"
        assert adata["applied"]["deleted"] == 1
        assert not log.exists()
        assert not offset_path.exists()


class TestExclusions:
    """T2-T6: every exclusion class is never listed and never deleted."""

    @pytest.mark.anyio
    async def test_undrained_tail_log_never_listed_or_deleted(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch, queue_ttl=100.0)
        log = _write_log(qm, "k1", [b'{"a":1}', b'{"a":2}'])
        size = log.stat().st_size
        _write_offset(qm, "k1", size - 5)  # committed < size
        _backdate(log, 200.0)

        resp = await client.get("/queues/gc")
        data = resp.json()
        assert all(c["key"] != "k1" for c in data["candidates"])
        assert data["excluded"]["undrained_tail"] == 1

        resp = await client.post("/queues/gc/apply")
        assert resp.json()["applied"]["deleted"] == 0
        assert log.exists()

    @pytest.mark.anyio
    async def test_log_with_no_offset_never_listed_or_deleted(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch, queue_ttl=100.0)
        log = _write_log(qm, "k1", [b'{"a":1}'])
        _backdate(log, 200.0)
        # deliberately no .offset file

        resp = await client.get("/queues/gc")
        data = resp.json()
        assert all(c["key"] != "k1" for c in data["candidates"])
        assert data["excluded"]["no_offset"] == 1

        resp = await client.post("/queues/gc/apply")
        assert resp.json()["applied"]["deleted"] == 0
        assert log.exists()

        # Non-vacuity: the SAME log WITH a matching .offset IS listed.
        _write_offset(qm, "k1", log.stat().st_size)
        resp = await client.get("/queues/gc")
        assert any(c["key"] == "k1" for c in resp.json()["candidates"])

    @pytest.mark.anyio
    async def test_actively_draining_log_never_touched(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch, queue_ttl=100.0)
        log = _write_log(qm, "k1", [b'{"a":1}'])
        _write_offset(qm, "k1", log.stat().st_size)
        _backdate(log, 200.0)
        _register_worker("k1")

        resp = await client.get("/queues/gc")
        data = resp.json()
        assert all(c["key"] != "k1" for c in data["candidates"])
        assert data["excluded"]["live_worker"] == 1

        resp = await client.post("/queues/gc/apply")
        assert resp.json()["applied"]["deleted"] == 0
        assert log.exists()

        # Second half: remove the worker, re-run apply -> now deleted.
        registry._deregister("k1")
        resp = await client.post("/queues/gc/apply")
        adata = resp.json()
        entry = next(x for x in adata["candidates"] if x["key"] == "k1")
        assert entry["action"] == "deleted"
        assert not log.exists()

    @pytest.mark.anyio
    async def test_torn_tail_log_never_listed(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch, queue_ttl=100.0)
        log = qm.queues_dir / "k1.log"
        log.write_bytes(b'{"a":1}\n{"a":2}')  # no trailing newline -> torn
        _write_offset(qm, "k1", log.stat().st_size)
        _backdate(log, 200.0)

        resp = await client.get("/queues/gc")
        data = resp.json()
        assert all(c["key"] != "k1" for c in data["candidates"])
        assert data["excluded"]["torn_tail"] == 1

        resp = await client.post("/queues/gc/apply")
        assert resp.json()["applied"]["deleted"] == 0
        assert log.exists()

    @pytest.mark.anyio
    async def test_too_young_drained_log_never_listed(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch, queue_ttl=100.0)
        log = _write_log(qm, "k1", [b'{"a":1}'])
        _write_offset(qm, "k1", log.stat().st_size)
        # mtime left at "now" -- too young

        resp = await client.get("/queues/gc")
        data = resp.json()
        assert all(c["key"] != "k1" for c in data["candidates"])
        assert data["excluded"]["too_young"] == 1

        resp = await client.post("/queues/gc/apply")
        assert resp.json()["applied"]["deleted"] == 0
        assert log.exists()

        # Non-vacuity: backdate past TTL -> now listed.
        _backdate(log, 200.0)
        resp = await client.get("/queues/gc")
        assert any(c["key"] == "k1" for c in resp.json()["candidates"])


class TestDeadLetters:
    """T7-T8b: dead-letter enumeration and require_logless."""

    @pytest.mark.anyio
    async def test_logless_dead_letter_past_retention_listed_and_purged(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch, dead_letter_retention=100.0)
        dead = _write_dead(qm, "k1", 3)
        _backdate(dead, 200.0)

        resp = await client.get("/queues/gc")
        data = resp.json()
        entry = next(c for c in data["candidates"] if c["key"] == "k1")
        assert entry["class"] == "dead_letter"
        assert entry["reason"] == "logless_dead_letter"
        assert entry["records"] == 3

        purged_calls: list[int] = []
        monkeypatch.setattr(registry, "record_purged", lambda n: purged_calls.append(n))

        resp = await client.post("/queues/gc/apply")
        adata = resp.json()
        entry2 = next(c for c in adata["candidates"] if c["key"] == "k1")
        assert entry2["action"] == "deleted"
        assert not dead.exists()
        assert purged_calls == [3]

    @pytest.mark.anyio
    async def test_dead_letter_with_live_log_never_purged(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch, dead_letter_retention=100.0)
        dead = _write_dead(qm, "k1", 2)
        _backdate(dead, 200.0)
        _write_log(qm, "k1", [b'{"a":1}'])  # live .log present

        resp = await client.get("/queues/gc")
        data = resp.json()
        assert all(
            not (c["key"] == "k1" and c["class"] == "dead_letter")
            for c in data["candidates"]
        )
        assert data["excluded"]["log_present"] == 1

        resp = await client.post("/queues/gc/apply")
        assert dead.exists()

    @pytest.mark.anyio
    async def test_require_logless_refuses_when_log_present(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        qm = QueueManager(queues_dir=tmp_path / "queues")
        await qm.dead_letter("k1", b'{"a":1}', "boom")
        await qm.append("k1", b'{"a":1}')  # creates the .log

        caplog.set_level(logging.WARNING)
        result = await qm.purge_dead_letters("k1", require_logless=True)
        assert result == -1
        assert (qm.queues_dir / "k1.dead.jsonl").exists()
        assert any("gc_purge_refused" in r.getMessage() for r in caplog.records)

        # Polarity control: require_logless=False purges despite the live .log
        # (proving the default path is byte-for-byte unchanged for the two
        # existing callers).
        result2 = await qm.purge_dead_letters("k1", require_logless=False)
        assert result2 == 1
        assert not (qm.queues_dir / "k1.dead.jsonl").exists()


class TestToctou:
    """T9: a candidate that changed between scan and delete is SKIPPED, not deleted."""

    @pytest.mark.anyio
    async def test_toctou_candidate_changed_is_skipped_not_deleted(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch, queue_ttl=100.0)
        log = _write_log(qm, "k1", [b'{"a":1}'])
        _write_offset(qm, "k1", log.stat().st_size)
        _backdate(log, 200.0)

        resp = await client.get("/queues/gc")
        assert any(c["key"] == "k1" for c in resp.json()["candidates"])

        real_delete_drained = qm.delete_drained
        late_appended = False

        async def _delete_drained_with_late_append(session_id: str) -> bool:
            nonlocal late_appended
            if session_id == "k1" and not late_appended:
                late_appended = True
                # Real, awaited append -- lands under the SAME guard.file_lock
                # `delete_drained`'s own in-lock re-stat re-reads, driving the
                # real refusal (queue_manager.py :837-843), not a mock of it.
                await qm.append(session_id, b'{"late":true}')
            return await real_delete_drained(session_id)

        monkeypatch.setattr(qm, "delete_drained", _delete_drained_with_late_append)

        resp = await client.post("/queues/gc/apply")
        adata = resp.json()
        entry = next(c for c in adata["candidates"] if c["key"] == "k1")
        assert entry["action"] == "skipped"
        assert entry["skip_reason"] == "changed"
        assert adata["applied"]["deleted"] == 0

        # The appended record survives byte-intact.
        batch = await qm.read_batch("k1", max_items=10)
        assert batch.lines[-1] == b'{"late":true}'


class TestPreviewMutatesNothing:
    """T10: preview deletes NOTHING -- byte-for-byte directory snapshot."""

    @pytest.mark.anyio
    async def test_preview_deletes_nothing(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        qm = _point_registry_at(tmp_path)
        _configure_gc(
            monkeypatch, queue_ttl=50.0, dead_letter_retention=50.0, max_delete=1000
        )

        # safe log
        log1 = _write_log(qm, "safe1", [b'{"a":1}'])
        _write_offset(qm, "safe1", log1.stat().st_size)
        _backdate(log1, 100.0)

        # undrained tail
        log2 = _write_log(qm, "undr", [b'{"a":1}', b'{"a":2}'])
        _write_offset(qm, "undr", log2.stat().st_size - 3)
        _backdate(log2, 100.0)

        # no offset
        log3 = _write_log(qm, "noff", [b'{"a":1}'])
        _backdate(log3, 100.0)

        # live worker
        log4 = _write_log(qm, "live", [b'{"a":1}'])
        _write_offset(qm, "live", log4.stat().st_size)
        _backdate(log4, 100.0)
        _register_worker("live")

        # torn tail
        log5 = qm.queues_dir / "torn.log"
        log5.write_bytes(b'{"a":1}\n{"a":2}')
        _write_offset(qm, "torn", log5.stat().st_size)
        _backdate(log5, 100.0)

        # too young
        log6 = _write_log(qm, "young", [b'{"a":1}'])
        _write_offset(qm, "young", log6.stat().st_size)

        # dead-letter safe
        dead1 = _write_dead(qm, "dead-safe", 2)
        _backdate(dead1, 100.0)

        # dead-letter with live log
        dead2 = _write_dead(qm, "dead-live", 1)
        _backdate(dead2, 100.0)
        _write_log(qm, "dead-live", [b'{"a":1}'])

        def _snapshot() -> dict[str, tuple[int, float]]:
            return {
                p.name: (p.stat().st_size, p.stat().st_mtime)
                for p in sorted(qm.queues_dir.iterdir())
            }

        before = _snapshot()
        resp = await client.get("/queues/gc")
        assert resp.status_code == 200
        data = resp.json()
        after = _snapshot()
        assert before == after

        assert data["applied"] == {
            "deleted": 0,
            "skipped": 0,
            "failed": 0,
            "bytes_reclaimed_estimate": 0,
            "max_delete": 1000,
            "bounded_by_max_delete": False,
        }
        assert all(c["action"] == "preview" for c in data["candidates"])


class TestBounding:
    """T11-T11c: max_delete bounding, non-raisable ceiling, and 422 rejection."""

    @pytest.mark.anyio
    async def test_apply_bounded_by_max_delete(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch, queue_ttl=50.0, max_delete=1000)
        keys = [f"k{i}" for i in range(5)]
        for k in keys:
            log = _write_log(qm, k, [b'{"a":1}'])
            _write_offset(qm, k, log.stat().st_size)
            _backdate(log, 100.0)

        resp = await client.post("/queues/gc/apply", json={"max_delete": 2})
        data = resp.json()
        assert data["applied"]["deleted"] == 2
        assert data["applied"]["bounded_by_max_delete"] is True
        assert data["applied"]["max_delete"] == 2
        remaining = sum(1 for k in keys if (qm.queues_dir / f"{k}.log").exists())
        assert remaining == 3

    @pytest.mark.anyio
    async def test_apply_max_delete_cannot_raise_ceiling(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch, queue_ttl=50.0, max_delete=2)
        for i in range(5):
            k = f"k{i}"
            log = _write_log(qm, k, [b'{"a":1}'])
            _write_offset(qm, k, log.stat().st_size)
            _backdate(log, 100.0)

        resp = await client.post("/queues/gc/apply", json={"max_delete": 99})
        data = resp.json()
        assert data["applied"]["max_delete"] == 2
        assert data["applied"]["deleted"] <= 2

    @pytest.mark.anyio
    async def test_apply_max_delete_zero_or_negative_rejected(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch, queue_ttl=50.0)
        log = _write_log(qm, "k1", [b'{"a":1}'])
        _write_offset(qm, "k1", log.stat().st_size)
        _backdate(log, 100.0)

        resp = await client.post("/queues/gc/apply", json={"max_delete": 0})
        assert resp.status_code == 422
        assert log.exists()

        resp = await client.post("/queues/gc/apply", json={"max_delete": -1})
        assert resp.status_code == 422
        assert log.exists()


class TestIdempotent:
    """T12: a second apply after a full sweep deletes nothing."""

    @pytest.mark.anyio
    async def test_apply_idempotent(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch, queue_ttl=50.0)
        log = _write_log(qm, "k1", [b'{"a":1}'])
        _write_offset(qm, "k1", log.stat().st_size)
        _backdate(log, 100.0)

        resp1 = await client.post("/queues/gc/apply")
        assert resp1.json()["applied"]["deleted"] == 1

        resp2 = await client.post("/queues/gc/apply")
        data2 = resp2.json()
        assert data2["applied"]["deleted"] == 0
        assert data2["candidates"] == []


class TestAuth:
    """T13: GET is require_read, POST/apply is require_write."""

    @pytest.mark.anyio
    async def test_auth_required(
        self,
        auth_client: httpx.AsyncClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _point_registry_at(tmp_path)
        _configure_gc(monkeypatch)

        resp = await auth_client.get("/queues/gc")
        assert resp.status_code == 401
        resp = await auth_client.get(
            "/queues/gc", headers={"Authorization": "Bearer test-secret"}
        )
        assert resp.status_code == 200

        resp = await auth_client.post("/queues/gc/apply")
        assert resp.status_code == 401
        resp = await auth_client.post(
            "/queues/gc/apply", headers={"Authorization": "Bearer test-secret"}
        )
        assert resp.status_code == 200


class TestSidecarsUntouched:
    """T14: sidecars (.offset.tmp, .log.torn-*.bin, .log.compact.tmp) are never touched."""

    @pytest.mark.anyio
    async def test_sidecars_never_touched(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch, queue_ttl=50.0)
        log = _write_log(qm, "k1", [b'{"a":1}'])
        _write_offset(qm, "k1", log.stat().st_size)
        _backdate(log, 100.0)

        sidecars = {
            "k1.offset.tmp": b"stray",
            "k1.log.torn-1.bin": b"torn-bytes",
            "k1.log.compact.tmp": b"compact-bytes",
        }
        for name, content in sidecars.items():
            (qm.queues_dir / name).write_bytes(content)

        resp = await client.post("/queues/gc/apply")
        assert resp.json()["applied"]["deleted"] == 1
        assert not log.exists()
        assert not (qm.queues_dir / "k1.offset").exists()
        for name, content in sidecars.items():
            p = qm.queues_dir / name
            assert p.exists()
            assert p.read_bytes() == content


class TestScanNeverRaises:
    """T15: a directory-level scan failure degrades to an empty report, never a 500."""

    @pytest.mark.anyio
    async def test_scan_directory_unreadable_returns_empty_report(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch)

        class _BrokenDir:
            def glob(self, pattern: str):
                raise OSError("boom")

        monkeypatch.setattr(qm, "_dir", _BrokenDir())

        resp = await client.get("/queues/gc")
        assert resp.status_code == 200
        data = resp.json()
        assert data["candidates"] == []
        assert data["excluded"]["unreadable"] == 1
        assert data["scanned_keys"] == 0


class TestConfigValidators:
    """T16: gc_* config validators fail loud on bad input."""

    def test_gc_max_delete_per_pass_rejects_bad_values(self) -> None:
        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(gc_max_delete_per_pass=0)
        with pytest.raises(ValidationError):
            Settings(gc_max_delete_per_pass=-5)

    def test_gc_queue_ttl_seconds_rejects_negative(self) -> None:
        from context_intelligence_server.config import Settings

        with pytest.raises(ValidationError):
            Settings(gc_queue_ttl_seconds=-1)

    def test_gc_settings_defaults(self) -> None:
        from context_intelligence_server.config import Settings

        s = Settings()
        assert s.gc_queue_ttl_seconds == 2 * 86400.0
        assert s.gc_max_delete_per_pass == 1000


class TestApplyBootGate:
    """R1 (BLOCKER): apply is refused with 409 unless boot_state.phase is ready/failed."""

    @pytest.mark.anyio
    async def test_apply_refused_during_boot_preview_still_works(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from context_intelligence_server.status import boot_state

        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch, queue_ttl=50.0)
        log = _write_log(qm, "k1", [b'{"a":1}'])
        _write_offset(qm, "k1", log.stat().st_size)
        _backdate(log, 100.0)

        monkeypatch.setattr(boot_state, "phase", "reconcile")

        # Preview is UNGATED -- works during boot.
        resp = await client.get("/queues/gc")
        assert resp.status_code == 200
        assert any(c["key"] == "k1" for c in resp.json()["candidates"])

        # Apply is refused with 409 while still booting.
        resp = await client.post("/queues/gc/apply")
        assert resp.status_code == 409
        assert log.exists()

        # Once ready, apply proceeds normally.
        monkeypatch.setattr(boot_state, "phase", "ready")
        resp = await client.post("/queues/gc/apply")
        assert resp.status_code == 200
        assert resp.json()["applied"]["deleted"] == 1

    @pytest.mark.anyio
    async def test_apply_allowed_when_boot_failed(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from context_intelligence_server.status import boot_state

        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch, queue_ttl=50.0)
        log = _write_log(qm, "k1", [b'{"a":1}'])
        _write_offset(qm, "k1", log.stat().st_size)
        _backdate(log, 100.0)

        monkeypatch.setattr(boot_state, "phase", "failed")

        resp = await client.post("/queues/gc/apply")
        assert resp.status_code == 200
        assert resp.json()["applied"]["deleted"] == 1


class TestApplyBodyOptional:
    """R6: a bodyless POST /queues/gc/apply with a valid token must NOT 422."""

    @pytest.mark.anyio
    async def test_bodyless_apply_succeeds(
        self, client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        qm = _point_registry_at(tmp_path)
        _configure_gc(monkeypatch, queue_ttl=50.0)
        log = _write_log(qm, "k1", [b'{"a":1}'])
        _write_offset(qm, "k1", log.stat().st_size)
        _backdate(log, 100.0)

        resp = await client.post("/queues/gc/apply")
        assert resp.status_code == 200
        assert resp.json()["applied"]["deleted"] == 1
