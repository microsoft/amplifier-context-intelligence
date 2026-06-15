"""Tests for the GET /queues/dead-letter endpoint."""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest

from context_intelligence_server.main import registry
from context_intelligence_server.queue_manager import QueueManager


def _point_registry_at(tmp_path: Path) -> QueueManager:
    """Point the shared registry's durable infra at a tmp_path queues dir.

    Returns the QueueManager so tests can seed dead-letter records directly.
    """
    qm = QueueManager(queues_dir=tmp_path / "queues")
    registry._queue_manager = qm
    registry._write_semaphore = asyncio.Semaphore(2)
    registry._max_delivery_attempts = 5
    return qm


class TestDeadLetterList:
    """GET /queues/dead-letter aggregates dead-letter records per worker key."""

    @pytest.mark.anyio
    async def test_dead_letter_list_returns_entries(
        self, client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        qm = _point_registry_at(tmp_path)
        await qm.dead_letter("k1", b'{"a": 1}\n', "boom-1")
        await qm.dead_letter("k1", b'{"a": 2}\n', "boom-2")
        await qm.dead_letter("k2", b'{"b": 1}\n', "boom-k2")

        response = await client.get("/queues/dead-letter")
        assert response.status_code == 200
        data = response.json()
        entries = {e["worker_key"]: e for e in data["dead_letters"]}

        assert entries["k1"]["item_count"] == 2
        assert entries["k1"]["last_error"] == "boom-2"
        assert "last_ts" in entries["k1"]

        assert entries["k2"]["item_count"] == 1

    @pytest.mark.anyio
    async def test_dead_letter_list_empty(
        self, client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        _point_registry_at(tmp_path)

        response = await client.get("/queues/dead-letter")
        assert response.status_code == 200
        assert response.json() == {"dead_letters": []}

    @pytest.mark.anyio
    async def test_dead_letter_list_requires_auth(
        self, auth_client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        _point_registry_at(tmp_path)

        # No token -> 401
        response = await auth_client.get("/queues/dead-letter")
        assert response.status_code == 401

        # Valid token -> 200
        response = await auth_client.get(
            "/queues/dead-letter",
            headers={"Authorization": "Bearer test-secret"},
        )
        assert response.status_code == 200


class TestDeadLetterPurge:
    """POST /queues/dead-letter/{worker_key}/purge clears a worker's dead-letters."""

    @pytest.mark.anyio
    async def test_purge_removes_dead_letters(
        self, client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        qm = _point_registry_at(tmp_path)
        await qm.dead_letter("k1", b'{"a": 1}\n', "boom-1")
        await qm.dead_letter("k1", b'{"a": 2}\n', "boom-2")

        response = await client.post("/queues/dead-letter/k1/purge")
        assert response.status_code == 200
        assert response.json() == {"worker_key": "k1", "purged": 2}
        assert await qm.read_dead_letters("k1") == []

    @pytest.mark.anyio
    async def test_purge_missing_is_zero(
        self, client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        _point_registry_at(tmp_path)

        response = await client.post("/queues/dead-letter/nope/purge")
        assert response.status_code == 200
        assert response.json() == {"worker_key": "nope", "purged": 0}

    @pytest.mark.anyio
    async def test_purge_rejects_unsafe_key(
        self, client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        _point_registry_at(tmp_path)

        response = await client.post("/queues/dead-letter/a%2Fb/purge")
        assert response.status_code == 400


class TestDeadLetterReplay:
    """POST /queues/dead-letter/{worker_key}/replay re-enqueues then purges."""

    @pytest.mark.anyio
    async def test_replay_reenqueues_and_purges(
        self,
        client: httpx.AsyncClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        qm = _point_registry_at(tmp_path)
        await qm.dead_letter("k1", b'{"workspace": "ws1", "a": 1}\n', "boom-1")
        await qm.dead_letter("k1", b'{"workspace": "ws1", "a": 2}\n', "boom-2")

        # Stub get_or_create so no real worker/drain task is started.
        calls: list[tuple[str, str]] = []
        monkeypatch.setattr(
            registry,
            "get_or_create",
            lambda session_id, workspace: calls.append((session_id, workspace)),
        )

        before = registry.pipeline_counters()

        response = await client.post("/queues/dead-letter/k1/replay")
        assert response.status_code == 200
        assert response.json() == {"worker_key": "k1", "replayed": 2}

        # Re-enqueued: the worker's log now holds the 2 replayed lines.
        batch = await qm.read_batch("k1", max_items=10)
        assert len(batch.lines) == 2

        # Dead-letters purged.
        assert await qm.read_dead_letters("k1") == []

        # get_or_create was invoked for each replayed record.
        assert calls == [("k1", "ws1"), ("k1", "ws1")]

        # Conservation: replayed advances by 2, accepted is UNCHANGED.
        after = registry.pipeline_counters()
        assert after["replayed_total"] == before["replayed_total"] + 2
        assert after["accepted_total"] == before["accepted_total"]

    @pytest.mark.anyio
    async def test_replay_empty_is_zero(
        self,
        client: httpx.AsyncClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _point_registry_at(tmp_path)
        monkeypatch.setattr(
            registry,
            "get_or_create",
            lambda session_id, workspace: None,
        )

        before = registry.pipeline_counters()

        response = await client.post("/queues/dead-letter/nope/replay")
        assert response.status_code == 200
        assert response.json() == {"worker_key": "nope", "replayed": 0}

        after = registry.pipeline_counters()
        assert after["replayed_total"] == before["replayed_total"]
        assert after["accepted_total"] == before["accepted_total"]
