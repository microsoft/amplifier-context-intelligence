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
