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


class TestDeadLetterReplayPreservesContributor:
    """Replay MUST carry each line's ``created_by`` into ``get_or_create``.

    INCIDENT 2026-09-09 (production, team-shared): 82,880 events were
    dead-lettered, 78,422 of them healthy events lost to a transient Neo4j
    pool timeout. Recovering them means replaying the dead-letter files.

    But ``created_by`` is stamped ``ON CREATE SET`` in Neo4j
    (``neo4j_store.py:183``) -- WRITE-ONCE. Replay happens long after the
    session ended, so the worker has been deregistered and ``get_or_create``
    takes the CREATE branch. Replaying without ``created_by`` therefore
    stamps every recovered node ``NULL``, permanently: the events are back
    in the graph but invisible to every per-contributor report, and the
    ``.dead.jsonl`` that proved what happened has been purged.

    The contributor IS on the queued line -- ``post_events`` stamps
    ``body_obj["created_by"] = contributor_id`` at ``main.py:1183``. Replay
    just has to read it back, exactly as the boot-recovery path already does
    via ``_parse_workspace_and_creator`` (``main.py:113-129``).

    A recovery that silently loses attribution is worse than no recovery:
    it looks like it worked.
    """

    @pytest.mark.anyio
    async def test_replay_passes_created_by_from_the_queued_line(
        self,
        client: httpx.AsyncClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The contributor stamped at ingest survives the round trip."""
        qm = _point_registry_at(tmp_path)
        await qm.dead_letter(
            "k1",
            b'{"workspace": "ws1", "created_by": "sam", "a": 1}\n',
            "pool-timeout",
        )

        calls: list[tuple[str, str, str | None]] = []
        monkeypatch.setattr(
            registry,
            "get_or_create",
            lambda session_id, workspace, created_by=None: calls.append(
                (session_id, workspace, created_by)
            ),
        )

        response = await client.post("/queues/dead-letter/k1/replay")
        assert response.status_code == 200

        assert calls == [("k1", "ws1", "sam")], (
            "replay dropped created_by; recovered nodes would be stamped NULL "
            "by the write-once ON CREATE SET and vanish from per-user reports"
        )

    @pytest.mark.anyio
    async def test_replay_survives_an_unparseable_line(
        self,
        client: httpx.AsyncClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A torn line is still re-enqueued, not raised over.

        13 of the 82,880 production dead letters are JSON parse errors from
        torn queue framing. A replay that raises on the first of them aborts
        mid-loop, leaving the dead-letter file unpurged and its worker key
        unrecoverable. Parsing here is best-effort: no workspace, no
        contributor, but the bytes still go back on the log.
        """
        qm = _point_registry_at(tmp_path)
        await qm.dead_letter("k1", b'{"workspace": "ws1", "trunc', "torn-line")
        await qm.dead_letter(
            "k1", b'{"workspace": "ws1", "created_by": "sam"}\n', "pool-timeout"
        )

        calls: list[tuple[str, str, str | None]] = []
        monkeypatch.setattr(
            registry,
            "get_or_create",
            lambda session_id, workspace, created_by=None: calls.append(
                (session_id, workspace, created_by)
            ),
        )

        response = await client.post("/queues/dead-letter/k1/replay")
        assert response.status_code == 200
        assert response.json() == {"worker_key": "k1", "replayed": 2}

        # Both lines re-enqueued; the torn one carries no attribution.
        assert calls == [("k1", "", None), ("k1", "ws1", "sam")]
        assert await qm.read_dead_letters("k1") == []


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
        calls: list[tuple[str, str, str | None]] = []
        monkeypatch.setattr(
            registry,
            "get_or_create",
            lambda session_id, workspace, created_by=None: calls.append(
                (session_id, workspace, created_by)
            ),
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
        assert calls == [("k1", "ws1", None), ("k1", "ws1", None)]

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
            lambda session_id, workspace, created_by=None: None,
        )

        before = registry.pipeline_counters()

        response = await client.post("/queues/dead-letter/nope/replay")
        assert response.status_code == 200
        assert response.json() == {"worker_key": "nope", "replayed": 0}

        after = registry.pipeline_counters()
        assert after["replayed_total"] == before["replayed_total"]
        assert after["accepted_total"] == before["accepted_total"]
