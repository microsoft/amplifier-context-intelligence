"""Tests for the dead-letter endpoints (list / purge / replay).

Step 3 (doc 16 W2): list is gated by require_read at GET /queues/dead-letter;
purge/replay are gated by require_admin and live UNDER /admin at
POST /admin/queues/dead-letter/{worker}/{purge,replay}. This file exercises the
dead-letter *business logic* (aggregation, purge, replay mechanics) with the
require_admin dependency bypassed via the standard FastAPI override mechanism —
the same pattern used by the /admin router's own route tests. Authorization
itself (tier boundary + the TB-1 positive-admin proof) is covered separately by
tests/test_dead_letter_requires_admin.py, which deliberately does NOT override
require_admin.
"""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from pathlib import Path

import httpx
import pytest

from context_intelligence_server.authz import require_read
from context_intelligence_server.main import app, registry
from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.routers.admin import require_admin


@pytest.fixture(autouse=True)
def _bypass_dead_letter_auth() -> Generator[None, None, None]:
    """!!! AUTHORIZATION IS DISABLED FOR EVERY TEST IN THIS FILE !!!

    This autouse fixture overrides BOTH dead-letter gates to no-ops —
    require_read (the list gate) and require_admin (the purge/replay gate) — so
    the tests below exercise dead-letter BUSINESS LOGIC ONLY (aggregation /
    purge / replay mechanics). It deliberately provides ZERO authorization
    coverage. (Overriding require_read also keeps these tests independent of the
    shared module-level app.state.allow_unauthenticated flag, which a sibling
    test constructing an auth-enabled app via create_asgi_app can flip.)

    REAL authorization — the tier boundary (list open to any authenticated
    principal; purge/replay admin-only) AND the council TB-1 positive-admin
    proof (static admin key + entra admin role reaching the handler through the
    real gate) — is proven in tests/test_dead_letter_requires_admin.py, which
    routes through the real asgi_app and does NOT override either gate.

    ⚠️ If you add a NEW route test HERE, it inherits these overrides and gets NO
    auth coverage. Either add the auth assertion to test_dead_letter_requires_admin.py
    or scope/remove these overrides for your test. (Council: cranky-old-sam + tester-breaker.)
    """
    app.dependency_overrides[require_admin] = lambda: None
    app.dependency_overrides[require_read] = lambda: None
    try:
        yield
    finally:
        app.dependency_overrides.pop(require_admin, None)
        app.dependency_overrides.pop(require_read, None)


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
        """Middleware-level authentication is still enforced (401 without any
        token) even though this file bypasses the require_admin authorization
        gate. Real non-admin-principal authorization (403) is covered by
        tests/test_dead_letter_requires_admin.py."""
        _point_registry_at(tmp_path)

        # No token -> 401 (BearerTokenMiddleware, unaffected by the
        # require_admin override above).
        response = await auth_client.get("/queues/dead-letter")
        assert response.status_code == 401

        # Valid token -> 200 (require_admin bypassed by the module fixture).
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

        response = await client.post("/admin/queues/dead-letter/k1/purge")
        assert response.status_code == 200
        assert response.json() == {"worker_key": "k1", "purged": 2}
        assert await qm.read_dead_letters("k1") == []

    @pytest.mark.anyio
    async def test_purge_missing_is_zero(
        self, client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        _point_registry_at(tmp_path)

        response = await client.post("/admin/queues/dead-letter/nope/purge")
        assert response.status_code == 200
        assert response.json() == {"worker_key": "nope", "purged": 0}

    @pytest.mark.anyio
    async def test_purge_rejects_unsafe_key(
        self, client: httpx.AsyncClient, tmp_path: Path
    ) -> None:
        _point_registry_at(tmp_path)

        response = await client.post("/admin/queues/dead-letter/a%2Fb/purge")
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

        response = await client.post("/admin/queues/dead-letter/k1/replay")
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

        response = await client.post("/admin/queues/dead-letter/nope/replay")
        assert response.status_code == 200
        assert response.json() == {"worker_key": "nope", "replayed": 0}

        after = registry.pipeline_counters()
        assert after["replayed_total"] == before["replayed_total"]
        assert after["accepted_total"] == before["accepted_total"]
