"""Tests for POST/GET /admin/maintenance (WS-3c) and the small WS-3b op
wiring (maintenance_ops.run_maintenance_operation).

Covers the WS-3 spec's unit test plan (sec 7c), items C1-C6:
single-flight 409/202 (C1, C2), prompt-return (C3), honest idempotent
re-scan (C4), failure recording (C5), and admin-auth enforcement (C6). C7-C9
are DTU-only (real Neo4j + real restart) and are out of scope here.

None of these tests touch a real Neo4j: ``run_maintenance_operation`` (or,
for C5, the underlying ``neo4j_store.run_repair`` it wraps) is monkeypatched
per-test so the CAS/HTTP/bookkeeping logic under test is exercised without a
live driver. ``reset_maintenance_coordinator`` (conftest.py, autouse) resets
the process-wide coordinator singleton before and after every test.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import httpx
import pytest
from context_intelligence_server.maintenance import coordinator

# ---------------------------------------------------------------------------
# Fixtures -- admin-override client (no real auth; require_admin no-op'd)
# ---------------------------------------------------------------------------


@pytest.fixture
async def admin_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Client with require_admin overridden to a no-op (T4-style override).

    Used for C1-C5: these tests exercise the endpoint's OWN logic (CAS,
    prompt-return, status reporting), not the auth layer -- that is C6's job,
    which uses a real, non-overridden auth client below.
    """
    from context_intelligence_server.main import app
    from context_intelligence_server.routers.admin import require_admin

    app.dependency_overrides[require_admin] = lambda: None
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as c:
            yield c
    finally:
        app.dependency_overrides.pop(require_admin, None)


def _stub_op(*, sleep_seconds: float = 0.0, records_affected: int = 0) -> Any:
    """Build a fake ``run_maintenance_operation`` replacement.

    Signature-compatible with the real function: ``(driver, run_id, *,
    quiesce_seconds)``. Reports success via ``coordinator.finish_op`` after
    an optional sleep, exactly like the real function would after its own
    quiesce + run_repair call -- but without touching Neo4j.
    """

    async def _fake(driver: Any, run_id: str, *, quiesce_seconds: float) -> None:
        if sleep_seconds:
            await asyncio.sleep(sleep_seconds)
        coordinator.finish_op(run_id, records_affected=records_affected, error=None)

    return _fake


# ---------------------------------------------------------------------------
# C1 -- POST while an op runs -> 409 with the current run_id
# ---------------------------------------------------------------------------


class TestSingleFlight409:
    async def test_post_while_running_returns_409_with_current_run_id(
        self, admin_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Never-finishing stub -- the op stays "running" for the whole test.
        monkeypatch.setattr(
            "context_intelligence_server.routers.admin.run_maintenance_operation",
            _stub_op(sleep_seconds=10.0),
        )

        first = await admin_client.post("/admin/maintenance")
        assert first.status_code == 202
        first_run_id = first.json()["run_id"]
        assert first.json()["state"] == "running"

        second = await admin_client.post("/admin/maintenance")
        assert second.status_code == 409
        body = second.json()
        assert body["run_id"] == first_run_id
        assert body["state"] == "running"
        assert "already running" in body["detail"]

    # -- C2: 20 concurrent POSTs -> exactly one 202, nineteen 409 -----------

    async def test_twenty_concurrent_posts_yield_exactly_one_202(
        self, admin_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "context_intelligence_server.routers.admin.run_maintenance_operation",
            _stub_op(sleep_seconds=0.05),
        )

        responses = await asyncio.gather(
            *[admin_client.post("/admin/maintenance") for _ in range(20)]
        )
        statuses = [r.status_code for r in responses]
        assert statuses.count(202) == 1, (
            f"expected exactly one 202 among 20 concurrent POSTs, got: {statuses}"
        )
        assert statuses.count(409) == 19


# ---------------------------------------------------------------------------
# C3 -- POST returns promptly; does NOT await the op's full duration
# ---------------------------------------------------------------------------


class TestReturnsPromptly:
    async def test_post_returns_before_op_completes(
        self, admin_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """MUST-FIX #5a: the handler must not block for the op's duration.

        The stub sleeps 0.5s before finishing; the POST itself must return in
        well under that, and an immediate GET must still observe "running".
        """
        monkeypatch.setattr(
            "context_intelligence_server.routers.admin.run_maintenance_operation",
            _stub_op(sleep_seconds=0.5),
        )

        loop = asyncio.get_event_loop()
        start = loop.time()
        resp = await admin_client.post("/admin/maintenance")
        elapsed = loop.time() - start

        assert resp.status_code == 202
        assert elapsed < 0.3, (
            f"POST took {elapsed:.3f}s -- should return well before the "
            f"0.5s op completes (MUST-FIX #5a: prompt return)"
        )

        get_resp = await admin_client.get("/admin/maintenance")
        assert get_resp.json()["state"] == "running"


# ---------------------------------------------------------------------------
# C4 -- POST on a clean graph: honest re-scan, fresh run_id + completed_at
# ---------------------------------------------------------------------------


class TestCleanGraphHonestRescan:
    async def test_post_on_clean_graph_reports_succeeded_zero_affected(
        self, admin_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "context_intelligence_server.routers.admin.run_maintenance_operation",
            _stub_op(sleep_seconds=0.0, records_affected=0),
        )

        resp = await admin_client.post("/admin/maintenance")
        assert resp.status_code == 202
        run_id = resp.json()["run_id"]
        assert resp.json()["started_at"] is not None

        # Let the (instant) stub task actually run before polling GET.
        await asyncio.sleep(0.02)

        get_resp = await admin_client.get("/admin/maintenance")
        body = get_resp.json()
        assert body["state"] == "succeeded"
        assert body["run_id"] == run_id
        assert body["records_affected"] == 0
        assert body["completed_at"] is not None
        assert body["error"] is None

    async def test_second_post_after_completion_gets_a_fresh_run_id(
        self, admin_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """D-H: re-running on an already-clean graph is a genuine re-scan --
        not a short-circuit -- proved by a NEW run_id/completed_at each time.
        """
        monkeypatch.setattr(
            "context_intelligence_server.routers.admin.run_maintenance_operation",
            _stub_op(sleep_seconds=0.0, records_affected=0),
        )

        first = await admin_client.post("/admin/maintenance")
        assert first.status_code == 202
        first_run_id = first.json()["run_id"]
        await asyncio.sleep(0.02)

        second = await admin_client.post("/admin/maintenance")
        assert second.status_code == 202
        second_run_id = second.json()["run_id"]
        await asyncio.sleep(0.02)

        assert second_run_id != first_run_id
        get_resp = await admin_client.get("/admin/maintenance")
        assert get_resp.json()["run_id"] == second_run_id
        assert get_resp.json()["state"] == "succeeded"


# ---------------------------------------------------------------------------
# C5 -- op raises -> state "failed", error populated and persists
# ---------------------------------------------------------------------------


class TestOpFailure:
    async def test_run_repair_exception_is_recorded_as_failed(
        self, admin_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Exercises the REAL run_maintenance_operation's except-clause (only
        the underlying run_repair call is stubbed to raise) -- proving the
        actual exception -> finish_op(error=...) wiring, not a re-implemented
        fake of it.
        """

        async def _raising_run_repair(driver: Any, database: str = "neo4j") -> Any:
            raise RuntimeError("simulated repair failure")

        monkeypatch.setattr(
            "context_intelligence_server.maintenance_ops.run_repair",
            _raising_run_repair,
        )
        # Skip the quiesce sleep so the test doesn't wait on the 2.0s default.
        monkeypatch.setattr(
            "context_intelligence_server.routers.admin.get_settings",
            lambda: _FakeSettingsZeroQuiesce(),
        )

        resp = await admin_client.post("/admin/maintenance")
        assert resp.status_code == 202
        run_id = resp.json()["run_id"]

        # No quiesce sleep configured -- give the task one scheduling slot.
        await asyncio.sleep(0.02)

        get_resp = await admin_client.get("/admin/maintenance")
        body = get_resp.json()
        assert body["state"] == "failed"
        assert body["run_id"] == run_id
        assert body["error"] is not None
        assert "simulated repair failure" in body["error"]
        assert body["completed_at"] is not None

        # Persistence: a second GET still shows the same failed record.
        get_resp_2 = await admin_client.get("/admin/maintenance")
        assert get_resp_2.json()["state"] == "failed"
        assert get_resp_2.json()["error"] == body["error"]


class _FakeSettingsZeroQuiesce:
    """Minimal settings stand-in: only the one attribute the endpoint reads."""

    maintenance_quiesce_seconds = 0.0


# ---------------------------------------------------------------------------
# C6 -- GET/POST without admin auth -> 401/403 (inherited require_admin)
# ---------------------------------------------------------------------------

FAKE_ADMIN_RAW_KEY = "maint-test-admin-key-do-not-use"
FAKE_ADMIN_KEY_DIGEST = hashlib.sha256(FAKE_ADMIN_RAW_KEY.encode()).hexdigest()
FAKE_DATA_RAW_KEY = "maint-test-data-key-ordinary-user"
FAKE_DATA_KEY_DIGEST = hashlib.sha256(FAKE_DATA_RAW_KEY.encode()).hexdigest()
FAKE_CONTRIBUTOR = "maint-tester"


def _make_static_settings_with_admin(tmp_path: Path) -> Any:
    from context_intelligence_server.config import Settings

    return Settings(
        auth_mode="static",
        api_keys={FAKE_DATA_KEY_DIGEST: {"id": FAKE_CONTRIBUTOR}},
        admin_api_key=FAKE_ADMIN_RAW_KEY,
        api_keys_store_path=str(tmp_path / "api-keys.json"),
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
    )


@pytest.fixture
async def real_auth_client(tmp_path: Path) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Client routed through the REAL auth middleware (no override) -- for
    C6, proving /admin/maintenance inherits require_admin's fail-closed
    401/403 matrix exactly like every other /admin/* route."""
    from context_intelligence_server.main import create_asgi_app

    settings = _make_static_settings_with_admin(tmp_path)
    middleware = create_asgi_app(settings=settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=middleware), base_url="http://test"
    ) as c:
        yield c


class TestAdminAuthEnforced:
    async def test_post_no_token_401(self, real_auth_client: httpx.AsyncClient) -> None:
        resp = await real_auth_client.post("/admin/maintenance")
        assert resp.status_code == 401

    async def test_get_no_token_401(self, real_auth_client: httpx.AsyncClient) -> None:
        resp = await real_auth_client.get("/admin/maintenance")
        assert resp.status_code == 401

    async def test_post_data_key_403(self, real_auth_client: httpx.AsyncClient) -> None:
        resp = await real_auth_client.post(
            "/admin/maintenance",
            headers={"Authorization": f"Bearer {FAKE_DATA_RAW_KEY}"},
        )
        assert resp.status_code == 403

    async def test_get_data_key_403(self, real_auth_client: httpx.AsyncClient) -> None:
        resp = await real_auth_client.get(
            "/admin/maintenance",
            headers={"Authorization": f"Bearer {FAKE_DATA_RAW_KEY}"},
        )
        assert resp.status_code == 403

    async def test_get_admin_key_200(
        self, real_auth_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Sanity: the admin key itself DOES authenticate + authorize (no
        false-positive 401/403 for the correct credential)."""
        resp = await real_auth_client.get(
            "/admin/maintenance",
            headers={"Authorization": f"Bearer {FAKE_ADMIN_RAW_KEY}"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["state"] == "unknown"  # never ran in this fresh coordinator
