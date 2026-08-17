"""Tests for FastAPI app — GET /status and POST /events endpoints."""

import asyncio
import contextlib
import json
import logging
from pathlib import Path
from collections.abc import AsyncGenerator
from typing import Any, Self
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

import context_intelligence_server.main as main_module
from context_intelligence_server.auth import BearerTokenMiddleware
from context_intelligence_server.main import app, lifespan, registry
from context_intelligence_server.models import CypherRequest
from tests.conftest import MockNeo4jDriver


@pytest.fixture(autouse=True)
def _clear_idempotency_cache() -> None:
    main_module.idempotency_cache.clear()


async def test_status_returns_200(client: httpx.AsyncClient) -> None:
    response = await client.get("/status")
    assert response.status_code == 200


async def test_status_body(client: httpx.AsyncClient) -> None:
    response = await client.get("/status")
    data = response.json()
    assert data["status"] == "ok"
    assert data["uptime_seconds"] >= 0
    assert data["active_sessions"] == 0


async def test_post_events_returns_202(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws",
            "data": {
                "session_id": "sess-1",
                "timestamp": "2026-06-16T20:17:11.604690+00:00",
            },
        },
    )
    assert response.status_code == 202


async def test_event_enqueued_not_logged(
    client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.DEBUG, logger="context_intelligence_server"):
        response = await client.post(
            "/events",
            json={
                "event": "tool_use",
                "workspace": "/ws",
                "data": {
                    "session_id": "sess-1",
                    "timestamp": "2026-06-16T20:17:11.604690+00:00",
                },
            },
        )
    assert response.status_code == 202
    assert not any("event_enqueued" in record.getMessage() for record in caplog.records)


async def test_post_events_body(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws",
            "data": {
                "session_id": "sess-1",
                "timestamp": "2026-06-16T20:17:11.604690+00:00",
            },
        },
    )
    data = response.json()
    assert data["status"] == "queued"
    assert data["session_id"] == "sess-1"


async def test_post_events_duplicate_idempotency_key_skips_enqueue(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A duplicate idempotency_key must NOT append a second durable line."""
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *args, **kwargs: MagicMock()
    )
    appended: list[tuple[str, bytes]] = []

    async def _fake_append(worker_key: str, raw: bytes) -> None:
        appended.append((worker_key, raw))

    monkeypatch.setattr(main_module.registry.queue_manager, "append", _fake_append)

    payload = {
        "event": "tool_use",
        "workspace": "/ws",
        "idempotency_key": "aci-event-v1:test-key",
        "data": {
            "session_id": "sess-dupe",
            "timestamp": "2026-06-16T20:17:11.604690+00:00",
        },
    }

    first = await client.post("/events", json=payload)
    second = await client.post("/events", json=payload)

    assert first.status_code == 202
    assert first.json()["status"] == "queued"
    assert second.status_code == 202
    assert second.json()["status"] == "duplicate"
    assert len(appended) == 1


async def test_post_events_replay_bypasses_idempotency_guard(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """replay=true bypasses the idempotency guard, appending a second line."""
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *args, **kwargs: MagicMock()
    )
    appended: list[tuple[str, bytes]] = []

    async def _fake_append(worker_key: str, raw: bytes) -> None:
        appended.append((worker_key, raw))

    monkeypatch.setattr(main_module.registry.queue_manager, "append", _fake_append)

    payload = {
        "event": "tool_use",
        "workspace": "/ws",
        "idempotency_key": "aci-event-v1:test-key",
        "data": {
            "session_id": "sess-replay",
            "timestamp": "2026-06-16T20:17:11.604690+00:00",
        },
    }

    first = await client.post("/events", json=payload)
    replay = await client.post("/events?replay=true", json=payload)

    assert first.status_code == 202
    assert replay.status_code == 202
    assert replay.json()["status"] == "queued"
    assert len(appended) == 2


async def test_post_events_increments_accepted_counter(
    client: httpx.AsyncClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A durably-accepted event increments the registry accepted_total (D2)."""
    from context_intelligence_server.queue_manager import QueueManager

    # Point the registry at a tmp queue dir so the durable append is isolated.
    monkeypatch.setattr(
        main_module.registry, "_queue_manager", QueueManager(queues_dir=tmp_path)
    )
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *args, **kwargs: MagicMock()
    )

    before = main_module.registry.pipeline_counters()["accepted_total"]

    response = await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws",
            "data": {
                "session_id": "sess-accepted",
                "timestamp": "2026-06-16T20:17:11.604690+00:00",
            },
        },
    )

    assert response.status_code == 202
    after = main_module.registry.pipeline_counters()["accepted_total"]
    assert after == before + 1


async def test_post_events_increments_active_sessions(
    client: httpx.AsyncClient,
) -> None:
    await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws",
            "data": {
                "session_id": "sess-inc",
                "timestamp": "2026-06-16T20:17:11.604690+00:00",
            },
        },
    )
    status_response = await client.get("/status")
    assert status_response.json()["active_sessions"] >= 1


async def test_post_events_missing_event_returns_422(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/events",
        json={"workspace": "/ws", "data": {}},
    )
    assert response.status_code == 422


async def test_post_events_no_session_id_returns_null(
    client: httpx.AsyncClient,
) -> None:
    # data has a valid timestamp but no session_id — response session_id must be null.
    response = await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws",
            "data": {"timestamp": "2026-06-16T20:17:11.604690+00:00"},
        },
    )
    assert response.status_code == 202
    data = response.json()
    assert data["session_id"] is None


async def test_drain_loop_processes_event(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A posted event is drained from the durable log by the sticky drainer.

    Migrated off the vestigial in-memory worker.queue (Phase B2, Task 5): the
    durable drain loop reads from the on-disk QueueManager log, so success is
    observed by polling that log to empty rather than worker.queue.join().
    """
    from context_intelligence_server.neo4j_store import Neo4jGraphStore

    proc = AsyncMock()
    monkeypatch.setattr("context_intelligence_server.registry.process_event", proc)
    monkeypatch.setattr(Neo4jGraphStore, "flush", AsyncMock())
    monkeypatch.setattr(Neo4jGraphStore, "close", AsyncMock())

    await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws",
            "data": {
                "session_id": "sess-drain",
                "timestamp": "2026-06-16T20:17:11.604690+00:00",
            },
        },
    )

    qm = registry.queue_manager
    for _ in range(400):
        await asyncio.sleep(0.01)
        if (await qm.read_batch("sess-drain", 10)).lines == []:
            break

    assert (await qm.read_batch("sess-drain", 10)).lines == []
    assert proc.await_count >= 1


async def test_list_blobs_returns_empty_for_session_with_no_blobs(
    client: httpx.AsyncClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /blobs/{session_id} returns 200 with empty blobs list for session with no blobs."""
    monkeypatch.setattr(main_module._settings, "blob_path", str(tmp_path))

    response = await client.get("/blobs/no-blobs-session")
    assert response.status_code == 200
    data = response.json()
    assert data["session_id"] == "no-blobs-session"
    assert data["blobs"] == []


async def test_list_blobs_returns_correct_uris_for_existing_blobs(
    client: httpx.AsyncClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /blobs/{session_id} returns 200 with correct ci-blob:// URIs for existing blobs."""
    monkeypatch.setattr(main_module._settings, "blob_path", str(tmp_path))

    session_id = "blob-list-session"
    blob_dir = tmp_path / session_id / "blobs"
    blob_dir.mkdir(parents=True, exist_ok=True)
    (blob_dir / "alpha.json").write_text("{}", encoding="utf-8")
    (blob_dir / "beta.json").write_text("{}", encoding="utf-8")

    response = await client.get(f"/blobs/{session_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["session_id"] == session_id
    assert data["blobs"] == [
        f"ci-blob://{session_id}/alpha",
        f"ci-blob://{session_id}/beta",
    ]


async def test_get_blob_returns_200_with_content(
    client: httpx.AsyncClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /blobs/{session_id}/{key} returns 200 with blob content for existing blob."""
    monkeypatch.setattr(main_module._settings, "blob_path", str(tmp_path))

    session_id = "test-session"
    key = "my-key"
    blob_data = {"foo": "bar", "count": 42}

    blob_dir = tmp_path / session_id / "blobs"
    blob_dir.mkdir(parents=True, exist_ok=True)
    (blob_dir / f"{key}.json").write_text(json.dumps(blob_data), encoding="utf-8")

    response = await client.get(f"/blobs/{session_id}/{key}")
    assert response.status_code == 200
    assert response.json() == blob_data


async def test_get_blob_returns_404_for_missing_blob(
    client: httpx.AsyncClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /blobs/{session_id}/{key} returns 404 with 'not found' in detail for missing blob."""
    monkeypatch.setattr(main_module._settings, "blob_path", str(tmp_path))

    response = await client.get("/blobs/missing-session/missing-key")
    assert response.status_code == 404
    data = response.json()
    assert "not found" in data["detail"].lower()
    assert "ci-blob://" in data["detail"]


# ---------------------------------------------------------------------------
# POST /cypher tests
# ---------------------------------------------------------------------------


async def test_cypher_request_model_validation() -> None:
    """CypherRequest model validates correctly with required fields and defaults."""
    req = CypherRequest(query="MATCH (n) RETURN n")
    assert req.query == "MATCH (n) RETURN n"
    assert req.params == {}
    assert req.workspace is None


async def test_cypher_request_model_with_workspace() -> None:
    """CypherRequest model accepts workspace and params."""
    req = CypherRequest(
        query="MATCH (n) RETURN n",
        params={"key": "value"},
        workspace="/my/workspace",
    )
    assert req.workspace == "/my/workspace"
    assert req.params == {"key": "value"}


async def test_cypher_proxy_returns_results(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /cypher returns 200 with {results: [...]} from Neo4j."""
    mock_row = {"name": "Alice"}
    # /cypher reads app.state.neo4j_query_driver (two-client split, doc 12),
    # not the admin neo4j_driver.
    monkeypatch.setattr(
        main_module.app.state,
        "neo4j_query_driver",
        MockNeo4jDriver(rows=[mock_row]),
        raising=False,
    )
    monkeypatch.setattr(
        main_module.app.state, "neo4j_query_access_mode", "READ", raising=False
    )

    response = await client.post("/cypher", json={"query": "MATCH (n) RETURN n"})
    assert response.status_code == 200
    data = response.json()
    assert "results" in data
    assert data["results"] == [mock_row]


async def test_cypher_workspace_injection(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /cypher injects workspace into params when workspace is not None or '*'."""
    captured_params: dict[str, Any] = {}
    # /cypher reads app.state.neo4j_query_driver (two-client split, doc 12),
    # not the admin neo4j_driver.
    monkeypatch.setattr(
        main_module.app.state,
        "neo4j_query_driver",
        MockNeo4jDriver(captured=captured_params),
        raising=False,
    )
    monkeypatch.setattr(
        main_module.app.state, "neo4j_query_access_mode", "READ", raising=False
    )

    await client.post(
        "/cypher",
        json={
            "query": "MATCH (n) RETURN n",
            "workspace": "/my/ws",
            "params": {"id": 42},
        },
    )
    assert captured_params.get("workspace") == "/my/ws"
    assert (
        captured_params.get("id") == 42
    )  # user-supplied param preserved after injection


async def test_cypher_star_workspace_not_injected(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /cypher does NOT inject workspace when workspace='*' (cross-workspace)."""
    captured_params: dict[str, Any] = {}
    # /cypher reads app.state.neo4j_query_driver (two-client split, doc 12),
    # not the admin neo4j_driver.
    monkeypatch.setattr(
        main_module.app.state,
        "neo4j_query_driver",
        MockNeo4jDriver(captured=captured_params),
        raising=False,
    )
    monkeypatch.setattr(
        main_module.app.state, "neo4j_query_access_mode", "READ", raising=False
    )

    await client.post(
        "/cypher",
        json={"query": "MATCH (n) RETURN n", "workspace": "*"},
    )
    assert "workspace" not in captured_params


async def test_cypher_neo4j_error_returns_500(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /cypher returns 500 with error detail when Neo4j raises an exception."""
    # /cypher reads app.state.neo4j_query_driver (two-client split, doc 12),
    # not the admin neo4j_driver.
    monkeypatch.setattr(
        main_module.app.state,
        "neo4j_query_driver",
        MockNeo4jDriver(exc=RuntimeError("Connection refused")),
        raising=False,
    )
    monkeypatch.setattr(
        main_module.app.state, "neo4j_query_access_mode", "READ", raising=False
    )

    response = await client.post("/cypher", json={"query": "MATCH (n) RETURN n"})
    assert response.status_code == 500
    data = response.json()
    assert "Connection refused" in data["detail"]


# ---------------------------------------------------------------------------
# Enriched /status tests
# ---------------------------------------------------------------------------


async def test_status_includes_sessions_list(client: httpx.AsyncClient) -> None:
    """GET /status returns dict with sessions and recent_events list fields."""
    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert "sessions" in data
    assert isinstance(data["sessions"], list)
    assert "recent_events" in data
    assert isinstance(data["recent_events"], list)


async def test_status_session_detail_after_event(client: httpx.AsyncClient) -> None:
    """After posting an event, /status sessions list includes session detail."""
    await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws-detail",
            "data": {
                "session_id": "sess-detail",
                "timestamp": "2026-06-16T20:17:11.604690+00:00",
            },
        },
    )
    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert data["active_sessions"] >= 1
    session_ids = [s["session_id"] for s in data["sessions"]]
    assert "sess-detail" in session_ids
    sess = next(s for s in data["sessions"] if s["session_id"] == "sess-detail")
    assert sess["workspace"] == "/ws-detail"
    assert "events_processed" in sess


async def test_status_includes_completed_sessions(client: httpx.AsyncClient) -> None:
    """GET /status response includes a 'completed_sessions' list field."""
    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert "completed_sessions" in data
    assert isinstance(data["completed_sessions"], list)


async def test_status_includes_error_count_last_hour(client: httpx.AsyncClient) -> None:
    """GET /status response includes an 'error_count_last_hour' int field."""
    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert "error_count_last_hour" in data
    assert isinstance(data["error_count_last_hour"], int)


# ---------------------------------------------------------------------------
# Auth middleware integration tests
# ---------------------------------------------------------------------------


@contextlib.asynccontextmanager
async def _auth_client(
    token: str = "test-secret",
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Yield an AsyncClient pre-wrapped with BearerTokenMiddleware (keystore API)."""
    import hashlib

    keystore = {hashlib.sha256(token.encode()).hexdigest(): "owner"}
    wrapped = BearerTokenMiddleware(app, keystore=keystore)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wrapped),
        base_url="http://test",
    ) as c:
        yield c


class TestAuthMiddleware:
    """Bearer token middleware integration tests against the real app."""

    async def test_status_accessible_without_token(self) -> None:
        """/status is always accessible through middleware, even when api_key is set."""
        async with _auth_client() as c:
            response = await c.get("/status")
        assert response.status_code == 200

    async def test_events_returns_401_without_token_when_api_key_set(self) -> None:
        """POST /events returns 401 when api_key is configured and no token sent."""
        async with _auth_client() as c:
            response = await c.post(
                "/events",
                json={
                    "event": "tool_use",
                    "workspace": "/ws",
                    "data": {
                        "session_id": "s1",
                        "timestamp": "2026-06-16T20:17:11.604690+00:00",
                    },
                },
            )
        assert response.status_code == 401

    async def test_events_returns_202_with_valid_token(self) -> None:
        """POST /events returns 202 when correct bearer token is provided."""
        async with _auth_client() as c:
            response = await c.post(
                "/events",
                json={
                    "event": "tool_use",
                    "workspace": "/ws",
                    "data": {
                        "session_id": "s1",
                        "timestamp": "2026-06-16T20:17:11.604690+00:00",
                    },
                },
                headers={"Authorization": "Bearer test-secret"},
            )
        assert response.status_code == 202

    async def test_cypher_returns_401_without_token(self) -> None:
        """POST /cypher returns 401 when api_key is configured and no token sent."""
        async with _auth_client() as c:
            response = await c.post("/cypher", json={"query": "MATCH (n) RETURN n"})
        assert response.status_code == 401

    async def test_no_auth_when_api_key_is_none(
        self, client: httpx.AsyncClient
    ) -> None:
        """When api_key is None (default), no auth is required — backward compat."""
        response = await client.post(
            "/events",
            json={
                "event": "tool_use",
                "workspace": "/ws",
                "data": {
                    "session_id": "s1",
                    "timestamp": "2026-06-16T20:17:11.604690+00:00",
                },
            },
        )
        assert response.status_code == 202


# ---------------------------------------------------------------------------
# main() dispatch tests
# ---------------------------------------------------------------------------


class TestMainDispatch:
    """Tests for the CLI entrypoint main() argparse dispatch (serve / doctor).

    INVARIANT under test: no args (the bare console-script invocation systemd
    and the macOS launchd agent use) MUST dispatch to serve(). This is the
    single most important test in this class -- breaking it would break
    every production restart.
    """

    def test_main_with_no_args_calls_run(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """main() with no argv reads real sys.argv (empty here) -- must serve."""
        from unittest.mock import patch as _patch

        import context_intelligence_server.main as _main_mod

        monkeypatch.setattr("sys.argv", ["context-intelligence-server"])

        with _patch.object(_main_mod, "run") as mock_run:
            _main_mod.main()

        mock_run.assert_called_once()

    def test_main_empty_argv_list_calls_run_and_not_doctor(self) -> None:
        """main([]) -- the explicit empty-args form -- calls run() (gunicorn)
        and must NOT touch the doctor module."""
        import context_intelligence_server.main as _main_mod

        with (
            patch.object(_main_mod, "run") as mock_run,
            patch("context_intelligence_server.doctor.run_doctor") as mock_doctor,
        ):
            _main_mod.main([])

        mock_run.assert_called_once()
        mock_doctor.assert_not_called()

    def test_main_explicit_serve_calls_run(self) -> None:
        """main(["serve"]) explicitly dispatches to serve, same as no args."""
        import context_intelligence_server.main as _main_mod

        with patch.object(_main_mod, "run") as mock_run:
            _main_mod.main(["serve"])

        mock_run.assert_called_once()

    def test_main_doctor_dispatches_run_doctor_fix_false(self) -> None:
        """main(["doctor"]) calls doctor.run_doctor(fix=False) and sys.exits
        with its return code."""
        import context_intelligence_server.main as _main_mod

        with patch(
            "context_intelligence_server.doctor.run_doctor",
            new_callable=AsyncMock,
            return_value=0,
        ) as mock_doctor:
            with pytest.raises(SystemExit) as exc_info:
                _main_mod.main(["doctor"])

        mock_doctor.assert_awaited_once_with(fix=False)
        assert exc_info.value.code == 0

    def test_main_doctor_fix_dispatches_run_doctor_fix_true(self) -> None:
        """main(["doctor", "--fix"]) calls doctor.run_doctor(fix=True)."""
        import context_intelligence_server.main as _main_mod

        with patch(
            "context_intelligence_server.doctor.run_doctor",
            new_callable=AsyncMock,
            return_value=1,
        ) as mock_doctor:
            with pytest.raises(SystemExit) as exc_info:
                _main_mod.main(["doctor", "--fix"])

        mock_doctor.assert_awaited_once_with(fix=True)
        assert exc_info.value.code == 1


async def test_lifespan_creates_and_closes_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lifespan creates BOTH Neo4j drivers (admin + query) at startup and
    closes BOTH at shutdown (Neo4j two-client split, doc 12)."""
    mock_driver = MagicMock()
    mock_driver.close = AsyncMock()

    with (
        patch(
            "context_intelligence_server.main.setup_logging",
        ) as mock_setup_logging,
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ) as mock_driver_factory,
    ):
        async with lifespan(main_module.app):
            # setup_logging() is called once during startup
            mock_setup_logging.assert_called_once()
            # During lifespan: driver factory must have been called TWICE --
            # once for the admin (read/write) client, once for the
            # cypher_query (read-intent) client.
            assert mock_driver_factory.call_count == 2
            # Both drivers are accessible via app.state (same mock object
            # here since the factory is patched with a single return_value).
            assert main_module.app.state.neo4j_driver is mock_driver
            assert main_module.app.state.neo4j_query_driver is mock_driver
            # The resolved query access_mode is stashed for /cypher.
            assert main_module.app.state.neo4j_query_access_mode == "READ"

        # After lifespan exits: close() must have been awaited for BOTH
        # drivers (admin + query).
        assert mock_driver.close.await_count == 2


# ---------------------------------------------------------------------------
# Lifespan crash-recovery + workers==1 guard tests (Phase B2)
# ---------------------------------------------------------------------------


def _patched_lifespan_deps() -> Any:
    """Return the patch context managers that stub the lifespan's Neo4j deps."""
    mock_driver = MagicMock()
    mock_driver.close = AsyncMock()
    return mock_driver


async def test_lifespan_recovers_and_respawns_drainers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lifespan respawns a drainer for each session with an undrained line,
    using the workspace parsed from that session's first log line."""
    sid = "sess-recover"
    qm = registry.queue_manager
    body = json.dumps(
        {
            "event": "tool_use",
            "workspace": "/recovered-ws",
            "data": {"session_id": sid},
        }
    ).encode("utf-8")
    await qm.append(sid, body)

    spawned: list[tuple] = []
    monkeypatch.setattr(
        registry,
        "get_or_create",
        lambda s, w, **kw: spawned.append((s, w)),
    )

    mock_driver = _patched_lifespan_deps()
    with (
        patch("context_intelligence_server.main.setup_logging"),
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ),
        patch(
            "context_intelligence_server.main.ensure_neo4j_schema",
            new=AsyncMock(),
        ),
    ):
        async with lifespan(main_module.app):
            pass

    assert (sid, "/recovered-ws") in spawned


async def test_lifespan_skips_recovery_for_empty_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A session whose first line has an empty workspace is NOT respawned
    (spawning a workspace='' worker would violate the non-empty-workspace
    invariant)."""
    sid = "sess-empty-ws"
    qm = registry.queue_manager
    body = json.dumps(
        {
            "event": "tool_use",
            "workspace": "",
            "data": {"session_id": sid},
        }
    ).encode("utf-8")
    await qm.append(sid, body)

    spawned: list[tuple] = []
    monkeypatch.setattr(
        registry,
        "get_or_create",
        lambda s, w, **kw: spawned.append((s, w)),
    )

    mock_driver = _patched_lifespan_deps()
    with (
        patch("context_intelligence_server.main.setup_logging"),
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ),
        patch(
            "context_intelligence_server.main.ensure_neo4j_schema",
            new=AsyncMock(),
        ),
    ):
        async with lifespan(main_module.app):
            pass

    assert spawned == []


# ---------------------------------------------------------------------------
# Bounded crash-recovery respawn (Change 1): crash_recovery_respawn_limit
# ---------------------------------------------------------------------------


async def _seed_recoverable_session(qm: Any, sid: str, workspace: str) -> None:
    body = json.dumps(
        {
            "event": "tool_use",
            "workspace": workspace,
            "data": {"session_id": sid},
        }
    ).encode("utf-8")
    await qm.append(sid, body)


async def test_lifespan_default_respawns_all_recovered_sessions_unbounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default (crash_recovery_respawn_limit=None) MUST preserve today's
    behaviour exactly: every recovered session is respawned on this boot,
    no matter how many there are."""
    qm = registry.queue_manager
    sids = [f"sess-unbounded-{i}" for i in range(10)]
    for sid in sids:
        await _seed_recoverable_session(qm, sid, "/ws")

    spawned: list[tuple] = []
    monkeypatch.setattr(
        registry, "get_or_create", lambda s, w, **kw: spawned.append((s, w))
    )
    assert main_module._settings.crash_recovery_respawn_limit is None

    mock_driver = _patched_lifespan_deps()
    with (
        patch("context_intelligence_server.main.setup_logging"),
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ),
        patch("context_intelligence_server.main.ensure_neo4j_schema", new=AsyncMock()),
    ):
        async with lifespan(main_module.app):
            pass

    assert {s for s, _w in spawned} == set(sids)


async def test_lifespan_respawn_cap_defers_remainder_and_logs_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """With a finite crash_recovery_respawn_limit, only that many sessions
    are respawned THIS boot; the remainder are deferred and a WARNING names
    the exact respawned/deferred counts + the setting to raise."""
    qm = registry.queue_manager
    sids = [f"sess-cap-{i}" for i in range(5)]
    for sid in sids:
        await _seed_recoverable_session(qm, sid, "/ws")

    spawned: list[tuple] = []
    monkeypatch.setattr(
        registry, "get_or_create", lambda s, w, **kw: spawned.append((s, w))
    )
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", 2)

    mock_driver = _patched_lifespan_deps()
    with (
        patch("context_intelligence_server.main.setup_logging"),
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ),
        patch("context_intelligence_server.main.ensure_neo4j_schema", new=AsyncMock()),
        caplog.at_level(logging.WARNING, logger="context_intelligence_server"),
    ):
        async with lifespan(main_module.app):
            pass

    # Exactly the cap's worth of sessions were respawned -- never more.
    assert len(spawned) == 2
    # The un-respawned sessions were NEVER passed to get_or_create at all.
    assert {s for s, _w in spawned}.issubset(set(sids))

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("crash-recovery respawn cap reached" in r.getMessage() for r in warnings)
    cap_warning = next(
        r for r in warnings if "crash-recovery respawn cap reached" in r.getMessage()
    )
    msg = cap_warning.getMessage()
    assert "2/2 respawned" in msg  # respawned/attempted this boot
    assert "3 session(s) deferred" in msg  # 5 recovered - 2 processed = 3
    assert "crash_recovery_respawn_limit" in msg


async def test_lifespan_deferred_sessions_untouched_and_recoverable_next_boot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Deferred sessions are left completely untouched on disk -- no read, no
    write -- so a SUBSEQUENT boot's recover() call reports them again and can
    respawn them (no data loss, no corruption)."""
    qm = registry.queue_manager
    sids = [f"sess-defer-{i}" for i in range(4)]
    for sid in sids:
        await _seed_recoverable_session(qm, sid, "/ws")

    spawned_boot1: list[tuple] = []
    monkeypatch.setattr(
        registry, "get_or_create", lambda s, w, **kw: spawned_boot1.append((s, w))
    )
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", 1)

    mock_driver = _patched_lifespan_deps()
    with (
        patch("context_intelligence_server.main.setup_logging"),
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ),
        patch("context_intelligence_server.main.ensure_neo4j_schema", new=AsyncMock()),
    ):
        async with lifespan(main_module.app):
            pass

    assert len(spawned_boot1) == 1
    deferred_sids = set(sids) - {s for s, _w in spawned_boot1}
    assert len(deferred_sids) == 3

    # The deferred sessions' queue lines are STILL fully intact and
    # recoverable: recover() (a fresh scan, same on-disk state) reports them
    # again, exactly as before this boot ran.
    recovered_again = await qm.recover()
    assert deferred_sids <= set(recovered_again)
    for sid in deferred_sids:
        batch = await qm.read_batch(sid, max_items=1)
        assert batch.lines != []  # data neither dropped nor corrupted


async def test_lifespan_respawn_cap_zero_defers_everything(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cap of 0 is a valid opt-in (never respawn automatically at boot);
    every recovered session is deferred, none is touched."""
    qm = registry.queue_manager
    sid = "sess-cap-zero"
    await _seed_recoverable_session(qm, sid, "/ws")

    spawned: list[tuple] = []
    monkeypatch.setattr(
        registry, "get_or_create", lambda s, w, **kw: spawned.append((s, w))
    )
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", 0)

    mock_driver = _patched_lifespan_deps()
    with (
        patch("context_intelligence_server.main.setup_logging"),
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ),
        patch("context_intelligence_server.main.ensure_neo4j_schema", new=AsyncMock()),
    ):
        async with lifespan(main_module.app):
            pass

    assert spawned == []
    recovered_again = await qm.recover()
    assert sid in recovered_again


# ---------------------------------------------------------------------------
# Deferred-backlog sweep (ci_pr73-rt7): a finite crash_recovery_respawn_limit
# must NOT permanently strand the deferred tail. The periodic sweep re-runs
# recovery and tops the drainer pool up to the ceiling as head sessions drain.
# ---------------------------------------------------------------------------


async def test_crash_recovery_topup_drains_deferred_tail_across_passes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The deferred tail is not stranded: with ceiling=2, pass 1 dispatches the
    2 head sessions; once those finish draining (drop out of recover()), pass 2
    dispatches the previously-deferred 2. Live recovered drainers never exceed
    the ceiling."""
    qm = registry.queue_manager
    sids = sorted(f"sess-sweep-{i}" for i in range(4))
    for sid in sids:
        await _seed_recoverable_session(qm, sid, "/ws")

    spawned: list[str] = []
    monkeypatch.setattr(registry, "get_or_create", lambda s, w, **kw: spawned.append(s))

    # Pass 1: only the ceiling's worth (2) are dispatched; the tail is deferred.
    dispatched = await main_module._crash_recovery_topup(2)
    assert dispatched == 2
    assert set(spawned) == set(sids[:2])

    # The 2 head sessions finish draining -> commit them to EOF so recover()
    # stops reporting them (exactly what a real drainer does on completion).
    for sid in sids[:2]:
        batch = await qm.read_batch(sid, max_items=10)
        await qm.commit(sid, batch.end_offset, None)

    # Pass 2: the previously-DEFERRED tail is now dispatched -- not stranded.
    spawned.clear()
    dispatched = await main_module._crash_recovery_topup(2)
    assert dispatched == 2
    assert set(spawned) == set(sids[2:])


async def test_lifespan_enables_sweep_under_finite_limit(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A finite ceiling with a positive interval starts the background sweep
    (logged), so the deferred tail drains progressively rather than only on
    restart."""
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", 2)
    monkeypatch.setattr(
        main_module._settings, "crash_recovery_sweep_interval_seconds", 300
    )
    monkeypatch.setattr(registry, "get_or_create", lambda *a, **kw: MagicMock())

    mock_driver = _patched_lifespan_deps()
    with (
        patch("context_intelligence_server.main.setup_logging"),
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ),
        patch("context_intelligence_server.main.ensure_neo4j_schema", new=AsyncMock()),
        caplog.at_level(logging.INFO, logger="context_intelligence_server"),
    ):
        async with lifespan(main_module.app):
            pass  # task is created on entry and cancelled cleanly on exit

    assert any(
        "crash_recovery_sweep: enabled" in r.getMessage() for r in caplog.records
    )


async def test_lifespan_no_sweep_when_limit_unbounded(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Default (unbounded) ceiling: there is no deferred tail, so NO sweep task
    is started -- existing deployments are completely unaffected."""
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", None)
    monkeypatch.setattr(registry, "get_or_create", lambda *a, **kw: MagicMock())

    mock_driver = _patched_lifespan_deps()
    with (
        patch("context_intelligence_server.main.setup_logging"),
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ),
        patch("context_intelligence_server.main.ensure_neo4j_schema", new=AsyncMock()),
        caplog.at_level(logging.INFO, logger="context_intelligence_server"),
    ):
        async with lifespan(main_module.app):
            pass

    assert not any(
        "crash_recovery_sweep: enabled" in r.getMessage() for r in caplog.records
    )


async def test_lifespan_no_sweep_when_interval_zero(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """interval=0 is an explicit opt-out: even under a finite ceiling the sweep
    is not started (documented tradeoff: tail drains only on restart/new event)."""
    monkeypatch.setattr(main_module._settings, "crash_recovery_respawn_limit", 2)
    monkeypatch.setattr(
        main_module._settings, "crash_recovery_sweep_interval_seconds", 0
    )
    monkeypatch.setattr(registry, "get_or_create", lambda *a, **kw: MagicMock())

    mock_driver = _patched_lifespan_deps()
    with (
        patch("context_intelligence_server.main.setup_logging"),
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ),
        patch("context_intelligence_server.main.ensure_neo4j_schema", new=AsyncMock()),
        caplog.at_level(logging.INFO, logger="context_intelligence_server"),
    ):
        async with lifespan(main_module.app):
            pass

    assert not any(
        "crash_recovery_sweep: enabled" in r.getMessage() for r in caplog.records
    )


# ---------------------------------------------------------------------------
# Deploy-safe boot (council amendment, 2026-08-12; see
# docs/plans/2026-08-12-deploy-safe-boot-spec.md in the workspace root):
# a deploy (restart) MUST NEVER crash-loop on graph migration/reachability
# state. Cold start now calls ensure_neo4j_schema with the SAME
# fail_on_data_conflict=False default the mid-flight flush path always used,
# and the untagged-node probe no longer raises on a positive count -- both
# feed a tri-state schema_health signal ("healthy" / "degraded" / "unknown")
# surfaced on GET /status (never GET /version -- see B7). The B1 boundary
# additionally catches ANY exception from the startup sequence (not just
# these two named sites), so boot proceeds regardless of which of the ~11
# startup raise sites fails. Only run_repair / `doctor --fix` still opts
# into fail_on_data_conflict=True -- see
# tests/neo4j/test_node_identity_migration.py for that (unchanged) contract.
# ---------------------------------------------------------------------------


async def test_lifespan_calls_ensure_schema_with_fail_on_data_conflict_false() -> None:
    """Cold start must call ensure_neo4j_schema with fail_on_data_conflict=False
    -- boot never fails closed on a genuine :Node constraint data conflict.
    That fail-closed contract now belongs ONLY to run_repair/`doctor --fix`."""
    mock_driver = _patched_lifespan_deps()
    mock_ensure_schema = AsyncMock(return_value=True)
    with (
        patch("context_intelligence_server.main.setup_logging"),
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ),
        patch(
            "context_intelligence_server.main.ensure_neo4j_schema",
            new=mock_ensure_schema,
        ),
        patch(
            "context_intelligence_server.main.count_untagged_nodes",
            new=AsyncMock(return_value=0),
        ),
    ):
        async with lifespan(main_module.app):
            pass

    mock_ensure_schema.assert_awaited_once()
    _args, kwargs = mock_ensure_schema.await_args
    assert kwargs.get("fail_on_data_conflict") is False, (
        "lifespan must call ensure_neo4j_schema with fail_on_data_conflict=False "
        "-- deploy-safe boot never fails closed on graph data state (only "
        "run_repair/`doctor --fix` opts into True)."
    )
    assert main_module.app.state.schema_health == "healthy"


async def test_lifespan_does_not_raise_when_ensure_schema_itself_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """B1: a non-data-conflict exception escaping ensure_neo4j_schema itself
    (e.g. a Neo4jError re-raised by _create_index, a TransientError during
    the ordinary ACA cold-start reachability race, or a rejected credential)
    is caught by the single lifespan try/except boundary -- boot proceeds
    and schema_health reports "unknown" (never coerced to "healthy"), never
    propagating a RuntimeError out of lifespan."""
    mock_driver = _patched_lifespan_deps()
    with (
        patch("context_intelligence_server.main.setup_logging"),
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ),
        patch(
            "context_intelligence_server.main.ensure_neo4j_schema",
            new=AsyncMock(
                side_effect=RuntimeError("Neo4j unreachable (TransientError)")
            ),
        ),
        caplog.at_level(logging.ERROR),
    ):
        async with lifespan(main_module.app):  # MUST NOT raise
            pass

    assert main_module.app.state.schema_health == "unknown"
    assert "startup sequence failed" in (
        main_module.app.state.schema_degraded_reason or ""
    )
    assert any(
        "startup_degraded" in record.getMessage() for record in caplog.records
    ), "Expected a loud ERROR-level startup_degraded log."


async def test_lifespan_does_not_raise_on_untagged_nodes() -> None:
    """On an un-migrated graph (untagged :Node count > 0), startup no longer
    raises -- boot proceeds and schema_health reports "degraded" with the
    untagged count surfaced, rather than refusing to serve."""
    mock_driver = _patched_lifespan_deps()
    with (
        patch("context_intelligence_server.main.setup_logging"),
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ),
        patch(
            "context_intelligence_server.main.ensure_neo4j_schema",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "context_intelligence_server.main.count_untagged_nodes",
            new=AsyncMock(return_value=42),
        ),
    ):
        async with lifespan(main_module.app):  # MUST NOT raise
            pass

    assert main_module.app.state.schema_health == "degraded"
    assert main_module.app.state.schema_untagged_nodes == 42
    assert "42" in (main_module.app.state.schema_degraded_reason or ""), (
        f"Expected the untagged count in degraded_reason, got: "
        f"{main_module.app.state.schema_degraded_reason!r}"
    )


async def test_lifespan_does_not_raise_on_clean_graph() -> None:
    """On a fully-migrated graph (untagged count == 0, no constraint
    conflict), startup does NOT raise and schema_health reports "healthy"."""
    mock_driver = _patched_lifespan_deps()
    with (
        patch("context_intelligence_server.main.setup_logging"),
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ),
        patch(
            "context_intelligence_server.main.ensure_neo4j_schema",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "context_intelligence_server.main.count_untagged_nodes",
            new=AsyncMock(return_value=0),
        ) as mock_count,
    ):
        async with lifespan(main_module.app):
            pass

    mock_count.assert_awaited_once()
    assert main_module.app.state.schema_health == "healthy"
    assert main_module.app.state.schema_degraded_reason is None


async def test_lifespan_reports_unknown_when_health_check_itself_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A health-check probe failure (e.g. count_untagged_nodes raising due to
    a transient connectivity blip, or Neo4j being unreachable at boot) must
    NOT be coerced to "healthy" (B3) -- schema_health reports "unknown" and
    boot proceeds regardless. This is the exact ACA cold-start race the
    deploy-safe boot fix targets: unreachable-then-reachable must never
    crash-loop."""
    mock_driver = _patched_lifespan_deps()
    with (
        patch("context_intelligence_server.main.setup_logging"),
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ),
        patch(
            "context_intelligence_server.main.ensure_neo4j_schema",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "context_intelligence_server.main.count_untagged_nodes",
            new=AsyncMock(side_effect=RuntimeError("transient connectivity blip")),
        ),
        caplog.at_level(logging.WARNING),
    ):
        async with lifespan(main_module.app):  # MUST NOT raise
            pass

    assert main_module.app.state.schema_health == "unknown"
    assert main_module.app.state.schema_untagged_nodes is None
    assert any(
        "migration-health probe failed" in record.getMessage()
        for record in caplog.records
    ), "Expected a WARNING logging the probe failure."


async def test_lifespan_recovery_quarantines_one_bad_session_and_continues(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """B6: a single session whose recovery raises (a corrupt per-session
    .offset/dead-letter) is quarantined -- logged and skipped -- while other
    recovered sessions still respawn and boot still completes."""
    sid_bad = "sess-corrupt"
    sid_good = "sess-good"
    qm = registry.queue_manager
    # Both lines parse fine at the JSON level (a malformed/torn line is
    # already handled gracefully by _recover_one_session's own internal
    # try/except -- see test_lifespan_skips_recovery_for_empty_workspace).
    # B6's NEW defensive guard covers failures that surface only once
    # recovery actually tries to respawn the drainer for that session (e.g.
    # a corrupt on-disk queue file the registry discovers at get_or_create
    # time) -- simulated here via a flaky get_or_create.
    bad_body = json.dumps(
        {
            "event": "tool_use",
            "workspace": "/bad-ws",
            "data": {"session_id": sid_bad},
        }
    ).encode("utf-8")
    good_body = json.dumps(
        {
            "event": "tool_use",
            "workspace": "/good-ws",
            "data": {"session_id": sid_good},
        }
    ).encode("utf-8")
    await qm.append(sid_bad, bad_body)
    await qm.append(sid_good, good_body)

    spawned: list[tuple] = []

    def _flaky_get_or_create(sid: str, workspace: str, **kw: object) -> None:
        if sid == sid_bad:
            raise RuntimeError("simulated corrupt per-session recovery failure")
        spawned.append((sid, workspace))

    monkeypatch.setattr(registry, "get_or_create", _flaky_get_or_create)

    mock_driver = _patched_lifespan_deps()
    with (
        patch("context_intelligence_server.main.setup_logging"),
        patch(
            "context_intelligence_server.main.AsyncGraphDatabase.driver",
            return_value=mock_driver,
        ),
        patch(
            "context_intelligence_server.main.ensure_neo4j_schema",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "context_intelligence_server.main.count_untagged_nodes",
            new=AsyncMock(return_value=0),
        ),
        caplog.at_level(logging.ERROR),
    ):
        async with lifespan(main_module.app):  # MUST NOT raise
            pass

    assert (sid_good, "/good-ws") in spawned, (
        "The healthy session must still be recovered despite the other "
        "session's recovery failing."
    )
    assert any(
        "recovery_session_quarantined" in record.getMessage()
        and sid_bad in record.getMessage()
        for record in caplog.records
    ), "Expected the corrupt session to be logged as quarantined."
    # Boot still reaches a healthy schema state -- the quarantine did not
    # propagate into (or get masked by) the B1 boundary.
    assert main_module.app.state.schema_health == "healthy"


async def test_status_exposes_schema_health_fields(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /status surfaces schema_health, untagged_nodes, schema_checked_at,
    and degraded_reason.

    WS-3a DE-LATCHES schema_health/schema_checked_at/degraded_reason: none of
    these are read verbatim from the app.state boot snapshot anymore --
    schema_health and degraded_reason are now both derived live from the
    MaintenanceCoordinator's TTL-cached constraint probe (the same probe/
    reason the 503 body reuses), and schema_checked_at is the live-probe
    timestamp rather than the boot-time one. Only untagged_nodes remains the
    boot-time value (documented as such; explicitly not a gate input).

    This pins the fix for the stale-signal bug: `degraded_reason` used to be
    read verbatim from the app.state boot snapshot, so it kept asserting a
    stale constraint-absent condition even after a live repair de-latched
    `mode`/`schema_health`. Here the boot-time `schema_degraded_reason` is
    deliberately set to a DIFFERENT sentence than the live coordinator's
    `reason` -- the assertion below only passes if degraded_reason is
    sourced from the live value, not the stale boot snapshot.
    """
    from context_intelligence_server.maintenance import MaintenanceStatus, OpRecord

    main_module.app.state.schema_untagged_nodes = 3
    # Deliberately a DIFFERENT sentence than the live coordinator reason
    # below, so the test fails if degraded_reason ever regresses back to
    # reading the boot-time snapshot.
    main_module.app.state.schema_degraded_reason = "STALE boot-time reason"
    monkeypatch.setattr(
        main_module.coordinator,
        "status",
        AsyncMock(
            return_value=MaintenanceStatus(
                mode="degraded",
                constraint_present=False,
                reason=":Node uniqueness constraint absent -- migration required",
                started_at=None,
                elapsed_seconds=None,
                op=OpRecord(
                    state="unknown",
                    run_id=None,
                    started_at=None,
                    completed_at=None,
                    records_affected=None,
                    error=None,
                ),
            )
        ),
    )
    try:
        response = await client.get("/status")
        data = response.json()
        assert data["schema_health"] == "degraded"
        assert data["untagged_nodes"] == 3  # boot-time value, unchanged
        assert data["schema_checked_at"] is not None  # live probe timestamp now
        # LIVE coordinator reason, NOT the stale boot-time snapshot.
        assert data["degraded_reason"] == (
            ":Node uniqueness constraint absent -- migration required"
        )
    finally:
        # Reset so this test doesn't leak state into siblings sharing the
        # module-level app singleton.
        main_module.app.state.schema_untagged_nodes = None
        main_module.app.state.schema_degraded_reason = None


# ---------------------------------------------------------------------------
# W-4: GET /status advisory graph_schema_version / schema_version_current
# ---------------------------------------------------------------------------


class _SchemaMetaResult:
    """Async-iterable result double yielding a fixed list of row dicts.

    Mirrors ``_RowsResult`` in ``tests/test_neo4j_store.py`` -- exercises the
    ``async for record in result`` path ``read_graph_schema_version`` uses
    (not ``.single()``), so this double alone is sufficient.
    """

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def __aiter__(self) -> Any:
        return self._agen()

    async def _agen(self) -> Any:
        for row in self._rows:
            yield row


class _SchemaMetaDriverStub:
    """Driver double for ``read_graph_schema_version``: doubles as its own
    session context manager and returns a canned ``:SchemaMeta`` row (or no
    rows at all, simulating an absent/uninitialized singleton).
    """

    def __init__(self, schema_version: int | None) -> None:
        self._schema_version = schema_version

    def session(self, database: str = "neo4j") -> Self:
        return self

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def run(self, statement: str) -> _SchemaMetaResult:
        rows: list[dict[str, Any]] = (
            []
            if self._schema_version is None
            else [{"schema_version": self._schema_version}]
        )
        return _SchemaMetaResult(rows)


async def test_status_exposes_graph_schema_version_when_current(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /status surfaces graph_schema_version sourced from the STORED
    :SchemaMeta singleton, and schema_version_current: true when it matches
    the server's compiled-in SCHEMA_VERSION (advisory only -- no gating)."""
    from context_intelligence_server.status import SCHEMA_VERSION

    monkeypatch.setattr(
        main_module.app.state,
        "neo4j_driver",
        _SchemaMetaDriverStub(SCHEMA_VERSION),
        raising=False,
    )

    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert data["graph_schema_version"] == SCHEMA_VERSION
    assert data["schema_version_current"] is True


async def test_status_exposes_graph_schema_version_mismatch_detectable(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """W-4: a stored schema_version DIFFERENT from the compiled-in
    SCHEMA_VERSION is reflected verbatim on /status (detectable drift) and
    schema_version_current flips to false -- still purely advisory, no
    behavior change results from the mismatch."""
    from context_intelligence_server.status import SCHEMA_VERSION

    stored_version = SCHEMA_VERSION - 1 if SCHEMA_VERSION > 0 else SCHEMA_VERSION + 1
    assert stored_version != SCHEMA_VERSION  # sanity: the whole point of this test
    monkeypatch.setattr(
        main_module.app.state,
        "neo4j_driver",
        _SchemaMetaDriverStub(stored_version),
        raising=False,
    )

    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert data["graph_schema_version"] == stored_version
    assert data["schema_version_current"] is False


async def test_status_graph_schema_version_none_when_singleton_absent(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bootstrap case: no :SchemaMeta singleton yet (server never completed
    startup against this graph) -> graph_schema_version and
    schema_version_current are both None, never an error / 500."""
    monkeypatch.setattr(
        main_module.app.state,
        "neo4j_driver",
        _SchemaMetaDriverStub(None),
        raising=False,
    )

    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert data["graph_schema_version"] is None
    assert data["schema_version_current"] is None


async def test_status_graph_schema_version_none_when_no_driver(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defensive: /status must never 500 when neo4j_driver is unset."""
    if hasattr(main_module.app.state, "neo4j_driver"):
        monkeypatch.delattr(main_module.app.state, "neo4j_driver", raising=False)

    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert data["graph_schema_version"] is None
    assert data["schema_version_current"] is None


async def test_status_degraded_reason_self_clears_after_live_repair_no_restart(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE STALE-SIGNAL BUG, DIRECTLY: after an in-server repair (e.g.
    `POST /admin/maintenance` recreating the :Node uniqueness constraint),
    `degraded_reason` must update to reflect the new live state -- WITHOUT a
    restart -- exactly like `mode`/`schema_health` already do (WS-3a).

    Before the fix, `degraded_reason` was read verbatim from the app.state
    boot snapshot, which is only ever written once during `lifespan`. A
    live repair flips the coordinator's probe (and thus `mode`), but the
    boot snapshot never changes for the life of the process -- so
    `degraded_reason` kept asserting a constraint-absent condition that was
    now false. Here the SAME running app (no restart, no re-entering
    `lifespan`) observes the coordinator's probe result change between two
    successive `GET /status` calls and the reason string tracks it.
    """
    from context_intelligence_server.maintenance import MaintenanceStatus, OpRecord

    def _status(
        *, constraint_present: bool | None, reason: str | None, mode: str
    ) -> MaintenanceStatus:
        return MaintenanceStatus(
            mode=mode,  # type: ignore[arg-type]
            constraint_present=constraint_present,
            reason=reason,
            started_at=None,
            elapsed_seconds=None,
            op=OpRecord(
                state="unknown",
                run_id=None,
                started_at=None,
                completed_at=None,
                records_affected=None,
                error=None,
            ),
        )

    # Boot-time snapshot: set ONCE, never touched again in this test --
    # simulating the real lifespan-populated app.state that a stale
    # implementation would keep reading forever.
    main_module.app.state.schema_degraded_reason = (
        ":Node uniqueness constraint absent (data conflict)"
    )
    try:
        # --- Before repair: constraint absent, live-degraded. ---
        monkeypatch.setattr(
            main_module.coordinator,
            "status",
            AsyncMock(
                return_value=_status(
                    constraint_present=False,
                    reason=(
                        ":Node uniqueness constraint absent -- migration required"
                    ),
                    mode="maintenance",
                )
            ),
        )
        before = (await client.get("/status")).json()
        assert before["degraded_reason"] == (
            ":Node uniqueness constraint absent -- migration required"
        )

        # --- Repair happens out-of-band (no restart of this process): the
        # constraint is recreated, the coordinator's live probe now reports
        # present/healthy. Re-point the SAME coordinator's `status` method --
        # nothing about app.state.schema_degraded_reason changes.
        monkeypatch.setattr(
            main_module.coordinator,
            "status",
            AsyncMock(
                return_value=_status(
                    constraint_present=True,
                    reason=None,
                    mode="healthy",
                )
            ),
        )
        after = (await client.get("/status")).json()

        # THE FIX: degraded_reason clears to null, tracking the live probe --
        # it does NOT still say "constraint absent" (the stale boot value).
        assert after["degraded_reason"] is None, (
            f"degraded_reason must clear once the live probe reports the "
            f"constraint present again; got stale value: "
            f"{after['degraded_reason']!r}"
        )
        # Non-vacuity: prove the two calls actually observed different
        # coordinator states (otherwise this test would trivially pass).
        assert before["degraded_reason"] != after["degraded_reason"]
        # And prove it never degenerated into the OLD stale-boot-snapshot
        # behavior, which would have returned this sentence unchanged on
        # both calls.
        assert after["degraded_reason"] != (
            main_module.app.state.schema_degraded_reason
        )
    finally:
        main_module.app.state.schema_degraded_reason = None


async def test_status_schema_health_defaults_to_unknown_when_unset(
    client: httpx.AsyncClient,
) -> None:
    """Before lifespan has run (or if app.state was never populated), GET
    /status must report schema_health="unknown", never a false "healthy"
    (B3: a probe that hasn't run yet is not the same as a clean graph).

    degraded_reason is now sourced LIVE from the coordinator (see the
    stale-signal fix), so with no driver bound the coordinator's own probe
    reports "unknown" WITH a reason explaining why (unlike the old
    boot-snapshot-only field, which could be a bare None here purely because
    the app.state attributes were deleted, never reflecting the true
    tri-state semantics)."""
    for attr in (
        "schema_health",
        "schema_untagged_nodes",
        "schema_checked_at",
        "schema_degraded_reason",
    ):
        if hasattr(main_module.app.state, attr):
            delattr(main_module.app.state, attr)

    response = await client.get("/status")
    data = response.json()
    assert data["schema_health"] == "unknown"
    assert data["untagged_nodes"] is None
    assert data["degraded_reason"] is not None
    assert "could not determine" in data["degraded_reason"]


async def test_registry_exposed_on_app_state() -> None:
    """The module registry singleton is exposed on app.state for routers.

    Routers read the singleton via request.app.state.registry rather than
    importing the module-level name (avoids a circular import).
    """
    assert main_module.app.state.registry is main_module.registry


async def test_lifespan_seeds_counters_from_disk(tmp_path: Path) -> None:
    """A fresh registry reusing a queue dir seeds conservation counters to a
    zero residual: 1 committed + 1 pending line yields accepted=2, written=1,
    in_queue=1, residual=0 after reconcile -> seed_counts -> seed_counters.
    """
    from context_intelligence_server.queue_manager import QueueManager
    from context_intelligence_server.registry import SessionRegistry

    # Seed a queue dir with one committed line and one still-pending line.
    seed_qm = QueueManager(queues_dir=tmp_path)
    sid = "sess-seed"
    line1 = json.dumps({"event": "a", "workspace": "/ws", "data": {}}).encode("utf-8")
    line2 = json.dumps({"event": "b", "workspace": "/ws", "data": {}}).encode("utf-8")
    await seed_qm.append(sid, line1)
    await seed_qm.append(sid, line2)
    committed = len(line1) + 1  # +1 for the appended trailing newline
    await seed_qm.commit(sid, committed, None)

    # Fresh registry reusing the same on-disk queue dir.
    reg = SessionRegistry()
    reg._queue_manager = QueueManager(queues_dir=tmp_path)

    # Production order: reconcile dead lines BEFORE seeding the counts.
    await reg.queue_manager.recovery_reconcile_dead()
    accepted_seed, written_seed = await reg.queue_manager.recovery_seed_counts()
    reg.seed_counters(accepted_seed, written_seed)

    metrics = await reg.pipeline_metrics()
    assert metrics["accepted_total"] == 2
    assert metrics["written_total"] == 1
    assert metrics["in_queue_total"] == 1
    assert metrics["residual"] == 0


def test_validate_single_worker_trips_on_effective_web_concurrency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_validate_single_worker fails loud when effective WEB_CONCURRENCY != 1."""
    monkeypatch.setenv("WEB_CONCURRENCY", "4")
    with pytest.raises((RuntimeError, SystemExit)):
        main_module._validate_single_worker()


def test_validate_single_worker_passes_when_effective_is_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_validate_single_worker returns 1 when WEB_CONCURRENCY is unset."""
    monkeypatch.delenv("WEB_CONCURRENCY", raising=False)
    assert main_module._validate_single_worker() == 1


# ---------------------------------------------------------------------------
# /status neo4j_connected field tests
# ---------------------------------------------------------------------------


async def test_status_includes_neo4j_connected_true(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """/status includes neo4j_connected: true when driver.verify_connectivity() succeeds."""
    mock_driver = AsyncMock()
    mock_driver.verify_connectivity = AsyncMock(return_value=None)
    monkeypatch.setattr(
        main_module.app.state, "neo4j_driver", mock_driver, raising=False
    )

    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert data["neo4j_connected"] is True


async def test_status_includes_neo4j_connected_false_on_error(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """/status includes neo4j_connected: false when driver.verify_connectivity() raises."""
    mock_driver = AsyncMock()
    mock_driver.verify_connectivity = AsyncMock(
        side_effect=Exception("connection refused")
    )
    monkeypatch.setattr(
        main_module.app.state, "neo4j_driver", mock_driver, raising=False
    )

    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert data["neo4j_connected"] is False


async def test_status_includes_neo4j_connected_false_when_no_driver(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """/status includes neo4j_connected: false when neo4j_driver is not set on app.state."""
    if hasattr(main_module.app.state, "neo4j_driver"):
        monkeypatch.delattr(main_module.app.state, "neo4j_driver", raising=False)

    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert data["neo4j_connected"] is False


# ---------------------------------------------------------------------------
# Concern B (council review) -- /status neo4j_query_connected field tests
# ---------------------------------------------------------------------------


async def test_status_includes_neo4j_query_connected_true(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """/status additively includes neo4j_query_connected: true when the query
    (read-intent) driver's verify_connectivity() succeeds."""
    mock_driver = AsyncMock()
    mock_driver.verify_connectivity = AsyncMock(return_value=None)
    monkeypatch.setattr(
        main_module.app.state, "neo4j_query_driver", mock_driver, raising=False
    )

    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert data["neo4j_query_connected"] is True
    # Additive: existing field is untouched by this test's monkeypatch.
    assert "neo4j_connected" in data


async def test_status_includes_neo4j_query_connected_false_on_error(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """/status includes neo4j_query_connected: false when the query driver's
    verify_connectivity() raises -- a misconfigured cypher_query client must
    surface here, not just on the first /cypher call."""
    mock_driver = AsyncMock()
    mock_driver.verify_connectivity = AsyncMock(
        side_effect=Exception("connection refused")
    )
    monkeypatch.setattr(
        main_module.app.state, "neo4j_query_driver", mock_driver, raising=False
    )

    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert data["neo4j_query_connected"] is False


async def test_status_includes_neo4j_query_connected_false_when_no_driver(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """/status includes neo4j_query_connected: false when neo4j_query_driver is
    not set on app.state (defensive -- must never 500)."""
    if hasattr(main_module.app.state, "neo4j_query_driver"):
        monkeypatch.delattr(main_module.app.state, "neo4j_query_driver", raising=False)

    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert data["neo4j_query_connected"] is False


# ---------------------------------------------------------------------------
# /status pipeline metrics block (D3)
# ---------------------------------------------------------------------------


async def test_status_includes_metrics_block(client: httpx.AsyncClient) -> None:
    """/status carries the additive aggregate-only pipeline metrics block.

    The block is additive (existing status keys remain) and aggregate-only:
    because /status is unauthenticated it must NOT expose the per-key table,
    the dead-letter listing, or the deferred oldest_unflushed_age signal.
    """
    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()

    # Additive: existing status keys still present.
    assert "active_sessions" in data
    assert "sessions" in data

    # Aggregate metrics block present with the conservation fields.
    metrics = data["metrics"]
    for key in (
        "accepted_total",
        "written_total",
        "replayed_total",
        "write_retries_total",
        "in_queue_total",
        "dead_letter_total",
        "residual",
        "degraded",
    ):
        assert key in metrics

    # Deferred / authenticated-only fields must be absent.
    assert "oldest_unflushed_age" not in metrics
    assert "per_key" not in metrics
    assert "dead_letters" not in data


# ---------------------------------------------------------------------------
# /status spool block (Change 2): pending_sessions + spool_bytes_total
# ---------------------------------------------------------------------------


async def test_status_includes_spool_block(client: httpx.AsyncClient) -> None:
    """/status carries an additive, aggregate-only spool block so a growing
    on-disk backlog is never invisible (the 38 GB / two-day incident this
    guards against had zero signal anywhere)."""
    response = await client.get("/status")
    assert response.status_code == 200
    data = response.json()

    assert "spool" in data
    spool = data["spool"]
    assert set(spool.keys()) == {
        "pending_sessions",
        "spool_bytes_total",
        "corrupt_offsets",
    }
    assert isinstance(spool["pending_sessions"], int)
    assert isinstance(spool["spool_bytes_total"], int)
    assert isinstance(spool["corrupt_offsets"], int)


async def test_status_spool_block_reflects_real_backlog(
    client: httpx.AsyncClient,
) -> None:
    """The spool block's numbers move when there's real undrained data on
    disk -- not a hardcoded placeholder."""
    qm = registry.queue_manager
    body = json.dumps(
        {
            "event": "tool_use",
            "workspace": "/ws",
            "data": {"session_id": "sess-spool-visible"},
        }
    ).encode("utf-8")
    await qm.append("sess-spool-visible", body)
    # get_or_create is bypassed here (raw append only) so this line stays
    # undrained -- exactly the "pending" shape spool_stats() measures.

    response = await client.get("/status")
    data = response.json()

    assert data["spool"]["pending_sessions"] >= 1
    assert data["spool"]["spool_bytes_total"] > 0


async def test_status_spool_block_never_leaks_session_identifiers(
    client: httpx.AsyncClient,
) -> None:
    """/status is unauthenticated: the spool block must never carry a
    session id, workspace name, or any per-key table (aggregate-only)."""
    qm = registry.queue_manager
    secret_sid = "super-secret-session-id-should-not-leak"
    body = json.dumps(
        {
            "event": "tool_use",
            "workspace": "/very/private/workspace",
            "data": {"session_id": secret_sid},
        }
    ).encode("utf-8")
    await qm.append(secret_sid, body)

    response = await client.get("/status")
    raw_text = response.text

    assert secret_sid not in raw_text
    assert "/very/private/workspace" not in raw_text
    assert "per_key" not in response.json()["spool"]


async def test_status_corrupt_offset_returns_200_and_surfaces_count(
    client: httpx.AsyncClient,
) -> None:
    """Regression (ci_pr73-267): a corrupt .offset previously 500'd /status via
    derive_all_stats() (which runs before spool_stats in get_status). /status
    must now stay 200 AND surface the corruption as spool.corrupt_offsets -- the
    only signal (no logging, so the polled health path is never flooded)."""
    qm = registry.queue_manager
    await qm.append("sess-corrupt-offset", b"a")
    qm._offset_path("sess-corrupt-offset").write_text("not-a-number", encoding="utf-8")
    qm._spool_cache = None  # bypass TTL cache so the corruption is seen now

    response = await client.get("/status")

    assert response.status_code == 200  # was 500 before the fix
    assert response.json()["spool"]["corrupt_offsets"] >= 1


async def test_status_returns_200_when_spool_dir_unavailable(
    client: httpx.AsyncClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression (ci_pr73-ueh): /status is the ACA health probe. spool_stats()
    scans the queue dir with iterdir() (raises on a missing dir), unlike the
    glob()-based sibling readers. A transiently-unavailable queue dir (e.g. an
    Azure Files SMB remount) must NOT turn /status into a 500 -> failed probe
    -> container restart loop. It must return 200 with a degraded sentinel."""
    qm = registry.queue_manager
    # Point the scan at a directory that does not exist so iterdir() raises,
    # exactly as it would during an SMB mount drop. Bypass the TTL cache so the
    # scan actually runs on this call.
    monkeypatch.setattr(qm, "_dir", tmp_path / "gone")
    monkeypatch.setattr(qm, "_spool_cache", None)

    response = await client.get("/status")

    assert response.status_code == 200
    assert response.json()["spool"] == {
        "pending_sessions": -1,
        "spool_bytes_total": -1,
        "corrupt_offsets": -1,
    }


# ---------------------------------------------------------------------------
# /queues/* data endpoints stay authenticated (C1; survives the C2 page removal)
# ---------------------------------------------------------------------------


async def test_queues_data_endpoint_still_requires_auth(
    auth_client: httpx.AsyncClient,
) -> None:
    """The /queues/* data endpoints stay protected — exempting the page must
    NOT exempt the data routes."""
    response = await auth_client.get("/queues/dead-letter")
    assert response.status_code == 401


# ---------------------------------------------------------------------------
# T12-T15: per-user API keys — post_events stamping and crash recovery
# ---------------------------------------------------------------------------


async def test_post_events_stamps_contributor_id_from_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T12: post_events stamps created_by from scope state into the queued body."""
    import hashlib

    import httpx

    from context_intelligence_server.main import asgi_app

    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *args, **kwargs: MagicMock()
    )
    captured: list[bytes] = []

    async def _fake_append(worker_key: str, raw: bytes) -> None:
        captured.append(raw)

    monkeypatch.setattr(main_module.registry.queue_manager, "append", _fake_append)

    # Temporarily configure a StaticKeyResolver on asgi_app so auth injects contributor_id.
    # T2: middleware now uses resolver= seam; patch asgi_app.resolver, not asgi_app.keystore.
    from context_intelligence_server.auth import StaticKeyResolver  # noqa: PLC0415

    test_token = "test-secret"
    test_keystore = {hashlib.sha256(test_token.encode()).hexdigest(): "alice"}
    monkeypatch.setattr(asgi_app, "resolver", StaticKeyResolver(test_keystore))

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app),
        base_url="http://test",
    ) as ac:
        response = await ac.post(
            "/events",
            json={
                "event": "tool_use",
                "workspace": "/ws",
                "data": {
                    "session_id": "s1",
                    "timestamp": "2026-06-16T20:17:11.604690+00:00",
                },
            },
            headers={"Authorization": f"Bearer {test_token}"},
        )

    assert response.status_code == 202
    assert len(captured) == 1
    body_obj = json.loads(captured[0])
    assert body_obj["created_by"] == "alice"


async def test_post_events_overwrites_client_supplied_created_by(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T13: post_events overwrites any client-supplied created_by (anti-spoof)."""
    import hashlib

    import httpx

    from context_intelligence_server.main import asgi_app

    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *args, **kwargs: MagicMock()
    )
    captured: list[bytes] = []

    async def _fake_append(worker_key: str, raw: bytes) -> None:
        captured.append(raw)

    monkeypatch.setattr(main_module.registry.queue_manager, "append", _fake_append)

    # T2: middleware now uses resolver= seam; patch asgi_app.resolver, not asgi_app.keystore.
    from context_intelligence_server.auth import StaticKeyResolver  # noqa: PLC0415

    test_token = "test-secret"
    test_keystore = {hashlib.sha256(test_token.encode()).hexdigest(): "real-owner"}
    monkeypatch.setattr(asgi_app, "resolver", StaticKeyResolver(test_keystore))

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app),
        base_url="http://test",
    ) as ac:
        response = await ac.post(
            "/events",
            json={
                "event": "tool_use",
                "workspace": "/ws",
                "data": {
                    "session_id": "s2",
                    "timestamp": "2026-06-16T20:17:11.604690+00:00",
                },
                "created_by": "hacker",  # client-supplied spoofed value
            },
            headers={"Authorization": f"Bearer {test_token}"},
        )

    assert response.status_code == 202
    assert len(captured) == 1
    body_obj = json.loads(captured[0])
    # Server wins — "hacker" must be replaced by the authenticated id
    assert body_obj["created_by"] == "real-owner"
    assert body_obj["created_by"] != "hacker"


async def test_post_events_stamps_none_when_no_auth(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T14: post_events stamps created_by=None when auth is not configured."""
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *args, **kwargs: MagicMock()
    )
    captured: list[bytes] = []

    async def _fake_append(worker_key: str, raw: bytes) -> None:
        captured.append(raw)

    monkeypatch.setattr(main_module.registry.queue_manager, "append", _fake_append)

    response = await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws",
            "data": {
                "session_id": "s3",
                "timestamp": "2026-06-16T20:17:11.604690+00:00",
            },
        },
    )

    assert response.status_code == 202
    assert len(captured) == 1
    body_obj = json.loads(captured[0])
    # No auth configured: created_by is present with null (None) value
    assert "created_by" in body_obj
    assert body_obj["created_by"] is None


async def test_post_events_lifts_working_dir_into_data(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """I1: a top-level working_dir envelope field is lifted into data.working_dir."""
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *args, **kwargs: MagicMock()
    )
    captured: list[bytes] = []

    async def _fake_append(worker_key: str, raw: bytes) -> None:
        captured.append(raw)

    monkeypatch.setattr(main_module.registry.queue_manager, "append", _fake_append)

    response = await client.post(
        "/events",
        json={
            "event": "session:start",
            "workspace": "/ws",
            "working_dir": "/home/user/my-project",
            "data": {
                "session_id": "s4",
                "timestamp": "2026-06-16T20:17:11.604690+00:00",
            },
        },
    )

    assert response.status_code == 202
    assert len(captured) == 1
    body_obj = json.loads(captured[0])
    assert body_obj["data"]["working_dir"] == "/home/user/my-project"


async def test_post_events_absent_working_dir_leaves_data_unset(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """I1: omitting working_dir must not add the key to data at all (forward-only)."""
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *args, **kwargs: MagicMock()
    )
    captured: list[bytes] = []

    async def _fake_append(worker_key: str, raw: bytes) -> None:
        captured.append(raw)

    monkeypatch.setattr(main_module.registry.queue_manager, "append", _fake_append)

    response = await client.post(
        "/events",
        json={
            "event": "session:start",
            "workspace": "/ws",
            "data": {
                "session_id": "s5",
                "timestamp": "2026-06-16T20:17:11.604690+00:00",
            },
        },
    )

    assert response.status_code == 202
    assert len(captured) == 1
    body_obj = json.loads(captured[0])
    assert "working_dir" not in body_obj["data"]


async def test_crash_recovery_passes_created_by_to_get_or_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T15: crash recovery reads created_by from first queued line and passes it to get_or_create.

    Exercises the REAL ``_recover_one_session()`` from main.py — not an inline
    reimplementation.  The function accepts ``str | bytes`` so no QueueManager
    plumbing is required here; the queue-read step is the caller's responsibility
    (tested end-to-end by ``test_lifespan_recovers_and_respawns_drainers``).
    """
    import context_intelligence_server.main as _main_mod

    sid = "recovery-session-t15"
    first_line = json.dumps(
        {
            "event": "session:start",
            "workspace": "/test-workspace",
            "created_by": "recovered-contributor",
            "data": {"session_id": sid},
        },
        separators=(",", ":"),
    )

    calls: list[dict] = []

    def _fake_get_or_create(
        session_id: str,
        workspace: str,
        created_by: str | None = None,
    ) -> MagicMock:
        calls.append(
            {"session_id": session_id, "workspace": workspace, "created_by": created_by}
        )
        return MagicMock()

    # Call the REAL recovery function — not a reimplementation of its logic.
    result = _main_mod._recover_one_session(sid, first_line, _fake_get_or_create)

    assert result is True
    assert len(calls) == 1
    assert calls[0]["session_id"] == sid
    assert calls[0]["created_by"] == "recovered-contributor"
    assert calls[0]["workspace"] == "/test-workspace"


@pytest.mark.parametrize(
    "bad_line",
    [
        '{"event":"session:start","workspace":"/ws","crea',  # truncated JSON
        "} totally invalid json {",  # pure garbage
        "",  # empty line
    ],
    ids=["truncated-json", "garbage", "empty"],
)
async def test_crash_recovery_truncated_line_safe_skip(bad_line: str) -> None:
    """T15b: crash recovery handles a truncated/garbage JSONL line deterministically.

    A queue whose final line is truncated or garbage must be safe-skipped — the
    recovery must not raise and must not spawn a drainer for the bad line.

    Exercises the REAL ``_recover_one_session()`` from main.py.
    """
    import context_intelligence_server.main as _main_mod

    sid = "recovery-session-t15b"
    calls: list[dict] = []

    def _fake_get_or_create(
        session_id: str,
        workspace: str,
        created_by: str | None = None,
    ) -> MagicMock:
        calls.append(
            {"session_id": session_id, "workspace": workspace, "created_by": created_by}
        )
        return MagicMock()

    # Must not raise — bad JSON triggers ValueError which is caught internally.
    result = _main_mod._recover_one_session(sid, bad_line, _fake_get_or_create)

    assert result is False, (
        f"Bad/truncated line must be safe-skipped (return False), got {result!r}"
    )
    assert calls == [], "No drainer must be spawned for a bad/truncated line"


# ---------------------------------------------------------------------------
# data.timestamp ingest validation (Option A: fail at boundary with HTTP 400)
# ---------------------------------------------------------------------------


async def test_post_events_data_timestamp_missing_returns_400(
    client: httpx.AsyncClient,
) -> None:
    """POST /events with data lacking timestamp returns 400, not 202 then silent dead-letter."""
    response = await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws",
            "data": {"session_id": "sess-ts-missing"},
        },
    )
    assert response.status_code == 400
    assert "data.timestamp" in response.json()["detail"]


async def test_post_events_data_timestamp_empty_returns_400(
    client: httpx.AsyncClient,
) -> None:
    """POST /events with data.timestamp == '' returns 400."""
    response = await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws",
            "data": {"session_id": "sess-ts-empty", "timestamp": ""},
        },
    )
    assert response.status_code == 400
    assert "data.timestamp" in response.json()["detail"]


async def test_post_events_data_timestamp_whitespace_returns_400(
    client: httpx.AsyncClient,
) -> None:
    """POST /events with data.timestamp == whitespace-only returns 400."""
    response = await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws",
            "data": {"session_id": "sess-ts-ws", "timestamp": "   "},
        },
    )
    assert response.status_code == 400
    assert "data.timestamp" in response.json()["detail"]


async def test_post_events_data_timestamp_non_string_returns_400(
    client: httpx.AsyncClient,
) -> None:
    """POST /events with data.timestamp that is not a string returns 400."""
    response = await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws",
            "data": {"session_id": "sess-ts-int", "timestamp": 12345},
        },
    )
    assert response.status_code == 400
    assert "data.timestamp" in response.json()["detail"]


async def test_post_events_data_timestamp_invalid_iso_returns_400(
    client: httpx.AsyncClient,
) -> None:
    """POST /events with data.timestamp that is not valid ISO-8601 returns 400."""
    response = await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws",
            "data": {"session_id": "sess-ts-bad", "timestamp": "not-a-date"},
        },
    )
    assert response.status_code == 400
    assert "data.timestamp" in response.json()["detail"]


async def test_post_events_data_timestamp_invalid_detail_names_value(
    client: httpx.AsyncClient,
) -> None:
    """400 detail for an invalid ISO timestamp includes the bad value for easy debugging."""
    response = await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws",
            "data": {"session_id": "sess-ts-detail", "timestamp": "2026-99-99"},
        },
    )
    assert response.status_code == 400
    # detail must name the bad value so operator knows what to fix
    assert "2026-99-99" in response.json()["detail"]


async def test_post_events_valid_iso_timestamp_returns_202(
    client: httpx.AsyncClient,
) -> None:
    """POST /events with a valid ISO-8601 data.timestamp returns 202 (happy path unchanged)."""
    response = await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws",
            "data": {
                "session_id": "sess-ts-ok",
                "timestamp": "2026-06-16T20:17:11.604690+00:00",
            },
        },
    )
    assert response.status_code == 202


async def test_post_events_valid_utc_z_timestamp_returns_202(
    client: httpx.AsyncClient,
) -> None:
    """POST /events with Z-suffix UTC timestamp (real Amplifier format) returns 202."""
    response = await client.post(
        "/events",
        json={
            "event": "session:start",
            "workspace": "/ws",
            "data": {
                "session_id": "sess-ts-utcz",
                "timestamp": "2026-06-16T20:17:11.604690106+00:00",
            },
        },
    )
    assert response.status_code == 202


# ---------------------------------------------------------------------------
# Headless OpenAPI/Swagger surface: end-to-end, against a booted app
# ---------------------------------------------------------------------------
#
# These replace the coverage lost when tests/test_web_ui_switch.py was deleted.
# The prior test asserted the "/openapi.json bypass" behaviour at the mock
# level; this proves the *accepted risk* through the real ASGI app + real
# BearerTokenMiddleware: the doc surface is reachable unauthenticated, but the
# data API it documents is NOT, and the admin surface is not disclosed at all.


class TestHeadlessDocsSurface:
    """/docs + /openapi.json are intentionally unauthenticated; data/admin stay gated."""

    async def test_docs_exempt_but_events_gated_end_to_end(self) -> None:
        """Unauth GET /docs and /openapi.json -> 200; unauth POST /events -> 401.

        Proves the ratified decision end-to-end: the API *shape* is public, but
        no data call can be made without a bearer token.
        """
        async with _auth_client() as c:
            docs = await c.get("/docs")
            openapi = await c.get("/openapi.json")
            events = await c.post(
                "/events",
                json={
                    "event": "tool_use",
                    "workspace": "/ws",
                    "data": {
                        "session_id": "s1",
                        "timestamp": "2026-06-16T20:17:11.604690+00:00",
                    },
                },
            )
        assert docs.status_code == 200
        assert openapi.status_code == 200
        assert events.status_code == 401

    async def test_admin_schema_not_disclosed_in_openapi(self) -> None:
        """/admin/* MUST NOT appear in the unauthenticated OpenAPI schema.

        The admin router is registered with include_in_schema=False so the
        operator-only identity/key surface is not handed to unauthenticated
        recon via /openapi.json (routing + auth are unaffected).
        """
        async with _auth_client() as c:
            openapi = await c.get("/openapi.json")
        assert openapi.status_code == 200
        paths = openapi.json().get("paths", {})
        admin_paths = [p for p in paths if p.startswith("/admin")]
        assert admin_paths == [], (
            f"/admin/* must not appear in the unauthenticated OpenAPI schema; "
            f"found: {admin_paths}"
        )


# ---------------------------------------------------------------------------
# WS-3a: maintenance-mode gate + /status additive fields (spec sec 7a)
# ---------------------------------------------------------------------------


def _maintenance_mode_status() -> Any:
    """Build a MaintenanceStatus reporting mode=="maintenance", for tests
    that force the gate closed without a real Neo4j driver bound."""
    from context_intelligence_server.maintenance import MaintenanceStatus, OpRecord

    return MaintenanceStatus(
        mode="maintenance",
        constraint_present=False,
        reason=":Node uniqueness constraint absent -- migration required",
        started_at="2026-08-13T00:00:00+00:00",
        elapsed_seconds=1.0,
        op=OpRecord(
            state="unknown",
            run_id=None,
            started_at=None,
            completed_at=None,
            records_affected=None,
            error=None,
        ),
    )


class TestMaintenanceGateHttp:
    """A8-A11 (WS-3a spec sec 7a)."""

    async def test_allow_listed_paths_bypass_the_gate(
        self, client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A9: /status, /version, /admin/maintenance are NEVER 503'd by the
        maintenance gate, even while mode == "maintenance"."""
        monkeypatch.setattr(
            main_module.coordinator,
            "status",
            AsyncMock(return_value=_maintenance_mode_status()),
        )
        status_resp = await client.get("/status")
        version_resp = await client.get("/version")
        # /admin/maintenance IS on the allow-list, so the maintenance GATE must
        # never intercept it. The route now EXISTS (WS-3c) and may legitimately
        # return 503 for an UNRELATED reason (no admin_api_key configured in this
        # test app -> "admin API not enabled"), so assert specifically that this
        # is NOT the gate's structured maintenance-503, rather than a blanket
        # != 503. The gate's 503 carries a Retry-After header + a
        # {"status": "maintenance"} body; the admin-auth 503 carries neither.
        admin_maint_resp = await client.post("/admin/maintenance")

        assert status_resp.status_code != 503
        assert version_resp.status_code != 503
        gate_intercepted = (
            admin_maint_resp.status_code == 503
            and admin_maint_resp.headers.get("retry-after") is not None
        )
        assert not gate_intercepted, (
            "maintenance gate must not intercept the allow-listed /admin/maintenance"
        )

    async def test_non_allow_listed_path_is_503d_while_gated(
        self, client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A8 (partial -- unauthenticated path): a non-allow-listed route
        returns the structured 503 (status/reason/retry_after/schema_health/
        maintenance_started_at) with a Retry-After header, while gated."""
        monkeypatch.setattr(
            main_module.coordinator,
            "status",
            AsyncMock(return_value=_maintenance_mode_status()),
        )
        resp = await client.get("/blobs/does-not-matter")
        assert resp.status_code == 503
        assert "Retry-After" in resp.headers
        body = resp.json()
        assert body["status"] == "maintenance"
        assert body["reason"] == (
            ":Node uniqueness constraint absent -- migration required"
        )
        assert body["retry_after"] == int(resp.headers["Retry-After"])
        assert body["schema_health"] == "degraded"
        assert body["maintenance_started_at"] == "2026-08-13T00:00:00+00:00"

    async def test_events_cypher_and_dead_letter_replay_503_while_gated(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A8: all three drainer-spawn-adjacent paths (POST /events, POST
        /cypher, POST /queues/dead-letter/{k}/replay) are gated, through the
        SAME auth-wrapped app real clients use."""
        monkeypatch.setattr(
            main_module.coordinator,
            "status",
            AsyncMock(return_value=_maintenance_mode_status()),
        )
        headers = {"Authorization": "Bearer test-secret"}
        async with _auth_client() as c:
            events_resp = await c.post(
                "/events",
                json={
                    "event": "tool_use",
                    "workspace": "/ws",
                    "data": {
                        "session_id": "s-gated",
                        "timestamp": "2026-08-13T00:00:00+00:00",
                    },
                },
                headers=headers,
            )
            cypher_resp = await c.post(
                "/cypher", json={"query": "MATCH (n) RETURN n"}, headers=headers
            )
            replay_resp = await c.post(
                "/queues/dead-letter/some-key/replay", headers=headers
            )

        for resp in (events_resp, cypher_resp, replay_resp):
            assert resp.status_code == 503
            assert "Retry-After" in resp.headers
            assert resp.json()["status"] == "maintenance"

    async def test_status_exposes_maintenance_mode_fields_additively(
        self, client: httpx.AsyncClient
    ) -> None:
        """A11: /status exposes mode/maintenance_started_at/
        maintenance_elapsed_seconds, and every pre-existing key is still
        present (additive, no regression)."""
        response = await client.get("/status")
        assert response.status_code == 200
        data = response.json()
        for key in (
            "mode",
            "maintenance_started_at",
            "maintenance_elapsed_seconds",
            # pre-existing keys, unchanged:
            "schema_health",
            "untagged_nodes",
            "schema_checked_at",
            "degraded_reason",
            "neo4j_connected",
            "neo4j_query_connected",
            "metrics",
            "auth",
            "queue_health",
        ):
            assert key in data, f"expected pre-existing/additive key {key!r} in /status"
        assert data["mode"] in {"healthy", "maintenance", "degraded", "unknown"}

    def test_maintenance_endpoint_allow_listed_assertion_passes_today(self) -> None:
        """A10 (positive case): the real MAINTENANCE_ALLOW_LIST satisfies the
        startup assertion as shipped."""
        main_module._assert_maintenance_endpoint_allow_listed()

    def test_maintenance_endpoint_allow_listed_assertion_raises_if_removed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A10: if /admin/maintenance were removed from the allow-list, the
        startup assertion raises -- proving the assertion is load-bearing,
        not a no-op (non-vacuity)."""
        monkeypatch.setattr(
            main_module,
            "MAINTENANCE_ALLOW_LIST",
            frozenset({"/status", "/version"}),
        )
        with pytest.raises(RuntimeError, match="MAINTENANCE_ALLOW_LIST"):
            main_module._assert_maintenance_endpoint_allow_listed()


class TestQueueHealthSeparateFromSchemaHealth:
    """A14 (W-2): a queue-recovery exception sets queue_health="degraded" and
    leaves schema_health untouched (a queue fault is not a schema fault)."""

    async def test_queue_recovery_failure_degrades_queue_health_only(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()

        with (
            patch("context_intelligence_server.main.setup_logging"),
            patch(
                "context_intelligence_server.main.AsyncGraphDatabase.driver",
                return_value=mock_driver,
            ),
            patch(
                "context_intelligence_server.main.ensure_neo4j_schema",
                new=AsyncMock(return_value=True),
            ),
            patch(
                "context_intelligence_server.main.count_untagged_nodes",
                new=AsyncMock(return_value=0),
            ),
            patch.object(
                registry.queue_manager,
                "recovery_reconcile_dead",
                AsyncMock(side_effect=RuntimeError("disk corrupt")),
            ),
        ):
            async with lifespan(main_module.app):
                pass

        assert main_module.app.state.queue_health == "degraded"
        # The schema fault signal is UNAFFECTED by the queue fault (the
        # entire point of separating the two try/except boundaries).
        assert main_module.app.state.schema_health == "healthy"

    async def test_queue_recovery_success_leaves_queue_health_healthy(self) -> None:
        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()

        with (
            patch("context_intelligence_server.main.setup_logging"),
            patch(
                "context_intelligence_server.main.AsyncGraphDatabase.driver",
                return_value=mock_driver,
            ),
            patch(
                "context_intelligence_server.main.ensure_neo4j_schema",
                new=AsyncMock(return_value=True),
            ),
            patch(
                "context_intelligence_server.main.count_untagged_nodes",
                new=AsyncMock(return_value=0),
            ),
        ):
            async with lifespan(main_module.app):
                pass

        assert main_module.app.state.queue_health == "healthy"
