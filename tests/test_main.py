"""Tests for FastAPI app — GET /status and POST /events endpoints."""

import asyncio
import contextlib
import json
import socket
from pathlib import Path
from collections.abc import AsyncGenerator
from typing import Any
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


def _neo4j_reachable() -> bool:
    """Return True if Neo4j is reachable at neo4j:7687 (only resolves inside Docker)."""
    try:
        with socket.create_connection(("neo4j", 7687), timeout=1):
            return True
    except OSError:
        return False


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
            "data": {"session_id": "sess-1"},
        },
    )
    assert response.status_code == 202


async def test_post_events_body(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws",
            "data": {"session_id": "sess-1"},
        },
    )
    data = response.json()
    assert data["status"] == "queued"
    assert data["session_id"] == "sess-1"


async def test_post_events_duplicate_idempotency_key_skips_enqueue(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = MagicMock()
    worker.queue = AsyncMock()
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *args, **kwargs: worker
    )

    payload = {
        "event": "tool_use",
        "workspace": "/ws",
        "idempotency_key": "aci-event-v1:test-key",
        "data": {"session_id": "sess-dupe"},
    }

    first = await client.post("/events", json=payload)
    second = await client.post("/events", json=payload)

    assert first.status_code == 202
    assert first.json()["status"] == "queued"
    assert second.status_code == 202
    assert second.json()["status"] == "duplicate"
    assert worker.queue.put.await_count == 1


async def test_post_events_replay_bypasses_idempotency_guard(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = MagicMock()
    worker.queue = AsyncMock()
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *args, **kwargs: worker
    )

    payload = {
        "event": "tool_use",
        "workspace": "/ws",
        "idempotency_key": "aci-event-v1:test-key",
        "data": {"session_id": "sess-replay"},
    }

    first = await client.post("/events", json=payload)
    replay = await client.post("/events?replay=true", json=payload)

    assert first.status_code == 202
    assert replay.status_code == 202
    assert replay.json()["status"] == "queued"
    assert worker.queue.put.await_count == 2


async def test_post_events_increments_active_sessions(
    client: httpx.AsyncClient,
) -> None:
    await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws",
            "data": {"session_id": "sess-inc"},
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
    response = await client.post(
        "/events",
        json={"event": "tool_use", "workspace": "/ws", "data": {}},
    )
    assert response.status_code == 202
    data = response.json()
    assert data["session_id"] is None


@pytest.mark.skipif(not _neo4j_reachable(), reason="Neo4j not reachable at neo4j:7687")
async def test_drain_loop_processes_event(client: httpx.AsyncClient) -> None:
    await client.post(
        "/events",
        json={
            "event": "tool_use",
            "workspace": "/ws",
            "data": {"session_id": "sess-drain"},
        },
    )
    worker = registry.get_or_create("sess-drain", "/ws")
    await asyncio.wait_for(worker.queue.join(), timeout=5.0)
    assert worker.queue.empty()


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
    monkeypatch.setattr(
        main_module.app.state,
        "neo4j_driver",
        MockNeo4jDriver(rows=[mock_row]),
        raising=False,
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
    monkeypatch.setattr(
        main_module.app.state,
        "neo4j_driver",
        MockNeo4jDriver(captured=captured_params),
        raising=False,
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
    monkeypatch.setattr(
        main_module.app.state,
        "neo4j_driver",
        MockNeo4jDriver(captured=captured_params),
        raising=False,
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
    monkeypatch.setattr(
        main_module.app.state,
        "neo4j_driver",
        MockNeo4jDriver(exc=RuntimeError("Connection refused")),
        raising=False,
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
            "data": {"session_id": "sess-detail"},
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
    assert "queue_depth" in sess
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


async def test_dashboard_returns_html(client: httpx.AsyncClient) -> None:
    """GET / returns 200 with HTML landing page containing polling JavaScript."""
    response = await client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    body = response.text
    assert "Context Intelligence" in body
    assert "setInterval" in body


# ---------------------------------------------------------------------------
# Index page Neo4j status indicator tests
# ---------------------------------------------------------------------------


async def test_index_no_neo4j_browser_link(client: httpx.AsyncClient) -> None:
    """GET / must NOT contain a link to localhost:7474 (Neo4j Browser)."""
    response = await client.get("/")
    assert response.status_code == 200
    body = response.text
    assert "localhost:7474" not in body


async def test_index_has_neo4j_status_elements(client: httpx.AsyncClient) -> None:
    """GET / must contain neo4j-status-desc and neo4j-status-badge elements."""
    response = await client.get("/")
    assert response.status_code == 200
    body = response.text
    assert 'id="neo4j-status-desc"' in body
    assert 'id="neo4j-status-badge"' in body


async def test_index_js_reads_neo4j_connected(client: httpx.AsyncClient) -> None:
    """GET / inline JS must read neo4j_connected from status response and update elements."""
    response = await client.get("/")
    assert response.status_code == 200
    body = response.text
    assert "neo4j_connected" in body
    assert "neo4j-status-desc" in body
    assert "neo4j-status-badge" in body


# ---------------------------------------------------------------------------
# Dashboard HTML content tests
# ---------------------------------------------------------------------------


async def test_dashboard_html_includes_completed_sessions_section(
    client: httpx.AsyncClient,
) -> None:
    """GET /dashboard returns HTML with Completed Sessions table body element."""
    response = await client.get("/dashboard")
    assert response.status_code == 200
    body = response.text
    assert "completed-body" in body
    assert "Completed sessions" in body


async def test_dashboard_html_includes_error_badge(
    client: httpx.AsyncClient,
) -> None:
    """GET /dashboard returns HTML with error-badge element."""
    response = await client.get("/dashboard")
    assert response.status_code == 200
    body = response.text
    assert "error-badge" in body


async def test_dashboard_html_sessions_table_displays_event_name_not_timestamp(
    client: httpx.AsyncClient,
) -> None:
    """Dashboard JS must render s.last_event directly, not via timeAgo().

    s.last_event is an event-name string (e.g. 'tool:pre'), not a Unix timestamp.
    Passing it to timeAgo() coerces the string to NaN and renders 'NaNs ago'.
    JS logic is now in the external dashboard.js module.
    """
    response = await client.get("/static/js/dashboard.js")
    assert response.status_code == 200
    body = response.text
    # Bug pattern must be absent
    assert "timeAgo(s.last_event)" not in body
    # Fix pattern must be present
    assert "(s.last_event || '-')" in body


# ---------------------------------------------------------------------------
# Lifespan tests
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# GET /logs/stream SSE tests
# ---------------------------------------------------------------------------


async def test_logs_stream_returns_200_event_stream(
    client: httpx.AsyncClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /logs/stream returns 200 with content-type text/event-stream."""
    from starlette.requests import Request as StarletteRequest

    log_file = tmp_path / "server.jsonl"
    log_file.write_text("")
    monkeypatch.setattr(main_module._settings, "log_path", str(log_file))

    # httpx ASGI transport never signals disconnect, so patch is_disconnected
    # to return True so the SSE generator's tail loop terminates cleanly.
    async def mock_is_disconnected(self: StarletteRequest) -> bool:  # noqa: ARG001
        return True

    monkeypatch.setattr(StarletteRequest, "is_disconnected", mock_is_disconnected)

    async with client.stream("GET", "/logs/stream") as response:
        assert response.status_code == 200
        assert "text/event-stream" in response.headers["content-type"]


async def test_logs_stream_backfills_existing_lines(
    client: httpx.AsyncClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /logs/stream backfills existing log lines as SSE data frames."""
    from starlette.requests import Request as StarletteRequest

    log_file = tmp_path / "server.jsonl"
    lines = [json.dumps({"level": "INFO", "msg": f"line {i}"}) for i in range(5)]
    log_file.write_text("\n".join(lines) + "\n")
    monkeypatch.setattr(main_module._settings, "log_path", str(log_file))

    # httpx ASGI transport never signals disconnect, so patch is_disconnected
    # to return True so the SSE generator's tail loop terminates cleanly.
    async def mock_is_disconnected(self: StarletteRequest) -> bool:  # noqa: ARG001
        return True

    monkeypatch.setattr(StarletteRequest, "is_disconnected", mock_is_disconnected)

    data_lines: list[str] = []
    async with client.stream("GET", "/logs/stream") as response:
        async for line in response.aiter_lines():
            if line.startswith("data: "):
                data_lines.append(line[len("data: ") :])

    assert len(data_lines) == 5
    for i, content in enumerate(data_lines):
        assert content == lines[i]


async def test_logs_stream_absent_log_file_returns_empty_stream(
    client: httpx.AsyncClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /logs/stream returns 200 with no data lines when log file does not exist.

    Previously crashed with FileNotFoundError (500) when log_path was absent.
    """
    from starlette.requests import Request as StarletteRequest

    absent_file = tmp_path / "nonexistent.jsonl"
    # Deliberately do NOT create the file
    assert not absent_file.exists()
    monkeypatch.setattr(main_module._settings, "log_path", str(absent_file))

    async def mock_is_disconnected(self: StarletteRequest) -> bool:  # noqa: ARG001
        return True

    monkeypatch.setattr(StarletteRequest, "is_disconnected", mock_is_disconnected)

    data_lines: list[str] = []
    async with client.stream("GET", "/logs/stream") as response:
        assert response.status_code == 200
        assert "text/event-stream" in response.headers["content-type"]
        async for line in response.aiter_lines():
            if line.startswith("data: "):
                data_lines.append(line[len("data: ") :])

    assert data_lines == []


async def test_logs_stream_tail_lines_have_no_trailing_newline(
    client: httpx.AsyncClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SSE frames from backfill must not contain trailing newlines in the data field.

    Previously, splitlines() in backfill was correct, but the tail path used
    f.readline() which includes the trailing '\\n', producing triple-newline frames.
    This test covers the backfill path (splitlines strips correctly).
    """
    from starlette.requests import Request as StarletteRequest

    log_file = tmp_path / "server.jsonl"
    log_file.write_text('{"level": "INFO", "msg": "hello"}\n')
    monkeypatch.setattr(main_module._settings, "log_path", str(log_file))

    async def mock_is_disconnected(self: StarletteRequest) -> bool:  # noqa: ARG001
        return True

    monkeypatch.setattr(StarletteRequest, "is_disconnected", mock_is_disconnected)

    data_lines: list[str] = []
    async with client.stream("GET", "/logs/stream") as response:
        async for line in response.aiter_lines():
            if line.startswith("data: "):
                data_lines.append(line[len("data: ") :])

    assert len(data_lines) == 1
    assert not data_lines[0].endswith("\n"), (
        "data field must not contain trailing newline"
    )


# ---------------------------------------------------------------------------
# Lifespan tests
# ---------------------------------------------------------------------------


async def test_dashboard_html_includes_log_viewer(client: httpx.AsyncClient) -> None:
    """GET /dashboard returns HTML with log viewer panel; JS module wires up EventSource."""
    # The dashboard HTML contains the DOM elements
    html_response = await client.get("/dashboard")
    assert html_response.status_code == 200
    assert "log-container" in html_response.text
    # EventSource and /logs/stream are in the external dashboard.js module
    js_response = await client.get("/static/js/dashboard.js")
    assert js_response.status_code == 200
    js_body = js_response.text
    assert "EventSource" in js_body
    assert "/logs/stream" in js_body


# ---------------------------------------------------------------------------
# TestCursorPurgeEndpoints
# ---------------------------------------------------------------------------


class TestCursorPurgeEndpoints:
    """Tests for DELETE /sessions/{session_id}/cursors and DELETE /sessions/cursors."""

    async def test_purge_single_session_cursors(
        self,
        client: httpx.AsyncClient,
        tmp_path: Path,
    ) -> None:
        """DELETE /sessions/{session_id}/cursors returns 200 and removes cursor file."""
        # The autouse safe_cursor_path fixture in conftest.py patches
        # registry.get_settings() → cursor_path=tmp_path for all tests.
        session_id = "test-session-purge"
        session_dir = tmp_path / session_id
        session_dir.mkdir()
        cursor_file = session_dir / "cursors.json"
        cursor_file.write_text(
            '{"last_updated": "2024-01-01T00:00:00Z", "cursors": {}}',
            encoding="utf-8",
        )
        assert cursor_file.exists()

        response = await client.delete(f"/sessions/{session_id}/cursors")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["session_id"] == session_id
        assert not cursor_file.exists()

    async def test_purge_all_session_cursors(
        self,
        client: httpx.AsyncClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """DELETE /sessions/cursors returns 200 and removes all cursor files."""
        # The purge-all endpoint reads cursor_path directly from main_module._settings
        # (not via registry.get_settings()), so the autouse safe_cursor_path conftest
        # fixture alone is insufficient here — we must patch _settings directly.
        monkeypatch.setattr(main_module._settings, "cursor_path", str(tmp_path))

        sessions = ["session-a", "session-b", "session-c"]
        cursor_files: list[Path] = []
        for sid in sessions:
            session_dir = tmp_path / sid
            session_dir.mkdir()
            cursor_file = session_dir / "cursors.json"
            cursor_file.write_text(
                '{"last_updated": "2024-01-01T00:00:00Z", "cursors": {}}',
                encoding="utf-8",
            )
            cursor_files.append(cursor_file)

        response = await client.delete("/sessions/cursors")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["purged"] == len(sessions)
        for cursor_file in cursor_files:
            assert not cursor_file.exists()

    async def test_purge_single_session_cursors_when_no_file_returns_ok(
        self,
        client: httpx.AsyncClient,
    ) -> None:
        """DELETE /sessions/{session_id}/cursors returns 200 even when no cursor file exists.

        Verifies the idempotent contract: calling delete on a session with no
        cursor file is not an error.
        """
        response = await client.delete("/sessions/nonexistent-session/cursors")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["session_id"] == "nonexistent-session"

    async def test_purge_all_session_cursors_when_empty_returns_ok(
        self,
        client: httpx.AsyncClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """DELETE /sessions/cursors returns 200 with purged=0 when no cursor files exist.

        Verifies the idempotent contract: calling delete-all with no files to
        remove is not an error and accurately reports zero deletions.
        """
        monkeypatch.setattr(main_module._settings, "cursor_path", str(tmp_path))

        response = await client.delete("/sessions/cursors")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["purged"] == 0


# ---------------------------------------------------------------------------
# replay flag tests
# ---------------------------------------------------------------------------


async def test_post_events_replay_flag_passes_to_get_or_create(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /events?replay=true calls get_or_create with replay=True."""
    captured_kwargs: dict = {}

    original = registry.get_or_create

    def capturing_get_or_create(
        session_id: str, workspace: str, replay: bool = False
    ) -> Any:
        captured_kwargs["replay"] = replay
        return original(session_id, workspace, replay=replay)

    with patch.object(registry, "get_or_create", side_effect=capturing_get_or_create):
        response = await client.post(
            "/events?replay=true",
            json={
                "event": "tool_use",
                "workspace": "/ws",
                "data": {"session_id": "sess-replay-flag"},
            },
        )
    assert response.status_code == 202
    assert captured_kwargs["replay"] is True


# ---------------------------------------------------------------------------
# Auth middleware integration tests
# ---------------------------------------------------------------------------


@contextlib.asynccontextmanager
async def _auth_client(
    api_key: str = "test-secret",
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Yield an AsyncClient pre-wrapped with BearerTokenMiddleware."""
    wrapped = BearerTokenMiddleware(app, api_key=api_key)
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
                    "data": {"session_id": "s1"},
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
                    "data": {"session_id": "s1"},
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
                "data": {"session_id": "s1"},
            },
        )
        assert response.status_code == 202


# ---------------------------------------------------------------------------
# main() dispatch tests
# ---------------------------------------------------------------------------


class TestMainDispatch:
    """Tests for the CLI entrypoint main() dispatch function."""

    def test_main_with_init_subcommand_calls_init_main(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """main() calls init_command.main() when first arg is 'init', not run()."""
        from unittest.mock import patch as _patch

        import context_intelligence_server.main as _main_mod

        monkeypatch.setattr(
            "sys.argv",
            [
                "context-intelligence-server",
                "init",
                "--config-path",
                "/tmp/test.yaml",
                "--neo4j-password",
                "pw",
            ],
        )

        with (
            _patch.object(_main_mod, "run") as mock_run,
            _patch("context_intelligence_server.init_command.main") as mock_init_main,
        ):
            _main_mod.main()

        mock_init_main.assert_called_once()
        mock_run.assert_not_called()

    def test_main_with_no_args_calls_run(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """main() calls run() when no subcommand is given."""
        from unittest.mock import patch as _patch

        import context_intelligence_server.main as _main_mod

        monkeypatch.setattr("sys.argv", ["context-intelligence-server"])

        with (
            _patch.object(_main_mod, "run") as mock_run,
            _patch("context_intelligence_server.init_command.main") as mock_init_main,
        ):
            _main_mod.main()

        mock_run.assert_called_once()
        mock_init_main.assert_not_called()

    def test_main_with_non_init_flag_calls_run(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """main() calls run() when first arg is not 'init' (e.g. --workers 2)."""
        from unittest.mock import patch as _patch

        import context_intelligence_server.main as _main_mod

        monkeypatch.setattr(
            "sys.argv", ["context-intelligence-server", "--workers", "2"]
        )

        with (
            _patch.object(_main_mod, "run") as mock_run,
            _patch("context_intelligence_server.init_command.main") as mock_init_main,
        ):
            _main_mod.main()

        mock_run.assert_called_once()
        mock_init_main.assert_not_called()

    def test_main_init_strips_init_from_argv(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """main() removes 'init' from sys.argv before delegating to init_command.main()."""
        import sys
        from unittest.mock import patch as _patch

        import context_intelligence_server.main as _main_mod

        monkeypatch.setattr(
            "sys.argv",
            [
                "context-intelligence-server",
                "init",
                "--neo4j-password",
                "secret",
            ],
        )
        captured_argv: list[str] = []

        def capture_init() -> None:
            captured_argv.extend(sys.argv)

        with (
            _patch(
                "context_intelligence_server.init_command.main",
                side_effect=capture_init,
            ),
        ):
            _main_mod.main()

        assert "init" not in captured_argv
        assert "--neo4j-password" in captured_argv


async def test_lifespan_creates_and_closes_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lifespan creates a Neo4j driver at startup and closes it at shutdown."""
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
            # During lifespan: driver factory must have been called
            mock_driver_factory.assert_called_once()
            # The driver is accessible via app.state
            assert main_module.app.state.neo4j_driver is mock_driver

        # After lifespan exits: close() must have been called
        mock_driver.close.assert_awaited_once()


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
