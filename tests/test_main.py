"""Tests for FastAPI app — GET /status and POST /events endpoints."""

import asyncio
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

import context_intelligence_server.main as main_module
from context_intelligence_server.main import lifespan, registry
from context_intelligence_server.models import CypherRequest
from tests.conftest import MockNeo4jDriver


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
    """GET / returns 200 with HTML dashboard containing polling JavaScript."""
    response = await client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    body = response.text
    assert "Context Intelligence Server" in body
    assert "setInterval" in body


# ---------------------------------------------------------------------------
# Dashboard HTML content tests
# ---------------------------------------------------------------------------


async def test_dashboard_html_includes_completed_sessions_section(
    client: httpx.AsyncClient,
) -> None:
    """GET / returns HTML with Completed Sessions table body element."""
    response = await client.get("/")
    assert response.status_code == 200
    body = response.text
    assert "completed-body" in body
    assert "Completed Sessions" in body


async def test_dashboard_html_includes_error_badge(
    client: httpx.AsyncClient,
) -> None:
    """GET / returns HTML with error-badge element."""
    response = await client.get("/")
    assert response.status_code == 200
    body = response.text
    assert "error-badge" in body


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


# ---------------------------------------------------------------------------
# Lifespan tests
# ---------------------------------------------------------------------------


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
