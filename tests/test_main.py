"""Tests for FastAPI app — GET /status and POST /events endpoints."""

import asyncio
import json
from pathlib import Path

import httpx
import pytest

from context_intelligence_server.main import registry


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
    import context_intelligence_server.main as main_module

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
    import context_intelligence_server.main as main_module

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
    import context_intelligence_server.main as main_module

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
    import context_intelligence_server.main as main_module

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
    from context_intelligence_server.models import CypherRequest

    req = CypherRequest(query="MATCH (n) RETURN n")
    assert req.query == "MATCH (n) RETURN n"
    assert req.params == {}
    assert req.workspace is None


async def test_cypher_request_model_with_workspace() -> None:
    """CypherRequest model accepts workspace and params."""
    from context_intelligence_server.models import CypherRequest

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
    import context_intelligence_server.main as main_module

    mock_row = {"name": "Alice"}

    class MockResult:
        def __aiter__(self) -> "MockResult":
            self._sent = False
            return self

        async def __anext__(self) -> dict:
            if not self._sent:
                self._sent = True
                return mock_row
            raise StopAsyncIteration

    class MockSession:
        async def run(self, query: str, params: dict) -> MockResult:
            return MockResult()

        async def __aenter__(self) -> "MockSession":
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

    class MockDriver:
        def session(self) -> MockSession:
            return MockSession()

    monkeypatch.setattr(
        main_module.app.state, "neo4j_driver", MockDriver(), raising=False
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
    import context_intelligence_server.main as main_module

    captured_params: dict = {}

    class MockResult:
        def __aiter__(self) -> "MockResult":
            return self

        async def __anext__(self) -> dict:
            raise StopAsyncIteration

    class MockSession:
        async def run(self, query: str, params: dict) -> MockResult:
            captured_params.update(params)
            return MockResult()

        async def __aenter__(self) -> "MockSession":
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

    class MockDriver:
        def session(self) -> MockSession:
            return MockSession()

    monkeypatch.setattr(
        main_module.app.state, "neo4j_driver", MockDriver(), raising=False
    )

    await client.post(
        "/cypher",
        json={"query": "MATCH (n) RETURN n", "workspace": "/my/ws"},
    )
    assert captured_params.get("workspace") == "/my/ws"


async def test_cypher_neo4j_error_returns_500(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /cypher returns 500 with error detail when Neo4j raises an exception."""
    import context_intelligence_server.main as main_module

    class MockSession:
        async def run(self, query: str, params: dict) -> None:
            raise RuntimeError("Connection refused")

        async def __aenter__(self) -> "MockSession":
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

    class MockDriver:
        def session(self) -> MockSession:
            return MockSession()

    monkeypatch.setattr(
        main_module.app.state, "neo4j_driver", MockDriver(), raising=False
    )

    response = await client.post("/cypher", json={"query": "MATCH (n) RETURN n"})
    assert response.status_code == 500
    data = response.json()
    assert "Connection refused" in data["detail"]
