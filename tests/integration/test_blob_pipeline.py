"""Integration test — Blob Pipeline End-to-End.

Verifies the complete blob offloading flow:
1. POST /events with blob-eligible 'result' field → 202
2. Wait for drain loop to process event (asyncio.wait_for, 10s timeout)
3. GET /blobs/{session_id} lists stored blob URIs with '__result' in name
4. Parse session_id and key from URI
5. GET /blobs/{session_id}/{key} returns original content
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path

import httpx
import pytest

from context_intelligence_server.config import get_settings
from context_intelligence_server.main import app, registry


# ---------------------------------------------------------------------------
# integration_env fixture
# ---------------------------------------------------------------------------


@pytest.fixture
async def integration_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Override blob_path to tmp_path and create AsyncClient with ASGITransport.

    - Clears the get_settings LRU cache so the fresh call picks up the
      overridden CI_SERVER_BLOB_PATH environment variable.
    - Patches the module-level ``_settings`` object in main.py so the
      /blobs/* route handlers also see the new blob root.
    - Yields an AsyncClient backed by ASGITransport (no real server needed).
    """
    import context_intelligence_server.main as main_module

    blob_dir = tmp_path / "blobs"
    blob_dir.mkdir()

    # Reset cached settings so the env-var override takes effect
    get_settings.cache_clear()
    monkeypatch.setenv("CI_SERVER_BLOB_PATH", str(blob_dir))

    # Build fresh settings with the overridden blob_path
    new_settings = get_settings()

    # Patch the module-level singleton used by the /blobs/* route handlers
    monkeypatch.setattr(main_module, "_settings", new_settings)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client

    # Restore: clear the cache so subsequent tests get their own fresh settings
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBlobPipelineEndToEnd:
    """End-to-end blob pipeline tests via HTTP endpoints."""

    async def test_post_events_returns_202(
        self, integration_env: httpx.AsyncClient
    ) -> None:
        """POST /events with a blob-eligible 'result' field returns 202."""
        client = integration_env
        response = await client.post(
            "/events",
            json={
                "event": "tool:post",
                "workspace": "test-workspace",
                "data": {
                    "session_id": "blob-integ-202-check",
                    "timestamp": "2024-06-01T09:00:05+00:00",
                    "tool_call_id": "call_202_check",
                    "result": {"output": "hello", "exit_code": 0},
                },
            },
        )
        assert response.status_code == 202

    async def test_drain_loop_processes_event_within_timeout(
        self, integration_env: httpx.AsyncClient
    ) -> None:
        """Drain loop completes processing within 10 seconds."""
        client = integration_env
        session_id = "blob-integ-drain-check"

        await client.post(
            "/events",
            json={
                "event": "tool:post",
                "workspace": "test-workspace",
                "data": {
                    "session_id": session_id,
                    "timestamp": "2024-06-01T09:00:05+00:00",
                    "tool_call_id": "call_drain_check",
                    "result": {"output": "drain test"},
                },
            },
        )

        worker = registry._workers[session_id]
        # Must complete within 10 seconds (timeout raises asyncio.TimeoutError)
        await asyncio.wait_for(worker.queue.join(), timeout=10.0)

    async def test_blob_pipeline_full_flow(
        self, integration_env: httpx.AsyncClient
    ) -> None:
        """Full end-to-end blob pipeline: POST → drain → list → fetch."""
        client = integration_env
        session_id = "blob-integ-full-001"
        result_payload = {"output": "hello from tool", "exit_code": 0}

        # ----------------------------------------------------------------
        # Step 1: POST /events with blob-eligible 'result' field → 202
        # ----------------------------------------------------------------
        response = await client.post(
            "/events",
            json={
                "event": "tool:post",
                "workspace": "test-workspace",
                "data": {
                    "session_id": session_id,
                    "timestamp": "2024-06-01T09:00:05+00:00",
                    "tool_call_id": "call_blob_full_001",
                    "result": result_payload,
                },
            },
        )
        assert response.status_code == 202

        # ----------------------------------------------------------------
        # Step 2: Wait for drain loop to process (asyncio.wait_for, 10s)
        # ----------------------------------------------------------------
        worker = registry._workers[session_id]
        await asyncio.wait_for(worker.queue.join(), timeout=10.0)

        # ----------------------------------------------------------------
        # Step 3: GET /blobs/{session_id} → at least 1 URI with '__result'
        # ----------------------------------------------------------------
        response = await client.get(f"/blobs/{session_id}")
        assert response.status_code == 200
        data = response.json()

        blobs: list[str] = data["blobs"]
        result_uris = [uri for uri in blobs if "__result" in uri]
        assert len(result_uris) >= 1, (
            f"Expected >= 1 blob URI containing '__result', got: {blobs}"
        )

        # ----------------------------------------------------------------
        # Step 4: Parse session_id and key from URI
        # ----------------------------------------------------------------
        uri = result_uris[0]
        # URI format: ci-blob://<session_id>/<key>
        remainder = uri.removeprefix("ci-blob://")
        parsed_session_id, _, key = remainder.partition("/")
        assert parsed_session_id == session_id
        assert "__result" in key

        # ----------------------------------------------------------------
        # Step 5: GET /blobs/{session_id}/{key} → original content
        # ----------------------------------------------------------------
        response = await client.get(f"/blobs/{parsed_session_id}/{key}")
        assert response.status_code == 200
        content = response.json()
        assert content == result_payload
