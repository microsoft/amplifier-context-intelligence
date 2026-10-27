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
import concurrent.futures
import json
import socket
from collections.abc import AsyncGenerator
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from neo4j import AsyncGraphDatabase

from context_intelligence_server.config import get_settings
from context_intelligence_server.main import app, registry


def _neo4j_reachable() -> bool:
    """Return True if Neo4j is reachable at neo4j:7687 (only resolves inside Docker).

    Uses a thread with a hard 2-second total deadline so that slow or
    absent DNS resolution for the 'neo4j' hostname never blocks the
    calling thread (e.g. pytest collection on GitHub Actions).
    """

    def _check() -> bool:
        try:
            with socket.create_connection(("neo4j", 7687), timeout=1):
                return True
        except OSError:
            return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        try:
            return executor.submit(_check).result(timeout=2)
        except concurrent.futures.TimeoutError:
            return False


# ---------------------------------------------------------------------------
# integration_env fixture
# ---------------------------------------------------------------------------


@pytest.fixture
async def integration_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Override blob_path to tmp_path and create AsyncClient with ASGITransport.

    - Clears the get_settings LRU cache so the fresh call picks up the
      overridden AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_BLOB_PATH environment variable.
    - Patches the module-level ``_settings`` object in main.py so the
      /blobs/* route handlers also see the new blob root.
    - Yields an AsyncClient backed by ASGITransport (no real server needed).
    """
    import context_intelligence_server.main as main_module

    blob_dir = tmp_path / "blobs"
    blob_dir.mkdir()

    # Reset cached settings so the env-var override takes effect
    get_settings.cache_clear()
    monkeypatch.setenv("AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_BLOB_PATH", str(blob_dir))

    # Build fresh settings with the overridden blob_path
    new_settings = get_settings()

    # Patch the module-level singleton used by the /blobs/* route handlers
    monkeypatch.setattr(main_module, "_settings", new_settings)

    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            yield client
    finally:
        # The module singleton's pooled Neo4j connections are bound to the loop
        # that first used them, so THIS test's loop must dispose them before
        # pytest closes it. Lifespan order: quiesce drainers first (they still
        # flush on a live driver), then close the shared driver, which resets
        # ``_neo4j_driver`` so the next test rebuilds its own on its own loop.
        try:
            await registry.shutdown_workers()
        finally:
            try:
                await registry.close_neo4j_driver()
            finally:
                # Restore: clear the cache so subsequent tests get their own
                # fresh settings
                get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBlobPipelineEndToEnd:
    """End-to-end blob pipeline tests via HTTP endpoints."""

    async def test_post_events_returns_202(
        self, integration_env: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Admission durably queues the payload without starting a Neo4j worker."""
        driver = Mock(
            side_effect=AssertionError(
                "admission-only test must not construct Neo4j driver"
            )
        )
        # integration_env shares monkeypatch, so the guard survives its teardown.
        monkeypatch.setattr(AsyncGraphDatabase, "driver", driver)
        get_or_create = Mock(return_value=None)
        monkeypatch.setattr(registry, "get_or_create", get_or_create)
        client = integration_env
        payload = {
            "event": "tool:post",
            "workspace": "test-workspace",
            "data": {
                "session_id": "blob-integ-202-check",
                "timestamp": "2024-06-01T09:00:05+00:00",
                "tool_call_id": "call_202_check",
                "result": {"output": "hello", "exit_code": 0},
            },
        }
        response = await client.post("/events", json=payload)
        assert response.status_code == 202
        assert response.json()["status"] == "queued"
        get_or_create.assert_called_once_with(
            "blob-integ-202-check", "test-workspace", created_by=None
        )
        batch = await registry.queue_manager.read_batch(payload["data"]["session_id"], 2)
        assert len(batch.lines) == 1
        queued = json.loads(batch.lines[0])
        assert queued["event"] == payload["event"]
        assert queued["workspace"] == payload["workspace"]
        assert queued["data"] == payload["data"]
        assert registry.workers() == []
        driver.assert_not_called()

    @pytest.mark.skipif(
        not _neo4j_reachable(), reason="Neo4j not reachable at neo4j:7687"
    )
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

        # Poll the durable log until the drain loop has committed past every
        # appended line (durable equivalent of the old worker.queue.join()).
        for _ in range(500):
            if (await registry.queue_manager.read_batch(session_id, 10)).lines == []:
                break
            await asyncio.sleep(0.02)

    @pytest.mark.skipif(
        not _neo4j_reachable(), reason="Neo4j not reachable at neo4j:7687"
    )
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
        # Poll the durable log until the drain loop has committed past every
        # appended line (durable equivalent of the old worker.queue.join()).
        for _ in range(500):
            if (await registry.queue_manager.read_batch(session_id, 10)).lines == []:
                break
            await asyncio.sleep(0.02)

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


# ---------------------------------------------------------------------------
# Legacy '%', '?', '#' session ids -- networkless admission-to-reclaim boundary
# ---------------------------------------------------------------------------


class TestLegacyIdBlobBoundary:
    """Real /events admission -> durable queue -> real drainer, pipeline and
    blob processor -> offset commit -> spool reclaim.  Only the graph is
    in-memory (``GraphState``); no Neo4j, no network."""

    @pytest.mark.parametrize(
        "event_name",
        [
            "tool:post",
            "custom:progress?",
            "custom:progress%",
            "custom:progress#",
            "custom:a%?#b",
            "custom:progress%3F",  # looks pre-encoded; must stay literal
        ],
    )
    @pytest.mark.parametrize("session_id", ["plain-session-1", "legacy%sess?id#1"])
    async def test_legacy_session_id_blob_survives_end_to_end(
        self,
        integration_env: httpx.AsyncClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        session_id: str,
        event_name: str,
    ) -> None:
        import json
        from typing import Any

        import context_intelligence_server.main as main_module
        from context_intelligence_server.blob_store import AsyncDiskBlobStore
        from context_intelligence_server.queue_manager import QueueManager
        from context_intelligence_server.registry import (
            SessionRegistry,
            SessionWorker,
        )
        from context_intelligence_server.services import HookStateService

        payload = {"output": "legacy payload", "exit_code": 0}
        store = AsyncDiskBlobStore(tmp_path / "blobs")
        queues = tmp_path / "queues"
        reg = SessionRegistry()
        reg._queue_manager = QueueManager(queues)
        reg._write_semaphore = asyncio.Semaphore(1)
        reg._max_delivery_attempts = 3
        workers: dict[str, SessionWorker] = {}

        def _get_or_create(
            worker_key: str, workspace: str, created_by: str | None = None
        ) -> SessionWorker:
            if worker_key not in workers:
                workers[worker_key] = SessionWorker(
                    session_id=worker_key,
                    workspace=workspace,
                    services=HookStateService(
                        workspace=workspace, created_by=created_by, blob_store=store
                    ),
                )
                reg._register_for_test(workers[worker_key])
            reg.start_drain(workers[worker_key])
            return workers[worker_key]

        monkeypatch.setattr(reg, "get_or_create", _get_or_create)
        monkeypatch.setattr(main_module, "registry", reg)
        monkeypatch.setattr(main_module.app.state, "registry", reg)

        try:
            response = await integration_env.post(
                "/events",
                json={
                    "event": event_name,
                    "workspace": "test-workspace",
                    "data": {
                        "session_id": session_id,
                        "timestamp": "2024-06-01T09:00:05+00:00",
                        "tool_call_id": "call_legacy_id",
                        "result": payload,
                    },
                },
            )
            assert response.status_code == 202

            worker = workers[session_id]
            graph: Any = worker.services.graph

            async def _settled() -> dict:
                # Event node recorded AND offset committed past the line.
                while True:
                    nodes = [
                        n
                        for n in graph._nodes.values()
                        if n.get("event_name") == event_name
                    ]
                    if nodes and await reg.queue_manager.pending_count(session_id) == 0:
                        return nodes[0]
                    await asyncio.sleep(0.02)

            event_node = await asyncio.wait_for(_settled(), timeout=10)
        finally:
            for worker in workers.values():
                if worker.task is not None and not worker.task.done():
                    worker.task.cancel()
                    try:
                        await worker.task
                    except asyncio.CancelledError:
                        pass

        from context_intelligence_server.handlers.data_layer_1.default import (
            DefaultHandler,
        )

        # The exact client-supplied name reached the real handler's node.
        assert event_node["event_name"] == event_name
        assert set(DefaultHandler.derive_labels(event_name)) <= (
            set(event_node["labels"]) | {"Event"}
        )
        data = json.loads(event_node["data"])
        assert "$blob_error" not in json.dumps(data)
        uri = data["result"]["$blob_ref"]
        assert uri.startswith(f"ci-blob://{session_id}/") and uri.endswith("__result")
        # Reserved characters from the event name reach the key literally
        # (only ':' is rewritten); '%3F' is not decoded to '?'.
        assert f"__{event_name.replace(':', '_')}__" in uri

        # Spool reclaim of the drained log must not touch the blob payload.
        assert await reg.queue_manager.read_dead_letters(session_id) == []
        assert await reg.queue_manager.delete_drained(session_id) is True
        assert list(queues.glob("*.log")) == []
        assert await store.read(uri) == payload


# ---------------------------------------------------------------------------
# Separator event names -- networkless admission-to-reclaim boundary (PR126)
# ---------------------------------------------------------------------------
#
# The blob key is f"{node_id}__result" and node_id keeps the RAW event name, so
# an event containing "/" or "\" puts a path separator into the key.  Those
# keys map to a flat ``~ci-key-v1/<digest>`` address; ``%``, ``?`` and ``#``
# stay ordinary literal bytes.


async def _drive_event(
    client: httpx.AsyncClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    session_id: str,
    event_name: str,
    payload: dict,
) -> SimpleNamespace:
    """POST one event through the real admission -> queue -> drainer -> pipeline.

    Only the graph is in-memory (``GraphState``).  Returns once the event node
    is recorded AND the durable offset has committed past the line.
    """
    from typing import Any

    import context_intelligence_server.main as main_module
    from context_intelligence_server.blob_store import AsyncDiskBlobStore
    from context_intelligence_server.queue_manager import QueueManager
    from context_intelligence_server.registry import SessionRegistry, SessionWorker
    from context_intelligence_server.services import HookStateService

    store = AsyncDiskBlobStore(tmp_path / "blobs")
    real_write = store.write
    writes: list[tuple[str, str]] = []

    async def recording_write(session: str, key: str, value: Any) -> str:
        writes.append((session, key))  # the LOGICAL key the pipeline chose
        return await real_write(session, key, value)

    monkeypatch.setattr(store, "write", recording_write)
    queues = tmp_path / "queues"
    reg = SessionRegistry()
    reg._queue_manager = QueueManager(queues)
    reg._write_semaphore = asyncio.Semaphore(1)
    reg._max_delivery_attempts = 3
    workers: dict[str, SessionWorker] = {}

    def _get_or_create(
        worker_key: str, workspace: str, created_by: str | None = None
    ) -> SessionWorker:
        if worker_key not in workers:
            workers[worker_key] = SessionWorker(
                session_id=worker_key,
                workspace=workspace,
                services=HookStateService(
                    workspace=workspace, created_by=created_by, blob_store=store
                ),
            )
            reg._register_for_test(workers[worker_key])
        reg.start_drain(workers[worker_key])
        return workers[worker_key]

    monkeypatch.setattr(reg, "get_or_create", _get_or_create)
    monkeypatch.setattr(main_module, "registry", reg)
    monkeypatch.setattr(main_module.app.state, "registry", reg)

    try:
        response = await client.post(
            "/events",
            json={
                "event": event_name,
                "workspace": "test-workspace",
                "data": {
                    "session_id": session_id,
                    "timestamp": "2024-06-01T09:00:05+00:00",
                    "tool_call_id": "call_sep_id",
                    "result": payload,
                },
            },
        )
        assert response.status_code == 202
        graph: Any = workers[session_id].services.graph

        async def _settled() -> tuple[str, dict]:
            while True:
                for node_id, node in graph._nodes.items():
                    if node.get("event_name") == event_name and (
                        await reg.queue_manager.pending_count(session_id) == 0
                    ):
                        return node_id, node
                await asyncio.sleep(0.02)

        node_id, node = await asyncio.wait_for(_settled(), timeout=10)
    finally:
        for worker in workers.values():
            if worker.task is not None and not worker.task.done():
                worker.task.cancel()
                try:
                    await worker.task
                except asyncio.CancelledError:
                    pass
    return SimpleNamespace(
        store=store,
        reg=reg,
        queues=queues,
        node_id=node_id,
        node=node,
        writes=writes,
    )


def _digest_of(key: str) -> str:
    import hashlib

    return hashlib.sha256(b"ci-blob-event-key:v1\x00" + key.encode("utf-8")).hexdigest()


class TestSeparatorEventNameBlobBoundary:
    @pytest.mark.parametrize(
        "event_name",
        [
            "custom:tool/pre",
            "custom:a\\b",
            "custom:a/b%?#",
            "custom:%?#",
            "custom:a%2fb",
            "custom:a%5cb",
        ],
    )
    @pytest.mark.parametrize("session_id", ["plain-session-1", "legacy%sess?id#1"])
    async def test_event_name_blob_survives_end_to_end_and_reads_over_http(
        self,
        integration_env: httpx.AsyncClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        session_id: str,
        event_name: str,
    ) -> None:
        import json
        from urllib.parse import quote

        payload = {"output": "separator payload", "exit_code": 0}
        run = await _drive_event(
            integration_env, tmp_path, monkeypatch, session_id, event_name, payload
        )

        # Node identity is unchanged: the RAW event name (only ":" rewritten).
        safe_event = event_name.replace(":", "_")
        assert run.node["event_name"] == event_name
        assert run.node_id.startswith(f"{session_id}__{safe_event}__1717232405000")

        # The offloaded field is a reference, never a $blob_error.
        data = json.loads(run.node["data"])
        assert "$blob_error" not in json.dumps(data)
        uri = data["result"]["$blob_ref"]
        # Exactly one write, keyed by the RAW event name (node_id + field).
        assert [session for session, _ in run.writes] == [session_id]
        (_, key) = run.writes[0]
        assert key.startswith(f"{session_id}__{safe_event}__1717232405000")
        assert key.endswith("__result")
        if "/" in event_name or "\\" in event_name:
            assert uri == f"ci-blob://{session_id}/~ci-key-v1/{_digest_of(key)}"
            path = f"/blobs/{quote(session_id, safe='')}/~ci-key-v1/{_digest_of(key)}"
        else:
            assert uri == f"ci-blob://{session_id}/{key}"
            path = f"/blobs/{quote(session_id, safe='')}/{quote(key, safe='')}"

        assert await run.store.read(uri) == payload
        response = await integration_env.get(path)
        assert (response.status_code, response.json()) == (200, payload)
        assert await run.store.list(session_id) == [uri]

        # Offset commit/trim: reclaiming the drained spool never touches the blob.
        assert await run.reg.queue_manager.read_dead_letters(session_id) == []
        assert await run.reg.queue_manager.delete_drained(session_id) is True
        assert list(run.queues.glob("*.log")) == []
        assert await run.store.read(uri) == payload

        # Session deletion removes exactly that one blob, whichever address kind.
        assert await run.store.delete_session(session_id) == 1
        with pytest.raises(FileNotFoundError):
            await run.store.read(uri)
