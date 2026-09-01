"""Integration — working-dir attribution survives the durable queue.

The folder a session ran in arrives as a TOP-LEVEL envelope field on POST
/events. ``post_events`` persists the raw request body verbatim, so the value
is on disk with the event; the drainer reads it back off the queue line and
attributes the Session node to it.

That indirection is the whole point, and these tests are its regression guard:
a worker respawned by crash recovery or dead-letter replay never sees the
original HTTP request, so any design that binds working_dir to the in-memory
worker loses it on every restart. Draining a persisted line with a FRESH
worker — exactly what recovery does — is what proves it does not.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock

import httpx

import context_intelligence_server.main as main_module
from context_intelligence_server import registry as registry_module
from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService

WORKING_DIR = "/home/user/project"


def _line(event: str, session_id: str, working_dir: str | None) -> bytes:
    obj: dict[str, object] = {
        "event": event,
        "workspace": "-home-user-project",
        "data": {"session_id": session_id, "timestamp": "2024-01-01T00:00:00+00:00"},
    }
    if working_dir is not None:
        obj["working_dir"] = working_dir
    return json.dumps(obj).encode("utf-8")


async def _drain_once(registry: SessionRegistry, worker: SessionWorker) -> None:
    """Run the real drain loop until the worker's queue is empty."""
    task = asyncio.create_task(registry.drain_worker(worker, flush_timeout=10.0))
    for _ in range(400):
        await asyncio.sleep(0.01)
        if (await registry.queue_manager.read_batch(worker.session_id, 10)).lines == []:
            break
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def test_post_events_persists_working_dir_on_the_queue_line(
    client: httpx.AsyncClient,
) -> None:
    """The durable record carries working_dir at the top level, beside workspace."""
    resp = await client.post(
        "/events",
        json={
            "event": "session:start",
            "workspace": "-home-user-project",
            "working_dir": WORKING_DIR,
            "data": {
                "session_id": "sess-wd-persist",
                "timestamp": "2024-01-01T00:00:00+00:00",
            },
        },
    )
    assert resp.status_code == 202

    batch = await main_module.registry.queue_manager.read_batch("sess-wd-persist", 10)
    assert len(batch.lines) == 1
    obj = json.loads(batch.lines[0].decode("utf-8"))
    assert obj["working_dir"] == WORKING_DIR
    # Session attribute, not event content: it must NOT be smuggled into data,
    # which is stored verbatim as a blob on every Event node.
    assert "working_dir" not in obj["data"]


async def test_working_dir_reaches_the_session_node_after_a_restart() -> None:
    """A worker built WITHOUT any HTTP context still attributes the session.

    This is the crash-recovery shape: recovery respawns a drainer from the
    queue alone (main._recover_one_session), so the only surviving copy of the
    working directory is the one on the persisted line.
    """
    settings = registry_module.get_settings()
    sid = "sess-wd-recovered"
    qm = QueueManager(queues_dir=Path(settings.queues_path))
    await qm.append(sid, _line("session:start", sid, WORKING_DIR))

    registry = SessionRegistry()
    worker = SessionWorker(
        session_id=sid,
        workspace="-home-user-project",
        services=HookStateService(workspace="-home-user-project"),
    )
    worker.services.graph.flush = AsyncMock()  # type: ignore[method-assign]
    worker.services.graph.close = AsyncMock()  # type: ignore[method-assign]
    registry._register_for_test(worker)

    await _drain_once(registry, worker)

    node = await worker.services.graph.get_node(sid)
    assert node is not None
    assert node.get("working_dir") == WORKING_DIR


async def test_reimport_backfills_a_session_node_that_predates_working_dir() -> None:
    """Re-ingesting an old session fills in a Session node that lacks the folder.

    A node written before working_dir was recorded takes the node-exists branch
    of ensure_session_node on the next drain. Populate-if-missing is what makes
    the already-ingested corpus recoverable rather than permanently unattributed.
    """
    settings = registry_module.get_settings()
    sid = "sess-wd-backfill"
    qm = QueueManager(queues_dir=Path(settings.queues_path))
    await qm.append(sid, _line("session:resume", sid, WORKING_DIR))

    registry = SessionRegistry()
    services = HookStateService(workspace="-home-user-project")
    # A node from an earlier run: no working_dir, and NOT in _seen_sessions.
    await services.graph.upsert_node(
        sid, {"labels": ["Session"], "status": "running", "session_id": sid}
    )
    services.graph.flush = AsyncMock()  # type: ignore[method-assign]
    services.graph.close = AsyncMock()  # type: ignore[method-assign]
    worker = SessionWorker(
        session_id=sid, workspace="-home-user-project", services=services
    )
    registry._register_for_test(worker)

    await _drain_once(registry, worker)

    node = await services.graph.get_node(sid)
    assert node is not None
    assert node.get("working_dir") == WORKING_DIR


async def test_existing_working_dir_is_never_re_attributed() -> None:
    """An event reporting a different folder does not move an attributed session."""
    settings = registry_module.get_settings()
    sid = "sess-wd-stable"
    qm = QueueManager(queues_dir=Path(settings.queues_path))
    await qm.append(sid, _line("session:resume", sid, "/somewhere/else"))

    registry = SessionRegistry()
    services = HookStateService(workspace="-home-user-project")
    await services.graph.upsert_node(
        sid,
        {
            "labels": ["Session"],
            "status": "running",
            "session_id": sid,
            "working_dir": WORKING_DIR,
        },
    )
    services.graph.flush = AsyncMock()  # type: ignore[method-assign]
    services.graph.close = AsyncMock()  # type: ignore[method-assign]
    worker = SessionWorker(
        session_id=sid, workspace="-home-user-project", services=services
    )
    registry._register_for_test(worker)

    await _drain_once(registry, worker)

    node = await services.graph.get_node(sid)
    assert node is not None
    assert node.get("working_dir") == WORKING_DIR
