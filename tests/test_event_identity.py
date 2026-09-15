"""Regression coverage for server-owned durable event identity."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

import context_intelligence_server.main as main_module
import context_intelligence_server.registry as registry_module
from context_intelligence_server.handlers.data_layer_1.default import DefaultHandler
from context_intelligence_server.pipeline import PipelineHandlers, process_event, setup_handlers
from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService
from context_intelligence_server.utils import SPOOL_EVENT_IDENTITY, make_node_id


class _RecordingBlobStore:
    def __init__(self) -> None:
        self.keys: list[str] = []

    async def write(self, session_id: str, key: str, value: Any) -> str:
        self.keys.append(key)
        return f"ci-blob://{session_id}/{key}"


async def test_post_assigns_distinct_server_owned_ids_for_same_timestamp_events(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Different request keys retain both same-time Event nodes."""
    captured: list[bytes] = []
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *args, **kwargs: MagicMock()
    )

    async def capture(_worker_key: str, raw: bytes) -> None:
        captured.append(raw)

    monkeypatch.setattr(main_module.registry.queue_manager, "append", capture)
    timestamp = "2026-09-15T12:00:00+00:00"
    for key, marker in (("collision-key-one", "first"), ("collision-key-two", "second")):
        response = await client.post(
            "/events",
            json={
                "event": "custom:marker",
                "workspace": "identity-test",
                "idempotency_key": key,
                "data": {
                    "session_id": "same-time-session",
                    "timestamp": timestamp,
                    "marker": marker,
                },
            },
        )
        assert response.status_code == 202

    records = [json.loads(raw) for raw in captured]
    identities = [record[SPOOL_EVENT_IDENTITY] for record in records]
    assert len(set(identities)) == 2
    assert all(identity.startswith("key-") for identity in identities)
    assert all("collision-key" not in identity for identity in identities)

    services = HookStateService(workspace="identity-test")
    worker = SessionWorker(
        session_id="same-time-session", workspace="identity-test", services=services
    )
    handlers = PipelineHandlers(default=DefaultHandler(services), enrichers=[])
    node_ids: list[str] = []
    for raw in captured:
        event, _workspace, _working_dir, data, identity = SessionRegistry._parse_line(raw)
        await process_event(worker, event, data, handlers, event_identity=identity)
        node_ids.append(
            f"{make_node_id('same-time-session', 'custom:marker', timestamp)}__{identity}"
        )

    assert node_ids[0] != node_ids[1]
    assert json.loads((await services.graph.get_node(node_ids[0]))["data"])["marker"] == "first"  # type: ignore[index]
    assert json.loads((await services.graph.get_node(node_ids[1]))["data"])["marker"] == "second"  # type: ignore[index]


async def test_keyed_terminal_reprocessing_reuses_event_and_blob_identity(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The intentionally twice-dispatched terminal line resolves to one event ID."""
    captured: list[bytes] = []
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *args, **kwargs: MagicMock()
    )

    async def capture(_worker_key: str, raw: bytes) -> None:
        captured.append(raw)

    monkeypatch.setattr(main_module.registry.queue_manager, "append", capture)
    timestamp = "2026-09-15T12:01:00+00:00"
    response = await client.post(
        "/events",
        json={
            "event": "session:end",
            "workspace": "identity-test",
            "idempotency_key": "terminal-key",
            "data": {
                "session_id": "terminal-session",
                "timestamp": timestamp,
                "debug": {"reason": "test"},
            },
        },
    )
    assert response.status_code == 202

    _event, _workspace, _working_dir, _first_data, identity = SessionRegistry._parse_line(
        captured[0]
    )
    _event, _workspace, _working_dir, _second_data, second_identity = (
        SessionRegistry._parse_line(captured[0])
    )
    assert identity == second_identity

    blob_store = _RecordingBlobStore()
    services = HookStateService(workspace="identity-test", blob_store=blob_store)
    worker = SessionWorker(
        session_id="terminal-session", workspace="identity-test", services=services
    )
    handlers = setup_handlers(services)
    registry = SessionRegistry()
    await registry.queue_manager.append("terminal-session", captured[0])
    batch = await registry.queue_manager.read_batch("terminal-session", 1)
    safe_count, terminal_at = await registry._process_batch(worker, batch, handlers)
    assert safe_count == 0
    assert terminal_at == batch.records[0].start
    # The terminal record remains uncommitted after the first path and is
    # parsed/processed again by finalization's drain-to-EOF path.
    assert await registry._drain_to_eof(worker, handlers)

    event_id = f"{make_node_id('terminal-session', 'session:end', timestamp)}__{identity}"
    assert await services.graph.get_node(event_id) is not None
    sourced_from = await services.graph.get_edge("terminal-session", event_id)
    assert sourced_from is not None
    assert sourced_from["type"] == "SOURCED_FROM"
    assert blob_store.keys == [f"{event_id}__debug", f"{event_id}__debug"]


async def test_scoped_identity_agrees_between_event_blob_and_sourced_from() -> None:
    """Every same-record Event reference uses the one persisted identity suffix."""
    timestamp = "2026-09-15T12:02:00+00:00"
    identity = "new-record-identity"
    blob_store = _RecordingBlobStore()
    services = HookStateService(workspace="identity-test", blob_store=blob_store)
    worker = SessionWorker(
        session_id="cancel-session", workspace="identity-test", services=services
    )
    data = {
        "session_id": "cancel-session",
        "timestamp": timestamp,
        "immediate": True,
        "debug": {"reason": "test"},
    }

    await process_event(
        worker, "cancel:completed", data, setup_handlers(services), event_identity=identity
    )

    event_id = f"{make_node_id('cancel-session', 'cancel:completed', timestamp)}__{identity}"
    cancellation_id = f"cancel-session::cancellation::{timestamp}"
    assert await services.graph.get_node(event_id) is not None
    assert await services.graph.get_edge(cancellation_id, event_id) == {
        "type": "SOURCED_FROM"
    }
    assert blob_store.keys == [f"{event_id}__debug"]


async def test_keyless_identity_survives_dead_letter_replay_and_queue_recreation(
    client: httpx.AsyncClient, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A keyless record is stable on replay but new after its queue is recreated."""
    queue_manager = QueueManager(queues_dir=tmp_path / "queues")
    monkeypatch.setattr(main_module.registry, "_queue_manager", queue_manager)
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *args, **kwargs: MagicMock()
    )
    payload = {
        "event": "custom:keyless",
        "workspace": "identity-test",
        "data": {
            "session_id": "keyless-session",
            "timestamp": "2026-09-15T12:03:00+00:00",
        },
    }

    assert (await client.post("/events", json=payload)).status_code == 202
    first = (await queue_manager.read_batch("keyless-session", 1)).records[0]
    _event, _workspace, _working_dir, _data, first_identity = SessionRegistry._parse_line(
        first.raw
    )
    await queue_manager.commit("keyless-session", first.end)
    assert await queue_manager.delete_drained("keyless-session")

    assert (await client.post("/events", json=payload)).status_code == 202
    second = (await queue_manager.read_batch("keyless-session", 1)).records[0]
    _event, _workspace, _working_dir, _data, second_identity = SessionRegistry._parse_line(
        second.raw
    )
    assert first_identity is not None
    assert second_identity is not None
    assert first_identity != second_identity

    await queue_manager.commit("keyless-session", second.end)
    assert await queue_manager.delete_drained("keyless-session")
    await queue_manager.dead_letter("keyless-session", first.raw, "forced test failure")
    response = await client.post("/queues/dead-letter/keyless-session/replay")
    assert response.status_code == 200
    replayed = (await queue_manager.read_batch("keyless-session", 1)).records[0]
    _event, _workspace, _working_dir, _data, replayed_identity = SessionRegistry._parse_line(
        replayed.raw
    )
    assert replayed_identity == first_identity


async def test_registry_passes_persisted_identity_to_each_pipeline_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """New and legacy queue lines reach process_event with their own scope."""
    registry = SessionRegistry()
    worker = SessionWorker(
        session_id="registry-session",
        workspace="identity-test",
        services=MagicMock(),
    )
    captured: list[str | None] = []

    async def capture(*args: Any, **kwargs: Any) -> None:
        captured.append(kwargs["event_identity"])

    monkeypatch.setattr(registry_module, "process_event", capture)
    for identity in ("persisted-identity", None):
        body: dict[str, Any] = {
            "event": "custom:registry",
            "workspace": "identity-test",
            "data": {
                "session_id": "registry-session",
                "timestamp": "2026-09-15T12:03:30+00:00",
            },
        }
        if identity is not None:
            body[SPOOL_EVENT_IDENTITY] = identity
        await registry.queue_manager.append(
            "registry-session", json.dumps(body).encode("utf-8")
        )

    batch = await registry.queue_manager.read_batch("registry-session", 10)
    await registry._process_batch(worker, batch, handlers=MagicMock())
    assert captured == ["persisted-identity", None]


async def test_event_identity_scope_isolated_and_resets_after_failure_and_cancel() -> None:
    """ContextVar state cannot cross concurrent records or survive abnormal exits."""
    timestamp = "2026-09-15T12:04:00+00:00"
    worker = MagicMock()
    worker.services.ensure_session_node = AsyncMock()
    worker.services.touch_session = AsyncMock()
    worker.services.blob_store = None
    release = asyncio.Event()
    entered = [asyncio.Event(), asyncio.Event()]
    observed: dict[str, list[str]] = {"one": [], "two": []}

    async def concurrent_handler(_event: str, data: dict[str, Any]) -> None:
        marker = data["marker"]
        observed[marker].append(make_node_id("scope-session", "custom:scope", timestamp))
        entered[0 if marker == "one" else 1].set()
        await release.wait()
        observed[marker].append(make_node_id("scope-session", "custom:scope", timestamp))

    handlers = PipelineHandlers(default=concurrent_handler, enrichers=[])  # type: ignore[arg-type]
    tasks = [
        asyncio.create_task(
            process_event(
                worker,
                "custom:scope",
                {"session_id": "scope-session", "timestamp": timestamp, "marker": marker},
                handlers,
                event_identity=marker,
            )
        )
        for marker in ("one", "two")
    ]
    await asyncio.gather(*(event.wait() for event in entered))
    release.set()
    await asyncio.gather(*tasks)
    legacy = make_node_id("scope-session", "custom:scope", timestamp)
    assert observed == {
        "one": [f"{legacy}__one", f"{legacy}__one"],
        "two": [f"{legacy}__two", f"{legacy}__two"],
    }

    async def fails(_event: str, _data: dict[str, Any]) -> None:
        assert make_node_id("scope-session", "custom:scope", timestamp) == f"{legacy}__fails"
        raise RuntimeError("forced")

    with pytest.raises(RuntimeError, match="forced"):
        await process_event(
            worker,
            "custom:scope",
            {"session_id": "scope-session", "timestamp": timestamp},
            PipelineHandlers(default=fails, enrichers=[]),  # type: ignore[arg-type]
            event_identity="fails",
        )
    assert make_node_id("scope-session", "custom:scope", timestamp) == legacy

    blocked = asyncio.Event()

    async def blocks(_event: str, _data: dict[str, Any]) -> None:
        blocked.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(
        process_event(
            worker,
            "custom:scope",
            {"session_id": "scope-session", "timestamp": timestamp},
            PipelineHandlers(default=blocks, enrichers=[]),  # type: ignore[arg-type]
            event_identity="cancelled",
        )
    )
    await blocked.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert make_node_id("scope-session", "custom:scope", timestamp) == legacy


async def test_post_overwrites_a_client_supplied_event_identity(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The private spool marker is server-assigned, not an input contract."""
    captured: list[bytes] = []
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *args, **kwargs: MagicMock()
    )

    async def capture(_worker_key: str, raw: bytes) -> None:
        captured.append(raw)

    monkeypatch.setattr(main_module.registry.queue_manager, "append", capture)
    response = await client.post(
        "/events",
        json={
            "event": "custom:spoof",
            "workspace": "identity-test",
            "idempotency_key": "spoof-key",
            SPOOL_EVENT_IDENTITY: "attacker-selected",
            "data": {
                "session_id": "spoof-session",
                "timestamp": "2026-09-15T12:05:00+00:00",
            },
        },
    )
    assert response.status_code == 202
    persisted = json.loads(captured[0])
    assert persisted[SPOOL_EVENT_IDENTITY] != "attacker-selected"
    assert persisted[SPOOL_EVENT_IDENTITY] == main_module._new_event_identity("spoof-key")