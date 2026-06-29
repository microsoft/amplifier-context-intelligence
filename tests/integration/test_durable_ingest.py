"""Integration tests for the durable POST /events ingest path (Phase B2, Task 5).

These exercise the persist-then-202 contract: POST /events appends the EXACT
raw request body bytes to the per-worker durable log BEFORE returning 202, and
the sticky drainer later drains that line through the process_event pipeline.
"""

from __future__ import annotations

import asyncio
import json

import httpx
from unittest.mock import AsyncMock, MagicMock

import context_intelligence_server.main as main_module


async def test_post_events_persists_raw_body_then_returns_202(
    client: httpx.AsyncClient,
    monkeypatch: object,
) -> None:
    """POST /events durably appends the raw body, returns 202/'queued', and the
    stored line preserves the request JSON with safe newline framing."""
    # Prevent a real drainer from consuming the line so we can inspect it.
    monkeypatch.setattr(  # type: ignore[attr-defined]
        main_module.registry, "get_or_create", lambda *a, **k: MagicMock()
    )

    payload = {
        "event": "tool:pre",
        "workspace": "/ws",
        "idempotency_key": "aci-event-v1:durable-key",
        # data.timestamp is required by _validate_data_timestamp (ISO-8601 string)
        "data": {
            "session_id": "sess-durable",
            "timestamp": "2024-01-01T00:00:00+00:00",
        },
    }
    resp = await client.post("/events", json=payload)
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "queued"
    assert body["session_id"] == "sess-durable"

    batch = await main_module.registry.queue_manager.read_batch("sess-durable", 10)
    assert len(batch.lines) == 1
    stored = batch.lines[0]
    # Framing invariant: the durable record carries no literal newline byte, so
    # newline-delimited log framing is safe (read_batch strips the trailing \n).
    assert b"\n" not in stored
    obj = json.loads(stored.decode("utf-8"))
    assert obj["event"] == "tool:pre"
    assert obj["idempotency_key"] == "aci-event-v1:durable-key"
    assert obj["data"]["session_id"] == "sess-durable"


async def test_durable_line_is_drained_to_graph(
    client: httpx.AsyncClient,
    monkeypatch: object,
) -> None:
    """The sticky drainer reads the durable line and dispatches it through
    process_event, then commits the offset (the log drains to empty)."""
    from context_intelligence_server.neo4j_store import Neo4jGraphStore

    proc = AsyncMock()
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "context_intelligence_server.registry.process_event", proc
    )
    monkeypatch.setattr(Neo4jGraphStore, "flush", AsyncMock())  # type: ignore[attr-defined]
    monkeypatch.setattr(Neo4jGraphStore, "close", AsyncMock())  # type: ignore[attr-defined]

    resp = await client.post(
        "/events",
        json={
            "event": "tool:pre",
            "workspace": "/ws",
            # data.timestamp is required by _validate_data_timestamp (ISO-8601 string)
            "data": {
                "session_id": "sess-drain-graph",
                "timestamp": "2024-01-01T00:00:00+00:00",
            },
        },
    )
    assert resp.status_code == 202

    qm = main_module.registry.queue_manager
    for _ in range(400):
        await asyncio.sleep(0.01)
        if (await qm.read_batch("sess-drain-graph", 10)).lines == []:
            break

    assert (await qm.read_batch("sess-drain-graph", 10)).lines == []
    assert proc.await_count >= 1
