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


# --------------------------------------------------------------------------
# Deeply-nested sub-agent session ids -- the NAME_MAX incident, end to end.
#
# A 4-5 level sub-agent chain produces a session_id far longer than any
# filename the spool can hold once `commit()`'s `.offset.<32-hex>.tmp`
# suffix is added. Pre-fix this wedged the session permanently: `append()`
# succeeded, `commit()` raised ENAMETOOLONG, the drain worker died, and
# every later event for that session re-ran the same doomed cycle. This is
# the user-visible acceptance check -- ingest -> disk -> drain -> commit,
# through the real HTTP path, with the real filesystem.
# --------------------------------------------------------------------------

_DEEPLY_NESTED_SESSION_ID = (
    "b51ef331-0000-0000-0000-ad108f19540f4f8f_foundation:zen-architect"
    "-3902b5aa81a0423d_self"
    "-d48177fd03794541_foundation:explorer"
    "-a207119980694fe5_foundation:explorer"
    "-0cd27c98a2f947ea_foundation:explorer"
    "-5f91edb0dd7f4e41_foundation:explorer"
    "-7c3e1a0b9d4f42aa_foundation:explorer"
)


async def test_deeply_nested_session_id_ingests_drains_and_commits(
    client: httpx.AsyncClient,
) -> None:
    """A session_id too long to name a spool file still ingests AND drains.

    Asserts the three things that were broken in production:
      1. POST /events returns 202 (ingest accepted),
      2. every file the spool wrote fits inside NAME_MAX, and
      3. `commit()` advances the offset -- i.e. the session is no longer
         permanently wedged.

    Graph identity is unaffected: the ORIGINAL session_id round-trips in
    the response and in the stored envelope. Only the on-disk partition
    key is folded.
    """
    session_id = _DEEPLY_NESTED_SESSION_ID
    assert len(session_id.encode("utf-8")) > 211, "fixture must exceed the key budget"

    payload = {
        "event": "tool:pre",
        "workspace": "/ws",
        "data": {
            "session_id": session_id,
            "timestamp": "2024-01-01T00:00:00+00:00",
        },
    }
    resp = await client.post("/events", json=payload)
    assert resp.status_code == 202
    # Graph identity is NOT folded -- the long id round-trips verbatim.
    assert resp.json()["session_id"] == session_id

    qm = main_module.registry.queue_manager

    # Every artifact actually written fits inside NAME_MAX, with room for
    # the widest suffix commit() will later append.
    written = list(qm._dir.iterdir())
    assert written, "the event must be durably on disk"
    for path in written:
        assert len(path.name.encode("utf-8")) <= 255, path.name

    # Recover the worker key the way the server itself does -- from the
    # filename stem (QueueManager._all_worker_keys) -- so this test asserts
    # on observable disk state, not on a private helper.
    logs = list(qm._dir.glob("*.log"))
    assert len(logs) == 1, logs
    worker_key = logs[0].stem
    assert len(worker_key.encode("utf-8")) <= 211

    batch = await qm.read_batch(worker_key, 10)
    assert len(batch.lines) == 1
    assert json.loads(batch.lines[0])["data"]["session_id"] == session_id

    # The wedge: pre-fix this raised OSError(ENAMETOOLONG) and killed the
    # drain worker. The offset must now actually advance.
    await qm.commit(worker_key, batch.end_offset)
    assert (await qm.read_batch(worker_key, 10)).lines == []
