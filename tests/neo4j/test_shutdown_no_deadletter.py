"""Behavioral evidence that shutdown does not discard queued events.

The shared Neo4j driver made driver lifetime a cross-session concern: closing
it while a drain worker is still running is no longer "that session's driver
going away", it is *every* session's driver going away mid-flight.

A drainer that meets a closed driver fails its batch, spends its
``max_delivery_attempts`` budget in ~250 ms (5 attempts x the 50 ms
``_DRAIN_POLL_INTERVAL`` backoff), and lands in ``_handle_exhausted_batch`` --
which dead-letters each line AND commits the offset past it. Those are healthy
events that merely happened to be queued when the process stopped, and once
dead-lettered they never replay.

This drives real events through the real registry against a live Neo4j and
asserts the shutdown sequence used by ``lifespan`` (quiesce the drainers, then
close the shared driver) dead-letters nothing.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from context_intelligence_server.config import Settings
from context_intelligence_server.registry import SessionRegistry

pytestmark = pytest.mark.neo4j

# Enough to guarantee undrained lines are still queued when shutdown starts --
# _DRAIN_MAX_BATCH is 100, so this is several batches deep.
EVENT_COUNT = 400


def _event_line(session_id: str, i: int) -> bytes:
    return json.dumps(
        {
            "event": "tool:pre",
            "workspace": "/ws",
            "data": {
                "session_id": session_id,
                "timestamp": "2024-01-01T00:00:00+00:00",
                "tool_name": f"tool-{i}",
            },
        }
    ).encode("utf-8")


@pytest.mark.asyncio
async def test_shutdown_quiesce_then_close_deadletters_nothing(
    neo4j_container: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    settings = Settings(
        neo4j_url=neo4j_container["bolt_url"],
        neo4j_user=neo4j_container["user"],
        neo4j_password=neo4j_container["password"],
        blob_path=str(tmp_path / "blobs"),
        queues_path=str(tmp_path / "queues"),
    )
    monkeypatch.setattr(
        "context_intelligence_server.registry.get_settings", lambda: settings
    )

    reg = SessionRegistry()
    session_id = "shutdown-session"
    qm = reg.queue_manager

    dead_lettered: list[str] = []
    real_dead_letter = qm.dead_letter

    async def spy_dead_letter(sid: str, raw: bytes, error: str) -> None:
        dead_lettered.append(error)
        await real_dead_letter(sid, raw, error)

    monkeypatch.setattr(qm, "dead_letter", spy_dead_letter)

    for i in range(EVENT_COUNT):
        await qm.append(session_id, _event_line(session_id, i))

    reg.get_or_create(session_id, "/ws")
    # Let the drainer get into its loop with work still queued behind it.
    await asyncio.sleep(0.4)

    # The lifespan shutdown sequence, in order. Reversing these two lines is the
    # regression this test exists to catch.
    await reg.shutdown_workers()
    await reg.close_neo4j_driver()

    # Stay on the loop as the server would during its shutdown window: a
    # still-live drainer would burn its retry budget and dead-letter here.
    await asyncio.sleep(2.0)

    assert dead_lettered == [], (
        f"{len(dead_lettered)} healthy queued events were dead-lettered during "
        "shutdown; the drain workers must be quiesced before the shared driver "
        "closes. First error: "
        f"{dead_lettered[0] if dead_lettered else ''}"
    )
    assert reg._neo4j_driver is None
