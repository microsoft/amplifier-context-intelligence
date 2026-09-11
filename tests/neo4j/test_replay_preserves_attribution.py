"""Behavioural evidence, against a LIVE Neo4j, that replay keeps attribution.

INCIDENT 2026-09-09 (production, team-shared). ``GET /status`` reported
``dead_letter_total: 82,880`` against ``written_total: 116,954``. Attribution
across the 752 dead-letter worker keys put **78,422 of those events** under a
single error string::

    failed to obtain a connection from the pool within 30.0s (timeout)

They were healthy events. The Neo4j connection pool was momentarily exhausted;
the batch burned ``max_delivery_attempts`` (5 attempts x a flat 50 ms backoff --
a 250 ms timer, not a retry budget); ``_handle_exhausted_batch`` then
dead-lettered every line AND committed the offset past it. Neither a retry nor
boot recovery would ever see them again. The clients had already been told
``202``.

Recovering those events means replaying the ``.dead.jsonl`` files. This test
covers the thing that had to be true BEFORE anyone could safely do that: a
replayed event must keep the contributor it was ingested with.

It runs against a real container rather than a mocked store because the property
under test is a Neo4j write semantic -- ``created_by`` is ``ON CREATE SET``,
write-once -- and a fake graph would happily let a second write "fix" a NULL that
the real database makes permanent. The assertion is on the SPECIFIC forbidden
outcome (attribution destroyed), not on liveness.

NOTE ON SCOPE: an earlier draft of this file also tried to prove the
transient-vs-poison split by pausing the container mid-drain. That test was
REMOVED rather than shipped: the driver's 30 s ``max_transaction_retry_time``
means a paused database makes a flush hang rather than raise, so the pre-fix
code never reached its dead-letter path inside any practical timeout and the
test passed against the unfixed code. A test that cannot fail is the exact
false confidence this incident was made of. That split is covered instead by
``tests/test_registry.py::TestTransientInfraFailuresAreNeverDeadLettered``,
whose five cases ARE proven red on pre-fix code -- one of them by replaying the
verbatim production error string through the real driver exception type.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from context_intelligence_server.config import Settings
from context_intelligence_server.registry import SessionRegistry

pytestmark = pytest.mark.neo4j

# Several batches deep (_DRAIN_MAX_BATCH is 100) so the drainer is still
# working through the queue when the database is taken away.
EVENT_COUNT = 400


def _event_line(session_id: str, i: int, created_by: str = "sam") -> bytes:
    """Encode a line exactly as ``POST /events`` persists it.

    ``created_by`` is included because ``post_events`` stamps the resolved
    contributor into the queued bytes at ingest -- that is what makes the
    dead-letter replay path able to restore attribution.
    """
    return json.dumps(
        {
            "event": "tool:pre",
            "workspace": "/ws",
            "created_by": created_by,
            "data": {
                "session_id": session_id,
                # DISTINCT per event. node_id is make_node_id(session_id,
                # event_name, timestamp) -- a shared timestamp would MERGE all
                # N events onto ONE node and the count assertion below would
                # measure deduplication instead of delivery.
                "timestamp": f"2024-01-01T00:00:{i // 1000:02d}.{i % 1000:03d}+00:00",
                "tool_name": f"tool-{i}",
            },
        }
    ).encode("utf-8")


@pytest.mark.asyncio
async def test_replayed_dead_letters_keep_their_contributor(
    neo4j_container: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """The recovery path, end to end, against a real graph.

    ``created_by`` is written ``ON CREATE SET`` (write-once). Replay runs long
    after a session ended, so ``get_or_create`` takes the CREATE branch -- a
    replay that drops the contributor stamps every recovered node ``NULL``
    permanently. The events would be back in the graph and still absent from
    every per-contributor report, while the ``.dead.jsonl`` proving what
    happened is purged in the same call.

    This is the test that had to exist before anyone could safely replay the
    82,880 dead letters sitting on the production share.
    """
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
    qm = reg.queue_manager
    session_id = "replay-attribution-session"

    # Stand up the adverse state directly: lines sitting in .dead.jsonl exactly
    # as the incident left them, carrying the contributor stamped at ingest.
    for i in range(5):
        await qm.dead_letter(
            session_id,
            _event_line(session_id, i, created_by="sam") + b"\n",
            "failed to obtain a connection from the pool within 30.0s (timeout)",
        )
    assert len(await qm.read_dead_letters(session_id)) == 5

    # Drive the REAL route, not a reimplementation of it. Calling
    # get_or_create(created_by=...) by hand here would prove only that the
    # registry ACCEPTS a contributor -- which it always did. The defect was
    # that the route never PASSED one, so the route is what must be exercised.
    from types import SimpleNamespace

    from context_intelligence_server.routers.queues import replay_dead_letters

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(registry=reg)))
    result = await replay_dead_letters(session_id, request)  # type: ignore[arg-type]
    assert result["replayed"] == 5
    assert await qm.read_dead_letters(session_id) == []

    for _ in range(120):
        await asyncio.sleep(0.5)
        pending = await qm.read_batch(session_id, max_items=1)
        if not pending.records:
            break

    await reg.shutdown_workers()
    await reg.close_neo4j_driver()

    rows = await _fetch_created_by(neo4j_container, session_id)
    assert rows, "replay wrote nothing to the graph"
    nulls = [r for r in rows if r != "sam"]
    assert not nulls, (
        f"{len(nulls)} of {len(rows)} replayed nodes lost their contributor "
        f"(got {set(nulls)!r}, expected 'sam'). created_by is ON CREATE SET -- "
        f"write-once -- so these would be unattributable forever and invisible "
        f"in every per-user report."
    )


async def _fetch_created_by(
    container_info: dict[str, Any], session_id: str
) -> list[str | None]:
    from neo4j import AsyncGraphDatabase

    driver = AsyncGraphDatabase.driver(
        container_info["bolt_url"],
        auth=(container_info["user"], container_info["password"]),
    )
    try:
        async with driver.session() as s:
            result = await s.run(
                "MATCH (n:Event {session_id: $sid}) RETURN n.created_by AS cb",
                sid=session_id,
            )
            return [r["cb"] async for r in result]
    finally:
        await driver.close()
