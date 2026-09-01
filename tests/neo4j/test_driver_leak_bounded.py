"""Behavioral evidence that the per-session driver leak is gone.

Drives many sessions through the real registry construction path
(``get_or_create`` -> shared ``Neo4jGraphStore``) against a live Neo4j, doing
a real write per session so bolt connections are actually opened, then queries
the server's own connection list to prove the open bolt connections stay
bounded by the pool (never scale with the session count) and are released on
driver close.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from context_intelligence_server.config import Settings
from context_intelligence_server.registry import SessionRegistry
from neo4j import AsyncGraphDatabase  # type: ignore[attr-defined]

pytestmark = pytest.mark.neo4j

# Many more sessions than the pool can hold: if each session built its own
# driver (the leak), open bolt connections would scale with this number.
SESSION_COUNT = 30
# Small, explicit pool so the bound is unmistakable in the observed count.
POOL_SIZE = 8

# The Python driver identifies itself with this user-agent prefix; the probe
# driver below uses a different one so it is excluded from the count.
_PY_DRIVER_UA_PREFIX = "neo4j-python"
_PROBE_UA = "leak-probe/1.0"

_COUNT_QUERY = (
    "CALL dbms.listConnections() YIELD connector, userAgent "
    f"WHERE connector = 'bolt' AND userAgent STARTS WITH '{_PY_DRIVER_UA_PREFIX}' "
    "RETURN count(*) AS c"
)


async def _count_python_bolt_connections(probe_driver: Any) -> int:
    """Return the number of open bolt connections opened by the Python driver.

    Excludes the probe driver itself (distinct user-agent) so the count
    reflects only the registry's shared driver.
    """
    async with probe_driver.session() as session:
        result = await session.run(_COUNT_QUERY)
        record = await result.single()
        return int(record["c"])


@pytest.mark.asyncio
async def test_bolt_connections_stay_bounded_and_release(
    neo4j_container: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    capsys: Any,
) -> None:
    bolt_url = neo4j_container["bolt_url"]
    user = neo4j_container["user"]
    password = neo4j_container["password"]

    # Real settings pointing at the container, with a small bounded pool and
    # writable scratch paths. The registry resolves its shared driver from these.
    settings = Settings(
        neo4j_url=bolt_url,
        neo4j_user=user,
        neo4j_password=password,
        neo4j_max_connection_pool_size=POOL_SIZE,
        blob_path=str(tmp_path / "blobs"),
        queues_path=str(tmp_path / "queues"),
    )
    monkeypatch.setattr(
        "context_intelligence_server.registry.get_settings",
        lambda: settings,
    )

    probe_driver = AsyncGraphDatabase.driver(
        bolt_url, auth=(user, password), user_agent=_PROBE_UA
    )

    reg = SessionRegistry()

    baseline = await _count_python_bolt_connections(probe_driver)

    async def run_session(i: int) -> None:
        # Exactly the construction path the leak came from: the registry builds
        # (or reuses) its one shared driver and injects it into this session's
        # store. The write forces a real bolt connection through the pool.
        worker = reg.get_or_create(f"leak-session-{i}", f"/workspace/{i}")
        graph = worker.services.graph
        await graph.upsert_node(
            f"node-{i}", {"label": "Event", "session": f"leak-session-{i}"}
        )
        await graph.flush()

    await asyncio.gather(*(run_session(i) for i in range(SESSION_COUNT)))

    during_load = await _count_python_bolt_connections(probe_driver)

    # Stop the idle drain workers before tearing the driver down.
    for worker in list(reg._workers.values()):
        if worker.task is not None:
            worker.task.cancel()
    await asyncio.gather(
        *(w.task for w in reg._workers.values() if w.task is not None),
        return_exceptions=True,
    )

    # Reclaim: closing the one shared driver must release every bolt connection.
    await reg.close_neo4j_driver()

    # Poll briefly for the server to observe the closed connections.
    after_close = during_load
    for _ in range(20):
        after_close = await _count_python_bolt_connections(probe_driver)
        if after_close == 0:
            break
        await asyncio.sleep(0.25)

    await probe_driver.close()

    with capsys.disabled():
        print(
            f"\n[driver-leak evidence] sessions={SESSION_COUNT} pool_size={POOL_SIZE} "
            f"baseline={baseline} during_load={during_load} after_close={after_close}"
        )

    # One shared driver was built for all sessions, not one per session.
    assert reg._neo4j_driver is None  # closed above
    # The core proof: open bolt connections are bounded by the pool and do NOT
    # scale with the session count.
    assert during_load <= POOL_SIZE + 2, (
        f"open bolt connections ({during_load}) exceeded the pool bound "
        f"({POOL_SIZE}); a per-session driver would scale with {SESSION_COUNT}"
    )
    assert during_load < SESSION_COUNT, (
        f"open bolt connections ({during_load}) scaled with session count "
        f"({SESSION_COUNT}) -- the leak is not fixed"
    )
    # Reclaim proof: the shared driver's pool is fully released on close.
    assert after_close == 0, (
        f"bolt connections not released after driver close (still {after_close})"
    )
