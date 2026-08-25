"""Adverse-state test: shared driver survives one session's close while
another session is mid-drain.

Two Neo4jGraphStore instances share one driver (mirrors SessionRegistry's
per-session construction). Closing session A must not disturb session B's
in-flight write; the shared driver is closed exactly once, by its owner,
after both sessions are done with it.
"""

from __future__ import annotations

from typing import Any

import pytest
from context_intelligence_server.neo4j_store import Neo4jGraphStore
from neo4j import AsyncGraphDatabase  # type: ignore[attr-defined]

pytestmark = pytest.mark.neo4j


@pytest.mark.asyncio
async def test_session_a_close_does_not_disrupt_session_b(
    neo4j_container: dict[str, Any],
) -> None:
    shared_driver = AsyncGraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )

    store_a = Neo4jGraphStore(
        uri=neo4j_container["bolt_url"], driver=shared_driver, workspace="session-a"
    )
    store_b = Neo4jGraphStore(
        uri=neo4j_container["bolt_url"], driver=shared_driver, workspace="session-b"
    )

    await store_b.upsert_node("node-b", {"label": "Event"})

    # Session A finalizes and closes its store while B still has unflushed
    # work buffered -- this is the adverse state: A's close must not touch
    # the driver B is still using.
    await store_a.close()

    # B's write still lands: the shared driver was never closed under it.
    await store_b.flush()
    fetched = await store_b.get_node("node-b")
    assert fetched is not None

    await store_b.close()

    # The shared driver is still open after both stores are done with it --
    # neither store owned it. Its owner closes it exactly once, at shutdown.
    async with shared_driver.session() as session:
        result = await session.run("RETURN 1 AS one")
        record = await result.single()
        assert record is not None
        assert record["one"] == 1

    await shared_driver.close()
