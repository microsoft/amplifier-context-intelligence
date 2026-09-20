"""Real-Neo4j durability gates for cleanup-only session finalization.

Uses the disposable, random-port Neo4j fixture only. Every test confines its
write and cleanup to a unique workspace, so it never queries or deletes state
outside its own test fixture.
"""

from __future__ import annotations

from typing import Any

import pytest
from neo4j import GraphDatabase

from context_intelligence_server.neo4j_store import Neo4jGraphStore

pytestmark = pytest.mark.neo4j


def _cleanup_workspace(container: dict[str, Any], workspace: str) -> None:
    driver = GraphDatabase.driver(
        container["bolt_url"],
        auth=(container["user"], container["password"]),
    )
    try:
        with driver.session() as session:
            session.run(
                "MATCH (n {workspace: $workspace}) DETACH DELETE n", workspace=workspace
            )
    finally:
        driver.close()


def _store(container: dict[str, Any], workspace: str) -> Neo4jGraphStore:
    return Neo4jGraphStore(
        uri=container["bolt_url"],
        auth=(container["user"], container["password"]),
        workspace=workspace,
    )


async def test_durable_cleanup_completion_requires_persisted_completed_session(
    neo4j_container: dict[str, Any],
) -> None:
    workspace = "durable-cleanup-gate"
    other_workspace = "durable-cleanup-other"
    completed_id = "completed"
    buffered_id = "buffered"
    running_id = "running"
    non_session_id = "non-session"
    stores = [
        _store(neo4j_container, workspace),
        _store(neo4j_container, other_workspace),
    ]
    store, other_store = stores
    try:
        await store.upsert_node(
            buffered_id, {"labels": ["Session"], "status": "completed"}
        )
        assert not await store.is_session_durably_completed(buffered_id)

        await store.upsert_node(
            completed_id, {"labels": ["Session"], "status": "completed"}
        )
        await store.upsert_node(
            running_id, {"labels": ["Session"], "status": "running"}
        )
        await store.upsert_node(
            non_session_id, {"labels": ["Event"], "status": "completed"}
        )
        await store.flush()
        assert await store.is_session_durably_completed(buffered_id)
        assert not await other_store.is_session_durably_completed(completed_id)
        await other_store.upsert_node(
            completed_id, {"labels": ["Session"], "status": "completed"}
        )
        assert not await other_store.is_session_durably_completed(completed_id)
        await other_store.flush()
        assert await other_store.is_session_durably_completed(completed_id)

        assert await store.is_session_durably_completed(completed_id)
        assert not await store.is_session_durably_completed(running_id)
        assert not await store.is_session_durably_completed(non_session_id)
        assert not await store.is_session_durably_completed("missing")
    finally:
        for candidate in stores:
            await candidate.close()
        _cleanup_workspace(neo4j_container, workspace)
        _cleanup_workspace(neo4j_container, other_workspace)
