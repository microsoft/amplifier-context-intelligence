"""Real-Neo4j behavioral gates for working_dir populate-if-missing.

``ensure_session_node`` guards against overwriting an already-attributed
session in Python, but that guard only covers writers that went through this
process's in-memory node cache. Two workers draining concurrently, a replayed
batch, or a second server instance all reach the MERGE directly — so the
"never re-attribute" rule is enforced a second time in Cypher:

    SET n.working_dir = coalesce(n.working_dir, row.working_dir)

These gates drive the REAL flush path against a REAL Neo4j and verify the
resulting property with a raw sync driver. A unit test can only prove the
Cypher string contains ``coalesce``; only this proves Neo4j honours it.

ISOLATION GUARANTEE
--------------------
Uses ONLY the ephemeral Docker container from tests/neo4j/conftest.py
(random ports, remove=True, fixture-injected credentials). No production
Neo4j endpoint is referenced anywhere in this module.

Gates
------
Gate A — first write lands: a Session node is created carrying working_dir.
Gate B — second write with a DIFFERENT value does not overwrite it.
Gate C — a row with NO working_dir does not null out an existing value.
"""

from __future__ import annotations

from typing import Any

import pytest
from neo4j import GraphDatabase

from context_intelligence_server.neo4j_store import Neo4jGraphStore

pytestmark = pytest.mark.neo4j


def _sync_driver(container: dict[str, Any]):  # type: ignore[return]
    """Return a synchronous Neo4j driver for verification/teardown queries."""
    return GraphDatabase.driver(
        container["bolt_url"],
        auth=(container["user"], container["password"]),
    )


def _query_working_dir(
    container: dict[str, Any], node_id: str, workspace: str
) -> str | None:
    """Return ``n.working_dir`` for (node_id, workspace), or None if unset."""
    driver = _sync_driver(container)
    try:
        with driver.session() as session:
            rec = session.run(
                "MATCH (n {node_id: $nid, workspace: $ws}) RETURN n.working_dir AS wd",
                nid=node_id,
                ws=workspace,
            ).single()
            return rec["wd"] if rec else None
    finally:
        driver.close()


def _cleanup_workspace(container: dict[str, Any], workspace: str) -> None:
    """Delete all nodes (and their relationships) in *workspace*."""
    driver = _sync_driver(container)
    try:
        with driver.session() as session:
            session.run("MATCH (n {workspace: $ws}) DETACH DELETE n", ws=workspace)
    finally:
        driver.close()


def _store(container: dict[str, Any], workspace: str) -> Neo4jGraphStore:
    return Neo4jGraphStore(
        uri=container["bolt_url"],
        auth=(container["user"], container["password"]),
        workspace=workspace,
    )


@pytest.mark.neo4j
class TestWorkingDirCoalesceGates:
    """working_dir is written once and never re-written, enforced by Neo4j."""

    async def test_first_write_lands_on_the_session_node(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        ws = "wd-gate-a"
        sid = "wd-a-session"
        store = _store(neo4j_container, ws)
        try:
            await store.upsert_node(
                sid,
                {
                    "labels": ["Session"],
                    "status": "running",
                    "session_id": sid,
                    "working_dir": "/home/user/project",
                },
            )
            await store.flush()
            assert _query_working_dir(neo4j_container, sid, ws) == "/home/user/project"
        finally:
            await store.close()
            _cleanup_workspace(neo4j_container, ws)

    async def test_second_write_does_not_overwrite(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """A later flush carrying a DIFFERENT working_dir is ignored.

        This is the concurrency case the Python guard cannot cover: a second
        writer never consulted this process's cache, so only coalesce keeps the
        session attributed to the folder it actually ran in.
        """
        ws = "wd-gate-b"
        sid = "wd-b-session"
        first = _store(neo4j_container, ws)
        second = _store(neo4j_container, ws)
        try:
            await first.upsert_node(
                sid,
                {
                    "labels": ["Session"],
                    "status": "running",
                    "session_id": sid,
                    "working_dir": "/original/path",
                },
            )
            await first.flush()

            await second.upsert_node(
                sid,
                {
                    "labels": ["Session"],
                    "status": "running",
                    "session_id": sid,
                    "working_dir": "/different/path",
                },
            )
            await second.flush()

            assert _query_working_dir(neo4j_container, sid, ws) == "/original/path"
        finally:
            await first.close()
            await second.close()
            _cleanup_workspace(neo4j_container, ws)

    async def test_row_without_working_dir_does_not_null_existing(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """Rows carrying no working_dir leave an attributed session untouched.

        Most Session writes (status flips, label enrichment, touch_session)
        carry no working_dir at all. Those must be no-ops for the property,
        not a coalesce against null that erases it.
        """
        ws = "wd-gate-c"
        sid = "wd-c-session"
        store = _store(neo4j_container, ws)
        try:
            await store.upsert_node(
                sid,
                {
                    "labels": ["Session"],
                    "status": "running",
                    "session_id": sid,
                    "working_dir": "/home/user/project",
                },
            )
            await store.flush()

            # A later enrichment write with no working_dir key at all.
            await store.upsert_node(
                sid, {"labels": ["Session", "RootSession"], "status": "closed"}
            )
            await store.flush()

            assert _query_working_dir(neo4j_container, sid, ws) == "/home/user/project"
        finally:
            await store.close()
            _cleanup_workspace(neo4j_container, ws)
