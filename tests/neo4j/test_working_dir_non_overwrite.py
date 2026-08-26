"""Live E2E tests: working_dir is never clobbered at the DB level.

Root cause being guarded here: prior to this fix, the ONLY guarantee that an
already-populated ``working_dir`` is never overwritten lived in the Python layer
(``services.py``'s ``if data.get("working_dir") and not existing.get("working_dir")``
populate-if-missing check). That check reads the buffered/graph node BEFORE the
write is issued, so it cannot protect against a cross-writer or replica race: a
second concurrent flush that read the node before the first write committed would
still see no working_dir, and its ``SET n += row.props`` would blindly overwrite
whatever the first writer just set.

The fix adds a genuine DB-level guarantee: the Session-node MERGE in
``_write_batch`` excludes working_dir from the blind ``+=`` merge and instead
applies ``SET n.working_dir = coalesce(n.working_dir, row.working_dir)`` -- a
non-overwrite rule enforced by Neo4j itself, at the same MERGE lock hold, immune
to read-then-write races between writers.

Requires Docker and the docker Python package. Skip-if-absent via the
``neo4j_container`` fixture in tests/neo4j/conftest.py.

Run explicitly:
    cd amplifier-context-intelligence
    uv run pytest tests/neo4j/test_working_dir_non_overwrite.py -v -m neo4j
"""

from __future__ import annotations

from typing import Any

import pytest
from context_intelligence_server.neo4j_store import Neo4jGraphStore
from neo4j import GraphDatabase

pytestmark = pytest.mark.neo4j


async def _flush_session_node(
    container: dict[str, Any], node_id: str, data: dict[str, Any]
) -> None:
    """Drive a single Session node through the real flush path via a FRESH store.

    A fresh ``Neo4jGraphStore`` per call simulates independent writers (e.g. two
    drainer workers, or a writer racing a replica) rather than two writes queued
    through the same in-process buffer -- the scenario the Python-layer
    populate-if-missing check cannot see.
    """
    store = Neo4jGraphStore(
        uri=container["bolt_url"],
        auth=(container["user"], container["password"]),
        workspace="test",
    )
    try:
        await store.upsert_node(node_id, {"labels": ["Session"], **data})
        await store.flush()
    finally:
        await store.close()


def _read_working_dir(container: dict[str, Any], node_id: str) -> Any:
    driver = GraphDatabase.driver(
        container["bolt_url"], auth=(container["user"], container["password"])
    )
    try:
        with driver.session() as session:
            rec = session.run(
                "MATCH (n:Session {node_id: $nid, workspace: $ws}) "
                "RETURN n.working_dir AS wd",
                nid=node_id,
                ws="test",
            ).single()
        assert rec is not None, f"Session node {node_id!r} was not written"
        return rec["wd"]
    finally:
        driver.close()


async def test_existing_working_dir_survives_conflicting_later_write(
    neo4j_container: dict[str, Any],
) -> None:
    """DB-level guarantee: an already-set working_dir is never overwritten,
    even by a second independent writer (simulating a cross-writer/replica race).

    RED  (unfixed): ``SET n += row.props`` blindly overwrites -> working_dir
                    becomes "/y" -> this assertion fails.
    GREEN (fixed):  ``coalesce(n.working_dir, row.working_dir)`` keeps the
                    existing value -> working_dir stays "/x".
    """
    node_id = "sess-wd-no-clobber-live"

    # Writer 1: establishes working_dir="/x".
    await _flush_session_node(
        neo4j_container,
        node_id,
        {"status": "running", "working_dir": "/x"},
    )
    assert _read_working_dir(neo4j_container, node_id) == "/x"

    # Writer 2 (independent store instance): tries to write a DIFFERENT
    # working_dir for the SAME node -- must be rejected at the DB level.
    await _flush_session_node(
        neo4j_container,
        node_id,
        {"status": "running", "working_dir": "/y"},
    )

    assert _read_working_dir(neo4j_container, node_id) == "/x", (
        "An already-populated working_dir must never be overwritten by a "
        "later/concurrent writer -- DB-level coalesce guarantee failed"
    )


async def test_working_dir_fills_gap_at_db_level_when_previously_absent(
    neo4j_container: dict[str, Any],
) -> None:
    """DB-level populate-if-missing: a node with no working_dir gets filled in
    by a later write that supplies one (coalesce(null, value) -> value).

    Mirrors the Python-layer guarantee (services.py) but proves it also holds
    purely at the DB level, independent of the in-process buffer.
    """
    node_id = "sess-wd-fill-gap-live"

    # Writer 1: no working_dir supplied.
    await _flush_session_node(neo4j_container, node_id, {"status": "running"})
    assert _read_working_dir(neo4j_container, node_id) is None

    # Writer 2: supplies working_dir for the first time.
    await _flush_session_node(
        neo4j_container,
        node_id,
        {"status": "running", "working_dir": "/first-value"},
    )

    assert _read_working_dir(neo4j_container, node_id) == "/first-value", (
        "working_dir must be filled in at the DB level once a writer supplies "
        "a value for a node that previously had none"
    )


async def test_working_dir_absent_write_does_not_clear_existing_value(
    neo4j_container: dict[str, Any],
) -> None:
    """A later write that omits working_dir entirely must not null out an
    already-set value (coalesce(n.working_dir, null) -> unchanged).
    """
    node_id = "sess-wd-absent-no-clear-live"

    await _flush_session_node(
        neo4j_container, node_id, {"status": "running", "working_dir": "/keep-me"}
    )
    assert _read_working_dir(neo4j_container, node_id) == "/keep-me"

    # Second write carries no working_dir key at all (e.g. a touch/heartbeat event).
    await _flush_session_node(neo4j_container, node_id, {"status": "still-running"})

    assert _read_working_dir(neo4j_container, node_id) == "/keep-me", (
        "A write that omits working_dir must never null out an already-set value"
    )
