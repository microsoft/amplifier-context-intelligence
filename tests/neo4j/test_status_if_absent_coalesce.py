"""Real-Neo4j behavioral gates for status_if_absent populate-if-missing.

``ensure_session_node`` never writes ``status`` authoritatively: it writes
``status_if_absent="running"`` in both its existing-node and create branches.
That guard only covers writers that went through this process's in-memory
node cache (``_seen_sessions``) and buffer. Each per-session drain worker owns
its OWN ``Neo4jGraphStore`` (own buffer) and OWN ``HookStateService`` (own
``_seen_sessions`` cache) — so a CROSS-SESSION reference (a child's
session:start naming its parent, or a delegation naming its sub-session)
always reaches a DIFFERENT worker's cold cache. The "never revert a completed
session to running" rule is therefore enforced a second time in Cypher:

    SET n.status = coalesce(n.status, row.status_if_absent)

applied AFTER ``SET n += row.props`` so an authoritative ``status`` carried in
the SAME row (e.g. session:end's ``props.status = "completed"``) is already on
``n`` by the time the coalesce runs.

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
Gate A — first write lands: a fresh node gets status="running" via
         status_if_absent, and the pseudo-key never persists as a property.
Gate B — THE REPORTED BUG at the durable layer: a node already "completed"
         is not reverted to "running" by a SECOND, independent
         Neo4jGraphStore instance (the cross-worker case).
Gate C — intra-flush: one row carrying BOTH an authoritative status AND
         status_if_absent resolves to the authoritative value (proves clause
         order in the Cypher).
Gate D — the interleaved race Python cannot cover: a stub is buffered before
         a concurrent writer's "completed" lands, then flushed AFTER it —
         still "completed".
Gate E — end-to-end cross-session via HookStateService: a real parent session
         ends "completed" through one HookStateService/flush; a SECOND
         HookStateService with a cold _seen_sessions cache (the exact call
         handlers/data_layer_2/session.py:221 makes for a child naming its
         parent) calls ensure_session_node and flushes. Parent must stay
         "completed".

RED-BEFORE-GREEN-AFTER NOTE ON GATES B AND D
---------------------------------------------
Gates B and D poke ``Neo4jGraphStore`` directly with the literal
``status_if_absent`` key -- but that key is FIX-ONLY vocabulary: on
unreverted-fix source, ``_build_node_props`` does not know to exclude it, so
it is neither routed to the coalesce clause nor kept off the node -- it just
rides along in ``row.props`` as an ordinary, unrecognized property, and
``SET n += row.props`` adds it verbatim WITHOUT ever touching ``n.status``
(there is no literal "status" key in that write to begin with). The
consequence: a bare ``result["status"] == "completed"`` assertion in Gates B
and D is satisfied on unfixed source too, but for the wrong reason -- not
because coalesce protected it, but because the write never targeted
``status`` in the first place. That assertion alone does not discriminate
fixed from unfixed code, so each gate ALSO asserts
``has_status_if_absent is False`` (the property must never persist) --
that assertion genuinely differs: it fails on unfixed source (the key leaks
as a real property, since nothing excludes it) and passes on fixed source
(``_build_node_props`` excludes it AND the coalesce clause consumes it).
Gate E does not have this issue: it drives the real ``ensure_session_node``
caller, which emits a genuinely different literal key at each version of
``services.py`` (``"status": "running"`` unfixed vs. ``"status_if_absent":
"running"`` fixed), so its ``status == "completed"`` assertion is a direct,
unqualified regression proof on its own.
"""

from __future__ import annotations

from typing import Any

import pytest
from neo4j import GraphDatabase

from context_intelligence_server.neo4j_store import Neo4jGraphStore
from context_intelligence_server.services import HookStateService

pytestmark = pytest.mark.neo4j


def _sync_driver(container: dict[str, Any]):  # type: ignore[return]
    """Return a synchronous Neo4j driver for verification/teardown queries."""
    return GraphDatabase.driver(
        container["bolt_url"],
        auth=(container["user"], container["password"]),
    )


def _query_status(
    container: dict[str, Any], node_id: str, workspace: str
) -> dict[str, Any] | None:
    """Return {"status": ..., "has_status_if_absent": bool} for (node_id, workspace).

    ``has_status_if_absent`` proves the pseudo-key was never persisted as a
    real node property — independent of the store under test, using a raw
    sync driver query.
    """
    driver = _sync_driver(container)
    try:
        with driver.session() as session:
            rec = session.run(
                "MATCH (n {node_id: $nid, workspace: $ws}) "
                "RETURN n.status AS status, "
                "n.status_if_absent IS NOT NULL AS has_status_if_absent",
                nid=node_id,
                ws=workspace,
            ).single()
            if rec is None:
                return None
            return {
                "status": rec["status"],
                "has_status_if_absent": rec["has_status_if_absent"],
            }
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
class TestStatusIfAbsentCoalesceGates:
    """status_if_absent is populate-if-missing, enforced by Neo4j."""

    async def test_gate_a_first_write_lands_on_the_session_node(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """Gate A: a fresh node gets status="running" via status_if_absent,
        and the pseudo-key itself never persists as a real property."""
        ws = "sia-gate-a"
        sid = "sia-a-session"
        store = _store(neo4j_container, ws)
        try:
            await store.upsert_node(
                sid,
                {
                    "labels": ["Session"],
                    "status_if_absent": "running",
                    "session_id": sid,
                },
            )
            await store.flush()
            result = _query_status(neo4j_container, sid, ws)
            assert result is not None
            assert result["status"] == "running"
            assert result["has_status_if_absent"] is False, (
                "status_if_absent must NOT persist as a real node property"
            )
        finally:
            await store.close()
            _cleanup_workspace(neo4j_container, ws)

    async def test_gate_b_completed_session_survives_cross_worker_stub(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """Gate B — THE REPORTED BUG at the durable layer.

        A node is flushed to status="completed" by one store. A SECOND,
        independent Neo4jGraphStore instance (its own buffer — the
        cross-worker case) then writes status_if_absent="running" and
        flushes. The completed status must survive.
        """
        ws = "sia-gate-b"
        sid = "sia-b-session"
        first = _store(neo4j_container, ws)
        second = _store(neo4j_container, ws)
        try:
            await first.upsert_node(
                sid,
                {
                    "labels": ["Session"],
                    "status": "completed",
                    "session_id": sid,
                },
            )
            await first.flush()

            await second.upsert_node(
                sid,
                {
                    "labels": ["Session"],
                    "status_if_absent": "running",
                    "session_id": sid,
                },
            )
            await second.flush()

            result = _query_status(neo4j_container, sid, ws)
            assert result is not None
            assert result["status"] == "completed", (
                f"REGRESSION: cross-worker stub reverted status; got {result}"
            )
            # NOTE: for THIS specific write shape (the stub carries ONLY
            # status_if_absent, no literal "status" key), "status" surviving
            # unchanged is NOT by itself proof of the fix -- see this
            # module's docstring note on Gate B/D for why the assertion
            # below (the property must never leak) is the one that actually
            # discriminates fixed from unfixed source at this layer.
            assert result["has_status_if_absent"] is False, (
                "status_if_absent must never persist as a raw node property "
                "-- on unfixed source this leaks because _build_node_props "
                "does not exclude it"
            )
        finally:
            await first.close()
            await second.close()
            _cleanup_workspace(neo4j_container, ws)

    async def test_gate_c_intra_row_authoritative_status_wins(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """Gate C: ONE row carrying BOTH an authoritative status AND
        status_if_absent resolves to the authoritative value — proves the
        Cypher clause order (SET n += row.props BEFORE the coalesce)."""
        ws = "sia-gate-c"
        sid = "sia-c-session"
        store = _store(neo4j_container, ws)
        try:
            await store.upsert_node(
                sid,
                {
                    "labels": ["Session"],
                    "status": "completed",
                    "status_if_absent": "running",
                    "session_id": sid,
                },
            )
            await store.flush()
            result = _query_status(neo4j_container, sid, ws)
            assert result is not None
            assert result["status"] == "completed", (
                f"authoritative status in the same row must win; got {result}"
            )
            assert result["has_status_if_absent"] is False, (
                "status_if_absent must never persist as a raw node property"
            )
        finally:
            await store.close()
            _cleanup_workspace(neo4j_container, ws)

    async def test_gate_d_interleaved_flush_race(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """Gate D — the interleaved race Python cannot cover.

        store1 buffers the stub (status_if_absent) FIRST — as it would on
        the create-branch path where get_node legitimately saw nothing yet.
        store2 then writes and flushes "completed" while store1's stub is
        still only buffered. store1 flushes SECOND. The completed status,
        written to Neo4j strictly between store1's buffer and store1's
        flush, must survive store1's later flush.
        """
        ws = "sia-gate-d"
        sid = "sia-d-session"
        store1 = _store(neo4j_container, ws)
        store2 = _store(neo4j_container, ws)
        try:
            # store1 buffers the stub but does NOT flush yet.
            await store1.upsert_node(
                sid,
                {
                    "labels": ["Session"],
                    "status_if_absent": "running",
                    "session_id": sid,
                },
            )

            # store2 independently writes and flushes the authoritative
            # completed status, landing in Neo4j strictly between store1's
            # buffer and store1's eventual flush.
            await store2.upsert_node(
                sid,
                {
                    "labels": ["Session"],
                    "status": "completed",
                    "session_id": sid,
                },
            )
            await store2.flush()

            # store1 flushes its buffered stub AFTER store2's completed write
            # is already durable.
            await store1.flush()

            result = _query_status(neo4j_container, sid, ws)
            assert result is not None
            assert result["status"] == "completed", (
                f"REGRESSION: interleaved stub flush reverted status; got {result}"
            )
            # See the note on Gate B: for this write shape the "status"
            # assertion alone survives on unfixed source too (there is no
            # literal "status" key in store1's row to overwrite anything
            # with), so the property-leak assertion below is what actually
            # discriminates fixed from unfixed source at this layer.
            assert result["has_status_if_absent"] is False, (
                "status_if_absent must never persist as a raw node property "
                "-- on unfixed source this leaks because _build_node_props "
                "does not exclude it"
            )
        finally:
            await store1.close()
            await store2.close()
            _cleanup_workspace(neo4j_container, ws)

    async def test_gate_e_cross_session_hookstateservice_end_to_end(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """Gate E — end-to-end cross-session, the actual failure the report
        describes.

        A parent session ends "completed" through its own HookStateService
        and flush. A SECOND, independent HookStateService — its own
        Neo4jGraphStore, its own cold _seen_sessions cache, over the SAME
        Neo4j/workspace — then calls ensure_session_node(parent_id, {}),
        exactly the call handlers/data_layer_2/session.py:221 makes when a
        child's session:start names its parent. After that service's flush,
        the parent must still be "completed".
        """
        ws = "sia-gate-e"
        parent_id = "sia-e-parent"

        parent_store = _store(neo4j_container, ws)
        parent_services = HookStateService(workspace=ws, graph_store=parent_store)
        child_store = _store(neo4j_container, ws)
        child_services = HookStateService(workspace=ws, graph_store=child_store)
        try:
            # Parent session runs its full lifecycle and ends "completed",
            # exactly as session:end (handlers/data_layer_2/session.py) does.
            await parent_services.ensure_session_node(
                parent_id, {"started_at": "2026-01-01T00:00:00Z"}
            )
            await parent_services.graph.upsert_node(
                parent_id, {"status": "completed", "ended_at": "2026-01-01T01:00:00Z"}
            )
            await parent_services.graph.flush()

            # Sanity check: parent genuinely landed as completed before the
            # cross-session reference arrives.
            precondition = _query_status(neo4j_container, parent_id, ws)
            assert precondition is not None
            assert precondition["status"] == "completed"

            # A DIFFERENT worker (its own HookStateService, cold
            # _seen_sessions cache) stubs a cross-session reference to the
            # parent — the exact call a child's session:start makes.
            await child_services.ensure_session_node(parent_id, {})
            await child_services.graph.flush()

            result = _query_status(neo4j_container, parent_id, ws)
            assert result is not None
            assert result["status"] == "completed", (
                f"REGRESSION: cross-session ensure_session_node reverted a "
                f"completed parent session; got {result}"
            )
        finally:
            await parent_store.close()
            await child_store.close()
            _cleanup_workspace(neo4j_container, ws)
