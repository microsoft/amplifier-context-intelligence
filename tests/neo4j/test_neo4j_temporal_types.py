"""Tier 2 Neo4j-backed integration tests for temporal type conversion.

These tests exercise the full write path:

    upsert_node -> flush() -> _convert_temporal_props -> _sanitize_properties -> driver write

and verify that properties registered in TEMPORAL_PROPS land in a real Neo4j
instance as native ZONED DATETIME values — not as plain strings.

Every test is marked @pytest.mark.neo4j and consumes the isolated throwaway
``neo4j_services`` fixture from tests/neo4j/conftest.py.  The production
context-intelligence-neo4j container is NEVER touched.

Run explicitly:
    uv run pytest tests/neo4j/test_neo4j_temporal_types.py -v -m neo4j
"""

from __future__ import annotations

from typing import Any

import pytest


# ---------------------------------------------------------------------------
# TestNodeTemporalTypes
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestNodeTemporalTypes:
    """Verify that ISO-string temporal properties are stored as ZONED DATETIME in Neo4j."""

    async def test_started_at_is_zoned_datetime(self, neo4j_services: Any) -> None:
        """started_at written as ISO string must land as ZONED DATETIME NOT NULL in Neo4j.

        Regression guard for Phase 1: _convert_temporal_props must convert the
        ISO-8601 string '2026-01-01T00:00:00Z' to a Python datetime before the
        Neo4j driver writes it, so Neo4j stores a native ZONED DATETIME rather
        than a plain STRING.

        If this test FAILS with 'STRING NOT NULL', Phase 1 is not complete.
        """
        await neo4j_services.graph.upsert_node(
            "temporal-node-1",
            {
                "labels": ["Session"],
                "node_id": "temporal-node-1",
                "started_at": "2026-01-01T00:00:00Z",
            },
        )
        await neo4j_services.graph.flush()

        records = await neo4j_services.graph.execute_query(
            "MATCH (n:Session {node_id: $nid}) RETURN valueType(n.started_at) AS vt",
            {"nid": "temporal-node-1"},
            workspace="*",
        )

        assert len(records) == 1, (
            f"Expected exactly 1 Session node with node_id='temporal-node-1', "
            f"got {len(records)} records"
        )
        actual_type = records[0]["vt"]
        assert actual_type == "ZONED DATETIME NOT NULL", (
            f"started_at must be stored as 'ZONED DATETIME NOT NULL' in Neo4j, "
            f"but valueType() returned {actual_type!r}. "
            "This means _convert_temporal_props did not convert the ISO string to a "
            "Python datetime before the driver write. Phase 1 is not complete."
        )

    async def test_last_updated_is_zoned_datetime(self, neo4j_services: Any) -> None:
        """last_updated written as ISO string must land as ZONED DATETIME NOT NULL in Neo4j.

        last_updated is the only temporal field that does NOT end in ``_at``.
        It is listed explicitly in TEMPORAL_PROPS so that the suffix-heuristic
        shortcut (``endswith('_at')``) can never silently miss it.

        If this test FAILS with 'STRING NOT NULL', last_updated is missing from
        TEMPORAL_PROPS in neo4j_store.py.
        """
        await neo4j_services.graph.upsert_node(
            "temporal-node-lu",
            {
                "labels": ["Session"],
                "node_id": "temporal-node-lu",
                "last_updated": "2026-03-15T08:30:00Z",
            },
        )
        await neo4j_services.graph.flush()

        records = await neo4j_services.graph.execute_query(
            "MATCH (n:Session {node_id: $nid}) RETURN valueType(n.last_updated) AS vt",
            {"nid": "temporal-node-lu"},
            workspace="*",
        )

        assert len(records) == 1, (
            f"Expected exactly 1 Session node with node_id='temporal-node-lu', "
            f"got {len(records)} records"
        )
        vt = records[0]["vt"]
        assert vt == "ZONED DATETIME NOT NULL", (
            f"last_updated must be stored as 'ZONED DATETIME NOT NULL' in Neo4j, "
            f"but valueType() returned {vt!r}. "
            "Check that 'last_updated' is present in TEMPORAL_PROPS in neo4j_store.py."
        )


# ---------------------------------------------------------------------------
# TestEdgeTemporalTypes
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestEdgeTemporalTypes:
    """Verify that occurred_at on edges is stored as ZONED DATETIME in Neo4j."""

    @pytest.mark.parametrize("edge_type", ["HAS_EVENT", "HAS_SUBSESSION", "FORKED"])
    async def test_edge_occurred_at_is_zoned_datetime(
        self, neo4j_services: Any, edge_type: str
    ) -> None:
        """occurred_at on an edge written as ISO string must land as ZONED DATETIME NOT NULL.

        The edge MERGE Cypher uses MATCH (src) MATCH (dst), so both endpoint
        nodes MUST exist in Neo4j before the edge flush runs.  This test
        upserts and flushes both nodes first, then upserts the edge.

        Parametrized over the three edge types that carry occurred_at:
        HAS_EVENT, HAS_SUBSESSION, FORKED.
        """
        src_id = f"edge-src-{edge_type.lower()}"
        dst_id = f"edge-dst-{edge_type.lower()}"

        # Endpoint nodes must exist before edge MERGE (MATCH requires them in Neo4j)
        await neo4j_services.graph.upsert_node(
            src_id,
            {"labels": ["Session"], "node_id": src_id},
        )
        await neo4j_services.graph.upsert_node(
            dst_id,
            {"labels": ["Session"], "node_id": dst_id},
        )
        await neo4j_services.graph.upsert_edge(
            src_id,
            dst_id,
            {"type": edge_type, "occurred_at": "2026-02-02T12:00:00Z"},
        )
        await neo4j_services.graph.flush()

        # edge_type is safe: comes exclusively from the hardcoded parametrize list
        records = await neo4j_services.graph.execute_query(
            f"MATCH (src {{node_id: $src}})-[r:{edge_type}]->(dst {{node_id: $dst}}) "
            "RETURN valueType(r.occurred_at) AS vt",
            {"src": src_id, "dst": dst_id},
            workspace="*",
        )

        assert len(records) == 1, (
            f"Expected exactly 1 {edge_type} edge from {src_id!r} to {dst_id!r}, "
            f"got {len(records)} records"
        )
        vt = records[0]["vt"]
        assert vt == "ZONED DATETIME NOT NULL", (
            f"occurred_at on a {edge_type} edge must be stored as 'ZONED DATETIME NOT NULL' "
            f"in Neo4j, but valueType() returned {vt!r}. "
            "Check that 'occurred_at' is present in TEMPORAL_PROPS in neo4j_store.py."
        )


# ---------------------------------------------------------------------------
# TestTemporalIdempotency
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestTemporalIdempotency:
    """Verify that flushing the same node twice keeps the correct ZONED DATETIME type."""

    async def test_double_flush_keeps_zoned_datetime(self, neo4j_services: Any) -> None:
        """A node flushed twice must still have ZONED DATETIME and no duplicates.

        Phase 1 converts ISO strings to Python datetime on write.  A second flush
        of the same node_data must:
        - Not create a duplicate node (MERGE must match the existing node).
        - Preserve the ZONED DATETIME type on started_at (not regress to STRING).

        If cnt > 1, the uniqueness constraint or MERGE key is broken.
        If vt != 'ZONED DATETIME NOT NULL', the second flush overwrites with a raw string.
        """
        node_data = {
            "labels": ["Session"],
            "node_id": "temporal-idem-1",
            "started_at": "2026-04-04T04:04:04Z",
        }

        # First flush: ISO string -> ZONED DATETIME
        await neo4j_services.graph.upsert_node("temporal-idem-1", node_data)
        await neo4j_services.graph.flush()

        # Second flush: same data again — must not duplicate or demote the type
        await neo4j_services.graph.upsert_node("temporal-idem-1", node_data)
        await neo4j_services.graph.flush()

        records = await neo4j_services.graph.execute_query(
            "MATCH (n:Session {node_id: $nid}) "
            "RETURN valueType(n.started_at) AS vt, count(n) AS cnt",
            {"nid": "temporal-idem-1"},
            workspace="*",
        )

        assert len(records) == 1, f"Expected exactly 1 result row, got {len(records)}"
        cnt = records[0]["cnt"]
        vt = records[0]["vt"]
        assert cnt == 1, (
            f"Expected exactly 1 Session node after double flush, got {cnt}. "
            "MERGE is not deduplicating correctly."
        )
        assert vt == "ZONED DATETIME NOT NULL", (
            f"started_at must remain 'ZONED DATETIME NOT NULL' after a second flush, "
            f"but valueType() returned {vt!r}. "
            "The second flush may be writing a raw ISO string that overwrites the datetime."
        )


# ---------------------------------------------------------------------------
# TestTouchSessionReadPath
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestTouchSessionReadPath:
    """End-to-end integration test for the touch_session read-path + services.py fix.

    Proves that:
    1. touch_session writes last_updated as a ZONED DATETIME in Neo4j.
    2. On a subsequent touch_session call, the read path normalises the stored
       neo4j.time.DateTime back to a Python datetime so the comparison in
       services.py runs without TypeError.
    3. An older timestamp does NOT overwrite an existing newer value (skip decision).
    4. A newer timestamp DOES overwrite the existing value (propagate decision).

    If any touch_session call raises TypeError (datetime vs str or datetime vs
    neo4j.time.DateTime), Phase 1 read-path normalisation or the services.py
    comparison fix is incomplete.
    """

    async def test_touch_session_propagation_decision(
        self, neo4j_services: Any
    ) -> None:
        """touch_session skips older timestamps and applies newer ones without TypeError.

        Sequence:
        - Write a bare Session node, flush.
        - touch_session(t1): last_updated must land as ZONED DATETIME NOT NULL.
        - touch_session(t0, older): last_updated must be unchanged (skip decision,
          no TypeError on the read-back Python datetime).
        - touch_session(t2, newer): last_updated must change (propagate decision,
          still ZONED DATETIME NOT NULL).
        """
        sid = "touch-readpath-1"

        # timestamps — t0 < t1 < t2
        t0 = "2026-01-01T06:00:00Z"
        t1 = "2026-01-01T12:00:00Z"
        t2 = "2026-01-02T12:00:00Z"

        # ------------------------------------------------------------------ #
        # Setup: create bare Session node and flush to Neo4j.                 #
        # ------------------------------------------------------------------ #
        await neo4j_services.ensure_session_node(sid, {})
        await neo4j_services.graph.flush()

        # ------------------------------------------------------------------ #
        # Step 1: touch_session(t1) — first write, no prior last_updated.    #
        # Expect: last_updated is written and stored as ZONED DATETIME.       #
        # ------------------------------------------------------------------ #
        await neo4j_services.touch_session(sid, t1)
        await neo4j_services.graph.flush()

        records = await neo4j_services.graph.execute_query(
            "MATCH (s:Session {node_id: $nid}) "
            "RETURN valueType(s.last_updated) AS vt, toString(s.last_updated) AS val",
            {"nid": sid},
            workspace="*",
        )
        assert len(records) == 1, (
            f"Expected exactly 1 Session node with node_id={sid!r}, "
            f"got {len(records)} records after touch_session(t1)"
        )
        vt = records[0]["vt"]
        assert vt == "ZONED DATETIME NOT NULL", (
            f"After touch_session(t1), last_updated must be stored as "
            f"'ZONED DATETIME NOT NULL' in Neo4j, but valueType() returned {vt!r}. "
            "Check that _convert_temporal_props converts the ISO string before the flush."
        )
        val_after_t1 = records[0]["val"]

        # ------------------------------------------------------------------ #
        # Step 2: touch_session(t0) — t0 is OLDER than t1.                  #
        # Expect: last_updated is UNCHANGED (skip decision).                  #
        # If this raises TypeError, read-path normalisation is incomplete.   #
        # ------------------------------------------------------------------ #
        await neo4j_services.touch_session(sid, t0)
        await neo4j_services.graph.flush()

        records = await neo4j_services.graph.execute_query(
            "MATCH (s:Session {node_id: $nid}) RETURN toString(s.last_updated) AS val",
            {"nid": sid},
            workspace="*",
        )
        assert len(records) == 1, (
            f"Expected exactly 1 Session node with node_id={sid!r}, "
            f"got {len(records)} records after touch_session(t0)"
        )
        val_after_t0 = records[0]["val"]
        assert val_after_t0 == val_after_t1, (
            f"touch_session(t0={t0!r}) must NOT overwrite last_updated={val_after_t1!r} "
            f"because t0 is older than t1. Got {val_after_t0!r} instead. "
            "The skip comparison in services.py failed or a TypeError was swallowed "
            "and the walk stopped silently without writing."
        )

        # ------------------------------------------------------------------ #
        # Step 3: touch_session(t2) — t2 is NEWER than t1.                  #
        # Expect: last_updated is UPDATED and still ZONED DATETIME NOT NULL. #
        # ------------------------------------------------------------------ #
        await neo4j_services.touch_session(sid, t2)
        await neo4j_services.graph.flush()

        records = await neo4j_services.graph.execute_query(
            "MATCH (s:Session {node_id: $nid}) "
            "RETURN valueType(s.last_updated) AS vt, toString(s.last_updated) AS val",
            {"nid": sid},
            workspace="*",
        )
        assert len(records) == 1, (
            f"Expected exactly 1 Session node with node_id={sid!r}, "
            f"got {len(records)} records after touch_session(t2)"
        )
        vt_after_t2 = records[0]["vt"]
        val_after_t2 = records[0]["val"]
        assert vt_after_t2 == "ZONED DATETIME NOT NULL", (
            f"After touch_session(t2), last_updated must still be stored as "
            f"'ZONED DATETIME NOT NULL' in Neo4j, but valueType() returned {vt_after_t2!r}."
        )
        assert val_after_t2 != val_after_t1, (
            f"touch_session(t2={t2!r}) must UPDATE last_updated because t2 is newer "
            f"than t1. Expected a different value from {val_after_t1!r}, "
            f"but got {val_after_t2!r}. "
            "The propagate decision in services.py failed — newer timestamp was not applied."
        )
