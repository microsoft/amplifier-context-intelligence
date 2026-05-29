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
