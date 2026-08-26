"""Neo4j integration proof for the SchemaMeta baseline singleton.

BASELINE DATA POINTS ONLY: this proves ``ensure_schema_version_baseline``'s
``:SchemaMeta {id: 'singleton'}`` write is create-if-absent (``ON CREATE SET``
only, no ``ON MATCH SET``) and O(1) -- calling it twice must leave exactly one
node in place with an UNCHANGED `last_updated`, proving the second call did
not clobber it. It also proves the uniqueness constraint on
``(:SchemaMeta).id`` is actually created, and that the constraint is what
makes the singleton race-free under real concurrency (N concurrent callers
still leave exactly one node) -- the whole point of this hardening
follow-up: moving the write off the per-worker-flush path onto a
single-writer startup call, backed by a uniqueness constraint so even a
violation of that single-writer invariant could never create a duplicate.

Also proves ``ensure_neo4j_schema`` itself no longer writes SchemaMeta at
all -- that responsibility now belongs exclusively to
``ensure_schema_version_baseline``, called once from the lifespan startup
handler, never from the per-flush / doctor-repair paths that call
``ensure_neo4j_schema``.

No comparison/upgrade/migration logic is exercised here; there is none to
exercise -- that is the point of this test.

Run: uv run pytest tests/neo4j/test_schema_version_baseline.py -v -m neo4j
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from context_intelligence_server.neo4j_store import (
    ensure_neo4j_schema,
    ensure_schema_version_baseline,
)
from context_intelligence_server.status import SCHEMA_VERSION
from neo4j import AsyncGraphDatabase


async def _schema_meta_rows(driver: Any) -> list[Any]:
    """Return all :SchemaMeta{id:'singleton'} rows (schema_version, last_updated)."""
    async with driver.session() as session:
        result = await session.run(
            "MATCH (m:SchemaMeta {id: 'singleton'}) "
            "RETURN m.schema_version AS schema_version, "
            "m.last_updated AS last_updated"
        )
        return [record async for record in result]


@pytest.mark.neo4j
class TestSchemaMetaBaselineIdempotence:
    """``ensure_schema_version_baseline`` writes the singleton create-if-absent only."""

    async def test_second_call_does_not_clobber_first(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        auth = (neo4j_container["user"], neo4j_container["password"])
        bolt = neo4j_container["bolt_url"]

        driver = AsyncGraphDatabase.driver(bolt, auth=auth)
        try:
            # First call: creates the singleton (and the uniqueness constraint).
            await ensure_schema_version_baseline(driver)

            rows = await _schema_meta_rows(driver)
            assert len(rows) == 1, (
                f"expected exactly one :SchemaMeta singleton after first call, "
                f"got {len(rows)}"
            )
            assert rows[0]["schema_version"] == SCHEMA_VERSION
            first_last_updated = rows[0]["last_updated"]
            assert first_last_updated is not None

            # Second call: must be a no-op on the existing node (ON CREATE only).
            await ensure_schema_version_baseline(driver)

            rows_after = await _schema_meta_rows(driver)
            assert len(rows_after) == 1, (
                f"expected exactly one :SchemaMeta node after second call "
                f"(no duplicate created), got {len(rows_after)}"
            )
            assert rows_after[0]["last_updated"] == first_last_updated, (
                "last_updated changed on the second call -- ON MATCH SET must "
                "not be present; the singleton must be left untouched once it "
                "exists"
            )
            assert rows_after[0]["schema_version"] == SCHEMA_VERSION
        finally:
            await driver.close()

    async def test_uniqueness_constraint_exists(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """The (:SchemaMeta).id uniqueness constraint is created and enforced."""
        auth = (neo4j_container["user"], neo4j_container["password"])
        bolt = neo4j_container["bolt_url"]

        driver = AsyncGraphDatabase.driver(bolt, auth=auth)
        try:
            await ensure_schema_version_baseline(driver)

            async with driver.session() as session:
                result = await session.run("SHOW CONSTRAINTS")
                constraints = [record async for record in result]

            schema_meta_constraints = [
                c
                for c in constraints
                if "SchemaMeta" in (c.get("labelsOrTypes") or [])
                and "id" in (c.get("properties") or [])
            ]
            assert schema_meta_constraints, (
                "expected a uniqueness constraint on (:SchemaMeta).id to exist "
                f"after ensure_schema_version_baseline; SHOW CONSTRAINTS returned: "
                f"{constraints}"
            )

            # Belt-and-suspenders: attempting to create a second singleton node
            # directly (bypassing MERGE) must be rejected by the constraint.
            with pytest.raises(Exception):  # noqa: B017 - Neo4jError subtype
                async with driver.session() as session:
                    await session.run("CREATE (m:SchemaMeta {id: 'singleton'})")
        finally:
            await driver.close()

    async def test_concurrent_calls_create_exactly_one_node(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """N concurrent baseline calls against the same DB leave exactly one node.

        This is the whole point of the uniqueness-constraint hardening: without
        it, concurrent MERGEs on a fresh singleton key can each pass the
        existence check and create divergent duplicate nodes. Fire many
        concurrent calls (each on its own driver, mirroring independent
        SessionWorker stores) and assert the constraint prevents any
        duplication.
        """
        auth = (neo4j_container["user"], neo4j_container["password"])
        bolt = neo4j_container["bolt_url"]

        n_concurrent = 20
        drivers = [
            AsyncGraphDatabase.driver(bolt, auth=auth) for _ in range(n_concurrent)
        ]
        try:
            await asyncio.gather(*(ensure_schema_version_baseline(d) for d in drivers))

            rows = await _schema_meta_rows(drivers[0])
            assert len(rows) == 1, (
                f"expected exactly one :SchemaMeta singleton after "
                f"{n_concurrent} concurrent calls, got {len(rows)} -- the "
                "uniqueness constraint should make concurrent creation race-free"
            )
            assert rows[0]["schema_version"] == SCHEMA_VERSION
        finally:
            for d in drivers:
                await d.close()


@pytest.mark.neo4j
class TestEnsureNeo4jSchemaNoLongerWritesSchemaMeta:
    """``ensure_neo4j_schema`` must not touch :SchemaMeta at all (hardening follow-up).

    That responsibility moved exclusively to ``ensure_schema_version_baseline``,
    called once from the lifespan startup handler -- NOT from the per-flush /
    doctor-repair paths that call ``ensure_neo4j_schema``. If ``ensure_neo4j_schema``
    still created the singleton, it would fire redundantly (and concurrently) on
    every SessionWorker's first flush.
    """

    async def test_ensure_neo4j_schema_does_not_create_schema_meta(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        auth = (neo4j_container["user"], neo4j_container["password"])
        bolt = neo4j_container["bolt_url"]

        driver = AsyncGraphDatabase.driver(bolt, auth=auth)
        try:
            # Remove any pre-existing singleton so this test is unambiguous
            # regardless of what earlier tests in this (session-scoped)
            # container have already written.
            async with driver.session() as session:
                await session.run(
                    "MATCH (m:SchemaMeta {id: 'singleton'}) DETACH DELETE m"
                )

            await ensure_neo4j_schema(driver)

            rows = await _schema_meta_rows(driver)
            assert rows == [], (
                "ensure_neo4j_schema must not write the :SchemaMeta singleton "
                f"-- that is ensure_schema_version_baseline's job now, but "
                f"found {len(rows)} node(s)"
            )
        finally:
            await driver.close()
