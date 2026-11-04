"""Compiler enforcement on isolated Neo4j Community, whose account can write."""

import pytest
from neo4j import AsyncGraphDatabase

from context_intelligence_server.query_guard import QueryNotReadOnlyError, read_query

pytestmark = pytest.mark.neo4j


@pytest.mark.timeout(120)
async def test_database_compiler_blocks_writes_without_executing_them(neo4j_container):
    uri = neo4j_container["bolt_url"]
    async with AsyncGraphDatabase.driver(uri, auth=("neo4j", "testpassword")) as driver:
        async with driver.session(default_access_mode="READ") as session:
            # A literal containing write keywords is still a read. CI does not
            # parse the query or blacklist strings in the user's expression.
            assert await read_query(
                session, "RETURN $value AS value", {"value": "CREATE DELETE SET"}
            ) == [{"value": "CREATE DELETE SET"}]
            assert isinstance(await read_query(session, "CALL db.labels()", {}), list)
            for query in (
                "CREATE (:CIReadGuardProof)",
                "MERGE (:CIReadGuardProof {key: 'proof'})",
                "MATCH (n:CIReadGuardProof) SET n.changed=true RETURN n",
                "MATCH (n:CIReadGuardProof) DETACH DELETE n",
                "CALL { CREATE (n:CIReadGuardProof) RETURN n } RETURN n",
                "CREATE INDEX ci_read_guard_proof IF NOT EXISTS FOR (n:CIReadGuardProof) ON (n.key)",
            ):
                with pytest.raises(QueryNotReadOnlyError):
                    await read_query(session, query, {})
        async with driver.session() as admin:
            # Independent full-privilege query proves the denied statements did
            # not write nodes or install the denied schema operation.
            row = await (
                await admin.run("MATCH (n:CIReadGuardProof) RETURN count(n) AS count")
            ).single()
            assert row["count"] == 0
            indexes = await (
                await admin.run(
                    "SHOW INDEXES YIELD name WHERE name='ci_read_guard_proof' RETURN name"
                )
            ).data()
            assert indexes == []
