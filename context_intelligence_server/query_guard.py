"""Use Neo4j's compiler classification before executing public Cypher reads.

READ_ACCESS is a routing hint, not authorization. EXPLAIN compiles without
executing the statement. Unknown, write and schema classifications fail closed;
CI never tries to implement a Cypher security parser in a client or the server.
Database-level read-only credentials remain recommended defense in depth.
"""

from __future__ import annotations


class QueryNotReadOnlyError(ValueError):
    """The database did not classify the statement as a read-only query."""


async def read_query(session, query: str, params: dict) -> list[dict]:
    planned = await session.run("EXPLAIN " + query, params)
    summary = await planned.consume()
    if summary.query_type != "r":
        raise QueryNotReadOnlyError("cypher_read_only_required")
    result = await session.run(query, params)
    return [dict(record) async for record in result]
