"""No raw statement executes unless the owning database compiler says read-only."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from context_intelligence_server.query_guard import QueryNotReadOnlyError, read_query


@pytest.mark.parametrize("query_type", ["w", "rw", "s", None, "unknown"])
async def test_non_read_classification_never_executes_query(query_type):
    plan = SimpleNamespace(
        consume=AsyncMock(return_value=SimpleNamespace(query_type=query_type))
    )
    session = SimpleNamespace(run=AsyncMock(return_value=plan))
    with pytest.raises(QueryNotReadOnlyError):
        await read_query(session, "CREATE (:Example)", {"key": "value"})
    session.run.assert_awaited_once_with("EXPLAIN CREATE (:Example)", {"key": "value"})


async def test_read_classification_preserves_query_and_parameters():
    class Rows:
        def __aiter__(self):
            async def rows():
                yield {"value": 7}

            return rows()

    plan = SimpleNamespace(
        consume=AsyncMock(return_value=SimpleNamespace(query_type="r"))
    )
    session = SimpleNamespace(run=AsyncMock(side_effect=[plan, Rows()]))
    assert await read_query(session, "RETURN $value AS value", {"value": 7}) == [
        {"value": 7}
    ]
    assert session.run.await_args_list[1].args == (
        "RETURN $value AS value",
        {"value": 7},
    )
