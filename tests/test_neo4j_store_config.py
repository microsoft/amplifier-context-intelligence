"""Tests for Neo4j driver configuration.

Verifies that the async Neo4j driver is created with an explicit, reviewable
auto-retry budget (``max_transaction_retry_time``) rather than relying on the
driver default implicitly.
"""

from __future__ import annotations

from context_intelligence_server import neo4j_store


def test_driver_configured_with_max_transaction_retry_time(monkeypatch):
    """The driver must be created with an explicit max_transaction_retry_time."""
    captured: dict = {}

    class _Dummy:
        async def close(self) -> None:  # pragma: no cover - trivial
            return None

    def fake_driver(uri, **kwargs):
        captured["uri"] = uri
        captured.update(kwargs)
        return _Dummy()

    monkeypatch.setattr(
        neo4j_store.AsyncGraphDatabase, "driver", staticmethod(fake_driver)
    )

    neo4j_store.Neo4jGraphStore(uri="bolt://example:7687", auth=("u", "p"))

    assert captured.get("max_transaction_retry_time") == 30.0


async def test_standalone_store_closes_the_driver_it_creates(monkeypatch) -> None:
    """A standalone store owns and closes the driver created by its constructor."""
    close_calls: list[str] = []

    class _Dummy:
        async def close(self) -> None:
            close_calls.append("closed")

    monkeypatch.setattr(
        neo4j_store.AsyncGraphDatabase,
        "driver",
        staticmethod(lambda *args, **kwargs: _Dummy()),
    )

    store = neo4j_store.Neo4jGraphStore(uri="bolt://example:7687", auth=("u", "p"))

    await store.close()

    assert close_calls == ["closed"]


async def test_shared_driver_store_does_not_create_or_close_a_driver(
    monkeypatch,
) -> None:
    """An externally supplied driver is shared infrastructure, not store-owned."""
    shared_driver = _DriverWithClose()
    monkeypatch.setattr(
        neo4j_store.AsyncGraphDatabase,
        "driver",
        staticmethod(
            lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("factory called")
            )
        ),
    )

    store = neo4j_store.Neo4jGraphStore(
        uri="bolt://example:7687",
        auth=("u", "p"),
        driver=shared_driver,
    )

    await store.close()

    assert shared_driver.close_calls == 0


class _DriverWithClose:
    def __init__(self) -> None:
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
