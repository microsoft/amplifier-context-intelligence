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
