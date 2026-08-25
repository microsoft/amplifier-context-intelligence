"""Tests for shared, pool-bounded Neo4j driver reuse across sessions.

Covers:
- Neo4jGraphStore accepts an injected driver and never closes it (owns_driver).
- The self-built path (no injected driver) is unchanged: it owns and closes
  its own driver.
- SessionRegistry hands the same driver instance to every per-session store
  instead of building one per session.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from context_intelligence_server.neo4j_store import Neo4jGraphStore
from context_intelligence_server.registry import SessionRegistry

# ---------------------------------------------------------------------------
# Injected-driver seam (owns_driver)
# ---------------------------------------------------------------------------


def test_injected_driver_reports_owns_driver_false() -> None:
    """A store built with driver= must report owns_driver False."""
    shared_driver = AsyncMock()

    store_a = Neo4jGraphStore(uri="bolt://unused:7687", driver=shared_driver)
    store_b = Neo4jGraphStore(uri="bolt://unused:7687", driver=shared_driver)

    assert store_a.owns_driver is False
    assert store_b.owns_driver is False
    assert store_a._driver is shared_driver
    assert store_b._driver is shared_driver


@pytest.mark.asyncio
async def test_close_on_injected_driver_does_not_close_it() -> None:
    """Closing one store sharing an injected driver must not close the driver
    out from under a second store still using it (the #489 safety property)."""
    shared_driver = AsyncMock()

    store_a = Neo4jGraphStore(uri="bolt://unused:7687", driver=shared_driver)
    store_b = Neo4jGraphStore(uri="bolt://unused:7687", driver=shared_driver)

    await store_a.close()

    shared_driver.close.assert_not_awaited()
    # store_b's driver reference is untouched and still the live shared mock --
    # a real driver would still be open and usable by store_b at this point.
    assert store_b._driver is shared_driver


@pytest.mark.asyncio
async def test_self_built_driver_still_owned_and_closed() -> None:
    """With driver=None (default), behavior is unchanged: the store builds
    and owns its driver, and close() closes it."""
    with patch(
        "context_intelligence_server.neo4j_store.AsyncGraphDatabase"
    ) as mock_adb:
        mock_driver = AsyncMock()
        mock_adb.driver.return_value = mock_driver

        store = Neo4jGraphStore(uri="bolt://localhost:7687", auth=("u", "p"))
        assert store.owns_driver is True

        await store.close()
        mock_driver.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# Registry driver reuse
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_registry_shares_one_driver_across_sessions(monkeypatch) -> None:
    """get_or_create must hand the SAME driver object to every per-session
    Neo4jGraphStore -- reuse, not a new driver per session_id."""
    reg = SessionRegistry()

    built_drivers: list[object] = []

    def fake_build_bounded_driver(config, **kwargs):
        driver = AsyncMock()
        built_drivers.append(driver)
        return driver

    monkeypatch.setattr(
        "context_intelligence_server.registry.build_bounded_neo4j_driver",
        fake_build_bounded_driver,
    )

    worker_a = reg.get_or_create("session-a", "/workspace/a")
    worker_b = reg.get_or_create("session-b", "/workspace/b")

    # Exactly one driver was ever built, and both sessions' stores share it.
    assert len(built_drivers) == 1
    assert worker_a.services.graph._driver is built_drivers[0]
    assert worker_b.services.graph._driver is built_drivers[0]
    assert worker_a.services.graph.owns_driver is False
    assert worker_b.services.graph.owns_driver is False

    for worker in (worker_a, worker_b):
        if worker.task is not None:
            worker.task.cancel()
