"""Tests for shared, pool-bounded Neo4j driver reuse across sessions.

Covers:
- Neo4jGraphStore accepts an injected driver and never closes it (owns_driver).
- The self-built path (no injected driver) is unchanged: it owns and closes
  its own driver.
- SessionRegistry hands the same driver instance to every per-session store
  instead of building one per session.
"""

from __future__ import annotations

import asyncio
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


def _spy_driver_factory(reg: SessionRegistry, monkeypatch) -> list[dict]:
    """Patch the registry's driver factory to record every build call.

    Returns the list of recorded builds; each entry is
    ``{"driver": <AsyncMock>, "kwargs": {...}}``.
    """
    builds: list[dict] = []

    def fake_build_bounded_driver(config, **kwargs):
        driver = AsyncMock()
        builds.append({"driver": driver, "kwargs": kwargs})
        return driver

    monkeypatch.setattr(
        "context_intelligence_server.registry.build_bounded_neo4j_driver",
        fake_build_bounded_driver,
    )
    return builds


def _cancel_workers(reg: SessionRegistry) -> None:
    """Cancel any real drain tasks started by get_or_create (test cleanup)."""
    for worker in reg._workers.values():
        if worker.task is not None:
            worker.task.cancel()


@pytest.mark.asyncio
async def test_n_sessions_build_exactly_one_driver(monkeypatch) -> None:
    """The core leak-gone proof: N distinct sessions must build the driver
    exactly ONCE, not once per session_id (which was the leak)."""
    reg = SessionRegistry()
    builds = _spy_driver_factory(reg, monkeypatch)

    n = 30
    workers = [reg.get_or_create(f"session-{i}", f"/workspace/{i}") for i in range(n)]

    assert len(builds) == 1, (
        f"expected exactly 1 driver build across {n} sessions, "
        f"got {len(builds)} (a per-session build is the leak)"
    )
    the_driver = builds[0]["driver"]
    for worker in workers:
        assert worker.services.graph._driver is the_driver
        assert worker.services.graph.owns_driver is False

    _cancel_workers(reg)


@pytest.mark.asyncio
async def test_shared_driver_built_with_bounded_kwargs(monkeypatch) -> None:
    """The single shared driver must be built WITH the bounded pool kwargs
    (default pool size 50, lifetime 3600.0s) -- an unbounded build is the leak."""
    reg = SessionRegistry()
    builds = _spy_driver_factory(reg, monkeypatch)

    reg.get_or_create("session-1", "/workspace/1")

    assert len(builds) == 1
    kwargs = builds[0]["kwargs"]
    assert kwargs["max_connection_pool_size"] == 50
    assert kwargs["max_connection_lifetime"] == 3600.0

    _cancel_workers(reg)


@pytest.mark.asyncio
async def test_concurrent_first_sessions_build_exactly_one_driver(monkeypatch) -> None:
    """Racing many get_or_create calls as the FIRST sessions must still build
    exactly one driver -- the lazy build is synchronous with no await between
    the None-check and the assignment, so concurrent coroutines cannot double-build."""
    reg = SessionRegistry()
    builds = _spy_driver_factory(reg, monkeypatch)

    async def make(i: int) -> None:
        # Wrap the sync get_or_create so many run concurrently under gather.
        reg.get_or_create(f"session-{i}", f"/workspace/{i}")

    await asyncio.gather(*(make(i) for i in range(40)))

    assert len(builds) == 1, (
        f"concurrent first-sessions raced into {len(builds)} driver builds; "
        "the lazy build must be single-shot"
    )

    _cancel_workers(reg)


@pytest.mark.asyncio
async def test_close_neo4j_driver_reclaims_once_and_is_idempotent(monkeypatch) -> None:
    """After sessions run, close_neo4j_driver() must close the shared driver
    exactly once and clear it; a second call is a safe no-op."""
    reg = SessionRegistry()
    builds = _spy_driver_factory(reg, monkeypatch)

    reg.get_or_create("session-1", "/workspace/1")
    reg.get_or_create("session-2", "/workspace/2")
    assert len(builds) == 1
    shared_driver = builds[0]["driver"]

    await reg.close_neo4j_driver()
    shared_driver.close.assert_awaited_once()
    assert reg._neo4j_driver is None

    # Second call: no driver left to close, must not raise or double-close.
    await reg.close_neo4j_driver()
    shared_driver.close.assert_awaited_once()
    assert reg._neo4j_driver is None

    _cancel_workers(reg)


@pytest.mark.asyncio
async def test_close_neo4j_driver_none_safe_when_no_session_ran() -> None:
    """close_neo4j_driver() must be safe when no session ever built a driver."""
    reg = SessionRegistry()
    assert reg._neo4j_driver is None
    # Must not raise.
    await reg.close_neo4j_driver()
    assert reg._neo4j_driver is None
