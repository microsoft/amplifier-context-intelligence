"""Connection-ownership invariants for the graph backend.

This suite is the successor to ``test_neo4j_driver_sharing.py``, which proved
that a *shared* driver was reused instead of one being built per session. That
property is still proved here, but the design it guarded has been replaced by a
stronger one, and the tests are written against the stronger claim:

    Exactly one object in the process can open a graph connection, and nothing
    else can obtain one.

The old suite could only assert that per-session stores *happened* to be given
a driver (``owns_driver is False``) -- the store retained a code path that built
its own unbounded pool whenever a caller omitted the argument, so the leak was
one forgotten keyword away at every construction site. The store no longer has
that path at all, so the first test below asserts something the old design could
not: that building a store without an injected connection is impossible.

Covered:
- A store cannot be constructed without an injected driver (structural).
- A store's close() never closes the driver (safety for sibling stores).
- The backend opens exactly two bounded pools, with the documented kwargs.
- start()/aclose() are idempotent, and a failed start() leaks nothing.
- The backend hands out stores, never drivers.
- N sessions through the registry open NO additional pools (the leak proof).
- A registry with no backend bound fails loud instead of opening a pool.
- shutdown_workers() quiesces every drainer (the ordering guard for close).
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from context_intelligence_server.config import Neo4jClientConfig, get_settings
from context_intelligence_server.graph_backend import GraphBackend
from context_intelligence_server.graph_store import GraphStore, QueryableStore
from context_intelligence_server.neo4j_backend import (
    Neo4jGraphBackend,
    _build_bounded_driver,
)
from context_intelligence_server.neo4j_store import Neo4jGraphStore
from context_intelligence_server.registry import SessionRegistry, SessionWorker

_WRITE = Neo4jClientConfig(
    url="bolt://unused:7687", username="u", password="p", access_mode="WRITE"
)
_READ = Neo4jClientConfig(
    url="bolt://unused:7687", username="u", password="p", access_mode="READ"
)


def _backend(**overrides) -> Neo4jGraphBackend:
    kwargs = {
        "write_config": _WRITE,
        "read_config": _READ,
        "max_connection_pool_size": 50,
        "lock_timeout": 30.0,
    }
    kwargs.update(overrides)
    return Neo4jGraphBackend(**kwargs)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# A store cannot open a connection
# ---------------------------------------------------------------------------


def test_store_cannot_be_built_without_an_injected_driver() -> None:
    """The structural core of the whole design.

    The predecessor of this test asserted ``owns_driver is False`` on a store
    that had been given a driver -- which proved the injection had happened,
    not that the alternative was unavailable. The alternative WAS available:
    omit the argument and the store built its own pool, unbounded, closed only
    if that particular store was closed. This asserts the alternative is gone.
    """
    with pytest.raises(TypeError):
        Neo4jGraphStore()  # type: ignore[call-arg]


def test_store_driver_is_keyword_only() -> None:
    """`driver` cannot be passed positionally, so it cannot be filled by
    accident of argument order by a caller porting old positional code."""
    with pytest.raises(TypeError):
        Neo4jGraphStore(AsyncMock())  # type: ignore[misc]


@pytest.mark.asyncio
async def test_close_never_closes_the_injected_driver() -> None:
    """Closing one store must not close the connection a sibling store is
    still using -- the safety property every shared pool rests on."""
    shared_driver = AsyncMock()

    store_a = Neo4jGraphStore(driver=shared_driver)
    store_b = Neo4jGraphStore(driver=shared_driver)

    await store_a.close()

    shared_driver.close.assert_not_awaited()
    assert store_b._driver is shared_driver


@pytest.mark.asyncio
async def test_repeated_store_closes_never_touch_the_driver() -> None:
    """Even a store closed twice leaves the connection alone. There is no
    ownership flag left to get wrong, so this cannot regress by a branch."""
    shared_driver = AsyncMock()
    store = Neo4jGraphStore(driver=shared_driver)

    await store.close()
    await store.close()

    shared_driver.close.assert_not_awaited()


# ---------------------------------------------------------------------------
# The backend is the only thing that opens a pool
# ---------------------------------------------------------------------------


def test_backend_satisfies_the_graph_backend_protocol() -> None:
    assert isinstance(_backend(), GraphBackend)


@pytest.mark.asyncio
async def test_start_opens_exactly_two_bounded_pools() -> None:
    """One write pool and one read pool -- no more.

    Before this refactor the process opened THREE: the lifespan's admin driver,
    the lifespan's query driver, and a third the session registry built lazily
    for itself. The first and third were built from the same config, against
    the same instance, for overlapping work.
    """
    backend = _backend()
    with patch(
        "context_intelligence_server.neo4j_backend.AsyncGraphDatabase"
    ) as mock_adb:
        mock_adb.driver.return_value = AsyncMock()
        await backend.start()

    assert mock_adb.driver.call_count == 2
    for call in mock_adb.driver.call_args_list:
        assert call.kwargs["max_connection_pool_size"] == 50
        assert call.kwargs["connection_acquisition_timeout"] == 30.0
        assert call.kwargs["max_transaction_retry_time"] == 30.0


@pytest.mark.asyncio
async def test_start_is_idempotent() -> None:
    """A second start() must not open a second pair of pools behind the first
    pair's back -- that would leak two pools with no reference to close them."""
    backend = _backend()
    with patch(
        "context_intelligence_server.neo4j_backend.AsyncGraphDatabase"
    ) as mock_adb:
        mock_adb.driver.return_value = AsyncMock()
        await backend.start()
        await backend.start()

    assert mock_adb.driver.call_count == 2


@pytest.mark.asyncio
async def test_failed_start_closes_what_it_already_opened() -> None:
    """If opening the read pool fails, the write pool must not be left open.

    A half-started backend holds a pool that nothing references and nothing
    will close -- the same class of leak this module exists to prevent, merely
    relocated to the startup path.
    """
    write_driver = AsyncMock()
    backend = _backend()

    with patch(
        "context_intelligence_server.neo4j_backend.AsyncGraphDatabase"
    ) as mock_adb:
        mock_adb.driver.side_effect = [write_driver, RuntimeError("read pool failed")]
        with pytest.raises(RuntimeError, match="read pool failed"):
            await backend.start()

    write_driver.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_aclose_closes_both_pools_once_and_is_idempotent() -> None:
    write_driver, read_driver = AsyncMock(), AsyncMock()
    backend = _backend()

    with patch(
        "context_intelligence_server.neo4j_backend.AsyncGraphDatabase"
    ) as mock_adb:
        mock_adb.driver.side_effect = [write_driver, read_driver]
        await backend.start()

    await backend.aclose()
    await backend.aclose()

    write_driver.close.assert_awaited_once()
    read_driver.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_aclose_before_start_is_safe() -> None:
    await _backend().aclose()  # must not raise


@pytest.mark.asyncio
async def test_requesting_a_store_before_start_fails_loud() -> None:
    """Fail loudly rather than lazily opening a pool nobody agreed to own.

    The registry's predecessor did exactly the opposite: it built a driver on
    first touch, so any code path that reached it outside the lifespan opened a
    pool that the lifespan would never close.
    """
    with pytest.raises(RuntimeError, match="start"):
        _backend().session_store(workspace="ws")


@pytest.mark.asyncio
async def test_backend_hands_out_stores_not_drivers() -> None:
    """Every accessor returns something satisfying a store protocol, and the
    backend exposes no public accessor that returns a connection at all."""
    backend = _backend()
    with patch(
        "context_intelligence_server.neo4j_backend.AsyncGraphDatabase"
    ) as mock_adb:
        mock_adb.driver.return_value = AsyncMock()
        await backend.start()

    assert isinstance(backend.session_store(workspace="ws"), GraphStore)
    assert isinstance(backend.admin_store(), GraphStore)
    assert isinstance(backend.query_store(), QueryableStore)

    public = [name for name in dir(backend) if not name.startswith("_")]
    assert not [name for name in public if "driver" in name.lower()], (
        f"backend exposes a driver-shaped public accessor: {public}"
    )


@pytest.mark.asyncio
async def test_session_store_binds_workspace_and_contributor() -> None:
    backend = _backend()
    with patch(
        "context_intelligence_server.neo4j_backend.AsyncGraphDatabase"
    ) as mock_adb:
        mock_adb.driver.return_value = AsyncMock()
        await backend.start()

    store = backend.session_store(workspace="/ws/a", created_by="alice")

    assert store.workspace == "/ws/a"
    assert store.created_by == "alice"


@pytest.mark.asyncio
async def test_read_store_routes_its_direct_queries_as_reads() -> None:
    """A read store must route EVERY read, not just the ones it opens a session for.

    Handing a store the read pool is not the same as routing its queries as
    reads, and the gap is invisible from the backend: ``default_access_mode``
    reaches only sessions the store opens itself, while the graph-resolution
    paths (session summary) call ``driver.execute_query`` directly -- which the
    driver defaults to WRITE routing. A read-intent store was still asking for
    a writer on a user-visible path.
    """
    from neo4j import RoutingControl

    backend = _backend()
    with patch(
        "context_intelligence_server.neo4j_backend.AsyncGraphDatabase"
    ) as mock_adb:
        mock_adb.driver.return_value = AsyncMock()
        await backend.start()

    read_store = backend.query_store()
    assert read_store._read_routing == {"routing_": RoutingControl.READ}  # type: ignore[attr-defined]

    # A write/admin store emits NO routing kwarg -- unchanged from before, so
    # the ingest and deletion paths keep their existing behaviour exactly.
    assert backend.admin_store()._read_routing == {}  # type: ignore[attr-defined]
    assert backend.session_store(workspace="ws")._read_routing == {}  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_read_store_passes_read_routing_to_the_driver() -> None:
    """End-to-end on the real call: get_node on a read store asks for READ."""
    from neo4j import RoutingControl

    driver = AsyncMock()
    driver.execute_query.return_value = MagicMock(records=[])
    store = Neo4jGraphStore(driver=driver, default_access_mode="READ")

    await store.get_node("some-node")

    assert driver.execute_query.call_args.kwargs["routing_"] is RoutingControl.READ


@pytest.mark.asyncio
async def test_write_store_sends_no_routing_kwarg() -> None:
    """The ingest path is untouched: no routing kwarg, exactly as before."""
    driver = AsyncMock()
    driver.execute_query.return_value = MagicMock(records=[])
    store = Neo4jGraphStore(driver=driver)

    await store.get_node("some-node")

    assert "routing_" not in driver.execute_query.call_args.kwargs


@pytest.mark.asyncio
async def test_query_store_carries_the_configured_read_intent() -> None:
    """The read client's access_mode reaches the session the store opens.

    The query endpoint used to apply this itself, by importing neo4j's
    access-mode constants into the application entrypoint. It now comes from
    config, through the backend, into the store -- so the endpoint needs no
    knowledge of the driver's vocabulary.
    """
    backend = _backend()
    with patch(
        "context_intelligence_server.neo4j_backend.AsyncGraphDatabase"
    ) as mock_adb:
        mock_adb.driver.return_value = AsyncMock()
        await backend.start()

    assert backend.query_store()._default_access_mode == "READ"  # type: ignore[attr-defined]
    assert backend.admin_store()._default_access_mode is None  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Driver-kwarg parity (the pool bound itself)
# ---------------------------------------------------------------------------


def test_build_bounded_driver_sets_pool_cap_and_budgets() -> None:
    """Regression guard: an earlier extraction of this construction into a
    shared helper silently dropped ``connection_acquisition_timeout`` (30.0 ->
    the driver's 60.0 default) and the explicit retry budget."""
    with patch(
        "context_intelligence_server.neo4j_backend.AsyncGraphDatabase"
    ) as mock_adb:
        _build_bounded_driver(
            _WRITE,
            max_connection_pool_size=50,
            connection_acquisition_timeout=30.0,
        )

    kwargs = mock_adb.driver.call_args.kwargs
    assert kwargs["max_connection_pool_size"] == 50
    assert kwargs["connection_acquisition_timeout"] == 30.0
    assert kwargs["max_transaction_retry_time"] == 30.0
    # Deliberately absent: the driver already defaults to 3600 s, so setting it
    # would be a knob that changes nothing.
    assert "max_connection_lifetime" not in kwargs


def test_build_bounded_driver_omits_acquisition_timeout_when_not_given() -> None:
    with patch(
        "context_intelligence_server.neo4j_backend.AsyncGraphDatabase"
    ) as mock_adb:
        _build_bounded_driver(_WRITE, max_connection_pool_size=50)

    kwargs = mock_adb.driver.call_args.kwargs
    assert "connection_acquisition_timeout" not in kwargs
    assert kwargs["max_transaction_retry_time"] == 30.0


def test_from_settings_applies_the_configured_pool_bound() -> None:
    """The bound reaching the driver comes from settings, not a literal.

    An unbounded pool is the original leak; this is the test that fails if the
    configured cap ever stops being threaded through.
    """
    settings = get_settings()
    backend = Neo4jGraphBackend.from_settings(settings)

    with patch(
        "context_intelligence_server.neo4j_backend.AsyncGraphDatabase"
    ) as mock_adb:
        mock_adb.driver.return_value = AsyncMock()
        asyncio.get_event_loop_policy()  # no-op; keeps this test sync-safe
        asyncio.run(backend.start())

    for call in mock_adb.driver.call_args_list:
        assert (
            call.kwargs["max_connection_pool_size"]
            == settings.neo4j_max_connection_pool_size
        )
        assert call.kwargs["connection_acquisition_timeout"] == (
            settings.neo4j_lock_timeout
        )


# ---------------------------------------------------------------------------
# Registry: N sessions, no additional pools
# ---------------------------------------------------------------------------


class _CountingBackend:
    """A backend stand-in that counts how many pools it was asked to open.

    It satisfies the parts of ``GraphBackend`` the registry uses. The point of
    counting here is that the registry must open ZERO -- the number of pools is
    a property of the backend's lifecycle, not of session traffic.
    """

    def __init__(self) -> None:
        self.stores: list[MagicMock] = []
        self.pools_opened = 0

    async def start(self) -> None:
        self.pools_opened += 1

    async def aclose(self) -> None:
        pass

    def session_store(self, *, workspace: str, created_by: str | None = None):
        store = MagicMock()
        store.workspace = workspace
        store.created_by = created_by
        self.stores.append(store)
        return store


def _cancel_workers(reg: SessionRegistry) -> None:
    for worker in reg._workers.values():
        if worker.task is not None:
            worker.task.cancel()


@pytest.mark.asyncio
async def test_n_sessions_open_no_additional_pools() -> None:
    """The leak proof, restated for the new design.

    The original defect was one driver -- and therefore one pool of up to 100
    bolt connections -- per ``session_id``, released only on a clean finalize.
    Sessions now receive stores; opening a pool is not something the session
    path can do at all.
    """
    backend = _CountingBackend()
    await backend.start()
    reg = SessionRegistry()
    reg.set_graph_backend(backend)  # type: ignore[arg-type]

    n = 30
    workers = [reg.get_or_create(f"session-{i}", f"/workspace/{i}") for i in range(n)]

    assert backend.pools_opened == 1, (
        f"{n} sessions caused {backend.pools_opened} pool opens; "
        "session traffic must never open a pool"
    )
    assert len(backend.stores) == n
    for worker, store in zip(workers, backend.stores, strict=True):
        assert worker.services.graph is store

    _cancel_workers(reg)


@pytest.mark.asyncio
async def test_repeat_sessions_reuse_their_existing_store() -> None:
    """A repeat event for a live session must not mint a second store."""
    backend = _CountingBackend()
    await backend.start()
    reg = SessionRegistry()
    reg.set_graph_backend(backend)  # type: ignore[arg-type]

    reg.get_or_create("session-1", "/workspace/1")
    reg.get_or_create("session-1", "/workspace/1")

    assert len(backend.stores) == 1

    _cancel_workers(reg)


def test_registry_without_a_backend_fails_loud() -> None:
    """No backend bound is an error, not an invitation to open a pool.

    This is the test that would have caught the previous design's real hazard:
    ``registry.neo4j_driver`` lazily built one on first access, so any path
    that reached the registry outside the lifespan silently opened a pool the
    lifespan would never close.
    """
    reg = SessionRegistry()
    with pytest.raises(RuntimeError, match="graph backend"):
        _ = reg.graph_backend


def test_registry_backend_can_be_unbound() -> None:
    """Shutdown unbinds, and the unbound registry is loud again."""
    reg = SessionRegistry()
    reg.set_graph_backend(_CountingBackend())  # type: ignore[arg-type]
    assert reg.graph_backend is not None

    reg.set_graph_backend(None)
    with pytest.raises(RuntimeError):
        _ = reg.graph_backend


# ---------------------------------------------------------------------------
# Shutdown quiesce (the ordering guard for closing the pools)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_shutdown_workers_cancels_and_awaits_every_drainer() -> None:
    """shutdown_workers() must cancel AND await every live drain worker.

    This is the ordering guard for the shared pool: a drainer still running
    when the backend closes fails its batch, exhausts its retry budget, and
    dead-letters healthy events (committing the offset past them).
    """
    reg = SessionRegistry()

    started = asyncio.Event()

    async def never_ending() -> None:
        started.set()
        await asyncio.sleep(3600)

    workers = []
    for i in range(3):
        worker = SessionWorker(
            session_id=f"s{i}",
            workspace=f"/ws/{i}",
            services=MagicMock(),
        )
        worker.task = asyncio.create_task(never_ending())
        reg._workers[worker.session_id] = worker
        workers.append(worker)

    await started.wait()

    await reg.shutdown_workers()

    for worker in workers:
        assert worker.task is not None
        assert worker.task.done(), "shutdown_workers must AWAIT, not just cancel"
        assert worker.task.cancelled()


@pytest.mark.asyncio
async def test_shutdown_workers_awaits_detached_store_closes() -> None:
    """The quiesce must also drain the fire-and-forget store-close tasks.

    A drainer that CRASHES does not close its store inline: its done-callback
    spawns a DETACHED ``_safe_close`` task, retained only in ``_close_tasks``.
    Awaiting the drain tasks does not await those. If the caller then closes
    the backend, that store's final flush runs against a closed connection,
    fails, and is swallowed by ``_safe_close`` -- the buffer is lost with only
    a log line to say so. This is the test that keeps the shutdown ordering
    honest for the crash path, not just the clean one.
    """
    reg = SessionRegistry()
    released = asyncio.Event()
    finished = asyncio.Event()

    async def slow_close() -> None:
        await released.wait()
        finished.set()

    reg._close_tasks.add(asyncio.create_task(slow_close()))
    await asyncio.sleep(0)

    quiesce = asyncio.create_task(reg.shutdown_workers())
    await asyncio.sleep(0)
    assert not quiesce.done(), "shutdown_workers returned while a close was in flight"

    released.set()
    await quiesce

    assert finished.is_set()


@pytest.mark.asyncio
async def test_shutdown_workers_no_op_with_no_workers() -> None:
    """shutdown_workers() must be safe when no session ever ran."""
    reg = SessionRegistry()
    await reg.shutdown_workers()  # must not raise


@pytest.mark.asyncio
async def test_shutdown_workers_survives_a_failing_drainer() -> None:
    """One worker raising during teardown must not abort the shutdown of the
    others -- shutdown is not derailable by a single bad drainer."""
    reg = SessionRegistry()

    async def raises_on_cancel() -> None:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            raise RuntimeError("teardown blew up") from None

    async def clean() -> None:
        await asyncio.sleep(3600)

    bad = SessionWorker(session_id="bad", workspace="/ws", services=MagicMock())
    bad.task = asyncio.create_task(raises_on_cancel())
    good = SessionWorker(session_id="good", workspace="/ws", services=MagicMock())
    good.task = asyncio.create_task(clean())
    reg._workers["bad"] = bad
    reg._workers["good"] = good
    await asyncio.sleep(0)

    await reg.shutdown_workers()  # must not raise

    assert bad.task.done()
    assert good.task.done()
