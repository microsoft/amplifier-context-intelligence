"""Tests for the MaintenanceCoordinator seam (WS-3a).

Covers the WS-3a spec's unit test plan (sec 7a), items A1-A5, A12, A13:
probe tri-state + fail-open, TTL caching, single-flight, the latch-defect
regression (A4), the op-running/constraint-present interaction (A5), and
transition logging (A12). Allow-list/gate/status HTTP-surface tests (A8-A11)
live in test_main.py; drain-loop offset-ordering tests (A6-A7) live in
test_registry.py; the docs tripwire (A15) lives in test_docs_entrypoint.py;
W-2 (A14) lives in test_main.py.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Self

import pytest
from context_intelligence_server.maintenance import (
    MAINTENANCE_ALLOW_LIST,
    MaintenanceCoordinator,
)

# NOTE: asyncio_mode = "auto" (pyproject.toml) runs async tests automatically
# -- no pytest.mark.asyncio needed. A blanket `pytestmark` would incorrectly
# tag the sync tests below (TestOpSeam, test_allow_list_contains_required_paths).


# ---------------------------------------------------------------------------
# Fake Neo4j driver -- controls the constraint-probe result and counts real
# "catalog reads" so tests can assert single-flight/caching behavior.
# ---------------------------------------------------------------------------


class _FakeConstraintResult:
    def __init__(self, count: int) -> None:
        self._count = count
        self._yielded = False

    def __aiter__(self) -> _FakeConstraintResult:
        return self

    async def __anext__(self) -> dict[str, int]:
        if self._yielded:
            raise StopAsyncIteration
        self._yielded = True
        return {"c": self._count}


class _FakeConstraintSession:
    def __init__(self, driver: _FakeConstraintDriver) -> None:
        self._driver = driver

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def run(self, cypher: str) -> _FakeConstraintResult:
        self._driver.call_count += 1
        if self._driver.raise_exc is not None:
            raise self._driver.raise_exc
        return _FakeConstraintResult(1 if self._driver.present else 0)


class _FakeConstraintDriver:
    """Test double: counts real probe hits, lets tests flip the result."""

    def __init__(
        self, present: bool = True, raise_exc: Exception | None = None
    ) -> None:
        self.present = present
        self.raise_exc = raise_exc
        self.call_count = 0

    def session(self, **kwargs: Any) -> _FakeConstraintSession:
        return _FakeConstraintSession(self)


# ---------------------------------------------------------------------------
# A1 -- tri-state probe + fail-open rule (D-E)
# ---------------------------------------------------------------------------


class TestProbeTriState:
    async def test_constraint_absent_closes_gate(self) -> None:
        coord = MaintenanceCoordinator()
        coord.bind_driver(_FakeConstraintDriver(present=False))
        assert await coord.gate_closed() is True

    async def test_constraint_present_opens_gate(self) -> None:
        coord = MaintenanceCoordinator()
        coord.bind_driver(_FakeConstraintDriver(present=True))
        assert await coord.gate_closed() is False

    async def test_probe_raises_yields_unknown_and_gate_open(self) -> None:
        coord = MaintenanceCoordinator()
        coord.bind_driver(
            _FakeConstraintDriver(raise_exc=RuntimeError("neo4j unreachable"))
        )
        st = await coord.status()
        assert st.constraint_present is None
        assert st.mode == "unknown"
        assert await coord.gate_closed() is False  # unknown must NOT close the gate


# ---------------------------------------------------------------------------
# A2 -- TTL cache works
# ---------------------------------------------------------------------------


class TestProbeTtlCache:
    async def test_two_calls_within_ttl_hit_driver_once(self) -> None:
        driver = _FakeConstraintDriver(present=True)
        coord = MaintenanceCoordinator()
        coord.bind_driver(driver, probe_ttl_seconds=10.0)

        await coord.gate_closed()
        await coord.gate_closed()

        assert driver.call_count == 1

    async def test_call_after_ttl_expiry_hits_driver_again(self) -> None:
        driver = _FakeConstraintDriver(present=True)
        coord = MaintenanceCoordinator()
        coord.bind_driver(driver, probe_ttl_seconds=0.05)

        await coord.gate_closed()
        # First in-window call: constraint catalog read (cold) + untagged
        # count (seeded warm at bind_driver, so 0 reads here) == 1 read.
        hits_after_first = driver.call_count
        assert hits_after_first == 1
        await asyncio.sleep(0.1)  # both caches expire
        await coord.gate_closed()

        # After expiry a FRESH probe runs -- the load-bearing property is that
        # the cache expired and re-probed, not the exact read count (which now
        # also includes the live O(1) untagged count, present-constraint path).
        assert driver.call_count > hits_after_first


# ---------------------------------------------------------------------------
# A3 -- single-flight: N concurrent callers at cache expiry -> ONE probe
# ---------------------------------------------------------------------------


class TestProbeSingleFlight:
    async def test_fifty_concurrent_calls_at_expiry_yield_one_catalog_read(
        self,
    ) -> None:
        driver = _FakeConstraintDriver(present=True)
        coord = MaintenanceCoordinator()
        coord.bind_driver(driver, probe_ttl_seconds=0.02)

        # Prime + expire the cache once, deterministically.
        await coord.gate_closed()
        await asyncio.sleep(0.05)
        hits_before = driver.call_count
        assert hits_before == 1

        results = await asyncio.gather(*[coord.gate_closed() for _ in range(50)])

        # Single-flight: all 50 callers at the expiry boundary collapse to ONE
        # probe window, NOT 50. That window now reads the constraint catalog
        # AND the live O(1) untagged count (constraint-present path), so the
        # increment is a small constant (<= 3), never proportional to the 50
        # callers -- which is the property this test guards.
        new_reads = driver.call_count - hits_before
        assert new_reads <= 3
        assert all(r is False for r in results)  # constraint present -> gate open


# ---------------------------------------------------------------------------
# A4 -- THE LATCH DEFECT, DIRECTLY: probe flips False->True with NO restart
# ---------------------------------------------------------------------------


class TestNoRestartSelfClear:
    async def test_gate_opens_within_ttl_with_no_restart(self) -> None:
        """This is the exact bug this spec exists to fix: a boot-latched
        schema_health never re-probed until the next restart. Here the SAME
        coordinator instance (no restart) observes the constraint appear."""
        driver = _FakeConstraintDriver(present=False)
        coord = MaintenanceCoordinator()
        coord.bind_driver(driver, probe_ttl_seconds=0.05)

        assert await coord.gate_closed() is True  # migration required

        # Out-of-band repair happens (e.g. `doctor --fix` in another
        # process) -- constraint now exists. NO restart of this process.
        driver.present = True
        await asyncio.sleep(0.1)  # let the TTL expire

        assert await coord.gate_closed() is False  # self-cleared, no restart

    async def test_non_vacuous_without_ttl_expiry_gate_stays_stale(self) -> None:
        """Non-vacuity proof (WS-2 precedent): if we DON'T wait out the TTL,
        the cached (stale) answer is returned -- proving the test above is
        actually exercising the cache-expiry path, not a tautology."""
        driver = _FakeConstraintDriver(present=False)
        coord = MaintenanceCoordinator()
        coord.bind_driver(driver, probe_ttl_seconds=10.0)  # long TTL

        assert await coord.gate_closed() is True

        driver.present = True  # flips immediately, but cache is still warm
        # NO sleep -- still within the 10s TTL window.
        assert await coord.gate_closed() is True  # stale cached answer


# ---------------------------------------------------------------------------
# A4b -- THE UNTAGGED LATCH: degraded->healthy self-clears with NO restart
# ---------------------------------------------------------------------------


class _FakeUntaggedResult:
    def __init__(self, count: int) -> None:
        self._count = count
        self._yielded = False

    def __aiter__(self) -> _FakeUntaggedResult:
        return self

    async def __anext__(self) -> dict[str, int]:
        if self._yielded:
            raise StopAsyncIteration
        self._yielded = True
        return {"c": self._count}


class _FakeUntaggedSession:
    def __init__(self, driver: _FakeUntaggedDriver) -> None:
        self._driver = driver

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def run(self, cypher: str, *args: Any, **kwargs: Any) -> _FakeUntaggedResult:
        self._driver.call_count += 1
        if "SHOW CONSTRAINTS" in cypher:
            return _FakeUntaggedResult(1)  # constraint present
        if ":Node)" in cypher:
            return _FakeUntaggedResult(self._driver.tagged)  # tagged-node count
        return _FakeUntaggedResult(self._driver.total)  # total-node count


class _FakeUntaggedDriver:
    """Test double: constraint always present; untagged = total - tagged, and
    both are flippable at runtime so a test can simulate an out-of-band repair
    tagging the last untagged node WITHOUT a restart."""

    def __init__(self, total: int, tagged: int) -> None:
        self.total = total
        self.tagged = tagged
        self.call_count = 0

    def session(self, **kwargs: Any) -> _FakeUntaggedSession:
        return _FakeUntaggedSession(self)


class TestUntaggedNoRestartSelfClear:
    async def test_degraded_self_clears_within_ttl_no_restart(self) -> None:
        """The untagged half of the latch: schema_health/mode report `degraded`
        (1 node lacking :Node) at boot; an out-of-band repair tags it; the SAME
        coordinator (no restart) reports `healthy` within one TTL."""
        driver = _FakeUntaggedDriver(total=1, tagged=0)  # 1 untagged -> degraded
        coord = MaintenanceCoordinator()
        coord.bind_driver(driver, untagged=1, probe_ttl_seconds=0.05)

        st = await coord.status()
        assert st.mode == "degraded"
        assert st.untagged_nodes == 1
        assert await coord.gate_closed() is False  # degraded NEVER closes gate

        # Out-of-band repair tags the node -- untagged now 0. NO restart.
        driver.tagged = 1
        await asyncio.sleep(0.1)  # let the untagged-probe TTL expire

        st2 = await coord.status()
        assert st2.mode == "healthy"  # self-cleared, no restart
        assert st2.untagged_nodes == 0
        assert st2.reason is None

    async def test_non_vacuous_within_ttl_stays_degraded(self) -> None:
        """Non-vacuity: without waiting out the TTL the seeded/cached count is
        returned -- proving the test above exercises the expiry path, not a
        tautology."""
        driver = _FakeUntaggedDriver(total=1, tagged=0)
        coord = MaintenanceCoordinator()
        coord.bind_driver(driver, untagged=1, probe_ttl_seconds=10.0)  # long TTL

        st = await coord.status()
        assert st.mode == "degraded"

        driver.tagged = 1  # repaired immediately, but cache still warm
        # NO sleep -- still within the 10s TTL window.
        st2 = await coord.status()
        assert st2.mode == "degraded"  # stale cached count
        assert st2.untagged_nodes == 1


# ---------------------------------------------------------------------------
# A5 -- op_running holds the gate closed even when constraint IS present
# ---------------------------------------------------------------------------


class TestOpRunningKeepsGateClosed:
    async def test_op_running_with_constraint_present_still_closed(self) -> None:
        coord = MaintenanceCoordinator()
        coord.bind_driver(_FakeConstraintDriver(present=True))
        assert await coord.gate_closed() is False  # sanity: open beforehand

        run_id = coord.try_begin_op()
        assert run_id is not None

        assert await coord.gate_closed() is True  # op_running term (D-C)

        coord.finish_op(run_id, records_affected=0, error=None)
        assert await coord.gate_closed() is False  # reopens once finished


# ---------------------------------------------------------------------------
# A12 -- transition logging fires EXACTLY once per transition
# ---------------------------------------------------------------------------


class TestTransitionLogging:
    async def test_closed_open_closed_emits_exactly_one_each(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        driver = _FakeConstraintDriver(
            present=False
        )  # start closed (migration required)
        coord = MaintenanceCoordinator()
        coord.bind_driver(driver, probe_ttl_seconds=0.02)

        with caplog.at_level(logging.INFO, logger="context_intelligence_server"):
            # Already "maintenance" from bind (closed=migration required).
            await coord.status()
            await coord.status()  # repeat call: must not double-log

            # Flip open.
            driver.present = True
            await asyncio.sleep(0.05)
            await coord.status()
            await coord.status()  # repeat: must not double-log

            # Flip closed again.
            driver.present = False
            await asyncio.sleep(0.05)
            await coord.status()

        entered = [r for r in caplog.records if r.getMessage() == "maintenance_entered"]
        completed = [
            r for r in caplog.records if r.getMessage() == "maintenance_completed"
        ]
        # Two "entered" (initial closed state, then re-closed) and one
        # "completed" (the middle open window).
        assert len(entered) == 2
        assert len(completed) == 1


# ---------------------------------------------------------------------------
# A13 -- no driver bound => gate OPEN (the existing-suite no-regression rule)
# ---------------------------------------------------------------------------


class TestNoDriverBound:
    async def test_unbound_coordinator_gate_is_open(self) -> None:
        coord = MaintenanceCoordinator()  # bind_driver() never called
        assert await coord.gate_closed() is False
        st = await coord.status()
        assert st.mode == "unknown"
        assert st.constraint_present is None


# ---------------------------------------------------------------------------
# try_begin_op / finish_op / current_op -- the CAS seam itself
# ---------------------------------------------------------------------------


class TestOpSeam:
    def test_try_begin_op_is_synchronous_cas(self) -> None:
        coord = MaintenanceCoordinator()
        run_id_1 = coord.try_begin_op()
        assert run_id_1 is not None
        run_id_2 = coord.try_begin_op()
        assert run_id_2 is None  # already running -- CAS refuses a second op

    def test_finish_op_sets_completed_at_and_state(self) -> None:
        coord = MaintenanceCoordinator()
        run_id = coord.try_begin_op()
        assert run_id is not None
        assert coord.current_op().completed_at is None

        coord.finish_op(run_id, records_affected=3, error=None)

        op = coord.current_op()
        assert op.state == "succeeded"
        assert op.completed_at is not None
        assert op.records_affected == 3

    def test_finish_op_with_error_marks_failed(self) -> None:
        coord = MaintenanceCoordinator()
        run_id = coord.try_begin_op()
        assert run_id is not None

        coord.finish_op(run_id, records_affected=None, error="boom")

        op = coord.current_op()
        assert op.state == "failed"
        assert op.error == "boom"

    def test_op_state_initializes_unknown_not_succeeded(self) -> None:
        """Council D4: never-run must not read as a false 'succeeded'."""
        coord = MaintenanceCoordinator()
        assert coord.current_op().state == "unknown"

    def test_finish_op_stale_run_id_is_ignored(self) -> None:
        coord = MaintenanceCoordinator()
        run_id = coord.try_begin_op()
        assert run_id is not None

        coord.finish_op("not-the-real-run-id", records_affected=1, error=None)

        # The real op is untouched -- still running, not clobbered by a
        # foreign/stale completion signal.
        assert coord.current_op().state == "running"


# ---------------------------------------------------------------------------
# Allow-list sanity (used by the startup assertion + the middleware)
# ---------------------------------------------------------------------------


def test_allow_list_contains_required_paths() -> None:
    assert "/admin/maintenance" in MAINTENANCE_ALLOW_LIST
    assert "/status" in MAINTENANCE_ALLOW_LIST
    assert "/version" in MAINTENANCE_ALLOW_LIST
