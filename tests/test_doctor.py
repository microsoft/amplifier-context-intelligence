"""Tests for context_intelligence_server.doctor -- the `doctor` / `doctor --fix`
CLI gesture that replaced the two O(graph-size) migration scans formerly run
unconditionally at cold start.

Post graph-backend-segregation (PR #109), `run_doctor` owns no connection of
its own: it builds a `Neo4jGraphBackend` via `Neo4jGraphBackend.from_settings`,
starts it, drives health/diagnose/repair through it, and closes it in a
`finally`. These tests patch `Neo4jGraphBackend` in the doctor module with a
fake backend double so no real config or Neo4j connection is required, and
assert on the backend's start/aclose/health/diagnose/repair calls instead of
a raw driver.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from context_intelligence_server import doctor as doctor_module
from context_intelligence_server.graph_backend import BackendHealth


class _FakeBackend:
    """GraphBackend double driving run_doctor's health/diagnose/repair calls.

    ``diagnose`` is a side_effect list so a test can express "before repair"
    and "after repair" snapshots the same way run_doctor consumes them (one
    call before repair, a second call after).
    """

    def __init__(
        self,
        *,
        write_connected: bool = True,
        diagnose_results: list[dict[str, int]] | None = None,
        repair_result: dict[str, int] | None = None,
    ) -> None:
        self.start = AsyncMock()
        self.aclose = AsyncMock()
        self.health = AsyncMock(
            return_value=BackendHealth(
                write_connected=write_connected,
                read_connected=write_connected,
                url="bolt://fake:7687",
                browser_url="",
            )
        )
        results = diagnose_results or [{"untagged_nodes": 0, "duplicate_nodes": 0}]
        self.diagnose = AsyncMock(side_effect=results)
        self.repair = AsyncMock(
            return_value=repair_result
            if repair_result is not None
            else {"duplicates_removed": 0, "nodes_tagged": 0}
        )


@pytest.fixture
def make_backend():
    """Factory fixture: patches Neo4jGraphBackend.from_settings to return a
    caller-configured _FakeBackend, and yields that backend for assertions."""

    def _make(**kwargs: object) -> _FakeBackend:
        backend = _FakeBackend(**kwargs)  # type: ignore[arg-type]
        patcher = patch.object(
            doctor_module.Neo4jGraphBackend, "from_settings", return_value=backend
        )
        patcher.start()
        _make.patchers.append(patcher)  # type: ignore[attr-defined]
        return backend

    _make.patchers = []  # type: ignore[attr-defined]
    yield _make
    for patcher in _make.patchers:  # type: ignore[attr-defined]
        patcher.stop()


async def test_run_doctor_healthy_returns_zero(make_backend) -> None:
    backend = make_backend(
        diagnose_results=[{"untagged_nodes": 0, "duplicate_nodes": 0}]
    )

    code = await doctor_module.run_doctor(fix=False)

    assert code == 0
    backend.start.assert_awaited_once()
    backend.aclose.assert_awaited_once()


async def test_run_doctor_unhealthy_report_only_returns_nonzero(make_backend) -> None:
    make_backend(diagnose_results=[{"untagged_nodes": 5, "duplicate_nodes": 0}])

    code = await doctor_module.run_doctor(fix=False)

    assert code != 0


async def test_run_doctor_fix_calls_run_repair_and_returns_zero_when_healthy_after(
    make_backend,
) -> None:
    backend = make_backend(
        diagnose_results=[
            {"untagged_nodes": 5, "duplicate_nodes": 2},  # before repair
            {"untagged_nodes": 0, "duplicate_nodes": 0},  # after repair
        ],
        repair_result={"duplicates_removed": 2, "nodes_tagged": 5},
    )

    code = await doctor_module.run_doctor(fix=True)

    assert code == 0
    backend.repair.assert_awaited_once()
    assert backend.diagnose.await_count == 2


async def test_run_doctor_fix_returns_nonzero_when_still_unhealthy_after(
    make_backend,
) -> None:
    make_backend(
        diagnose_results=[
            {"untagged_nodes": 5, "duplicate_nodes": 0},
            {"untagged_nodes": 3, "duplicate_nodes": 0},  # repair left residual
        ],
        repair_result={"duplicates_removed": 0, "nodes_tagged": 2},
    )

    code = await doctor_module.run_doctor(fix=True)

    assert code != 0


async def test_run_doctor_fix_does_not_repair_already_healthy_graph(
    make_backend,
) -> None:
    """fix=True on an already-healthy graph must not invoke backend.repair() at all."""
    backend = make_backend(
        diagnose_results=[{"untagged_nodes": 0, "duplicate_nodes": 0}]
    )

    code = await doctor_module.run_doctor(fix=True)

    assert code == 0
    backend.repair.assert_not_awaited()


async def test_run_doctor_neo4j_unreachable_returns_nonzero(make_backend) -> None:
    backend = make_backend(write_connected=False)

    code = await doctor_module.run_doctor(fix=False)

    assert code != 0
    backend.aclose.assert_awaited_once()


async def test_run_doctor_closes_driver_even_on_unreachable(make_backend) -> None:
    """The backend must be closed (finally-block) even when unreachable."""
    backend = make_backend(write_connected=False)

    await doctor_module.run_doctor(fix=True)

    backend.aclose.assert_awaited_once()
