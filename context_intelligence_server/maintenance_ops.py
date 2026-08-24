"""Maintenance operation execution -- the ONE shared logic home for running
the maintenance repair operation, used by both ``POST /admin/maintenance``
and, later, the standalone out-of-band migration script
sec 5.4, not built in this change).

This module writes NO new dedup/repair algorithm:
it wraps the existing ``neo4j_store.run_repair`` with exactly the two pieces
of bookkeeping an *in-process, gated* caller needs that a standalone script
would not:

- a bounded pre-op quiesce sleep to let ordinary in-flight
  flushes land before the schema DDL runs, and
- recording the outcome via the ``MaintenanceCoordinator`` seam
  (``finish_op``), so ``GET /admin/maintenance`` and ``/status`` observe it.

Kept standalone-friendly on purpose: ``run_maintenance_operation`` takes the
driver and coordinator explicitly (no FastAPI ``Request``, no import of
``main`` or ``routers.admin``), so a future standalone script can reuse it
directly if it ever wants coordinator-aware bookkeeping. No
bypass mechanism exists here or anywhere else -- this function reaches Neo4j
only through the driver it is given (the admin driver, ``app.state.neo4j_driver``
at the HTTP call site) and never consults ``coordinator.gate_closed()``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from context_intelligence_server.maintenance import MaintenanceCoordinator, coordinator
from context_intelligence_server.neo4j_store import run_repair

logger = logging.getLogger("context_intelligence_server")


async def run_maintenance_operation(
    driver: Any,
    run_id: str,
    *,
    quiesce_seconds: float,
    database: str = "neo4j",
    coord: MaintenanceCoordinator = coordinator,
) -> None:
    """Run one maintenance operation to completion and record its outcome.

    Args:
        driver: The Neo4j admin driver (``app.state.neo4j_driver`` at the
            HTTP call site). Never the query-only driver.
        run_id: The run id returned by ``coord.try_begin_op()`` -- the
            caller MUST have already won the CAS before scheduling this.
        quiesce_seconds: Seconds to sleep before calling ``run_repair`` (spec
            sec 5.3). Pass 0 to skip (e.g. direct unit testing).
        database: Neo4j database name, forwarded to ``run_repair``.
        coord: The coordinator instance to report back to. Defaults to the
            process-wide singleton; overridable for tests.

    On success, calls ``coord.finish_op(run_id, records_affected=n, error=None)``
    with ``n = duplicates_removed + nodes_tagged``. On any
    exception, calls ``coord.finish_op(run_id, records_affected=None,
    error=str(exc))`` -- the op is recorded ``failed``, never silently lost,
    and the gate stays closed (``op_running`` only clears via ``finish_op``)
    until an operator retries.
    """
    if quiesce_seconds > 0:
        logger.info("maintenance_quiesce run_id=%s seconds=%s", run_id, quiesce_seconds)
        await asyncio.sleep(quiesce_seconds)
    try:
        result = await run_repair(driver, database=database)
        records_affected = result["duplicates_removed"] + result["nodes_tagged"]
        coord.finish_op(run_id, records_affected=records_affected, error=None)
        logger.info(
            "maintenance_op_succeeded run_id=%s records_affected=%s",
            run_id,
            records_affected,
        )
    except Exception as exc:
        logger.exception("maintenance_op_failed run_id=%s", run_id)
        coord.finish_op(run_id, records_affected=None, error=str(exc))
