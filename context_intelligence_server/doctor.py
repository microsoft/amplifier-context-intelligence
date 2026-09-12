"""Headless CLI diagnostic/repair gesture for Neo4j graph health.

`context-intelligence-server doctor` (read-only) and `doctor --fix` (repair)
replace the two O(graph-size) migration scans that used to run
unconditionally on every cold start (duplicate-node dedup + universal
``:Node`` label backfill). Those scans are pure dead weight on an
already-migrated graph but still paid a full graph scan on every boot.

This module is presentation only. It owns no connection: it starts a graph
backend, asks it to diagnose or repair, and closes it -- the same object, with
the same pool bounds, the server itself runs on. Previously this file built a
driver of its own, which made it a fourth independent construction site whose
settings could drift from the server's without anything noticing.
"""

from __future__ import annotations

import logging

from context_intelligence_server.config import get_settings
from context_intelligence_server.neo4j_backend import Neo4jGraphBackend

_LOG = logging.getLogger("context_intelligence_server.doctor")

_OK = "\033[32m\u2713\033[0m"  # green check
_FAIL = "\033[31m\u2717\033[0m"  # red x
_WARN = "\033[33m!\033[0m"  # yellow warning


def _is_healthy(diagnosis: dict[str, int]) -> bool:
    """A graph is healthy when it has zero untagged and zero duplicate nodes."""
    return diagnosis["untagged_nodes"] == 0 and diagnosis["duplicate_nodes"] == 0


def _print_diagnosis(diagnosis: dict[str, int]) -> None:
    untagged = diagnosis["untagged_nodes"]
    mark = _OK if untagged == 0 else _WARN
    print(f"  {mark} Untagged :Node count: {untagged}")

    duplicates = diagnosis["duplicate_nodes"]
    mark = _OK if duplicates == 0 else _WARN
    print(f"  {mark} Duplicate node count: {duplicates}")


async def run_doctor(fix: bool) -> int:
    """Diagnose (and optionally repair) Neo4j graph health.

    Loads config the same way the server does (``get_settings()``, honoring
    ``CONFIG_FILE``) and starts the SAME graph backend the server's lifespan
    starts, so the doctor CLI and the running server can never connect
    differently -- there is only one way to connect left.

    Args:
        fix: When False, report only (read-only). When True, repair
             (dedup + :Node backfill + schema DDL) if the graph is unhealthy,
             then re-diagnose and report the after-state.

    Returns:
        Process exit code: 0 when the graph is healthy (immediately, or
        after a successful repair); non-zero when the graph is unreachable,
        remains unhealthy (report-only mode), or repair left problems.
    """
    backend = Neo4jGraphBackend.from_settings(get_settings())
    await backend.start()
    try:
        health = await backend.health()
        if not health.write_connected:
            print(f"  {_FAIL} Neo4j reachable -- {health.url}")
            return 1
        print(f"  {_OK} Neo4j reachable")

        diagnosis = await backend.diagnose()
        _print_diagnosis(diagnosis)

        if _is_healthy(diagnosis):
            print(f"  {_OK} Graph is healthy -- no repair needed.")
            return 0

        if not fix:
            print(
                f"  {_WARN} Graph has un-migrated legacy data. Re-run with "
                "--fix to repair: context-intelligence-server doctor --fix"
            )
            return 1

        print("Repairing (dedup + :Node backfill + schema DDL)...")
        result = await backend.repair()
        print(
            f"  {_OK} Repair complete: "
            f"{result['duplicates_removed']} duplicate(s) removed, "
            f"{result['nodes_tagged']} node(s) tagged :Node."
        )

        after = await backend.diagnose()
        _print_diagnosis(after)
        if _is_healthy(after):
            print(f"  {_OK} Graph is healthy after repair.")
            return 0
        print(f"  {_FAIL} Graph still has issues after repair -- see counts above.")
        return 1
    finally:
        await backend.aclose()
