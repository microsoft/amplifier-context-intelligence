"""Headless CLI diagnostic/repair gesture for Neo4j graph health.

`context-intelligence-server doctor` (read-only) and `doctor --fix` (repair)
replace the two O(graph-size) migration scans that used to run
unconditionally on every cold start (duplicate-node dedup + universal
``:Node`` label backfill). Those scans are pure dead weight on an
already-migrated graph but still paid a full graph scan on every boot.

This module is presentation + driver lifecycle only -- the actual
diagnostic/repair logic lives in ``neo4j_store`` (``diagnose`` /
``run_repair``), which is also what ``ensure_neo4j_schema`` relies on at
cold start for its (now cheap, scan-free) schema DDL.
"""

from __future__ import annotations

import logging

from context_intelligence_server.config import get_settings
from context_intelligence_server.main import build_neo4j_driver
from context_intelligence_server.neo4j_store import diagnose, run_repair

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
    ``CONFIG_FILE``) and constructs the admin Neo4j driver via the same
    ``build_neo4j_driver`` helper ``lifespan()`` uses, so the doctor CLI and
    the running server can never construct the connection differently.

    Args:
        fix: When False, report only (read-only). When True, repair
             (dedup + :Node backfill + schema DDL) if the graph is unhealthy,
             then re-diagnose and report the after-state.

    Returns:
        Process exit code: 0 when the graph is healthy (immediately, or
        after a successful repair); non-zero when Neo4j is unreachable, the
        graph remains unhealthy (report-only mode), or repair left problems.
    """
    settings = get_settings()
    admin = settings.resolve_neo4j_admin()
    driver = build_neo4j_driver(admin)
    try:
        try:
            await driver.verify_connectivity()
        except Exception as exc:  # noqa: BLE001
            print(f"  {_FAIL} Neo4j reachable -- {exc}")
            return 1
        print(f"  {_OK} Neo4j reachable")

        diagnosis = await diagnose(driver)
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
        result = await run_repair(driver)
        print(
            f"  {_OK} Repair complete: "
            f"{result['duplicates_removed']} duplicate(s) removed, "
            f"{result['nodes_tagged']} node(s) tagged :Node."
        )

        after = await diagnose(driver)
        _print_diagnosis(after)
        if _is_healthy(after):
            print(f"  {_OK} Graph is healthy after repair.")
            return 0
        print(f"  {_FAIL} Graph still has issues after repair -- see counts above.")
        return 1
    finally:
        await driver.close()
