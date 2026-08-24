"""Standalone, OUT-OF-BAND graph rectification tool.

``migrations/run.py`` is the local/VM/direct-Neo4j sibling of
``POST /admin/maintenance`` (the network-reachable channel for cloud
deployments). Both call the SAME underlying repair logic
(``neo4j_store.run_repair`` / ``diagnose``) -- this script adds no new
algorithm, only a CLI around the existing functions.

WHAT THIS DOES
    Rectifies an already-degraded graph: dedups duplicate legacy nodes,
    backfills the universal ``:Node`` label, and (re-)creates the
    ``:Node`` uniqueness constraint that the running server's maintenance
    gate (``context_intelligence_server/maintenance.py``) probes for.
    This is NOT a schema-version migration -- no stored node/edge shape
    changes, so ``SCHEMA_VERSION`` (``status.py``) stays ``1``.
    It is structural rectification of pre-existing degraded/un-migrated
    data, made necessary by upgrading the server past 6.7.x.

WHY IT EXISTS -- OUT-OF-BAND, NEVER AT STARTUP
    CORE PRINCIPLE (AGENTS.md, workspace root): migrations run OUT-OF-BAND
    against a live Neo4j instance -- never inside the server's
    startup/critical path. This script is NOT imported by the server and
    is NEVER invoked from ``lifespan()`` or any request handler. Run it by
    hand (local/VM) or trigger the equivalent in-server channel,
    ``POST /admin/maintenance`` (cloud, where a human cannot reach the
    private Neo4j directly) -- see the README "Upgrading" section.

IDEMPOTENT
    ``--status`` never writes. ``--apply`` calls ``run_repair``, which is
    idempotent (dedup -> :Node backfill -> constraint create, all
    ``IF NOT EXISTS`` / MERGE-based) -- re-running against an
    already-clean graph is a safe no-op.

USAGE
    python migrations/run.py --status
    uv run python migrations/run.py --apply
    python -m migrations.run --status

    Connection is resolved the same way the server resolves it: via
    ``config.Settings`` (``server-config.yaml`` then
    ``AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_*`` env vars), using the
    SAME driver constructor the server and ``doctor`` CLI use
    (``main.build_neo4j_driver``) so this tool can never connect
    differently than the process it is rectifying data for. Pass
    ``--neo4j-url`` / ``--neo4j-user`` / ``--neo4j-password`` to override
    any of the three independently (e.g. pointing at a different Neo4j
    than the local ``server-config.yaml`` without editing it).
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from context_intelligence_server.config import Neo4jClientConfig, get_settings
from context_intelligence_server.main import build_neo4j_driver
from context_intelligence_server.maintenance import _CONSTRAINT_NAME, _PROBE_CYPHER
from context_intelligence_server.neo4j_store import diagnose, run_repair
from context_intelligence_server.status import SCHEMA_VERSION, SERVER_VERSION

# Self-declared from -> to. This is a server-version / structural-
# rectification step, NOT a schema_version bump -- no stored node/edge
# shape changes, so schema_version stays 1 -> 1.
FROM_SERVER_VERSION = "6.7.x"
TO_SERVER_VERSION = "6.8.0"
FROM_SCHEMA_VERSION = 1
TO_SCHEMA_VERSION = 1

_OK = "\033[32m\u2713\033[0m"  # green check
_FAIL = "\033[31m\u2717\033[0m"  # red x
_WARN = "\033[33m!\033[0m"  # yellow warning


def _print_banner() -> None:
    print("=" * 72)
    print("context-intelligence-server -- out-of-band graph rectification")
    print(f"  server version : {FROM_SERVER_VERSION} -> {TO_SERVER_VERSION}")
    print(
        f"  schema_version : {FROM_SCHEMA_VERSION} -> {TO_SCHEMA_VERSION} (unchanged -- structural only)"
    )
    print(
        f"  running server reports: version={SERVER_VERSION} schema_version={SCHEMA_VERSION}"
    )
    print("  OUT-OF-BAND: never runs at server startup or on the request path.")
    print("  IDEMPOTENT: safe to re-run; --apply is a no-op on an already-clean graph.")
    print("=" * 72)


def _resolve_config(args: argparse.Namespace) -> Neo4jClientConfig:
    """Same resolution the server uses (``Settings``), with CLI overrides.

    Base config comes from ``get_settings().resolve_neo4j_admin()`` -- the
    identical call ``doctor.py`` and ``lifespan()`` make. ``--neo4j-*``
    flags override individual fields without requiring a
    ``server-config.yaml`` edit.
    """
    admin = get_settings().resolve_neo4j_admin()
    return admin.model_copy(
        update={
            k: v
            for k, v in (
                ("url", args.neo4j_url),
                ("username", args.neo4j_user),
                ("password", args.neo4j_password),
            )
            if v is not None
        }
    )


async def _constraint_present(driver: object) -> bool | None:
    """Tri-state ``:Node`` uniqueness constraint check.

    Reuses the exact catalog-read query the running server's maintenance
    gate probes with (``maintenance._PROBE_CYPHER`` / ``_CONSTRAINT_NAME``)
    -- one source of truth for "is the constraint present", never a
    second hardcoded copy of the constraint name.
    """
    try:
        async with driver.session() as session:  # type: ignore[attr-defined]
            result = await session.run(_PROBE_CYPHER)
            count = 0
            async for record in result:
                count = record["c"]
            return count > 0
    except Exception:  # noqa: BLE001 -- connectivity/catalog probe, report unknown
        return None


def _print_diagnosis(
    diagnosis: dict[str, int], constraint_present: bool | None
) -> None:
    mark = (
        _OK if constraint_present else (_WARN if constraint_present is False else _FAIL)
    )
    label = {True: "present", False: "ABSENT", None: "unknown (probe failed)"}[
        constraint_present
    ]
    print(f"  {mark} :Node uniqueness constraint ({_CONSTRAINT_NAME}): {label}")

    untagged = diagnosis["untagged_nodes"]
    mark = _OK if untagged == 0 else _WARN
    print(f"  {mark} Untagged :Node count: {untagged}")

    duplicates = diagnosis["duplicate_nodes"]
    mark = _OK if duplicates == 0 else _WARN
    print(f"  {mark} Duplicate node count: {duplicates}")


async def _run_status(driver: object) -> int:
    """Read-only report. Never writes. Exits 0 once Neo4j is reachable and
    the report has been printed, regardless of whether rectification is
    needed -- ``--status`` reports, it does not judge.
    """
    try:
        await driver.verify_connectivity()  # type: ignore[attr-defined]
    except Exception as exc:  # noqa: BLE001
        print(f"  {_FAIL} Neo4j reachable -- {exc}")
        return 1
    print(f"  {_OK} Neo4j reachable")

    constraint_present = await _constraint_present(driver)
    diagnosis = await diagnose(driver)
    _print_diagnosis(diagnosis, constraint_present)

    needs_rectification = (
        constraint_present is not True
        or diagnosis["untagged_nodes"]
        or diagnosis["duplicate_nodes"]
    )
    if needs_rectification:
        print(
            f"  {_WARN} Rectification needed -- re-run with --apply (or POST /admin/maintenance)."
        )
    else:
        print(f"  {_OK} Graph is healthy -- no rectification needed.")
    return 0


async def _run_apply(driver: object) -> int:
    """Rectify: dedup -> :Node backfill -> constraint create, via the
    SAME ``run_repair`` the server's ``/admin/maintenance`` endpoint and
    ``doctor --fix`` call. Prints before/after counts.
    """
    try:
        await driver.verify_connectivity()  # type: ignore[attr-defined]
    except Exception as exc:  # noqa: BLE001
        print(f"  {_FAIL} Neo4j reachable -- {exc}")
        return 1
    print(f"  {_OK} Neo4j reachable")

    print("-- before --")
    before = await diagnose(driver)
    _print_diagnosis(before, await _constraint_present(driver))

    print("Rectifying (dedup + :Node backfill + constraint create)...")
    result = await run_repair(driver)
    print(
        f"  {_OK} {result['duplicates_removed']} duplicate(s) removed, {result['nodes_tagged']} node(s) tagged :Node."
    )

    print("-- after --")
    after = await diagnose(driver)
    constraint_present = await _constraint_present(driver)
    _print_diagnosis(after, constraint_present)

    if (
        constraint_present is True
        and after["untagged_nodes"] == 0
        and after["duplicate_nodes"] == 0
    ):
        print(f"  {_OK} Graph is healthy after rectification.")
        return 0
    print(f"  {_FAIL} Graph still has issues after rectification -- see counts above.")
    return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="migrations/run.py",
        description="Out-of-band Neo4j graph rectification (never at server startup).",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--status",
        action="store_true",
        help="Read-only report (constraint presence, untagged/duplicate counts). Exit 0.",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="Rectify: dedup + :Node backfill + constraint create. Idempotent.",
    )
    parser.add_argument(
        "--neo4j-url",
        default=None,
        help="Override the Neo4j bolt URL (default: from Settings).",
    )
    parser.add_argument(
        "--neo4j-user",
        default=None,
        help="Override the Neo4j username (default: from Settings).",
    )
    parser.add_argument(
        "--neo4j-password",
        default=None,
        help="Override the Neo4j password (default: from Settings).",
    )
    return parser


async def _amain(args: argparse.Namespace) -> int:
    config = _resolve_config(args)
    driver = build_neo4j_driver(config)
    try:
        if args.apply:
            return await _run_apply(driver)
        return await _run_status(driver)
    finally:
        await driver.close()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _print_banner()
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    sys.exit(main())
