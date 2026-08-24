"""End-to-end migration CLI against a REAL Neo4j -- no mocked driver.

The unit test (tests/test_migrations_run.py) drives ``_amain`` with a mocked
driver and mocked ``run_repair``; this reconciles those assumptions with
reality: the CLI builds its OWN real driver from ``--neo4j-*`` flags, runs
``--status`` (read-only) then ``--apply`` against a genuinely dirty graph, and
the graph is actually rectified. Re-running ``--apply`` is a real no-op.

    uv run pytest tests/neo4j/test_migrations_cli_e2e.py -q -m neo4j
"""

from __future__ import annotations

from typing import Any

import migrations.run as run_module
import pytest
from neo4j import GraphDatabase

pytestmark = pytest.mark.neo4j

_WS = "migrate_e2e"


def _wipe(container: dict[str, Any]) -> None:
    driver = GraphDatabase.driver(
        container["bolt_url"], auth=(container["user"], container["password"])
    )
    try:
        with driver.session() as s:
            s.run("MATCH (n) DETACH DELETE n")
            for rec in list(s.run("SHOW CONSTRAINTS YIELD name RETURN name")):
                s.run(f"DROP CONSTRAINT {rec['name']} IF EXISTS")
            for rec in list(s.run("SHOW INDEXES YIELD name RETURN name")):
                try:
                    s.run(f"DROP INDEX {rec['name']} IF EXISTS")
                except Exception:  # noqa: BLE001 - constraint-backed/lookup indexes
                    pass
    finally:
        driver.close()


def _seed_dirty(container: dict[str, Any]) -> None:
    """Two duplicate untagged :Event nodes + one legacy :Session (no :Node)."""
    driver = GraphDatabase.driver(
        container["bolt_url"], auth=(container["user"], container["password"])
    )
    try:
        with driver.session() as s:
            s.run("CREATE (:Event {node_id: 'dup-1', workspace: $ws, v: 1})", ws=_WS)
            s.run("CREATE (:Event {node_id: 'dup-1', workspace: $ws, v: 2})", ws=_WS)
            s.run("CREATE (:Session {node_id: 'legacy-sess', workspace: $ws})", ws=_WS)
    finally:
        driver.close()


def _graph_state(container: dict[str, Any]) -> dict[str, int]:
    driver = GraphDatabase.driver(
        container["bolt_url"], auth=(container["user"], container["password"])
    )
    try:
        with driver.session() as s:
            untagged = s.run(
                "MATCH (n) WHERE n.node_id IS NOT NULL AND NOT n:Node RETURN count(n) AS c"
            ).single()["c"]
            dup1 = s.run(
                "MATCH (n {node_id: 'dup-1', workspace: $ws}) RETURN count(n) AS c",
                ws=_WS,
            ).single()["c"]
            constraint = len(
                list(s.run("SHOW CONSTRAINTS YIELD name RETURN name"))
            )
            return {"untagged": untagged, "dup1": dup1, "constraints": constraint}
    finally:
        driver.close()


def _args(container: dict[str, Any], flag: str):
    return run_module.build_parser().parse_args(
        [
            flag,
            "--neo4j-url",
            container["bolt_url"],
            "--neo4j-user",
            container["user"],
            "--neo4j-password",
            container["password"],
        ]
    )


async def test_migration_cli_status_then_apply_rectifies_real_graph(
    neo4j_container: dict[str, Any],
) -> None:
    _wipe(neo4j_container)
    try:
        _seed_dirty(neo4j_container)

        before = _graph_state(neo4j_container)
        assert before["untagged"] >= 3, before
        assert before["dup1"] == 2, before

        # --status is read-only: reports, never mutates.
        code = await run_module._amain(_args(neo4j_container, "--status"))
        assert code == 0
        after_status = _graph_state(neo4j_container)
        assert after_status == before, "status must not touch the graph"

        # --apply rectifies for real.
        code = await run_module._amain(_args(neo4j_container, "--apply"))
        assert code == 0
        after_apply = _graph_state(neo4j_container)
        assert after_apply["untagged"] == 0, after_apply
        assert after_apply["dup1"] == 1, after_apply
        assert after_apply["constraints"] >= 1, after_apply

        # Re-running --apply is a real idempotent no-op (still clean, still 0).
        code = await run_module._amain(_args(neo4j_container, "--apply"))
        assert code == 0
        assert _graph_state(neo4j_container) == after_apply
    finally:
        _wipe(neo4j_container)
