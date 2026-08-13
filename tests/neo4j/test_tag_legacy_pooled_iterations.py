"""Live Neo4j test coverage for scripts/tag_legacy_pooled_iterations.py.

The script's own module docstring states it has **NO test coverage** ("NOT
product code -- no unit tests"), despite mutating a live graph (it ``SET``s a
non-destructive marker property on a confirmed-corrupt subset of :Iteration
nodes). This module closes that gap against a REAL Neo4j test container (see
tests/neo4j/conftest.py), mirroring the direct-function-import pattern used
by ``test_relabel_incomplete_sessions.py`` -- both scripts share the same
``CALL { ... } IN TRANSACTIONS OF N ROWS`` batching shape and both expose
their session-taking functions (``run_dry_run``, ``run_apply``,
``tag_confirmed_corrupt``) as directly importable/testable seams, so no
subprocess/CLI invocation is needed.

Covers:

(a) Selector precision -- an :Iteration node reached by ``HAS_PART`` from
    ``>=2`` distinct :OrchestratorRun nodes (the confirmed-corrupt / legacy
    pooled shape) IS tagged, and a node with exactly ONE run parent (the
    clean, single-run shape that is the large majority of bare-id nodes in
    live data per the script's own docstring) is NEVER tagged. Both shapes
    are seeded in the SAME graph so the selector's precision is proven, not
    just its recall.
(b) The --apply gate -- ``run_dry_run()`` (the default / non-apply
    invocation) performs ZERO writes: the pooled node is left untagged.
    Only ``run_apply()`` (the --apply path) sets the marker.
(c) Idempotence -- a second ``tag_confirmed_corrupt``/``run_apply`` call
    against an already-tagged graph tags/matches zero additional rows, and
    a subsequent ``run_apply()`` still reports success (exit 0).

Run: uv run pytest tests/neo4j/test_tag_legacy_pooled_iterations.py -v -m neo4j
"""

from __future__ import annotations

from typing import Any

import pytest
from neo4j import GraphDatabase
from scripts import tag_legacy_pooled_iterations as tagger

WORKSPACE = "test"

pytestmark = pytest.mark.neo4j


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _driver(neo4j_container: dict) -> GraphDatabase:
    """Return a synchronous Neo4j driver for the test container."""
    return GraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )


def _seed_run(session, run_id: str) -> None:
    """Seed a bare :OrchestratorRun node."""
    session.run(
        "MERGE (r:OrchestratorRun {node_id: $run_id, workspace: $workspace})",
        run_id=run_id,
        workspace=WORKSPACE,
    )


def _seed_bare_iteration(session, iter_id: str) -> None:
    """Seed a bare-id :Iteration node.

    ``iter_id`` must NOT contain ``'::orch_run::'`` -- that is the pre-I5,
    per-session-counter shape the script's ``_CONFIRMED_CORRUPT_MATCH``
    selector considers at all. Run-scoped (post-I5) node_ids are excluded by
    the selector's ``WHERE NOT i.node_id CONTAINS '::orch_run::'`` clause.
    """
    session.run(
        "MERGE (i:Iteration {node_id: $iter_id, workspace: $workspace})",
        iter_id=iter_id,
        workspace=WORKSPACE,
    )


def _link(session, run_id: str, iter_id: str) -> None:
    """Seed the real ``(OrchestratorRun)-[:HAS_PART]->(Iteration)`` edge
    shape the script's selector matches (``MATCH (run:OrchestratorRun)
    -[:HAS_PART]->(i:Iteration)``)."""
    session.run(
        "MATCH (r:OrchestratorRun {node_id: $run_id, workspace: $workspace}) "
        "MATCH (i:Iteration {node_id: $iter_id, workspace: $workspace}) "
        "MERGE (r)-[:HAS_PART]->(i)",
        run_id=run_id,
        iter_id=iter_id,
        workspace=WORKSPACE,
    )


def _data_quality(session, iter_id: str) -> str | None:
    """Return the ``data_quality`` property of an :Iteration node, or None."""
    result = session.run(
        "MATCH (i:Iteration {node_id: $iter_id, workspace: $workspace}) "
        "RETURN i.data_quality AS dq",
        iter_id=iter_id,
        workspace=WORKSPACE,
    )
    record = result.single()
    if record is None:
        return None
    return record["dq"]


def _seed_pooled_pair(session) -> None:
    """Seed one pooled (>=2 run parents) and one clean (1 run parent)
    bare-id Iteration node -- the shared fixture used by every test below."""
    _seed_run(session, "run-1")
    _seed_run(session, "run-2")
    _seed_bare_iteration(session, "sess-pooled::iteration::1")
    _link(session, "run-1", "sess-pooled::iteration::1")
    _link(session, "run-2", "sess-pooled::iteration::1")

    _seed_run(session, "run-3")
    _seed_bare_iteration(session, "sess-clean::iteration::1")
    _link(session, "run-3", "sess-clean::iteration::1")


# ---------------------------------------------------------------------------
# Per-test isolation
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_neo4j(neo4j_container: dict) -> None:  # type: ignore[return]
    """Wipe the container clean before each test for complete isolation."""
    driver = GraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    try:
        with driver.session() as s:
            s.run("MATCH (n) DETACH DELETE n")
    finally:
        driver.close()


# ---------------------------------------------------------------------------
# (a) selector precision: >=2 parents tagged, exactly-1 parent NEVER tagged
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestSelectorPrecision:
    def test_pooled_node_tagged_clean_node_untouched(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                _seed_pooled_pair(s)

                exit_code = tagger.run_apply(s, batch_size=tagger.DEFAULT_BATCH_SIZE)

                assert exit_code == 0
                assert (
                    _data_quality(s, "sess-pooled::iteration::1") == tagger.TAG_VALUE
                ), "the >=2-distinct-run-parent node must be tagged"
                assert _data_quality(s, "sess-clean::iteration::1") is None, (
                    "the exactly-1-run-parent (clean, single-run) node must "
                    "NEVER be tagged -- this is exactly the shape the "
                    "script's own docstring says is the majority of live "
                    "bare-id nodes and must be left untouched"
                )
        finally:
            driver.close()


# ---------------------------------------------------------------------------
# (b) --apply gate: dry-run makes NO mutation; only --apply writes
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestApplyGate:
    def test_dry_run_makes_no_mutation_only_apply_writes(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                _seed_pooled_pair(s)

                # The default / non-apply invocation: run_dry_run() only
                # calls classify() (read-only counts, no SET anywhere in its
                # call graph). It must leave the confirmed-corrupt node
                # completely untagged.
                dry_exit_code = tagger.run_dry_run(s)
                assert dry_exit_code == 0
                assert _data_quality(s, "sess-pooled::iteration::1") is None, (
                    "run_dry_run must make ZERO writes -- the confirmed-"
                    "corrupt pooled node must remain untagged after a "
                    "dry-run invocation"
                )
                assert _data_quality(s, "sess-clean::iteration::1") is None

                # Only --apply (run_apply) is permitted to write the tag.
                apply_exit_code = tagger.run_apply(
                    s, batch_size=tagger.DEFAULT_BATCH_SIZE
                )
                assert apply_exit_code == 0
                assert (
                    _data_quality(s, "sess-pooled::iteration::1") == tagger.TAG_VALUE
                ), "run_apply must tag the pooled node once explicitly invoked"
        finally:
            driver.close()


# ---------------------------------------------------------------------------
# (c) idempotence: second apply tags/matches zero additional rows
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestIdempotence:
    def test_second_apply_tags_zero_additional_rows(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                _seed_pooled_pair(s)

                first_tagged = tagger.tag_confirmed_corrupt(
                    s, batch_size=tagger.DEFAULT_BATCH_SIZE
                )
                second_tagged = tagger.tag_confirmed_corrupt(
                    s, batch_size=tagger.DEFAULT_BATCH_SIZE
                )

                assert first_tagged == 1, (
                    "expected exactly 1 node tagged on the first apply "
                    f"(the pooled node only), got {first_tagged}"
                )
                assert second_tagged == 0, (
                    "a second apply against an already-tagged graph must "
                    f"match and write zero rows (idempotent), got {second_tagged}"
                )
                assert _data_quality(s, "sess-pooled::iteration::1") == tagger.TAG_VALUE
                assert _data_quality(s, "sess-clean::iteration::1") is None

                # The full run_apply() gate (not just the raw write helper)
                # must also report success and find nothing outstanding on
                # a third, still-idempotent re-run -- no error, no double-tag.
                exit_code = tagger.run_apply(s, batch_size=tagger.DEFAULT_BATCH_SIZE)
                assert exit_code == 0
        finally:
            driver.close()
