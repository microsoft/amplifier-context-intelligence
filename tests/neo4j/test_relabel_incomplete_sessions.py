"""Tier 3 Neo4j integration test module -- IncompleteSession backfill (Part 2).

Seeds real Session/SessionStartEvent/SessionForkEvent nodes and SOURCED_FROM
edges to exercise scripts/relabel_incomplete_sessions.py against a live
Neo4j container.  Covers the six scenarios from
docs/plans/2026-08-12-incomplete-session-relabel-spec.md, Part 2 "Tests":

(a) forked node with a real terminal label + stale marker -> cleared by the
    gated --apply path (run_apply).
(b) node with a linked SessionStartEvent (no terminal label yet) + stale
    marker -> cleared by the raw selector/write function (apply_relabel),
    proving B1's selector mechanics work on this node shape in isolation.
(c) genuine no-start/no-fork node -> RETAINED.
(d) idempotence: a second apply_relabel() call matches zero rows.
(e) the B2 reconciliation gate: seed a linked_but_untyped node and assert
    the gated --apply path (run_apply) REFUSES and makes NO write.
(f) --restore re-adds :IncompleteSession to exactly the touched ids.

Note: (b) and (e) seed the SAME node shape (IncompleteSession + a linked
start/fork event + no terminal label -- exactly "linked_but_untyped").  They
are deliberately not a contradiction: (b) proves the underlying B1 selector
mechanically heals that shape when invoked directly; (e) proves the B2
safety gate refuses the FULL --apply workflow when such a node exists
anywhere in the graph (the gate exists precisely because the assumption is
unverified on historical data, independent of whether the mechanics work).

Run: uv run pytest tests/neo4j/test_relabel_incomplete_sessions.py -v -m neo4j
"""

from __future__ import annotations

from typing import Any

import pytest
from neo4j import GraphDatabase
from scripts import relabel_incomplete_sessions as relabel

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


def _seed_session(session, node_id: str, extra_labels: list[str] | None = None) -> None:
    """Seed a bare :Session:IncompleteSession node, optionally with extra labels."""
    labels = "".join(f":{lbl}" for lbl in (extra_labels or []))
    session.run(
        f"MERGE (s:Session:IncompleteSession{labels} "
        "{node_id: $node_id, workspace: $workspace})",
        node_id=node_id,
        workspace=WORKSPACE,
    )


def _seed_linked_event(
    session, session_node_id: str, event_label: str, event_node_id: str
) -> None:
    """Seed a SessionStartEvent/SessionForkEvent node and the real
    (Session)-[:SOURCED_FROM]->(Event) edge direction (see the module
    docstring's "Selector direction correction" for why this direction, not
    the reverse, is what the shipping write path actually produces).
    """
    session.run(
        f"MATCH (s:Session {{node_id: $session_node_id, workspace: $workspace}}) "
        f"MERGE (e:{event_label} {{node_id: $event_node_id, workspace: $workspace}}) "
        "MERGE (s)-[:SOURCED_FROM]->(e)",
        session_node_id=session_node_id,
        event_node_id=event_node_id,
        workspace=WORKSPACE,
    )


def _labels(session, node_id: str) -> list[str]:
    """Return the labels of a node matched by node_id + workspace, or []."""
    result = session.run(
        "MATCH (n {node_id: $node_id, workspace: $workspace}) RETURN labels(n) AS lbls",
        node_id=node_id,
        workspace=WORKSPACE,
    )
    record = result.single()
    if record is None:
        return []
    return list(record["lbls"])


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
# (a) forked node with real terminal + stale marker -> cleared by --apply
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestApplyClearsTerminalLabeledNode:
    def test_forked_node_with_terminal_label_is_healed_by_gated_apply(
        self, neo4j_container: dict[str, Any], tmp_path: Any
    ) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                _seed_session(s, "sess-a", extra_labels=["ForkedSession"])

                exit_code = relabel.run_apply(
                    s,
                    batch_size=relabel.DEFAULT_BATCH_SIZE,
                    undo_log_path=str(tmp_path / "undo-a.json"),
                    neo4j_url=neo4j_container["bolt_url"],
                )

                assert exit_code == 0
                labels = _labels(s, "sess-a")
                assert "ForkedSession" in labels
                assert "IncompleteSession" not in labels
        finally:
            driver.close()


# ---------------------------------------------------------------------------
# (b) linked SessionStartEvent, no terminal label -> cleared by the raw
#     selector/write function (apply_relabel), proving B1 mechanics.
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestApplyRelabelClearsLinkedUntypedNode:
    def test_linked_start_event_no_terminal_label_healed_by_raw_selector(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                _seed_session(s, "sess-b")
                _seed_linked_event(s, "sess-b", "SessionStartEvent", "sess-b::start")

                touched = relabel.apply_relabel(
                    s, batch_size=relabel.DEFAULT_BATCH_SIZE
                )

                assert touched == ["sess-b"]
                labels = _labels(s, "sess-b")
                assert "IncompleteSession" not in labels
                assert "Session" in labels
        finally:
            driver.close()


# ---------------------------------------------------------------------------
# (c) genuine no-start/no-fork node -> RETAINED
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestGenuineIncompleteRetained:
    def test_genuine_node_with_no_linked_event_and_no_terminal_is_retained(
        self, neo4j_container: dict[str, Any], tmp_path: Any
    ) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                _seed_session(s, "sess-c")

                exit_code = relabel.run_apply(
                    s,
                    batch_size=relabel.DEFAULT_BATCH_SIZE,
                    undo_log_path=str(tmp_path / "undo-c.json"),
                    neo4j_url=neo4j_container["bolt_url"],
                )

                assert exit_code == 0
                labels = _labels(s, "sess-c")
                assert "IncompleteSession" in labels, (
                    "genuine incomplete session (no linked start/fork event) "
                    f"must be retained; got {labels}"
                )
        finally:
            driver.close()


# ---------------------------------------------------------------------------
# (d) idempotence: second apply_relabel() matches zero rows
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestIdempotence:
    def test_second_apply_relabel_touches_zero_rows(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                _seed_session(s, "sess-d", extra_labels=["ForkedSession"])

                first = relabel.apply_relabel(s, batch_size=relabel.DEFAULT_BATCH_SIZE)
                second = relabel.apply_relabel(s, batch_size=relabel.DEFAULT_BATCH_SIZE)

                assert first == ["sess-d"]
                assert second == []
                labels = _labels(s, "sess-d")
                assert "IncompleteSession" not in labels
                assert "ForkedSession" in labels
        finally:
            driver.close()


# ---------------------------------------------------------------------------
# (e) B2 reconciliation gate: linked_but_untyped node -> --apply REFUSES
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestReconciliationGateRefusesApply:
    def test_linked_but_untyped_node_causes_gated_apply_to_refuse(
        self, neo4j_container: dict[str, Any], tmp_path: Any
    ) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                # Same shape as (b) -- IncompleteSession + linked start event,
                # no terminal label -- but exercised through the GATED
                # run_apply() path instead of the raw apply_relabel().
                _seed_session(s, "sess-e")
                _seed_linked_event(s, "sess-e", "SessionStartEvent", "sess-e::start")

                diag = relabel.diagnostic_report(s)
                assert diag["linked_but_untyped"] == 1
                assert diag["linked_but_untyped_samples"] == ["sess-e"]

                exit_code = relabel.run_apply(
                    s,
                    batch_size=relabel.DEFAULT_BATCH_SIZE,
                    undo_log_path=str(tmp_path / "undo-e.json"),
                    neo4j_url=neo4j_container["bolt_url"],
                )

                assert exit_code == 1, (
                    "run_apply must REFUSE (non-zero exit) when the B2 "
                    "diagnostic finds a linked_but_untyped node"
                )
                # No write must have occurred: the marker is still present.
                labels = _labels(s, "sess-e")
                assert "IncompleteSession" in labels, (
                    "gate refusal must not mutate the graph; "
                    f"IncompleteSession missing from {labels}"
                )
                # The undo-log must not have been written either.
                assert not (tmp_path / "undo-e.json").exists()
        finally:
            driver.close()


# ---------------------------------------------------------------------------
# (f) --restore re-adds :IncompleteSession to exactly the touched ids
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestRestore:
    def test_restore_re_adds_marker_to_exactly_the_touched_ids(
        self, neo4j_container: dict[str, Any], tmp_path: Any
    ) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                _seed_session(s, "sess-f", extra_labels=["ForkedSession"])
                undo_log_path = tmp_path / "undo-f.json"

                exit_code = relabel.run_apply(
                    s,
                    batch_size=relabel.DEFAULT_BATCH_SIZE,
                    undo_log_path=str(undo_log_path),
                    neo4j_url=neo4j_container["bolt_url"],
                )
                assert exit_code == 0
                assert undo_log_path.exists()

                healed_labels = _labels(s, "sess-f")
                assert "IncompleteSession" not in healed_labels
                assert "ForkedSession" in healed_labels

                snap = relabel.read_undo_log(str(undo_log_path))
                assert snap["node_ids"] == ["sess-f"]
                assert "generated_at" in snap
                assert "neo4j_host" in snap

                restore_exit_code = relabel.run_restore(
                    s, str(undo_log_path), batch_size=relabel.DEFAULT_BATCH_SIZE
                )
                assert restore_exit_code == 0

                restored_labels = _labels(s, "sess-f")
                assert "IncompleteSession" in restored_labels
                # The terminal label must never have been touched.
                assert "ForkedSession" in restored_labels
        finally:
            driver.close()
