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


# ---------------------------------------------------------------------------
# (g) W-1: undo-log is written from a read-only PRE-MUTATION candidate
#     collection, EXACTLY matching the full false-positive set, and restore
#     from it fully re-adds the label to every candidate.
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestUndoLogCapturesFullPreMutationCandidateSet:
    """W-1: apply_relabel()'s batched CALL {...} IN TRANSACTIONS OF N ROWS
    commits per batch, so the undo-log must exist -- complete -- the moment
    any batch has committed, not only after apply_relabel() returns. This
    test seeds a NON-VACUOUS candidate set (two distinct false-positive
    shapes that both clear the B2 gate: a terminal-labeled node, and a
    terminal-labeled node that ALSO has a linked start event) and asserts
    the undo-log written by the gated --apply path (run_apply) contains
    EXACTLY that candidate set, and that restoring from it fully re-adds
    :IncompleteSession to both.
    """

    def test_undo_log_exactly_matches_candidates_and_restore_heals_all(
        self, neo4j_container: dict[str, Any], tmp_path: Any
    ) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                # Non-vacuity: two distinct false-positive shapes, so the
                # candidate set has more than one member and is not a
                # degenerate single-node case. Both carry a terminal label
                # (so NEITHER trips the B2 linked_but_untyped gate); the
                # second additionally has a linked start event, to prove the
                # selector's OR-shape doesn't affect completeness.
                _seed_session(s, "sess-g1", extra_labels=["ForkedSession"])
                _seed_session(s, "sess-g2", extra_labels=["SubSession"])
                _seed_linked_event(s, "sess-g2", "SessionStartEvent", "sess-g2::start")

                # Ground truth: the pre-mutation candidate set, read
                # independently via the same read-only helper run_apply
                # uses internally, BEFORE run_apply does anything.
                expected_candidates = sorted(relabel.collect_false_positive_ids(s))
                assert expected_candidates == ["sess-g1", "sess-g2"], (
                    "test setup must be non-vacuous: exactly two "
                    f"false-positive candidates expected, got {expected_candidates}"
                )

                undo_log_path = tmp_path / "undo-g.json"
                exit_code = relabel.run_apply(
                    s,
                    batch_size=relabel.DEFAULT_BATCH_SIZE,
                    undo_log_path=str(undo_log_path),
                    neo4j_url=neo4j_container["bolt_url"],
                )
                assert exit_code == 0
                assert undo_log_path.exists(), "undo-log file must exist after --apply"

                snap = relabel.read_undo_log(str(undo_log_path))
                logged_ids = sorted(snap["node_ids"])
                # (a) The undo log holds EXACTLY the candidate set -- no
                # more, no less.
                assert logged_ids == expected_candidates == ["sess-g1", "sess-g2"]

                # Both were actually mutated.
                assert "IncompleteSession" not in _labels(s, "sess-g1")
                assert "IncompleteSession" not in _labels(s, "sess-g2")

                # (b) Restoring from the undo log fully re-adds the marker
                # to every logged node_id.
                restore_exit_code = relabel.run_restore(
                    s, str(undo_log_path), batch_size=relabel.DEFAULT_BATCH_SIZE
                )
                assert restore_exit_code == 0
                assert "IncompleteSession" in _labels(s, "sess-g1")
                assert "IncompleteSession" in _labels(s, "sess-g2")
        finally:
            driver.close()


@pytest.mark.neo4j
class TestUndoLogSourcedFromPreMutationReadNotApplyReturnValue:
    """W-1: the undo-log must be sourced from a read-only, pre-mutation
    collection of the candidate set (``collect_false_positive_ids``) -- NOT
    derived from whatever ``apply_relabel()`` happens to report as touched.

    Proven by monkeypatching ``apply_relabel`` to still perform the REAL
    mutation (so the graph state and restore path stay meaningful) but
    report a deliberately WRONG, truncated touched set. Under the pre-fix
    ordering (log written from apply_relabel's return value, AFTER the
    mutation), the undo-log would contain only the truncated set. Under the
    fix, run_apply captures the full candidate set via a separate read
    BEFORE calling apply_relabel at all, so the log is unaffected by
    whatever apply_relabel reports.
    """

    def test_undo_log_unaffected_by_apply_relabels_return_value(
        self,
        neo4j_container: dict[str, Any],
        tmp_path: Any,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        driver = _driver(neo4j_container)
        try:
            with driver.session() as s:
                _seed_session(s, "sess-h1", extra_labels=["ForkedSession"])
                _seed_session(s, "sess-h2", extra_labels=["RootSession"])

                expected_candidates = sorted(relabel.collect_false_positive_ids(s))
                assert expected_candidates == ["sess-h1", "sess-h2"], (
                    "test setup must be non-vacuous: exactly two "
                    f"false-positive candidates expected, got {expected_candidates}"
                )

                real_apply_relabel = relabel.apply_relabel

                def _fake_apply_relabel(session, batch_size):
                    # Perform the REAL mutation (so the restore-path
                    # assertions below stay meaningful) but report a
                    # deliberately WRONG, truncated touched set -- the
                    # shape of divergence a mid-run crash would produce if
                    # the undo-log were sourced from this return value.
                    real_apply_relabel(session, batch_size)
                    return ["sess-h1"]  # deliberately omits sess-h2

                monkeypatch.setattr(relabel, "apply_relabel", _fake_apply_relabel)

                undo_log_path = tmp_path / "undo-h.json"
                exit_code = relabel.run_apply(
                    s,
                    batch_size=relabel.DEFAULT_BATCH_SIZE,
                    undo_log_path=str(undo_log_path),
                    neo4j_url=neo4j_container["bolt_url"],
                )
                assert exit_code == 0
                assert undo_log_path.exists()

                snap = relabel.read_undo_log(str(undo_log_path))
                logged_ids = sorted(snap["node_ids"])

                # THE core assertion: the log holds the FULL pre-mutation
                # candidate set, not apply_relabel's (faked, truncated)
                # return value. If run_apply sourced the log from
                # touched_ids (the pre-fix ordering), logged_ids would
                # equal ["sess-h1"] here.
                assert logged_ids == expected_candidates == ["sess-h1", "sess-h2"]
                assert logged_ids != ["sess-h1"], (
                    "undo log must not be derived from apply_relabel's "
                    "reported touched set"
                )

                # Restoring from the (correctly complete) log heals BOTH
                # nodes, including "sess-h2" which the faked touched-set
                # omitted -- proving the log's completeness has real
                # recovery value, not just structural equality.
                restore_exit_code = relabel.run_restore(
                    s, str(undo_log_path), batch_size=relabel.DEFAULT_BATCH_SIZE
                )
                assert restore_exit_code == 0
                assert "IncompleteSession" in _labels(s, "sess-h1")
                assert "IncompleteSession" in _labels(s, "sess-h2")
        finally:
            driver.close()
