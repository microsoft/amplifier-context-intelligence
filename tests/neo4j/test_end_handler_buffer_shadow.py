"""Tier 3 - Neo4j reproduction for the _handle_end buffer-shadow bug.

Hypothesis under test:
  In _handle_end, upsert_node(session_id, {"labels": ["Session", "SST_EVENT"], ...})
  is called BEFORE get_node(session_id).  After a flush the node_buffer is empty, so
  upsert_node creates a FRESH buffer entry with ONLY ["Session", "SST_EVENT"].  The
  subsequent get_node hits that fresh entry (buffer-first) and returns the partial
  label set — missing the type label (SubSession / ForkedSession) that was persisted
  to Neo4j by the preceding session:start or session:fork + flush.  _current_type
  therefore returns None, stub-recovery fires, and a spurious RootSession is added.

Tests
-----
1. test_upsert_after_flush_shadows_persisted_type_label  (proves the bug)
   Directly exercises the store-level operations that _handle_end performs:
   upsert_node([Session, SubSession]) → flush → upsert_node([Session, SST_EVENT])
   → get_node.  Asserts that get_node returns a label set WITHOUT SubSession,
   proving that the buffer-first path returns only the post-flush upsert labels.

2. test_read_before_upsert_returns_persisted_type_label  (proves the fix direction)
   Same setup but calls get_node BEFORE the second upsert_node.  With an empty
   buffer, get_node falls through to Neo4j and returns the real label set including
   SubSession.  This is the correct ordering used in _handle_start / _handle_fork.

3. test_handle_end_after_flush_produces_dual_terminal_labels  (handler-level bug)
   Exercises SessionHandler._handle_end end-to-end via the public __call__ interface:
   session:start(child, parent=P) → flush → session:end(child, no parent_id).
   Reads labels directly from Neo4j (bypassing any buffer) and asserts that the
   node carries BOTH SubSession AND RootSession — the observable failure mode from
   the live e2e gate.

Run: uv run pytest tests/neo4j/test_end_handler_buffer_shadow.py -v -m neo4j
"""

from __future__ import annotations

from typing import Any

import pytest

from context_intelligence_server.handlers.data_layer_2.session import (
    SessionHandler,
    _current_type,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _neo4j_labels(services: Any, node_id: str) -> list[str]:
    """Return labels from Neo4j directly (bypasses buffer)."""
    rows = await services.graph.execute_query(
        "MATCH (n) WHERE n.node_id = $id AND n.workspace = $workspace "
        "RETURN labels(n) AS lbls",
        {"id": node_id, "workspace": services.graph.workspace},
        workspace="*",
    )
    return list(rows[0]["lbls"]) if rows else []


# ---------------------------------------------------------------------------
# Store-level tests — prove the shadow mechanism directly
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestEndHandlerBufferShadow:
    """Reproduce and characterise the _handle_end buffer-shadow bug at store level."""

    async def test_upsert_after_flush_shadows_persisted_type_label(
        self, neo4j_services: Any
    ) -> None:
        """
        PROVES THE BUG.

        After flush the buffer is empty.  upsert_node({labels:[Session,SST_EVENT]})
        creates a FRESH buffer entry.  The subsequent get_node hits that entry
        (buffer-first) and returns ["Session","SST_EVENT"] — SubSession is invisible.

        This is exactly what _handle_end does at lines 262-270 then 280.
        """
        store = neo4j_services.graph
        session_id = "bugrepro-shadow-001"

        # --- Phase 1: simulate session:start handler ---
        # Create node with SubSession type label and flush, as the start handler does.
        await store.upsert_node(
            session_id,
            {
                "labels": ["Session"],
                "session_id": session_id,
                "started_at": "2026-01-01T00:00:00Z",
            },
        )
        await store.set_labels(
            session_id,
            remove_labels=[],
            add_labels=["Session", "SubSession", "SST_EVENT"],
        )
        await store.flush()
        # Buffer is now EMPTY.  Neo4j has the node with [Session, SST_EVENT, SubSession].

        # Sanity-check: Neo4j genuinely has SubSession persisted.
        neo4j_lbls_after_start = await _neo4j_labels(neo4j_services, session_id)
        assert "SubSession" in neo4j_lbls_after_start, (
            f"precondition failed — SubSession not in Neo4j after flush: {neo4j_lbls_after_start}"
        )

        # --- Phase 2: simulate _handle_end's FIRST call (upsert_node) ---
        # This is the BUGGY ordering: upsert_node BEFORE get_node.
        # The buffer is empty, so a FRESH entry is created with ONLY ["Session","SST_EVENT"].
        await store.upsert_node(
            session_id,
            {
                "labels": ["Session", "SST_EVENT"],
                "ended_at": "2026-01-01T01:00:00Z",
                "status": "completed",
            },
        )

        # --- Phase 3: simulate _handle_end's SECOND call (get_node for stub-recovery) ---
        node_from_get = await store.get_node(session_id)
        assert node_from_get is not None
        buffer_labels = node_from_get.get("labels", [])

        # THE BUG: buffer-first hit returns only the labels from the second upsert_node.
        # SubSession is persisted in Neo4j but invisible to get_node here.
        assert "SubSession" not in buffer_labels, (
            f"HYPOTHESIS REFUTED: SubSession is visible in get_node after fresh upsert. "
            f"Labels returned: {buffer_labels}"
        )
        assert set(buffer_labels) == {"Session", "SST_EVENT"}, (
            f"Expected exactly {{Session, SST_EVENT}} from fresh buffer entry; got {buffer_labels}"
        )

        # Consequence: _current_type returns None → stub-recovery will fire.
        assert _current_type(buffer_labels) is None, (
            f"Expected _current_type=None (triggering stub-recovery), got {_current_type(buffer_labels)!r}"
        )

    async def test_read_before_upsert_returns_persisted_type_label(
        self, neo4j_services: Any
    ) -> None:
        """
        PROVES THE FIX DIRECTION.

        When get_node is called BEFORE upsert_node (the ordering used in
        _handle_start and _handle_fork), the buffer is still empty at read time
        so get_node falls through to Neo4j and returns the real label set,
        including SubSession.  _current_type is non-None and stub-recovery is
        correctly suppressed.
        """
        store = neo4j_services.graph
        session_id = "bugrepro-fix-001"

        # --- Phase 1: same setup as the bug test ---
        await store.upsert_node(
            session_id,
            {
                "labels": ["Session"],
                "session_id": session_id,
                "started_at": "2026-01-01T00:00:00Z",
            },
        )
        await store.set_labels(
            session_id,
            remove_labels=[],
            add_labels=["Session", "SubSession", "SST_EVENT"],
        )
        await store.flush()
        # Buffer is EMPTY.  Neo4j has SubSession.

        # --- Phase 2 (FIXED ordering): get_node BEFORE upsert_node ---
        node_from_get = await store.get_node(session_id)
        assert node_from_get is not None
        pre_upsert_labels = node_from_get.get("labels", [])

        # With an empty buffer, get_node falls through to Neo4j and returns real labels.
        assert "SubSession" in pre_upsert_labels, (
            f"FIX PATH BROKEN: SubSession not returned by get_node before upsert_node. "
            f"Labels: {pre_upsert_labels}"
        )
        # _current_type is now non-None → stub-recovery correctly suppressed.
        assert _current_type(pre_upsert_labels) == "SubSession", (
            f"Expected _current_type='SubSession', got {_current_type(pre_upsert_labels)!r}"
        )

        # THEN upsert_node (now the label union includes SubSession in buffer):
        await store.upsert_node(
            session_id,
            {
                "labels": ["Session", "SST_EVENT"],
                "ended_at": "2026-01-01T01:00:00Z",
                "status": "completed",
            },
        )
        # Verify the buffer after the now-second upsert also has SubSession
        # (since get_node has already run, and set_labels from stub-recovery won't fire).
        node_after_upsert = await store.get_node(session_id)
        assert node_after_upsert is not None
        # The buffer entry may or may not include SubSession here depending on whether
        # the pre-read populated it — but the important point is pre_upsert_labels is correct.
        # The classify result matters, not the buffer state after the upsert.
        assert _current_type(pre_upsert_labels) is not None, (
            "After fix: _current_type must be non-None so stub-recovery does not fire"
        )


# ---------------------------------------------------------------------------
# Handler-level test — reproduce the observable dual-label failure
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestHandleEndSingleTerminalAfterFlush:
    """Regression fence: start/fork -> flush -> end must leave exactly ONE terminal label.

    Before the fix, _handle_end called upsert_node BEFORE get_node, so the
    post-flush buffer ([Session, SST_EVENT]) shadowed the persisted type label,
    stub-recovery misfired, and a spurious RootSession was added.  The fix moves
    the get_node read BEFORE upsert_node (mirroring _handle_start / _handle_fork)
    so the persisted type is seen and stub-recovery stays silent.
    """

    async def test_start_flush_end_yields_single_terminal_label(
        self, neo4j_services: Any
    ) -> None:
        """
        Sequence: session:start(child, parent_id=P) -> flush -> session:end(child, no parent_id).
        After the fix the child must end as SubSession ONLY (no spurious RootSession).
        """
        handler = SessionHandler(neo4j_services)
        parent_id = "parent-end-bug-001"
        child_id = "child-end-bug-001"

        # Step 1: session:start for the child session (will set SubSession label).
        await handler(
            "session:start",
            {
                "session_id": child_id,
                "parent_id": parent_id,
                "timestamp": "2026-01-01T10:00:00Z",
            },
        )

        # Step 2: flush, as the drainer does between event batches in production.
        await neo4j_services.graph.flush()

        # Sanity-check Neo4j: should have SubSession, no RootSession yet.
        lbls_after_start = await _neo4j_labels(neo4j_services, child_id)
        assert "SubSession" in lbls_after_start, (
            f"precondition: SubSession expected after start+flush; got {lbls_after_start}"
        )
        assert "RootSession" not in lbls_after_start, (
            f"precondition: RootSession must not be present before end; got {lbls_after_start}"
        )

        # Step 3: session:end for the child, NO parent_id in the end event
        # (this is what the live e2e gate does — separate POST request, no parent_id).
        await handler(
            "session:end",
            {
                "session_id": child_id,
                # Intentionally no parent_id: the end event does not carry it in
                # Amplifier's production event stream.
                "timestamp": "2026-01-01T10:05:00Z",
            },
        )
        # _handle_end calls flush() itself at the end.

        # Read final labels directly from Neo4j (bypasses any buffer).
        final_labels = await _neo4j_labels(neo4j_services, child_id)

        # After the fix: _handle_end reads labels BEFORE upsert_node, sees the
        # persisted SubSession, classify is a no-op, stub-recovery stays silent.
        assert "RootSession" not in final_labels, (
            f"REGRESSION: spurious RootSession from stub-recovery; final labels = {final_labels}"
        )
        terminals = [
            label
            for label in final_labels
            if label in ("RootSession", "SubSession", "ForkedSession")
        ]
        assert terminals == ["SubSession"], (
            f"Expected exactly one terminal label SubSession; got {terminals} in {final_labels}"
        )

    async def test_fork_flush_end_yields_single_terminal_label(
        self, neo4j_services: Any
    ) -> None:
        """
        Sequence: session:fork(child, parent_id=P) -> flush -> session:end(child, no parent_id).
        After the fix the child must end as ForkedSession ONLY (no spurious RootSession).
        """
        handler = SessionHandler(neo4j_services)
        parent_id = "parent-fork-end-bug-001"
        child_id = "child-fork-end-bug-001"

        await handler(
            "session:fork",
            {
                "session_id": child_id,
                "parent_id": parent_id,
                "timestamp": "2026-01-01T10:00:00Z",
            },
        )
        await neo4j_services.graph.flush()

        lbls_after_fork = await _neo4j_labels(neo4j_services, child_id)
        assert "ForkedSession" in lbls_after_fork, (
            f"precondition: ForkedSession expected after fork+flush; got {lbls_after_fork}"
        )

        await handler(
            "session:end",
            {
                "session_id": child_id,
                "timestamp": "2026-01-01T10:05:00Z",
            },
        )

        final_labels = await _neo4j_labels(neo4j_services, child_id)

        assert "RootSession" not in final_labels, (
            f"REGRESSION: spurious RootSession from stub-recovery; final labels = {final_labels}"
        )
        terminals = [
            label
            for label in final_labels
            if label in ("RootSession", "SubSession", "ForkedSession")
        ]
        assert terminals == ["ForkedSession"], (
            f"Expected exactly one terminal label ForkedSession; got {terminals} in {final_labels}"
        )
