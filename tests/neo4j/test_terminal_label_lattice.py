"""Focused tests for the terminal-label lattice normalization in _write_batch.

These tests verify the store-level convergence guarantee:
  ForkedSession > SubSession > RootSession

Both write paths are covered:
  - label_assignments path  (upsert_node with labels including a terminal)
  - patch_snapshot add path (set_labels)

Sequential and concurrent scenarios are both exercised so the race-safety
claim is proven by execution, not just by code-reading.

Run: uv run pytest tests/neo4j/test_terminal_label_lattice.py -v -m neo4j
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest

from context_intelligence_server.neo4j_store import Neo4jGraphStore, ensure_neo4j_schema

pytestmark = pytest.mark.neo4j


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _labels(store: Neo4jGraphStore, node_id: str) -> list[str]:
    """Return labels from Neo4j directly (bypasses in-memory buffer)."""
    rows = await store.execute_query(
        "MATCH (n) WHERE n.node_id = $id AND n.workspace = $workspace "
        "RETURN labels(n) AS lbls",
        {"id": node_id, "workspace": store.workspace},
        workspace="*",
    )
    return sorted(rows[0]["lbls"]) if rows else []


def _terminals(labels: list[str]) -> list[str]:
    return [
        lbl for lbl in labels if lbl in ("RootSession", "SubSession", "ForkedSession")
    ]


async def _create_bare_session(store: Neo4jGraphStore, node_id: str) -> None:
    """Create a bare :Session node (as the PARENT's drainer would)."""
    await store.upsert_node(node_id, {"labels": ["Session"], "session_id": node_id})
    await store.flush()


async def _make_driver(bolt: str, auth: tuple[str, str]) -> Any:
    from neo4j import AsyncGraphDatabase

    return AsyncGraphDatabase.driver(bolt, auth=auth)


# ---------------------------------------------------------------------------
# Sequential tests — label_assignments path (upsert_node)
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestLabelAssignmentsPath:
    """Sequential SET via upsert_node (label_assignments path in _write_batch)."""

    async def test_sub_then_forked_yields_forked_only(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """SubSession first flush, ForkedSession second flush → ForkedSession only."""
        bolt = neo4j_container["bolt_url"]
        auth = (neo4j_container["user"], neo4j_container["password"])
        ws = f"lattice-lp-sf-{uuid.uuid4().hex[:8]}"
        node_id = f"n-{uuid.uuid4().hex[:8]}"

        driver = await _make_driver(bolt, auth)
        await ensure_neo4j_schema(driver)
        await driver.close()

        store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)

        # Flush 1: add SubSession
        await store.upsert_node(
            node_id,
            {"labels": ["Session", "SubSession", "SST_EVENT"], "session_id": node_id},
        )
        await store.flush()

        mid = _terminals(await _labels(store, node_id))
        assert mid == ["SubSession"], (
            f"after first flush expected SubSession, got {mid}"
        )

        # Flush 2: add ForkedSession (simulates a concurrent drainer winning)
        await store.upsert_node(
            node_id,
            {
                "labels": ["Session", "ForkedSession", "SST_EVENT"],
                "session_id": node_id,
            },
        )
        await store.flush()

        final = _terminals(await _labels(store, node_id))
        assert final == ["ForkedSession"], (
            f"SubSession→ForkedSession: expected [ForkedSession], got {final}"
        )
        await store.close()

    async def test_forked_then_sub_yields_forked_only(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """ForkedSession first, SubSession second → ForkedSession only (lattice strips Sub)."""
        bolt = neo4j_container["bolt_url"]
        auth = (neo4j_container["user"], neo4j_container["password"])
        ws = f"lattice-lp-fs-{uuid.uuid4().hex[:8]}"
        node_id = f"n-{uuid.uuid4().hex[:8]}"

        driver = await _make_driver(bolt, auth)
        await ensure_neo4j_schema(driver)
        await driver.close()

        store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)

        await store.upsert_node(
            node_id,
            {
                "labels": ["Session", "ForkedSession", "SST_EVENT"],
                "session_id": node_id,
            },
        )
        await store.flush()

        # Now try to SET SubSession — lattice must strip it immediately
        await store.upsert_node(
            node_id,
            {"labels": ["Session", "SubSession", "SST_EVENT"], "session_id": node_id},
        )
        await store.flush()

        final = _terminals(await _labels(store, node_id))
        assert final == ["ForkedSession"], (
            f"ForkedSession→SubSession: expected [ForkedSession] (Sub stripped), got {final}"
        )
        await store.close()

    async def test_root_then_sub_yields_sub_only(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """RootSession first, SubSession second → SubSession only."""
        bolt = neo4j_container["bolt_url"]
        auth = (neo4j_container["user"], neo4j_container["password"])
        ws = f"lattice-lp-rs-{uuid.uuid4().hex[:8]}"
        node_id = f"n-{uuid.uuid4().hex[:8]}"

        driver = await _make_driver(bolt, auth)
        await ensure_neo4j_schema(driver)
        await driver.close()

        store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)

        await store.upsert_node(
            node_id,
            {"labels": ["Session", "RootSession", "SST_EVENT"], "session_id": node_id},
        )
        await store.flush()

        await store.upsert_node(
            node_id,
            {"labels": ["Session", "SubSession", "SST_EVENT"], "session_id": node_id},
        )
        await store.flush()

        final = _terminals(await _labels(store, node_id))
        assert final == ["SubSession"], (
            f"RootSession→SubSession: expected [SubSession], got {final}"
        )
        await store.close()

    async def test_non_terminal_labels_untouched(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """Non-terminal labels (Session, SST_EVENT, MountPlan) are never stripped."""
        bolt = neo4j_container["bolt_url"]
        auth = (neo4j_container["user"], neo4j_container["password"])
        ws = f"lattice-lp-nt-{uuid.uuid4().hex[:8]}"
        node_id = f"n-{uuid.uuid4().hex[:8]}"

        driver = await _make_driver(bolt, auth)
        await ensure_neo4j_schema(driver)
        await driver.close()

        store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)

        await store.upsert_node(
            node_id,
            {
                "labels": ["Session", "SubSession", "SST_EVENT", "MountPlan"],
                "session_id": node_id,
            },
        )
        await store.flush()

        all_labels = await _labels(store, node_id)
        for lbl in ("Session", "SST_EVENT", "MountPlan", "SubSession"):
            assert lbl in all_labels, (
                f"non-terminal label {lbl!r} was stripped: {all_labels}"
            )
        await store.close()


# ---------------------------------------------------------------------------
# Sequential tests — patch_snapshot add path (set_labels)
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestPatchPath:
    """Sequential SET via set_labels (patch_snapshot add path in _write_batch)."""

    async def test_patch_sub_then_forked_yields_forked_only(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """Patch-add SubSession first, then patch-add ForkedSession → ForkedSession only."""
        bolt = neo4j_container["bolt_url"]
        auth = (neo4j_container["user"], neo4j_container["password"])
        ws = f"lattice-pp-sf-{uuid.uuid4().hex[:8]}"
        node_id = f"n-{uuid.uuid4().hex[:8]}"

        driver = await _make_driver(bolt, auth)
        await ensure_neo4j_schema(driver)
        await driver.close()

        store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)

        await _create_bare_session(store, node_id)

        await store.set_labels(node_id, remove_labels=[], add_labels=["SubSession"])
        await store.flush()

        mid = _terminals(await _labels(store, node_id))
        assert mid == ["SubSession"], f"after sub patch, expected SubSession, got {mid}"

        await store.set_labels(node_id, remove_labels=[], add_labels=["ForkedSession"])
        await store.flush()

        final = _terminals(await _labels(store, node_id))
        assert final == ["ForkedSession"], (
            f"patch Sub→Forked: expected [ForkedSession], got {final}"
        )
        await store.close()

    async def test_patch_forked_then_sub_yields_forked_only(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """Patch-add ForkedSession first, then Sub → ForkedSession only."""
        bolt = neo4j_container["bolt_url"]
        auth = (neo4j_container["user"], neo4j_container["password"])
        ws = f"lattice-pp-fs-{uuid.uuid4().hex[:8]}"
        node_id = f"n-{uuid.uuid4().hex[:8]}"

        driver = await _make_driver(bolt, auth)
        await ensure_neo4j_schema(driver)
        await driver.close()

        store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)

        await _create_bare_session(store, node_id)

        await store.set_labels(node_id, remove_labels=[], add_labels=["ForkedSession"])
        await store.flush()

        # Lattice must strip Sub immediately when Forked is already there
        await store.set_labels(node_id, remove_labels=[], add_labels=["SubSession"])
        await store.flush()

        final = _terminals(await _labels(store, node_id))
        assert final == ["ForkedSession"], (
            f"patch Forked→Sub: expected [ForkedSession] (Sub stripped), got {final}"
        )
        await store.close()


# ---------------------------------------------------------------------------
# Concurrent tests — two independent stores race to write the same node
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestConcurrentLatticeConvergence:
    """Two independent Neo4jGraphStore instances write to the SAME node concurrently.

    This is the direct simulation of the two-drainer race that produced
    ForkedSession + SubSession duals in production.

    The lattice normalization in _write_batch must ensure convergence to exactly
    one terminal regardless of interleaving.
    """

    async def _run_concurrent(
        self,
        bolt: str,
        auth: tuple[str, str],
        ws: str,
        node_id: str,
        label_a: str,
        label_b: str,
    ) -> list[str]:
        """
        Create a bare node, then two stores concurrently SET label_a and label_b.
        Returns the terminal labels found after both commit.
        """
        # Bootstrap a bare Session node
        bootstrap = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
        await bootstrap.upsert_node(
            node_id, {"labels": ["Session"], "session_id": node_id}
        )
        await bootstrap.flush()
        await bootstrap.close()

        async def write_label(label: str) -> None:
            store = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
            await store.upsert_node(
                node_id,
                {"labels": ["Session", label, "SST_EVENT"], "session_id": node_id},
            )
            await store.flush()
            await store.close()

        await asyncio.gather(write_label(label_a), write_label(label_b))

        verify = Neo4jGraphStore(uri=bolt, auth=auth, workspace=ws)
        try:
            return _terminals(await _labels(verify, node_id))
        finally:
            await verify.close()

    async def test_concurrent_sub_and_forked_yields_one_terminal(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """Concurrent Sub + Forked → exactly one terminal (ForkedSession wins)."""
        bolt = neo4j_container["bolt_url"]
        auth = (neo4j_container["user"], neo4j_container["password"])
        ws = f"lattice-conc-sf-{uuid.uuid4().hex[:8]}"
        node_id = f"n-{uuid.uuid4().hex[:8]}"

        driver = await _make_driver(bolt, auth)
        await ensure_neo4j_schema(driver)
        await driver.close()

        terminals = await self._run_concurrent(
            bolt, auth, ws, node_id, "SubSession", "ForkedSession"
        )

        assert len(terminals) == 1, (
            f"CONCURRENT Sub+Forked: expected exactly 1 terminal, got {terminals}"
        )
        assert terminals == ["ForkedSession"], (
            f"CONCURRENT Sub+Forked: ForkedSession must win, got {terminals}"
        )

    async def test_concurrent_forked_and_sub_yields_one_terminal(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """Concurrent Forked + Sub (reversed order) → exactly one terminal."""
        bolt = neo4j_container["bolt_url"]
        auth = (neo4j_container["user"], neo4j_container["password"])
        ws = f"lattice-conc-fs-{uuid.uuid4().hex[:8]}"
        node_id = f"n-{uuid.uuid4().hex[:8]}"

        driver = await _make_driver(bolt, auth)
        await ensure_neo4j_schema(driver)
        await driver.close()

        terminals = await self._run_concurrent(
            bolt, auth, ws, node_id, "ForkedSession", "SubSession"
        )

        assert len(terminals) == 1, (
            f"CONCURRENT Forked+Sub: expected exactly 1 terminal, got {terminals}"
        )
        assert terminals == ["ForkedSession"], (
            f"CONCURRENT Forked+Sub: ForkedSession must win, got {terminals}"
        )

    async def test_concurrent_root_and_sub_yields_sub_only(
        self, neo4j_container: dict[str, Any]
    ) -> None:
        """Concurrent Root + Sub → SubSession only (Sub wins over Root)."""
        bolt = neo4j_container["bolt_url"]
        auth = (neo4j_container["user"], neo4j_container["password"])
        ws = f"lattice-conc-rs-{uuid.uuid4().hex[:8]}"
        node_id = f"n-{uuid.uuid4().hex[:8]}"

        driver = await _make_driver(bolt, auth)
        await ensure_neo4j_schema(driver)
        await driver.close()

        terminals = await self._run_concurrent(
            bolt, auth, ws, node_id, "RootSession", "SubSession"
        )

        assert len(terminals) == 1, (
            f"CONCURRENT Root+Sub: expected exactly 1 terminal, got {terminals}"
        )
        assert terminals == ["SubSession"], (
            f"CONCURRENT Root+Sub: SubSession must win, got {terminals}"
        )
