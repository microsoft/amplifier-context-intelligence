"""Phase 6 — real-Neo4j behavioral gate tests for created_by provenance.

Each gate proves a specific invariant of the ON CREATE SET / first-asserter-wins
provenance design by driving the REAL Neo4j Cypher through the REAL
Neo4jGraphStore.flush() path and verifying with a raw sync driver query.

ISOLATION GUARANTEE
--------------------
Every test in this file uses ONLY the ephemeral Docker container created by the
``neo4j_container`` fixture from tests/neo4j/conftest.py:

  * Random host ports picked by _get_free_port() — no fixed port 7687.
  * container started with ``remove=True`` — destroyed after the session.
  * Credentials are ``("neo4j", "testpassword")`` — never read from
    ``~/.amplifier``, ``server-config.yaml``, ``/data/credentials.yaml``,
    environment variables (AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_*), or any
    bolt URL other than the one the fixture injects via ``container["bolt_url"]``.
  * All graph writes come from stores created inside each test from
    ``container["bolt_url"]`` and are torn down either in a ``try/finally``
    or at container destruction.

No production Neo4j endpoint is referenced anywhere in this module.

Gates
------
Gate 1  — anti-spoof:   created_by inside node data cannot clobber the auth stamp.
Gate 2  — node write-once: ON CREATE SET fires only once (first asserter wins).
Gate 2E — edge write-once / first-asserter-wins (the council cross-session case).
Gate 3  — dev-mode/null: NULL created_by when auth is disabled — no error.

Run explicitly:
    cd amplifier-context-intelligence
    uv run pytest tests/neo4j/test_provenance_gates.py -v -p no:cacheprovider
"""

from __future__ import annotations

from typing import Any

import pytest
from neo4j import GraphDatabase

from context_intelligence_server.neo4j_store import Neo4jGraphStore

pytestmark = pytest.mark.neo4j

# ---------------------------------------------------------------------------
# Sync-driver helpers  (mirror the conftest.py teardown idiom exactly)
# ---------------------------------------------------------------------------


def _sync_driver(container: dict[str, Any]):  # type: ignore[return]
    """Return a synchronous Neo4j driver for verification/teardown queries."""
    return GraphDatabase.driver(
        container["bolt_url"],
        auth=(container["user"], container["password"]),
    )


def _query_node_created_by(
    container: dict[str, Any], node_id: str, workspace: str
) -> str | None:
    """Return ``n.created_by`` for the node identified by (node_id, workspace), or None if absent."""
    driver = _sync_driver(container)
    try:
        with driver.session() as session:
            rec = session.run(
                "MATCH (n {node_id: $nid, workspace: $ws}) RETURN n.created_by AS cb",
                nid=node_id,
                ws=workspace,
            ).single()
            return rec["cb"] if rec else None
    finally:
        driver.close()


def _query_rel_created_by(
    container: dict[str, Any], src_id: str, dst_id: str, workspace: str
) -> str | None:
    """Return ``r.created_by`` for the relationship whose src/dst match the given ids."""
    driver = _sync_driver(container)
    try:
        with driver.session() as session:
            rec = session.run(
                "MATCH ()-[r]->() "
                "WHERE r.src_id = $src AND r.dst_id = $dst AND r.workspace = $ws "
                "RETURN r.created_by AS cb",
                src=src_id,
                dst=dst_id,
                ws=workspace,
            ).single()
            return rec["cb"] if rec else None
    finally:
        driver.close()


def _cleanup_workspace(container: dict[str, Any], workspace: str) -> None:
    """Delete all nodes (and their relationships) in *workspace* — mirrors conftest teardown."""
    driver = _sync_driver(container)
    try:
        with driver.session() as session:
            session.run(
                "MATCH (n {workspace: $ws}) DETACH DELETE n",
                ws=workspace,
            )
    finally:
        driver.close()


# ---------------------------------------------------------------------------
# Gate 1 — anti-spoof
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestGate1AntiSpoof:
    """created_by supplied inside the node data dict cannot clobber the auth stamp.

    Production context: a connected client might embed ``"created_by": "hacker"``
    inside the event payload.  The server stamps ``created_by`` from the
    authenticated contributor id; ``_build_node_props`` strips the key from
    ``row.props`` so it never reaches Neo4j via ``SET n += row.props``.
    The only path to ``n.created_by`` in Neo4j is ``ON CREATE SET n.created_by =
    $created_by`` where ``$created_by`` is the store's authenticated value.
    """

    async def test_created_by_in_data_cannot_clobber_auth_stamp(
        self,
        neo4j_container: dict[str, Any],
    ) -> None:
        """Spoofed created_by in node data MUST NOT land as the provenance stamp."""
        ws = "gate1-antispoof"
        node_id = "g1-spoof-node"
        store = Neo4jGraphStore(
            uri=neo4j_container["bolt_url"],
            auth=(neo4j_container["user"], neo4j_container["password"]),
            workspace=ws,
        )
        store.created_by = "alice"
        try:
            # data includes a spoofed created_by — this is the attack vector
            await store.upsert_node(
                node_id,
                {
                    "created_by": "evil-hacker",  # spoofed value inside props
                    "labels": ["Event"],
                    "name": "gate1-test",
                },
            )
            await store.flush()

            stamped = _query_node_created_by(neo4j_container, node_id, ws)
            assert stamped is not None, "Node must exist in Neo4j after flush"
            assert stamped == "alice", (
                f"n.created_by must be 'alice' (authenticated stamp), got {stamped!r}. "
                "The spoofed value in data must NOT reach the stamp."
            )
            assert stamped != "evil-hacker", (
                "Spoofed 'evil-hacker' must never become the provenance stamp"
            )
        finally:
            await store._driver.close()
            _cleanup_workspace(neo4j_container, ws)


# ---------------------------------------------------------------------------
# Gate 2 — node write-once
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestGate2NodeWriteOnce:
    """ON CREATE SET fires only on the first MERGE — subsequent merges preserve the stamp.

    Production context: two workers may independently MERGE the same node in
    different flush cycles (e.g. a parent session written by both parent and
    child drainer).  The first writer wins; the second MERGE must NOT overwrite
    ``n.created_by``.
    """

    async def test_node_created_by_immutable_after_first_create(
        self,
        neo4j_container: dict[str, Any],
    ) -> None:
        """Second MERGE of the same node_id with a different created_by must leave the stamp unchanged."""
        ws = "gate2-writeonce"
        node_id = "g2-node"

        store_alice = Neo4jGraphStore(
            uri=neo4j_container["bolt_url"],
            auth=(neo4j_container["user"], neo4j_container["password"]),
            workspace=ws,
        )
        store_alice.created_by = "alice"

        store_bob = Neo4jGraphStore(
            uri=neo4j_container["bolt_url"],
            auth=(neo4j_container["user"], neo4j_container["password"]),
            workspace=ws,
        )
        store_bob.created_by = "bob"

        try:
            # --- First write: alice creates the node ---
            await store_alice.upsert_node(node_id, {"labels": ["Event"], "name": "first"})
            await store_alice.flush()

            after_alice = _query_node_created_by(neo4j_container, node_id, ws)
            assert after_alice == "alice", (
                f"After first flush n.created_by must be 'alice', got {after_alice!r}"
            )

            # --- Second write: bob attempts to MERGE the SAME node ---
            await store_bob.upsert_node(node_id, {"labels": ["Event"], "name": "second"})
            await store_bob.flush()

            after_bob = _query_node_created_by(neo4j_container, node_id, ws)
            assert after_bob == "alice", (
                f"After second flush n.created_by must STILL be 'alice' (write-once), "
                f"got {after_bob!r}.  ON CREATE SET must not fire on an existing node."
            )
            assert after_bob != "bob", (
                "bob's created_by must never overwrite alice's provenance stamp"
            )
        finally:
            await store_alice._driver.close()
            await store_bob._driver.close()
            _cleanup_workspace(neo4j_container, ws)


# ---------------------------------------------------------------------------
# Gate 2E — edge write-once / first-asserter-wins (cross-session case)
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestGate2EEdgeWriteOnce:
    """r.created_by is set ON CREATE only; cross-session re-MERGE must NOT flip it.

    This is the council-required cross-session first-asserter-wins test.

    Two aspects are verified:
    1. r.created_by is set by the first asserter and never overwritten.
    2. Bare endpoint placeholder nodes created by the edge MERGE (i.e. nodes
       that were NOT written via upsert_node first) have NULL created_by —
       per design, the edge MERGE Cypher stamps the relationship but not the
       endpoint placeholders it may auto-create.
    """

    async def test_cross_session_first_asserter_wins_and_stable(
        self,
        neo4j_container: dict[str, Any],
    ) -> None:
        """First asserter's created_by on an edge must survive a cross-session re-MERGE."""
        ws = "gate2e-edgeonce"
        src_id = "g2e-src"
        dst_id = "g2e-dst"

        # Neither src nor dst is pre-created via upsert_node —
        # the edge flush will MERGE them as bare :Node placeholders.
        store_alice = Neo4jGraphStore(
            uri=neo4j_container["bolt_url"],
            auth=(neo4j_container["user"], neo4j_container["password"]),
            workspace=ws,
        )
        store_alice.created_by = "alice"

        store_bob = Neo4jGraphStore(
            uri=neo4j_container["bolt_url"],
            auth=(neo4j_container["user"], neo4j_container["password"]),
            workspace=ws,
        )
        store_bob.created_by = "bob"

        try:
            # --- First assertion: alice creates edge (src)-[RELATED]->(dst) ---
            # Endpoints do NOT exist yet; the edge MERGE will create bare :Node placeholders.
            await store_alice.upsert_edge(src_id, dst_id, {"type": "RELATED"})
            await store_alice.flush()

            after_alice = _query_rel_created_by(neo4j_container, src_id, dst_id, ws)
            assert after_alice == "alice", (
                f"r.created_by must be 'alice' after first assertion, got {after_alice!r}"
            )

            # Bare endpoint placeholders must have NULL created_by (per design:
            # _edge_merge_cypher does not stamp endpoints, only the relationship).
            src_stamp = _query_node_created_by(neo4j_container, src_id, ws)
            dst_stamp = _query_node_created_by(neo4j_container, dst_id, ws)
            assert src_stamp is None, (
                f"Bare endpoint src node must have NULL created_by, got {src_stamp!r}. "
                "The edge MERGE must not stamp endpoint placeholders it auto-creates."
            )
            assert dst_stamp is None, (
                f"Bare endpoint dst node must have NULL created_by, got {dst_stamp!r}. "
                "The edge MERGE must not stamp endpoint placeholders it auto-creates."
            )

            # --- Second assertion: bob re-MERGEs the SAME edge (cross-session case) ---
            await store_bob.upsert_edge(src_id, dst_id, {"type": "RELATED"})
            await store_bob.flush()

            after_bob = _query_rel_created_by(neo4j_container, src_id, dst_id, ws)
            assert after_bob == "alice", (
                f"r.created_by must STILL be 'alice' after cross-session re-MERGE, "
                f"got {after_bob!r}.  ON CREATE SET must not fire on an existing relationship."
            )
            assert after_bob != "bob", (
                "bob's re-MERGE must never flip the relationship's provenance stamp"
            )
        finally:
            await store_alice._driver.close()
            await store_bob._driver.close()
            _cleanup_workspace(neo4j_container, ws)


# ---------------------------------------------------------------------------
# Gate 3 — dev-mode / null created_by
# ---------------------------------------------------------------------------


@pytest.mark.neo4j
class TestGate3DevModeNull:
    """Auth disabled (created_by=None) produces NULL provenance — no error, no crash.

    Production context: when the server is deployed without API-key auth, the
    contributor id is None.  Writes must succeed with created_by IS NULL on both
    nodes and relationships.  No error must propagate to the caller.
    """

    async def test_null_created_by_when_auth_disabled(
        self,
        neo4j_container: dict[str, Any],
    ) -> None:
        """created_by=None (dev mode) produces NULL provenance on nodes and edges without error."""
        ws = "gate3-devnull"
        node_id = "g3-node"
        src_id = "g3-src"
        dst_id = "g3-dst"

        store = Neo4jGraphStore(
            uri=neo4j_container["bolt_url"],
            auth=(neo4j_container["user"], neo4j_container["password"]),
            workspace=ws,
        )
        # created_by defaults to None (auth disabled)
        assert store.created_by is None, "Default created_by must be None"

        try:
            await store.upsert_node(node_id, {"labels": ["Event"], "name": "dev-test"})
            await store.upsert_edge(src_id, dst_id, {"type": "RELATED"})
            # Must not raise
            await store.flush()

            node_stamp = _query_node_created_by(neo4j_container, node_id, ws)
            assert node_stamp is None, (
                f"n.created_by must be NULL when auth is disabled, got {node_stamp!r}"
            )

            rel_stamp = _query_rel_created_by(neo4j_container, src_id, dst_id, ws)
            assert rel_stamp is None, (
                f"r.created_by must be NULL when auth is disabled, got {rel_stamp!r}"
            )
        finally:
            await store._driver.close()
            _cleanup_workspace(neo4j_container, ws)
