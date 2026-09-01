"""End-to-end proof: the session-delete HTTP routes work against a real
running server and a real Neo4j.

Every other test for this feature either fakes the service (tests/routers/
test_deletion.py) or calls DeletionService directly, skipping HTTP
(tests/neo4j/test_deletion_service.py). This is the one test that goes all
the way through: real POST /events -> real drain workers -> real Neo4j and
real blob store -> real GET/DELETE routes -> real Neo4j and blob store
again, to check the data is actually gone.

How the graph is built:
  - The session tree itself (one root, two subsessions, one fork) is built
    by POSTING REAL EVENTS to /events and waiting for the server's own
    drain workers to write them to Neo4j. This part is fully practical
    through the normal client path, so that is what is used.
  - The blobs on several of those sessions are ALSO produced by posting
    real events: an event carrying a field the server offloads to disk
    (see blob_processor.BLOB_FIELDS, e.g. "result") makes the server's own
    ingest pipeline write a real blob file, exactly as a real client would
    trigger. There is no hand-attached node property standing in for a
    blob here -- the summary/delete blob count now comes from asking the
    blob store what it holds for each session in the graph (see
    DeletionService), so a blob written this way is counted honestly.
  - The shared "concept" node (the thing every session is allowed to point
    at without owning it, e.g. an Agent), and the "still receiving data"
    session used for the 409 check, are added by writing directly through
    the same Neo4jGraphStore class the server itself uses, rather than
    posting more events. Two separate, deliberate reasons:
      * The shared concept node: building this edge shape through the real
        event pipeline would require reproducing a much longer
        agent-delegation event sequence that has nothing to do with
        delete.
      * The pending/409 session: reusing one of the sessions above would
        race against that session's own live background drain worker (it
        polls every 50ms and would likely drain an appended record before
        the test could observe it as pending) -- a separate session with
        no worker attached avoids that race entirely.

The GET summary call (the preview) and the DELETE calls (which now always
delete -- there is no dry-run flag any more) always go through HTTP, against
the real running FastAPI app. Neither call takes a workspace query
parameter: the server looks up which workspace a session id belongs to on
its own.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import context_intelligence_server.main as main_module
import context_intelligence_server.registry as registry_module
import context_intelligence_server.routers.deletion as deletion_router_module
import httpx
import pytest
from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.config import Neo4jClientConfig
from context_intelligence_server.neo4j_store import (
    Neo4jGraphStore,
    build_bounded_neo4j_driver,
)
from neo4j import AsyncGraphDatabase

# Reuse the real-Neo4j-container fixture from the neo4j test tier. Importing
# the fixture function directly (rather than duplicating it) is the normal
# pytest way to share a fixture defined in another folder's conftest.py.
from tests.neo4j.conftest import neo4j_container  # noqa: F401

pytestmark = [pytest.mark.neo4j, pytest.mark.timeout(120)]

WORKSPACE = "delete-e2e-ws"

ROOT = "delete-e2e-root"
SUB1 = "delete-e2e-sub1"
SUB2 = "delete-e2e-sub2"
FORK1 = "delete-e2e-fork1"
OTHER_ROOT = "delete-e2e-other-root"
SHARED_AGENT = "delete-e2e-shared-agent"
PENDING_ROOT = "delete-e2e-pending-root"

T0 = "2026-01-01T00:00:00+00:00"
T1 = "2026-01-01T00:01:00+00:00"
T2 = "2026-01-01T00:02:00+00:00"
T3 = "2026-01-01T00:03:00+00:00"
T4 = "2026-01-01T00:04:00+00:00"


class _E2ESettings:
    """A settings-shaped object pointing every store at the test Neo4j
    container and at this test's own tmp_path directories.

    Both the ingest path (context_intelligence_server.registry.get_settings)
    and the deletion routes (context_intelligence_server.routers.deletion.
    get_settings) are pointed at ONE instance of this class, so a blob
    written while posting an event and a blob looked up while building the
    delete route's blob store resolve to the exact same directory on disk.
    """

    def __init__(
        self, container: dict[str, Any], blob_path: str, queues_path: str
    ) -> None:
        self.blob_path = blob_path
        self.queues_path = queues_path
        self.write_concurrency = 8
        self.max_delivery_attempts = 5
        self.neo4j_flush_chunk_rows = 100
        self.neo4j_flush_chunk_bytes = 4_194_304
        self.neo4j_lock_timeout = 30.0
        self.neo4j_max_connection_pool_size = 50
        self._container = container

    def resolve_neo4j_admin(self) -> Neo4jClientConfig:
        return Neo4jClientConfig(
            url=self._container["bolt_url"],
            username=self._container["user"],
            password=self._container["password"],
            access_mode="WRITE",
        )

    def resolve_neo4j_query(self) -> Neo4jClientConfig:
        return Neo4jClientConfig(
            url=self._container["bolt_url"],
            username=self._container["user"],
            password=self._container["password"],
            access_mode="READ",
        )


@pytest.fixture
async def delete_e2e_client(
    neo4j_container: dict[str, Any],  # noqa: F811 -- pytest fixture parameter
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """The real FastAPI app, wired to the real test Neo4j container.

    Patches both places that build settings-derived stores for this feature:
    - registry.get_settings -- used when POST /events spawns a drain worker
      (its blob store, queue paths, and Neo4j admin driver).
    - routers.deletion.get_settings -- used when the summary/delete routes
      build their own blob store.
    Then builds the two Neo4j drivers the deletion routes read directly off
    app.state (neo4j_driver for writes, neo4j_query_driver for reads) and
    points them at the same container.
    """
    settings = _E2ESettings(
        neo4j_container,
        blob_path=str(tmp_path / "blobs"),
        queues_path=str(tmp_path / "queues"),
    )

    monkeypatch.setattr(registry_module, "get_settings", lambda: settings)
    monkeypatch.setattr(deletion_router_module, "get_settings", lambda: settings)

    # registry.neo4j_driver is a lazily-built module-level singleton; force a
    # rebuild against the patched settings above instead of reusing whatever
    # (if anything) a previous test built it as.
    main_module.registry._neo4j_driver = None

    admin_driver = build_bounded_neo4j_driver(
        settings.resolve_neo4j_admin(), max_connection_pool_size=50
    )
    query_driver = build_bounded_neo4j_driver(
        settings.resolve_neo4j_query(), max_connection_pool_size=50
    )
    monkeypatch.setattr(
        main_module.app.state, "neo4j_driver", admin_driver, raising=False
    )
    monkeypatch.setattr(
        main_module.app.state, "neo4j_query_driver", query_driver, raising=False
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main_module.app), base_url="http://test"
    ) as client:
        yield client

    # Shut down drain workers BEFORE closing the shared driver they use --
    # same ordering the real lifespan() uses (registry.shutdown_workers()
    # docstring explains why the order matters).
    await main_module.registry.shutdown_workers()
    await admin_driver.close()
    await query_driver.close()
    await main_module.registry.close_neo4j_driver()

    # Remove this test's own data so nothing leaks into another test that
    # might reuse the same (session-scoped) Neo4j container.
    cleanup_driver = AsyncGraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    await cleanup_driver.execute_query(
        "MATCH (n {workspace: $workspace}) DETACH DELETE n",
        {"workspace": WORKSPACE},
    )
    await cleanup_driver.close()


async def _post_event(
    client: httpx.AsyncClient,
    event: str,
    session_id: str,
    timestamp: str,
    *,
    parent_id: str | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    """POST one real event to /events, exactly like a real client would."""
    data: dict[str, Any] = {"session_id": session_id, "timestamp": timestamp}
    if parent_id is not None:
        data["parent_id"] = parent_id
    if extra:
        data.update(extra)
    response = await client.post(
        "/events",
        json={"event": event, "workspace": WORKSPACE, "data": data},
    )
    assert response.status_code == 202, response.text


async def _wait_drained(session_id: str, timeout_s: float = 15.0) -> None:
    """Wait until the durable queue for *session_id* has no pending lines.

    Polls the real queue manager the running server's drain workers use,
    the same way tests/integration/test_blob_pipeline.py does.
    """
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        batch = await main_module.registry.queue_manager.read_batch(session_id, 10)
        if batch.lines == []:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"session {session_id!r} did not drain within {timeout_s}s")


async def test_delete_session_endpoint_end_to_end(
    delete_e2e_client: httpx.AsyncClient,
    neo4j_container: dict[str, Any],  # noqa: F811 -- pytest fixture parameter
    tmp_path: Path,
) -> None:
    client = delete_e2e_client

    # ------------------------------------------------------------------
    # Step 1 -- build the session tree by posting real events. Root, sub2,
    # and fork1 each also get a second event carrying a "result" field --
    # a field the server offloads to disk (blob_processor.BLOB_FIELDS) --
    # so each of those three sessions ends up with one real blob file,
    # written by the server's own ingest pipeline exactly as a real client
    # would trigger it. No blob is attached by hand anywhere in this test.
    # ------------------------------------------------------------------
    await _post_event(client, "session:start", ROOT, T0)
    await _post_event(client, "session:start", SUB1, T1, parent_id=ROOT)
    await _post_event(client, "session:start", SUB2, T2, parent_id=SUB1)
    await _post_event(client, "session:fork", FORK1, T3, parent_id=ROOT)
    await _post_event(client, "session:start", OTHER_ROOT, T4)

    large_result = {"output": "result payload " * 500}
    await _post_event(
        client,
        "tool:post",
        ROOT,
        "2026-01-01T00:00:01+00:00",
        extra={"result": large_result},
    )
    await _post_event(
        client,
        "tool:post",
        SUB2,
        "2026-01-01T00:02:01+00:00",
        extra={"result": large_result},
    )
    await _post_event(
        client,
        "tool:post",
        FORK1,
        "2026-01-01T00:03:01+00:00",
        extra={"result": large_result},
    )

    for session_id in (ROOT, SUB1, SUB2, FORK1, OTHER_ROOT):
        await _wait_drained(session_id)

    # ------------------------------------------------------------------
    # Step 2 -- seed the shared concept node and the pending-check session
    # directly through the real Neo4jGraphStore class (see module docstring
    # for why these two pieces are not built through posted events).
    # ------------------------------------------------------------------
    helper_store = Neo4jGraphStore(
        uri=neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
        workspace=WORKSPACE,
    )
    # Same directory the server's own blob store just wrote the real blobs
    # to (see _E2ESettings.blob_path) -- used here only to VERIFY what the
    # server produced, never to write a blob by hand.
    blob_store = AsyncDiskBlobStore(root=tmp_path / "blobs")
    try:
        await helper_store.upsert_node(
            SHARED_AGENT, {"labels": ["Agent", "SST_CONCEPT"], "agent": SHARED_AGENT}
        )
        # fork1 and the unrelated other-root session both point at the same
        # shared concept node -- neither owns it.
        await helper_store.upsert_edge(
            FORK1, SHARED_AGENT, {"type": "HAS_AGENT", "sst_semantic": "EXPRESSES"}
        )
        await helper_store.upsert_edge(
            OTHER_ROOT, SHARED_AGENT, {"type": "HAS_AGENT", "sst_semantic": "EXPRESSES"}
        )

        # A separate, tiny session with no drain worker attached, used only
        # for the "still receiving data" (409) check below.
        await helper_store.upsert_node(
            PENDING_ROOT,
            {"labels": ["Session", "RootSession"], "started_at": T0},
        )
        await helper_store.flush()

        # --------------------------------------------------------------
        # Step 3 -- GET summary through HTTP, from a SUBsession id, with NO
        # workspace query param -- the server looks up which workspace the
        # session belongs to on its own. Checks it resolves the whole
        # graph, and that GET deletes nothing: blob_count must be greater
        # than 0 and match the number of real blobs the blob store lists
        # for the graph's sessions (the proof that the summary now sees
        # real blobs without any hand-attached node property), and every
        # node/blob is still there afterwards.
        # --------------------------------------------------------------
        real_blob_uris: list[str] = []
        for session_id in (ROOT, SUB1, SUB2, FORK1):
            real_blob_uris += await blob_store.list(session_id)
        assert len(real_blob_uris) == 3, (
            f"expected one real blob each for root/sub2/fork1, found {real_blob_uris!r}"
        )

        summary_resp = await client.get(f"/sessions/{SUB2}/summary")
        assert summary_resp.status_code == 200, summary_resp.text
        summary = summary_resp.json()
        assert summary["root_id"] == ROOT
        assert sorted(summary["session_ids"]) == sorted([ROOT, SUB1, SUB2, FORK1])
        assert summary["subsession_count"] == 3
        assert summary["blob_count"] > 0
        assert summary["blob_count"] == len(real_blob_uris)
        assert summary["deletable"] is True
        assert summary["pending_sessions"] == []

        for session_id in (ROOT, SUB1, SUB2, FORK1):
            assert await helper_store.get_node(session_id) is not None, (
                f"{session_id} should still exist after a GET summary"
            )
        assert await blob_store.list(ROOT) != []
        assert await blob_store.list(SUB2) != []
        assert await blob_store.list(FORK1) != []

        # --------------------------------------------------------------
        # Step 4 -- deleting a session that is still receiving data
        # returns 409 and deletes nothing. Uses the separate pending
        # session seeded above, with an uncommitted queue record and no
        # live drain worker to race against. No workspace query param here
        # either.
        # --------------------------------------------------------------
        await main_module.registry.queue_manager.append(
            PENDING_ROOT, b'{"event": "session:start", "data": {}}'
        )
        pending_delete_resp = await client.delete(f"/sessions/{PENDING_ROOT}")
        assert pending_delete_resp.status_code == 409, pending_delete_resp.text
        assert await helper_store.get_node(PENDING_ROOT) is not None, (
            "a 409 refusal must not delete anything"
        )

        # --------------------------------------------------------------
        # Step 5 -- the real delete, through HTTP. DELETE always deletes
        # now -- there is no apply flag, and no workspace query param.
        # --------------------------------------------------------------
        delete_resp = await client.delete(f"/sessions/{SUB2}")
        assert delete_resp.status_code == 200, delete_resp.text
        result = delete_resp.json()
        assert result["root_id"] == ROOT
        assert result["session_count"] == 4
        assert result["blobs_deleted"] == 3
        assert result["queue_sessions_cleaned"] == 4

        # --------------------------------------------------------------
        # Step 6 -- verify directly against the real stores.
        # --------------------------------------------------------------
        for session_id in (ROOT, SUB1, SUB2, FORK1):
            assert await helper_store.get_node(session_id) is None, (
                f"{session_id} should be gone"
            )
            assert await blob_store.list(session_id) == [], (
                f"{session_id}'s blobs should be gone"
            )
            qm = main_module.registry.queue_manager
            assert not qm._log_path(session_id).exists()
            assert not qm._offset_path(session_id).exists()
            assert not qm._dead_path(session_id).exists()

        agent_node = await helper_store.get_node(SHARED_AGENT)
        assert agent_node is not None
        assert "SST_CONCEPT" in agent_node.get("labels", [])

        assert await helper_store.get_node(OTHER_ROOT) is not None
        assert await helper_store.get_edge(OTHER_ROOT, SHARED_AGENT) is not None
    finally:
        await helper_store.close()
