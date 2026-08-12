"""Tier 3 -- Neo4j end-to-end tests for POST /admin/blobs/reclaim.

Covers the design in
``docs/plans/2026-08-12-blob-reclaim-endpoint-spec.md`` (see the "Council
amendment -- AUTHORITATIVE" section, which supersedes conflicting earlier
text): the ONE shared ``_select_orphans`` selection path, the B1 Event.data
invariant, the B2 structural-JSON-extraction requirement (no regex, no
APOC), and the B3 durable undrained-queue safety gate.

Requires Docker and the ``docker`` Python package -- skipped via the
``neo4j_container`` fixture in ``tests/neo4j/conftest.py`` when unavailable.

Run explicitly:
    cd amplifier-context-intelligence
    uv run pytest tests/neo4j/test_blob_reclaim.py -v -m neo4j
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import httpx
import pytest
from context_intelligence_server.config import get_settings
from context_intelligence_server.handlers.data_layer_1.default import DefaultHandler
from context_intelligence_server.registry import SessionWorker
from context_intelligence_server.services import HookStateService
from neo4j import AsyncGraphDatabase

pytestmark = pytest.mark.neo4j

# ---------------------------------------------------------------------------
# Fixture-data helpers
# ---------------------------------------------------------------------------

_OLD_AGE_SECONDS = 7_200.0  # 2 hours -- comfortably past any min_age_minutes used


def _write_blob(
    root: Path, session_id: str, key: str, *, age_seconds: float = 0.0
) -> Path:
    """Write a fake blob file at the exact layout AsyncDiskBlobStore uses.

    ``age_seconds > 0`` back-dates the file's mtime via ``os.utime`` so tests
    can exercise the min_age_minutes gate deterministically.
    """
    p = root / session_id / "blobs" / f"{key}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"payload": "x" * 32}), encoding="utf-8")
    if age_seconds > 0:
        ts = time.time() - age_seconds
        os.utime(p, (ts, ts))
    return p


async def _create_event_node(
    driver: Any, *, node_id: str, workspace: str, data: dict[str, Any]
) -> None:
    """Create a bare :Event node carrying *data* as a JSON string, mirroring
    what DefaultHandler persists in production (node_props["data"] =
    json.dumps(data))."""
    async with driver.session() as session:
        await session.run(
            "CREATE (:Event {node_id: $node_id, workspace: $workspace, data: $data})",
            {"node_id": node_id, "workspace": workspace, "data": json.dumps(data)},
        )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def neo4j_driver(
    neo4j_container: dict[str, Any],
) -> AsyncGenerator[Any, None]:
    """A standalone async driver for seeding/verifying graph state directly."""
    driver = AsyncGraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    yield driver
    await driver.close()


@pytest.fixture
async def admin_client(
    tmp_path: Path,
    neo4j_container: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """A live ASGI client for /admin/blobs/reclaim wired to the real container.

    - ``require_admin`` is overridden to a no-op (auth enforcement is proven
      separately in ``tests/routers/test_blob_reclaim_auth.py``).
    - ``blob_path`` is redirected to ``tmp_path/blobs`` via the env-var +
      ``get_settings.cache_clear()`` pattern (mirrors
      ``tests/integration/test_blob_pipeline.py::integration_env``) so the
      route's own ``get_settings()`` call sees it.
    - ``queues_path`` is redirected to the SAME ``tmp_path/queues`` the
      autouse ``safe_settings`` fixture (tests/conftest.py) already points
      ``registry.queue_manager`` at -- no extra wiring needed.
    - ``app.state.neo4j_query_driver`` is a REAL driver against the live test
      container (not a mock), so the reference scan runs for real.
    """
    import context_intelligence_server.main as main_module
    from context_intelligence_server.routers.admin import require_admin

    blob_dir = tmp_path / "blobs"
    blob_dir.mkdir()

    get_settings.cache_clear()
    monkeypatch.setenv("AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_BLOB_PATH", str(blob_dir))

    main_module.create_asgi_app()
    main_module.app.dependency_overrides[require_admin] = lambda: None

    query_driver = AsyncGraphDatabase.driver(
        neo4j_container["bolt_url"],
        auth=(neo4j_container["user"], neo4j_container["password"]),
    )
    monkeypatch.setattr(
        main_module.app.state, "neo4j_query_driver", query_driver, raising=False
    )
    monkeypatch.setattr(
        main_module.app.state, "neo4j_query_access_mode", "READ", raising=False
    )

    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main_module.app),
            base_url="http://test",
        ) as client:
            yield client
    finally:
        main_module.app.dependency_overrides.pop(require_admin, None)
        await query_driver.close()
        get_settings.cache_clear()
        # neo4j_container is session-scoped -- clean up between tests.
        cleanup_driver = AsyncGraphDatabase.driver(
            neo4j_container["bolt_url"],
            auth=(neo4j_container["user"], neo4j_container["password"]),
        )
        async with cleanup_driver.session() as session:
            await session.run("MATCH (n) DETACH DELETE n")
        await cleanup_driver.close()


# ---------------------------------------------------------------------------
# T-orphan / T-referenced / T-workspace-safety / T-in-flight-recent
# ---------------------------------------------------------------------------


async def test_orphan_listed_in_dry_run_and_deleted_on_apply(
    admin_client: httpx.AsyncClient, tmp_path: Path
) -> None:
    """T-orphan: unreferenced, old, drained blob -> dry-run lists it, apply deletes it."""
    blob_dir = tmp_path / "blobs"
    sid = "orphan-sess-1"
    path = _write_blob(blob_dir, sid, "node1__result", age_seconds=_OLD_AGE_SECONDS)
    uri = f"ci-blob://{sid}/node1__result"

    dry = await admin_client.post(
        "/admin/blobs/reclaim", json={"dry_run": True, "min_age_minutes": 15}
    )
    assert dry.status_code == 200
    dry_body = dry.json()
    assert dry_body["dry_run"] is True
    assert dry_body["orphans_found"] == 1
    assert uri in dry_body["sample"]
    assert dry_body["reclaimable_bytes"] > 0
    assert dry_body["rescanned"] is False
    assert dry_body["deleted"] == 0
    assert path.exists(), "dry-run must never delete"

    apply_resp = await admin_client.post(
        "/admin/blobs/reclaim",
        json={"dry_run": False, "min_age_minutes": 15, "max_delete": 10},
    )
    assert apply_resp.status_code == 200
    apply_body = apply_resp.json()
    assert apply_body["dry_run"] is False
    assert apply_body["rescanned"] is True
    assert apply_body["orphans_found"] == 1
    assert apply_body["deleted"] == 1
    assert apply_body["deleted_bytes"] > 0
    assert not path.exists(), "apply must delete the orphan file"


async def test_referenced_blob_never_a_candidate(
    admin_client: httpx.AsyncClient,
    tmp_path: Path,
    neo4j_driver: Any,
) -> None:
    """T-referenced: a blob referenced by :Event.data is never a candidate."""
    blob_dir = tmp_path / "blobs"
    sid = "referenced-sess-1"
    path = _write_blob(blob_dir, sid, "node1__result", age_seconds=_OLD_AGE_SECONDS)
    uri = f"ci-blob://{sid}/node1__result"

    await _create_event_node(
        neo4j_driver,
        node_id="evt-ref-1",
        workspace="ws-a",
        data={"session_id": sid, "result": {"$blob_ref": uri}},
    )

    resp = await admin_client.post(
        "/admin/blobs/reclaim", json={"dry_run": True, "min_age_minutes": 15}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["referenced_uris"] >= 1
    assert body["orphans_found"] == 0
    assert uri not in body["sample"]

    apply_resp = await admin_client.post(
        "/admin/blobs/reclaim",
        json={"dry_run": False, "min_age_minutes": 15, "max_delete": 10},
    )
    assert apply_resp.status_code == 200
    assert apply_resp.json()["deleted"] == 0
    assert path.exists(), "a referenced blob must never be deleted"


async def test_cross_workspace_reference_still_protects_blob(
    admin_client: httpx.AsyncClient,
    tmp_path: Path,
    neo4j_driver: Any,
) -> None:
    """T-workspace-safety: reference from a DIFFERENT workspace still protects the blob.

    Proves the reference scan is global (never workspace-filtered) -- a
    per-workspace scan would wrongly treat this blob as orphaned.
    """
    blob_dir = tmp_path / "blobs"
    sid = "cross-ws-sess-1"
    path = _write_blob(blob_dir, sid, "node1__result", age_seconds=_OLD_AGE_SECONDS)
    uri = f"ci-blob://{sid}/node1__result"

    # The referencing node lives in "workspace-b" -- a DIFFERENT workspace
    # than any the reclaim request could plausibly scope to (the endpoint
    # accepts no workspace parameter at all -- the scan is always global).
    await _create_event_node(
        neo4j_driver,
        node_id="evt-cross-ws-1",
        workspace="workspace-b",
        data={"session_id": sid, "result": {"$blob_ref": uri}},
    )

    resp = await admin_client.post(
        "/admin/blobs/reclaim", json={"dry_run": True, "min_age_minutes": 15}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["orphans_found"] == 0
    assert uri not in body["sample"]
    assert path.exists()


async def test_fresh_blob_skipped_as_recent(
    admin_client: httpx.AsyncClient, tmp_path: Path
) -> None:
    """T-in-flight-recent: a freshly-written, unreferenced blob is excluded via skipped_recent."""
    blob_dir = tmp_path / "blobs"
    sid = "fresh-sess-1"
    path = _write_blob(blob_dir, sid, "node1__result", age_seconds=0.0)

    resp = await admin_client.post(
        "/admin/blobs/reclaim", json={"dry_run": True, "min_age_minutes": 60}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["orphans_found"] == 0
    assert body["skipped_recent"] >= 1
    assert path.exists()


# ---------------------------------------------------------------------------
# T-pending-queue (B3 -- undrained-queue gate, both durable-log and
# live-worker OR clauses)
# ---------------------------------------------------------------------------


async def test_undrained_queue_skips_even_old_blob(
    admin_client: httpx.AsyncClient, tmp_path: Path
) -> None:
    """T-pending-queue (durable): an undrained .log skips the blob even though
    it is old -- the durable gate takes priority over the age gate."""
    from context_intelligence_server.main import registry as shared_registry

    blob_dir = tmp_path / "blobs"
    sid = "pending-queue-sess-1"
    path = _write_blob(blob_dir, sid, "node1__result", age_seconds=_OLD_AGE_SECONDS)

    # Append WITHOUT committing -- committed offset (0) stays behind the
    # complete-data end, so is_fully_drained(sid) is False.
    qm = shared_registry.queue_manager
    await qm.append(
        sid,
        json.dumps(
            {
                "event": "tool:pre",
                "workspace": "w",
                "data": {"session_id": sid, "timestamp": "2024-01-01T00:00:00+00:00"},
            }
        ).encode("utf-8"),
    )
    assert not await qm.is_fully_drained(sid), "test setup: queue must be undrained"

    resp = await admin_client.post(
        "/admin/blobs/reclaim", json={"dry_run": True, "min_age_minutes": 15}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["orphans_found"] == 0
    assert body["skipped_pending_session"] >= 1
    assert body["skipped_recent"] == 0, (
        "an old blob gated by the durable undrained-queue check must be "
        "counted as skipped_pending_session, NOT skipped_recent"
    )
    assert path.exists()


async def test_live_worker_skips_even_drained_old_blob(
    admin_client: httpx.AsyncClient, tmp_path: Path
) -> None:
    """T-pending-queue (live-worker OR clause): a registered live worker skips
    the blob even when its queue has no undrained data at all (drained)."""
    from context_intelligence_server.main import registry as shared_registry

    blob_dir = tmp_path / "blobs"
    sid = "live-worker-sess-1"
    path = _write_blob(blob_dir, sid, "node1__result", age_seconds=_OLD_AGE_SECONDS)

    # No .log file exists for this session at all -- is_fully_drained(sid)
    # reads True (0 >= 0). Only the live-worker registration should gate it.
    qm = shared_registry.queue_manager
    assert await qm.is_fully_drained(sid), "test setup: queue must read as drained"

    worker = SessionWorker(
        session_id=sid, workspace="w", services=HookStateService(workspace="w")
    )
    shared_registry._register_for_test(worker)
    assert sid in shared_registry.active_sessions()

    resp = await admin_client.post(
        "/admin/blobs/reclaim", json={"dry_run": True, "min_age_minutes": 15}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["orphans_found"] == 0
    assert body["skipped_pending_session"] >= 1
    assert path.exists()


# ---------------------------------------------------------------------------
# T-b2-specialchars (structural JSON extraction, not regex)
# ---------------------------------------------------------------------------


async def test_special_characters_in_session_id_classified_referenced(
    admin_client: httpx.AsyncClient,
    tmp_path: Path,
    neo4j_driver: Any,
) -> None:
    """T-b2-specialchars: a session_id containing a literal quote and a
    non-ASCII character, when referenced, MUST be classified referenced.

    The named regex pattern from the pre-amendment design (``ci-blob://[^"\\\\]+``)
    truncates at the first unescaped `"` in the SERIALIZED JSON string,
    misclassifying this exact case as orphan. Structural `json.loads` +
    recursive walk (B2) handles it correctly because the quote and the
    non-ASCII character are just ordinary characters in the DECODED string --
    json.loads has already resolved all escaping before the walk runs.
    """
    blob_dir = tmp_path / "blobs"
    # A literal double-quote (queue_manager._validate_session_id blocks only
    # '/', '\\', and '\\0' -- NOT '"') and a non-ASCII character (û).
    sid = 'weird"session-û'
    path = _write_blob(blob_dir, sid, "node1__result", age_seconds=_OLD_AGE_SECONDS)
    uri = f"ci-blob://{sid}/node1__result"

    await _create_event_node(
        neo4j_driver,
        node_id="evt-special-1",
        workspace="ws-special",
        data={"session_id": sid, "result": {"$blob_ref": uri}},
    )

    resp = await admin_client.post(
        "/admin/blobs/reclaim", json={"dry_run": True, "min_age_minutes": 15}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["referenced_uris"] >= 1
    assert body["orphans_found"] == 0, (
        f"special-character URI {uri!r} must be classified referenced via "
        "structural JSON extraction -- a regex-based extractor would "
        "truncate at the embedded quote and misclassify it as orphan"
    )
    assert uri not in body["sample"]
    assert path.exists()


# ---------------------------------------------------------------------------
# T-dry-apply-parity / T-idempotence / T-max-delete-required / T-max-delete-cap
# ---------------------------------------------------------------------------


async def test_dry_run_and_apply_select_the_same_candidate_set(
    admin_client: httpx.AsyncClient, tmp_path: Path
) -> None:
    """T-dry-apply-parity: dry-run's candidate set == apply's deleted set for
    identical fixtures (proves the ONE shared _select_orphans path)."""
    blob_dir = tmp_path / "blobs"
    sids_and_keys = [
        ("parity-sess-1", "n1__result"),
        ("parity-sess-2", "n2__result"),
        ("parity-sess-3", "n3__result"),
    ]
    expected_uris = set()
    for sid, key in sids_and_keys:
        _write_blob(blob_dir, sid, key, age_seconds=_OLD_AGE_SECONDS)
        expected_uris.add(f"ci-blob://{sid}/{key}")

    dry = await admin_client.post(
        "/admin/blobs/reclaim", json={"dry_run": True, "min_age_minutes": 15}
    )
    dry_body = dry.json()
    assert set(dry_body["sample"]) == expected_uris
    assert dry_body["orphans_found"] == len(expected_uris)

    apply_resp = await admin_client.post(
        "/admin/blobs/reclaim",
        json={"dry_run": False, "min_age_minutes": 15, "max_delete": 100},
    )
    apply_body = apply_resp.json()
    assert set(apply_body["sample"]) == expected_uris
    assert apply_body["deleted"] == len(expected_uris)


async def test_second_apply_is_idempotent(
    admin_client: httpx.AsyncClient, tmp_path: Path
) -> None:
    """T-idempotence: a second apply call deletes zero (files already gone)."""
    blob_dir = tmp_path / "blobs"
    sid = "idempotent-sess-1"
    _write_blob(blob_dir, sid, "node1__result", age_seconds=_OLD_AGE_SECONDS)

    first = await admin_client.post(
        "/admin/blobs/reclaim",
        json={"dry_run": False, "min_age_minutes": 15, "max_delete": 10},
    )
    assert first.json()["deleted"] == 1

    second = await admin_client.post(
        "/admin/blobs/reclaim",
        json={"dry_run": False, "min_age_minutes": 15, "max_delete": 10},
    )
    second_body = second.json()
    assert second_body["orphans_found"] == 0
    assert second_body["deleted"] == 0


async def test_apply_without_max_delete_is_422(
    admin_client: httpx.AsyncClient,
) -> None:
    """T-max-delete-required: dry_run=false without max_delete -> 422."""
    resp = await admin_client.post(
        "/admin/blobs/reclaim", json={"dry_run": False, "min_age_minutes": 15}
    )
    assert resp.status_code == 422


async def test_max_delete_cap_is_honored(
    admin_client: httpx.AsyncClient, tmp_path: Path
) -> None:
    """T-max-delete-cap: totals stay authoritative while the cap limits deletions."""
    blob_dir = tmp_path / "blobs"
    for i in range(3):
        _write_blob(
            blob_dir, f"cap-sess-{i}", "n__result", age_seconds=_OLD_AGE_SECONDS
        )

    resp = await admin_client.post(
        "/admin/blobs/reclaim",
        json={"dry_run": False, "min_age_minutes": 15, "max_delete": 1},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["orphans_found"] == 3, "totals must reflect the FULL candidate set"
    assert body["deleted"] == 1, "deletions must be capped at max_delete"
    assert body["reclaimable_bytes"] > body["deleted_bytes"] or body["deleted"] == 0


# ---------------------------------------------------------------------------
# T-b1-invariant (Event.data is the complete reference carrier)
# ---------------------------------------------------------------------------


async def test_b1_event_data_carries_every_blob_ref(
    neo4j_services: Any,
) -> None:
    """T-b1-invariant: an event carrying a $blob_ref-shaped value in tool_input
    AND in each BLOB_FIELD must have every exact URI present in the
    persisted :Event.data -- pinning the invariant the reclaim scan depends
    on (B1 of the council amendment).

    Uses DefaultHandler directly against a real Neo4j-backed HookStateService
    (``neo4j_services`` fixture, tests/neo4j/conftest.py) so this is a real
    handler run, not a synthetic Event.data string.
    """
    handler = DefaultHandler(neo4j_services)
    session_id = "b1-invariant-sess-1"
    tool_input_ref = "ci-blob://b1-invariant-sess-1/pre-existing-blob"

    # blob_processor.BLOB_FIELDS: {"raw", "result", "messages", "mount_plan",
    # "context_snapshot", "debug"}. Simulate each already offloaded to a
    # $blob_ref (as blob_processor.process_event_data would have done
    # upstream of DefaultHandler in the real pipeline).
    blob_field_refs = {
        field: f"ci-blob://{session_id}/node1__{field}"
        for field in (
            "raw",
            "result",
            "messages",
            "mount_plan",
            "context_snapshot",
            "debug",
        )
    }

    data: dict[str, Any] = {
        "session_id": session_id,
        "timestamp": "2024-01-01T00:00:00+00:00",
        "tool_input": {"$blob_ref": tool_input_ref},
        **{k: {"$blob_ref": v} for k, v in blob_field_refs.items()},
    }

    await handler("tool:pre", data)
    await neo4j_services.graph.flush()

    driver = neo4j_services.graph._driver
    async with driver.session() as session:
        result = await session.run(
            "MATCH (n:Event) WHERE n.event_name = 'tool:pre' "
            "AND n.data CONTAINS $sid RETURN n.data AS data",
            {"sid": session_id},
        )
        rows = [record["data"] async for record in result]

    assert len(rows) == 1, f"expected exactly one persisted Event node, got {rows}"
    persisted = json.loads(rows[0])

    assert persisted["tool_input"]["$blob_ref"] == tool_input_ref
    for field, expected_uri in blob_field_refs.items():
        assert persisted[field]["$blob_ref"] == expected_uri, (
            f"BLOB_FIELD {field!r} ref must survive verbatim into persisted "
            "Event.data -- this is the invariant the reclaim scan relies on"
        )
