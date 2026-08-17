"""Regression test for the reclaim APPLY single-flight guard (item -a1i).

Pins: while a first ``POST /admin/blobs/reclaim`` apply (``dry_run=false``)
is in-flight inside its own ``registry.blob_store.scan()``, a second
concurrent apply must get **409** ``{"detail": "blob reclaim apply already
running"}`` -- and must NOT itself run a scan/delete pass. See
``_try_begin_reclaim_apply``/``_finish_reclaim_apply`` in
``routers/admin.py`` for the CAS this pins.

No real Neo4j: ``_scan_referenced_uris`` is monkeypatched to return an empty
set, and the graph store is a fake whose ``scan()`` yields zero references
once released -- only the single-flight gate is under test.

**Why this calls ``reclaim_blobs`` directly instead of two real overlapping
HTTP requests over ASGITransport:** investigated first (per the task spec's
preference). A minimal repro -- a brand-new, otherwise-trivial ``POST``
route added straight to the real ``app``, no reclaim logic at all, no
request body -- deadlocks/crosses-streams identically under
``httpx.AsyncClient`` + ``ASGITransport`` when two concurrent ``POST``\\ s
overlap through this app's stack (``anyio``'s
``MemoryObjectReceiveStream`` raises ``_closed=True`` inside
``maintenance_gate_middleware``, a Starlette ``BaseHTTPMiddleware`` --
a documented Starlette limitation with genuinely-concurrent requests
sharing one middleware instance, not something introduced by or fixable
in the reclaim endpoint). Concurrent ``GET``\\ s through the same app were
unaffected, isolating the limitation to the middleware layer, not
``httpx``, not the fake store, not this test's synchronization. Per the
task's explicitly-sanctioned fallback, this test instead drives the CAS
directly: it awaits the ``reclaim_blobs`` coroutine itself (bypassing
Starlette/FastAPI routing and middleware entirely) with a hand-built
``Request`` stand-in exposing only what the function under test reads
(``request.app.state.registry``). This still exercises the real
``_try_begin_reclaim_apply``/``_finish_reclaim_apply`` CAS and the real
``reclaim_blobs`` control flow -- only the ASGI transport hop is skipped.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import pytest
from context_intelligence_server.blob_store import BlobReference
from context_intelligence_server.routers.admin import BlobReclaimBody, reclaim_blobs
from fastapi.responses import JSONResponse


class _BarrierBlobStore:
    """Fake ``BlobStore`` whose ``scan()`` parks on a barrier before finishing.

    Signals ``entered`` the instant ``scan()`` starts running -- before its
    own await -- so a test can deterministically wait until the CAS flag is
    already held by the in-flight request before ever issuing a second one.
    Once released, yields zero references: only the single-flight gate is
    under test here, not orphan selection.
    """

    def __init__(self) -> None:
        self.scan_calls = 0
        self.entered = asyncio.Event()
        self.barrier = asyncio.Event()

    async def scan(self) -> AsyncIterator[BlobReference]:
        self.scan_calls += 1
        self.entered.set()
        await self.barrier.wait()
        return
        yield  # pragma: no cover -- makes this an async generator function


class _FakeAppState:
    def __init__(self, registry: Any) -> None:
        self.registry = registry


class _FakeApp:
    def __init__(self, registry: Any) -> None:
        self.state = _FakeAppState(registry)


class _FakeRequest:
    """Stand-in for FastAPI's ``Request`` exposing only what ``reclaim_blobs``
    (and the ``_select_orphans`` it calls) actually reads:
    ``request.app.state.registry``. ``_scan_referenced_uris`` -- the only
    other function taking ``request`` in this call path -- is monkeypatched
    below, so it never touches ``request`` for anything Neo4j-shaped.
    """

    def __init__(self, registry: Any) -> None:
        self.app = _FakeApp(registry)


async def test_second_concurrent_apply_gets_409_and_never_scans(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Second overlapping apply is rejected with 409 and performs zero scans."""
    import context_intelligence_server.main as main_module

    fake_store = _BarrierBlobStore()
    # Same injection seam used elsewhere (test_main.py, test_m2_service_auth.py):
    # patch the registry's private slot so `registry.blob_store` (the property)
    # returns the fake without touching real settings/disk.
    monkeypatch.setattr(main_module.registry, "_blob_store", fake_store)

    async def _fake_scan_referenced_uris(request: Any) -> set[str]:
        return set()

    monkeypatch.setattr(
        "context_intelligence_server.routers.admin._scan_referenced_uris",
        _fake_scan_referenced_uris,
    )

    # Defensive isolation: the CAS flag is a bare module-level global with no
    # dedicated reset fixture (unlike maintenance's coordinator). Force it
    # to the clean starting state regardless of prior test ordering; this
    # test's own two calls exercise begin/finish exactly once each and
    # restore it to False on the happy path anyway.
    monkeypatch.setattr(
        "context_intelligence_server.routers.admin._reclaim_apply_running", False
    )

    request = _FakeRequest(main_module.registry)
    body = BlobReclaimBody(dry_run=False, min_age_minutes=15, max_delete=1)

    async def _second_request_once_first_has_entered() -> dict[str, Any] | JSONResponse:
        # Deterministic sync point (no time.sleep): request 1 runs
        # synchronously -- the synchronous CAS, then into _select_orphans ->
        # registry.blob_store.scan() -- until it hits the barrier await. By
        # the time `entered` is set, request 1 already holds the CAS flag
        # (nothing yields to the loop before that point), so issuing
        # request 2 now guarantees genuine overlap.
        await asyncio.wait_for(fake_store.entered.wait(), timeout=5.0)
        resp = await reclaim_blobs(body, request)  # type: ignore[arg-type]
        # Release request 1 only now, having proven the 409 happened while
        # it was still parked inside its own scan() -- true overlap, not
        # sequential completion.
        fake_store.barrier.set()
        return resp

    first, second = await asyncio.wait_for(
        asyncio.gather(
            reclaim_blobs(body, request),  # type: ignore[arg-type]
            _second_request_once_first_has_entered(),
        ),
        timeout=10.0,
    )

    assert isinstance(second, JSONResponse)
    assert second.status_code == 409
    assert json.loads(bytes(second.body)) == {
        "detail": "blob reclaim apply already running"
    }

    # The rejected second request never ran its own scan+delete pass.
    assert fake_store.scan_calls == 1

    assert isinstance(first, dict)
    assert first["dry_run"] is False
    assert first["deleted"] == 0
    assert first["scanned_disk_blobs"] == 0
