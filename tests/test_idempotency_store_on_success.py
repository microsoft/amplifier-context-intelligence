"""Idempotency key burned before the durable append.

The idempotency cache must not burn a key until ``queue_manager.append``
returns successfully: ``seen(key)`` (read-only duplicate check) runs before
the append; ``store(key)`` runs only after it succeeds. A failed append must
never poison a retry with a false "duplicate".
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest

import context_intelligence_server.main as main_module
from context_intelligence_server.idempotency import EventIdempotencyCache

_TIMESTAMP = "2026-06-16T20:17:11.604690+00:00"


@pytest.fixture(autouse=True)
def _clear_idempotency_cache() -> None:
    """Mirrors tests/test_main.py's autouse fixture -- this file is a
    separate module, so it needs its own clear (module-level fixtures are
    not shared across test files; the cache is a process-wide singleton).
    """
    main_module.idempotency_cache.clear()


def _payload(session_id: str, idempotency_key: str) -> dict:
    return {
        "event": "tool_use",
        "workspace": "/ws",
        "idempotency_key": idempotency_key,
        "data": {
            "session_id": session_id,
            "timestamp": _TIMESTAMP,
        },
    }


# ---------------------------------------------------------------------------
# Append fails => key not burned => retry honoured, becomes durable
# ---------------------------------------------------------------------------


async def test_t1_append_failure_leaves_key_unburned_retry_honoured(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST #1 (append raises) must not burn the key; POST #2 with the same
    key must be honoured (202 "queued", actually appended), not "duplicate"."""
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *a, **k: MagicMock()
    )
    appended: list[tuple[str, bytes]] = []
    call_count = 0

    async def _fake_append(worker_key: str, raw: bytes) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise OSError("simulated durable-write failure")
        appended.append((worker_key, raw))

    monkeypatch.setattr(main_module.registry.queue_manager, "append", _fake_append)

    payload = _payload("sess-d7-t1", "aci-event-v1:d7-t1-key")

    # POST #1: append fails. No exception handler is registered in main.py,
    # and the test client's ASGITransport uses raise_app_exceptions=True
    # (the httpx default) -- so the OSError propagates OUT of
    # `client.post(...)` itself. There is no response object to assert a
    # status code against.
    with pytest.raises(OSError):
        await client.post("/events", json=payload)

    assert call_count == 1
    assert appended == []

    # POST #2: same key. The key was never stored because append never
    # returned successfully, so the retry is honoured and becomes durable.
    second = await client.post("/events", json=payload)
    assert second.status_code == 202
    assert second.json()["status"] == "queued"
    assert call_count == 2
    assert len(appended) == 1
    worker_key, raw = appended[0]
    assert worker_key == "sess-d7-t1"
    assert json.loads(raw)["event"] == "tool_use"


# ---------------------------------------------------------------------------
# T1b -- the cache itself is untouched by a failed append
# ---------------------------------------------------------------------------


async def test_t1b_cache_untouched_by_failed_append(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Asserted directly on the cache object, not inferred from the response."""
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *a, **k: MagicMock()
    )

    async def _always_fail_append(worker_key: str, raw: bytes) -> None:
        raise OSError("simulated durable-write failure")

    monkeypatch.setattr(
        main_module.registry.queue_manager, "append", _always_fail_append
    )

    key = "aci-event-v1:d7-t1b-key"
    payload = _payload("sess-d7-t1b", key)

    with pytest.raises(OSError):
        await client.post("/events", json=payload)

    # seen(key) must be False -- nothing was stored.
    assert main_module.idempotency_cache.seen(key) is False


# ---------------------------------------------------------------------------
# Genuine duplicate still "duplicate"
# ---------------------------------------------------------------------------


async def test_t2_genuine_duplicate_still_duplicate_no_double_append(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two sequential POSTs, append always succeeds -> first "queued",
    second "duplicate", exactly one durable line."""
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *a, **k: MagicMock()
    )
    appended: list[tuple[str, bytes]] = []

    async def _fake_append(worker_key: str, raw: bytes) -> None:
        appended.append((worker_key, raw))

    monkeypatch.setattr(main_module.registry.queue_manager, "append", _fake_append)

    payload = _payload("sess-d7-t2", "aci-event-v1:d7-t2-key")

    first = await client.post("/events", json=payload)
    second = await client.post("/events", json=payload)

    assert first.status_code == 202
    assert first.json()["status"] == "queued"
    assert second.status_code == 202
    assert second.json()["status"] == "duplicate"
    assert len(appended) == 1


# ---------------------------------------------------------------------------
# Concurrent same-key double-POST: both append
# ---------------------------------------------------------------------------


async def test_t3_concurrent_same_key_double_post_both_append(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Hold POST #1 inside a patched ``append`` until POST #2 has passed its
    own ``seen()`` check, then release; gather both. Both must return 202
    "queued" and append (len == 2); neither may be answered "duplicate" --
    two same-key POSTs overlapping inside the check->store window both
    append, converging downstream via idempotent MERGE. Realizable only
    because ``append`` is monkeypatched, bypassing the real per-key
    admission lock; do not "fix" this test toward the real queue manager."""
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *a, **k: MagicMock()
    )
    appended: list[tuple[str, bytes]] = []
    release = asyncio.Event()
    call_count = 0

    async def _fake_append(worker_key: str, raw: bytes) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # Hold the first arrival open until the second arrival has
            # reached (and passed) its own seen() check and called append.
            await asyncio.wait_for(release.wait(), timeout=5)
        else:
            # The second arrival has now passed its check (it is executing
            # this very call) -- release the first so both complete.
            release.set()
        appended.append((worker_key, raw))

    monkeypatch.setattr(main_module.registry.queue_manager, "append", _fake_append)

    payload = _payload("sess-d7-t3", "aci-event-v1:d7-t3-key")

    r1, r2 = await asyncio.wait_for(
        asyncio.gather(
            client.post("/events", json=payload),
            client.post("/events", json=payload),
        ),
        timeout=10,
    )

    assert r1.status_code == 202
    assert r2.status_code == 202
    statuses = {r1.json()["status"], r2.json()["status"]}
    assert statuses == {"queued"}, (
        f"neither concurrent POST may be answered duplicate; got {statuses}"
    )
    assert len(appended) == 2


# ---------------------------------------------------------------------------
# Reservation leak: not applicable by construction
# ---------------------------------------------------------------------------


def test_t4_reservation_leak_not_applicable_by_construction() -> None:
    """The store-on-success design has no reservation state analogous to
    ``check_and_reserve``, so there is no reservation that can leak or a key
    that can be permanently blocked by a lost ``release``."""
    assert True  # documentation-only; see docstring


# ---------------------------------------------------------------------------
# Replay path unaffected (regression)
# ---------------------------------------------------------------------------


async def test_t5_replay_never_stores_key_non_replay_still_honoured(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``?replay=true`` twice with key K -> both "queued", 2 appends; then a
    non-replay POST with K -> "queued" (not "duplicate"), proving replay
    never stored K."""
    monkeypatch.setattr(
        main_module.registry, "get_or_create", lambda *a, **k: MagicMock()
    )
    appended: list[tuple[str, bytes]] = []

    async def _fake_append(worker_key: str, raw: bytes) -> None:
        appended.append((worker_key, raw))

    monkeypatch.setattr(main_module.registry.queue_manager, "append", _fake_append)

    payload = _payload("sess-d7-t5", "aci-event-v1:d7-t5-key")

    replay1 = await client.post("/events?replay=true", json=payload)
    replay2 = await client.post("/events?replay=true", json=payload)
    assert replay1.status_code == 202
    assert replay1.json()["status"] == "queued"
    assert replay2.status_code == 202
    assert replay2.json()["status"] == "queued"
    assert len(appended) == 2

    non_replay = await client.post("/events", json=payload)
    assert non_replay.status_code == 202
    assert non_replay.json()["status"] == "queued"  # NOT "duplicate"
    assert len(appended) == 3


# ---------------------------------------------------------------------------
# EventIdempotencyCache.seen/store unit behaviour
# ---------------------------------------------------------------------------


class TestEventIdempotencyCacheSeenStore:
    """Direct unit coverage of the seen()/store() split."""

    def test_seen_false_then_store_then_seen_true(self) -> None:
        cache = EventIdempotencyCache()
        assert cache.seen("k1") is False
        cache.store("k1")
        assert cache.seen("k1") is True

    def test_seen_purges_past_ttl_entries(self) -> None:
        cache = EventIdempotencyCache(ttl_seconds=10)
        cache.store("k1", now=0.0)
        assert cache.seen("k1", now=5.0) is True  # still within ttl
        assert cache.seen("k1", now=20.0) is False  # purged -- past ttl

    def test_store_trims_at_max_entries(self) -> None:
        cache = EventIdempotencyCache(max_entries=2)
        cache.store("k1", now=1.0)
        cache.store("k2", now=2.0)
        cache.store("k3", now=3.0)  # trims the oldest (k1)
        assert cache.seen("k1", now=3.0) is False
        assert cache.seen("k2", now=3.0) is True
        assert cache.seen("k3", now=3.0) is True

    def test_seen_refreshes_lru_recency_on_hit(self) -> None:
        cache = EventIdempotencyCache(max_entries=2)
        cache.store("k1", now=1.0)
        cache.store("k2", now=2.0)
        assert cache.seen("k1", now=2.5) is True  # touch k1 -> moves to end
        cache.store("k3", now=3.0)  # trims the now-LRU k2, not k1
        assert cache.seen("k1", now=3.0) is True
        assert cache.seen("k2", now=3.0) is False
        assert cache.seen("k3", now=3.0) is True

    def test_store_idempotent_under_repeat_calls(self) -> None:
        cache = EventIdempotencyCache()
        cache.store("k1", now=1.0)
        cache.store("k1", now=2.0)  # repeat store must not raise
        assert cache.seen("k1", now=2.0) is True


# ---------------------------------------------------------------------------
# No stale check_and_store references anywhere in the repo
# ---------------------------------------------------------------------------


def test_t7_no_stale_check_and_store_references() -> None:
    """``check_and_store`` must have no remaining call/def sites anywhere in
    the repo. Scoped to the ``check_and_store(`` call/def form (not the bare
    identifier) so explanatory prose mentioning the old name doesn't trip
    the scan; this file is excluded from the scan for the same reason."""
    repo_root = Path(__file__).resolve().parents[1]
    this_file = Path(__file__).resolve()
    target = "check_and_store("
    hits: list[str] = []
    for py_file in repo_root.rglob("*.py"):
        resolved = py_file.resolve()
        if resolved == this_file:
            continue
        if any(
            part in {".git", ".venv", "__pycache__", "node_modules"}
            for part in py_file.parts
        ):
            continue
        text = py_file.read_text(encoding="utf-8", errors="ignore")
        if target in text:
            hits.append(str(py_file))
    assert hits == [], f"stale check_and_store( call/def sites found in: {hits}"
