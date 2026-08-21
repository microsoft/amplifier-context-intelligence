"""Idempotency key burned before the durable append.

Verified against the working tree with the durable append-log framing,
drain supervision, boot-safety, writer-lease, steady-state reclaim, and
finalize-delete-ordering fixes already implemented.

Bug (pre-fix): ``EventIdempotencyCache.check_and_store`` burns the key
BEFORE the durable append (``main.py:1269`` -> ``:1291``). If anything in
that window raises (``get_or_create``, the body round-trip, or
``queue_manager.append`` itself), the key is permanently burned for an
event that has ZERO bytes on disk. A client retry with the same key is then
answered HTTP 202 ``{"status": "duplicate"}`` -- a success code -- for an
event no recovery path (boot recovery, finalize re-drain,
compaction) can ever resurrect, because it never reached the log.

Fix (store-on-success): split the cache into ``seen(key)``
(read-only duplicate check, called BEFORE the append) and ``store(key)``
(record the key, called ONLY after ``queue_manager.append`` returns
normally). No try/except, no new state, no new response code -- "release on
failure" is simply not reaching the store line.

Test map (with a correction applied to T1/T1b):

    T1  -- append fails -> key not burned -> retry honoured, becomes durable
    T1b -- the cache itself is untouched by a failed append
    T2  -- genuine duplicate still "duplicate"; polarity tripwire
    T3  -- concurrent same-key double-POST: both append (R-1, accepted)
    T4  -- reservation leak: N/A by construction (documented, not skipped)
    T5  -- replay path unaffected (regression)
    T6  -- EventIdempotencyCache.seen/store unit behaviour
    T7  -- no stale check_and_store references anywhere in the repo

tests/conftest.py's ``client`` fixture uses
``httpx.ASGITransport(app=app)`` with the httpx default
``raise_app_exceptions=True``, and ``main.py`` registers NO custom exception
handler. An unhandled exception raised inside ``post_events`` therefore
PROPAGATES OUT of ``await client.post(...)`` itself -- there is no response
object, so a bare "assert response.status_code == 5xx" is unachievable
through this test client. T1/T1b wrap POST #1 in ``pytest.raises(OSError)``
accordingly.
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
# T1 -- append fails => key not burned => retry honoured, becomes durable
# ---------------------------------------------------------------------------


async def test_t1_append_failure_leaves_key_unburned_retry_honoured(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The headline silent-loss bug, reproduced and then required-fixed.

    Monkeypatch ``registry.queue_manager.append`` to raise ``OSError`` on
    call 1 and record on call 2. POST #1 with key K must raise (D-1 -- no
    HTTP response is observable for this failure through the
    test client). POST #2 with the SAME key K must be honoured: 202
    "queued", and the event actually appended -- not answered "duplicate"
    for a still-nonexistent event (the pre-fix bug).
    """
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
    # status code against (D-1).
    with pytest.raises(OSError):
        await client.post("/events", json=payload)

    assert call_count == 1
    assert appended == []

    # POST #2: same key. Pre-fix (current code): idempotency_cache already
    # burned the key during POST #1 (check_and_store ran before append), so
    # this returns 202 "duplicate" with nothing appended -- the bug,
    # reproduced. Post-fix: the key was never stored because append never
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

    # Pre-fix: idempotency_cache.check_and_store already stored the key
    # before append was ever reached, so a post-fix-shaped ``.seen(key)``
    # check does not even exist yet on current code (AttributeError -- RED
    # by absence). Post-fix: seen(key) must be False -- nothing was stored.
    assert main_module.idempotency_cache.seen(key) is False


# ---------------------------------------------------------------------------
# T2 -- genuine duplicate still "duplicate"; polarity tripwire
# ---------------------------------------------------------------------------


async def test_t2_genuine_duplicate_still_duplicate_no_double_append(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two sequential POSTs, append always succeeds -> first "queued",
    second "duplicate", exactly one durable line.

    This assertion passes both before and after the fix under CORRECT
    polarity -- it mirrors tests/test_main.py:91 by design (T2:
    "passes today"). Its value as a RED signal is against a DELIBERATELY
    inverted implementation of the fix (the ``seen()``/``store()`` split's
    documented polarity trap -- ``check_and_store`` returned True for NEW;
    ``seen`` returns True for DUPLICATE, so a mechanical
    ``if not is_new:`` -> ``if not idempotency_cache.seen(...):`` rename
    inverts the guard and answers the FIRST POST "duplicate"). See the
    weaken/revert RED evidence captured during implementation (this test
    unmodified, main.py's guard temporarily inverted, reverted) for the
    live demonstration of this exact trap.
    """
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
# T3 -- concurrent same-key double-POST: both append (R-1, accepted)
# ---------------------------------------------------------------------------


async def test_t3_concurrent_same_key_double_post_both_append(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Hold POST #1 inside a patched ``append`` until POST #2 has passed its
    own ``seen()`` check, then release; gather both.

    Both must return 202 "queued", both must append (len == 2), and
    NEITHER may be answered "duplicate" -- this pins C6/R-1 (the accepted
    tradeoff: two same-key POSTs that overlap inside the check->store
    window both append; the duplicate converges downstream via idempotent
    MERGE). P4: this is realizable ONLY because ``append`` is
    monkeypatched, bypassing the real per-key admission lock that would
    otherwise serialize two same-key POSTs and remove the interleave --
    do not "fix" this test toward the real queue manager.

    Pre-fix (current code): POST #1's ``check_and_store`` runs to
    completion -- store included -- entirely BEFORE append is ever called
    (no await between check and store in the old synchronous method), so by
    the time POST #2 reaches its check the key is already burned. POST #2
    returns "duplicate" immediately without ever calling append, and
    ``release`` (only set by the second ``append`` call) is never set --
    POST #1 hangs forever inside ``append``. Bounded via
    ``asyncio.wait_for`` so this shows up as a clean RED (timeout), not an
    unbounded hang.
    """
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
# T4 -- reservation leak: N/A by construction (documented, not skipped)
# ---------------------------------------------------------------------------


def test_t4_reservation_leak_not_applicable_by_construction() -> None:
    """T4 (acceptance (d)): N/A by construction.

    The chosen design (store-on-success) creates no
    reservation state -- there is nothing analogous to
    ``check_and_reserve``, so there is no reservation that can leak and no
    key that can be permanently blocked by a lost ``release``. The adjacent
    real property IS covered elsewhere: T1b proves a failed attempt leaves
    zero cache state, and T1 proves the next attempt is a fresh, unblocked
    one. Recorded here as an honest N/A -- not a skipped requirement.
    """
    assert True  # documentation-only; see docstring


# ---------------------------------------------------------------------------
# T5 -- replay path unaffected (regression)
# ---------------------------------------------------------------------------


async def test_t5_replay_never_stores_key_non_replay_still_honoured(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``?replay=true`` twice with key K -> both "queued", 2 appends; then a
    NON-replay POST with K -> "queued" (not "duplicate"), proving replay
    never stored K. (tests/test_main.py:126 covers the single-replay case
    and must pass UNMODIFIED alongside this file -- verified separately.)
    """
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
# T6 -- EventIdempotencyCache.seen/store unit behaviour
# ---------------------------------------------------------------------------


class TestEventIdempotencyCacheSeenStore:
    """Direct unit coverage of the new seen()/store() split. New surface --
    RED by absence on current (pre-fix) code."""

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
# T7 -- no stale check_and_store references anywhere in the repo
# ---------------------------------------------------------------------------


def test_t7_no_stale_check_and_store_references() -> None:
    """C7: check_and_store is removed and must have no remaining CALL SITES
    or DEFINITIONS anywhere in the repo (product code, tests, scripts).

    Spec ambiguity note: the spec's own VERBATIM docstring text for the
    new ``seen()`` method explains the rename by name-dropping the old
    method in prose (e.g. "the former ``check_and_store``, byte for byte");
    that same docstring text is C7/S5's own authoritative code hunk, so a
    bare substring scan for "check_and_store" would flag the spec's own
    required docstring as a violation of its own success criterion.
    Resolved by scoping this check to the CALL/DEF form
    ``check_and_store(`` (the name immediately followed by an open paren)
    -- which is what "no remaining references" substantively guards against
    (dead code, live call sites) -- rather than banning the bare identifier
    in explanatory prose. This file itself is excluded from the scan (it
    discusses the removal in prose without that call-form).
    """
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
