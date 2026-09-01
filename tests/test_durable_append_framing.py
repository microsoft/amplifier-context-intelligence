"""Durable append-log framing: QueueManager serializes every write to a
session's files through a per-key ``_KeyGuard``, so concurrent or
split (SMB-style short) writes can never merge or tear a record.
``_SplitWriteOS`` models a non-atomic filesystem by splitting each
``os.write`` into multiple short writes against the real fd.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from context_intelligence_server import queue_manager as qm_module
from context_intelligence_server.queue_manager import QueueManager, _KeyGuard
from context_intelligence_server.registry import SessionRegistry

pytestmark = pytest.mark.integration

SESSION = "71afde0c-f061-4f4c-b124-9323f9d5b110"
WORKSPACE = "-Users-samule-repo-team-pulse-structure"

# Bounded wait for cross-thread handshakes (never a bare sleep); exists so a
# broken handshake fails loud instead of hanging.
_HANDSHAKE_TIMEOUT_S = 10.0

# Models one SMB write op; a small value keeps tests fast while still
# forcing multiple storage ops per logical write.
_SMB_OP_BYTES = 64 * 1024


def _event_bytes(event: str, *, filler: int = 0) -> bytes:
    """One event line, JSON-encoded; `filler` pads the payload to make a large record."""
    obj: dict[str, Any] = {
        "event": event,
        "workspace": WORKSPACE,
        "data": {"session_id": SESSION, "payload": "x" * filler},
        "created_by": "samueljklee",
    }
    return json.dumps(obj, separators=(",", ":")).encode("utf-8")


def _parses(raw: bytes) -> bool:
    """True iff the real drain-side parser accepts this line."""
    try:
        SessionRegistry._parse_line(raw)
    except Exception:  # noqa: BLE001 - mirrors the real parser's own broad catch
        return False
    return True


async def _poll_until(
    pred: Any, timeout: float = _HANDSHAKE_TIMEOUT_S, interval: float = 0.005
) -> None:
    """Poll a predicate until true, or fail loud on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s")


class _SplitWriteOS:
    """Splits each os.write call into multiple short writes against the
    real fd, modelling a non-atomic filesystem (e.g. SMB). ``on_chunk``
    fires after each chunk lands, for deterministic test handshakes.
    """

    def __init__(
        self, real_write: Any, *, chunk: int = _SMB_OP_BYTES, on_chunk: Any = None
    ) -> None:
        self._real_write = real_write
        self._chunk = chunk
        self._on_chunk = on_chunk
        self._counts: dict[int, int] = {}
        self._lock = threading.Lock()

    def __call__(self, fd: int, data: Any) -> int:
        buf = bytes(data)
        to_write = buf[: self._chunk] if len(buf) > self._chunk else buf
        n = self._real_write(fd, to_write)
        with self._lock:
            idx = self._counts.get(fd, 0)
            self._counts[fd] = idx + 1
        if self._on_chunk is not None:
            self._on_chunk(fd, idx, to_write[:n], len(buf))
        return n


# ---------------------------------------------------------------------------
# CONTROL -- baseline, real filesystem, no shim
# ---------------------------------------------------------------------------


async def test_control_local_o_append_is_atomic(tmp_path: Path) -> None:
    """BASELINE: on a local filesystem O_APPEND happens to be atomic, so
    every line parses even without the guard doing any work. Passing alone
    is not proof the guard works -- see test_smb_split_write_no_longer_merges_records.
    """
    qm = QueueManager(tmp_path)

    records: list[bytes] = []
    for i in range(12):
        records.append(_event_bytes(f"llm:request:{i}", filler=300 * 1024))
        records.append(_event_bytes(f"tool:call:{i}a"))
        records.append(_event_bytes(f"tool:call:{i}b"))

    await asyncio.gather(*(qm.append(SESSION, r) for r in records))

    batch = await qm.read_batch(SESSION, max_items=1000)

    assert len(batch.lines) == len(records), (
        f"expected {len(records)} complete lines, got {len(batch.lines)}"
    )
    bad = [i for i, ln in enumerate(batch.lines) if not _parses(ln)]
    assert not bad, f"lines failed _parse_line on a LOCAL filesystem: {bad}"
    assert sorted(batch.lines) == sorted(records)


# ---------------------------------------------------------------------------
# CONTROL: the shim tears without the gate -- isolates the guard from luck
# ---------------------------------------------------------------------------


def _raw_write_no_lock(path: Path, line: bytes) -> None:
    """Unguarded append: open O_APPEND, write, close -- no serialization."""
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_BINARY", 0)
    fd = os.open(path, flags, 0o644)
    try:
        view = memoryview(line)
        written = 0
        while written < len(view):
            n = os.write(fd, view[written:])
            if n == 0:
                raise OSError("os.write returned 0; refusing to spin")
            written += n
    finally:
        os.close(fd)


def test_smb_shim_tears_WITHOUT_the_gate_control(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CONTROL: without the guard's serialization, two threads writing to
    the same file via the split-write shim interleave and produce a line
    that fails to parse -- proving the shim actually models non-atomic
    append (and that the guard, not luck, is what fixes it next door).
    """
    real_write = os.write
    large_first_op = threading.Event()
    small_landed = threading.Event()

    def _on_chunk(fd: int, idx: int, chunk: bytes, total_len: int) -> None:
        if idx == 0 and total_len > _SMB_OP_BYTES:
            large_first_op.set()
            assert small_landed.wait(_HANDSHAKE_TIMEOUT_S), (
                "handshake failed: the small write never landed"
            )

    monkeypatch.setattr(
        qm_module.os, "write", _SplitWriteOS(real_write, on_chunk=_on_chunk)
    )

    log_path = tmp_path / "control-no-gate-key.log"
    record_large = _event_bytes("llm:request", filler=300 * 1024)
    record_small = _event_bytes("llm:stream_block_start")
    line_large = record_large if record_large.endswith(b"\n") else record_large + b"\n"
    line_small = record_small if record_small.endswith(b"\n") else record_small + b"\n"
    assert len(line_large) > _SMB_OP_BYTES

    def _write_large() -> None:
        _raw_write_no_lock(log_path, line_large)

    def _write_small() -> None:
        assert large_first_op.wait(_HANDSHAKE_TIMEOUT_S), (
            "the large writer's first sub-op never landed"
        )
        _raw_write_no_lock(log_path, line_small)
        small_landed.set()

    t_large = threading.Thread(target=_write_large)
    t_small = threading.Thread(target=_write_small)
    t_large.start()
    t_small.start()
    t_large.join(_HANDSHAKE_TIMEOUT_S)
    t_small.join(_HANDSHAKE_TIMEOUT_S)
    assert not t_large.is_alive() and not t_small.is_alive(), (
        "a writer thread failed to finish -- the handshake deadlocked"
    )

    raw_log = log_path.read_bytes()
    assert raw_log != line_large + line_small, (
        "control is VACUOUS: bytes landed byte-exact/sequential -- the "
        "forced handshake did not actually interleave the two writes"
    )

    physical_lines = raw_log.split(b"\n")
    if physical_lines and physical_lines[-1] == b"":
        physical_lines = physical_lines[:-1]
    bad = [ln for ln in physical_lines if not _parses(ln)]
    assert bad, (
        "expected at least one merged/torn physical line to FAIL "
        "_parse_line when nothing serializes the two writers -- if this "
        "assertion fails, the shim is not modelling non-atomic append, "
        "which would make T2's green PASS vacuous"
    )


# ---------------------------------------------------------------------------
# the inverted repro: concurrent write under contention
# ---------------------------------------------------------------------------


async def test_smb_split_write_no_longer_merges_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Framing holds under genuine contention: parks a large write mid-record
    via the shim while a second appender for the same key waits on the
    guard; both lines land whole, in order, and parse cleanly.
    """
    qm = QueueManager(tmp_path)
    real_write = os.write

    large_first_op = threading.Event()
    small_landed = threading.Event()

    def _on_chunk(fd: int, idx: int, chunk: bytes, total_len: int) -> None:
        if idx == 0 and total_len > _SMB_OP_BYTES:
            large_first_op.set()
            assert small_landed.wait(_HANDSHAKE_TIMEOUT_S), (
                "handshake failed: the small append never landed"
            )

    monkeypatch.setattr(
        qm_module.os, "write", _SplitWriteOS(real_write, on_chunk=_on_chunk)
    )

    large = _event_bytes("llm:request", filler=300 * 1024)
    small = _event_bytes("llm:stream_block_start")
    assert len(large) > _SMB_OP_BYTES

    async def _append_large() -> None:
        await qm.append(SESSION, large)

    async def _append_small() -> None:
        await asyncio.to_thread(
            lambda: large_first_op.wait(_HANDSHAKE_TIMEOUT_S) or None
        )
        guard = qm._guards[SESSION]
        assert guard.admission.locked()
        task = asyncio.ensure_future(qm.append(SESSION, small))
        await _poll_until(lambda: guard.waiters == 2)
        small_landed.set()
        await task

    await asyncio.gather(_append_large(), _append_small())

    raw_log = (tmp_path / f"{SESSION}.log").read_bytes()
    assert raw_log == large + b"\n" + small + b"\n", (
        "expected byte-exact [large]\\n[small]\\n -- no merged/torn line"
    )

    batch = await qm.read_batch(SESSION, max_items=100)
    assert len(batch.lines) == 2, f"expected 2 lines, got {len(batch.lines)}"
    assert _parses(batch.lines[0]) and _parses(batch.lines[1])


# ---------------------------------------------------------------------------
# concurrent appends over both key shapes, all parse
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("key", [SESSION, "_nosession_-workspace-abc"])
async def test_concurrent_appends_under_smb_shim_all_parse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, key: str
) -> None:
    qm = QueueManager(tmp_path)
    monkeypatch.setattr(qm_module.os, "write", _SplitWriteOS(os.write))

    records: list[bytes] = []
    for i in range(12):
        records.append(_event_bytes(f"llm:request:{i}", filler=300 * 1024))
    for i in range(24):
        records.append(_event_bytes(f"tool:call:{i}"))

    # precondition: no raw newline anywhere except the trailing terminator
    for r in records:
        stored = r if r.endswith(b"\n") else r + b"\n"
        assert b"\n" not in stored[:-1], f"P1 violated by record: {r[:80]!r}"

    await asyncio.gather(*(qm.append(key, r) for r in records))

    batch = await qm.read_batch(key, max_items=1000)
    assert len(batch.lines) == len(records)
    bad = [i for i, ln in enumerate(batch.lines) if not _parses(ln)]
    assert not bad, f"lines failed _parse_line: {bad}"
    assert sorted(batch.lines) == sorted(records)


# ---------------------------------------------------------------------------
# cancellation cannot reintroduce the tear
# ---------------------------------------------------------------------------


async def test_cancel_mid_write_never_releases_the_file_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    qm = QueueManager(tmp_path)
    key = SESSION

    entered_write = threading.Event()
    proceed = threading.Event()
    ops: list[tuple[str, int]] = []
    ops_lock = threading.Lock()

    record_a = _event_bytes("llm:request:A", filler=300 * 1024)
    record_b = _event_bytes("tool:call:B")
    # exact on-disk bytes (append() adds the trailing newline), used below to
    # identify which record a given short chunk belongs to
    line_a = record_a if record_a.endswith(b"\n") else record_a + b"\n"
    line_b = record_b if record_b.endswith(b"\n") else record_b + b"\n"
    real_write = os.write

    def _on_chunk(fd: int, idx: int, chunk: bytes, total_len: int) -> None:
        label = (
            "A"
            if chunk and line_a.startswith(chunk)
            else ("B" if chunk and line_b.startswith(chunk) else "?")
        )
        with ops_lock:
            ops.append((label, len(chunk)))
        if idx == 0 and total_len > _SMB_OP_BYTES:
            entered_write.set()
            assert proceed.wait(_HANDSHAKE_TIMEOUT_S), "proceed handshake timed out"

    monkeypatch.setattr(
        qm_module.os, "write", _SplitWriteOS(real_write, on_chunk=_on_chunk)
    )

    task_a = asyncio.ensure_future(qm.append(key, record_a))
    await asyncio.to_thread(lambda: entered_write.wait(_HANDSHAKE_TIMEOUT_S) or None)

    guard = qm._guards[key]
    assert guard.file_lock.locked(), (
        "file_lock must be held while A's write is mid-flight"
    )

    task_a.cancel()

    # dispatch B now -- it must not reach os.write while A's thread holds file_lock
    task_b = asyncio.ensure_future(qm.append(key, record_b))
    await asyncio.sleep(0.05)
    assert guard.file_lock.locked(), (
        "file_lock unexpectedly released before A's write finished"
    )
    assert all(label != "B" for label, _ in ops), (
        "B's bytes landed before A released file_lock"
    )

    proceed.set()  # let A's write finish

    a_landed_before_cancel_observed = False
    with pytest.raises(asyncio.CancelledError):
        try:
            await task_a
        except asyncio.CancelledError:
            # A's bytes must already be on disk at the moment cancellation is observed here
            current = (tmp_path / f"{key}.log").read_bytes()
            a_landed_before_cancel_observed = current.startswith(record_a + b"\n")
            raise

    assert a_landed_before_cancel_observed, (
        "CancelledError observed before A's bytes landed"
    )

    await task_b

    # (c) both records whole, in order, and _parse_line-clean.
    raw_log = (tmp_path / f"{key}.log").read_bytes()
    assert raw_log == record_a + b"\n" + record_b + b"\n"
    batch = await qm.read_batch(key, max_items=10)
    assert len(batch.lines) == 2
    assert _parses(batch.lines[0]) and _parses(batch.lines[1])

    # No interleaving: every A-chunk fully precedes every B-chunk.
    labels = [label for label, _ in ops]
    last_a = max(i for i, lbl in enumerate(labels) if lbl == "A")
    first_b = min(i for i, lbl in enumerate(labels) if lbl == "B")
    assert last_a < first_b, f"ops interleaved: {labels}"


# ---------------------------------------------------------------------------
# distinct keys append concurrently -- per-key parallelism preserved
# ---------------------------------------------------------------------------


async def test_distinct_keys_append_concurrently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    qm = QueueManager(tmp_path)
    key_a = "key-a"
    key_b = "key-b"

    entered = threading.Event()
    proceed = threading.Event()

    def _on_chunk(fd: int, idx: int, chunk: bytes, total_len: int) -> None:
        if idx == 0 and total_len > _SMB_OP_BYTES:
            entered.set()
            assert proceed.wait(_HANDSHAKE_TIMEOUT_S)

    monkeypatch.setattr(
        qm_module.os, "write", _SplitWriteOS(os.write, on_chunk=_on_chunk)
    )

    large_a = _event_bytes("large-a", filler=300 * 1024)
    small_b = _event_bytes("small-b")

    task_a = asyncio.ensure_future(qm.append(key_a, large_a))
    await asyncio.to_thread(lambda: entered.wait(_HANDSHAKE_TIMEOUT_S) or None)

    # key B is a DIFFERENT key -- must complete while A is still parked.
    await qm.append(key_b, small_b)
    batch_b = await qm.read_batch(key_b, max_items=10)
    assert batch_b.lines == [small_b]

    proceed.set()
    await task_a
    batch_a = await qm.read_batch(key_a, max_items=10)
    assert batch_a.lines == [large_a]


# ---------------------------------------------------------------------------
# partial write failure discards the record; failure is loud
# ---------------------------------------------------------------------------


async def test_partial_write_failure_discards_the_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    qm = QueueManager(tmp_path)
    key = "fail-key"
    real_write = os.write
    calls = {"n": 0}

    def _flaky_write(fd: int, data: Any) -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            return real_write(fd, bytes(data)[:8])
        if calls["n"] == 2:
            raise OSError("simulated append failure")
        return real_write(fd, bytes(data))  # rollback's newline write succeeds

    monkeypatch.setattr(qm_module.os, "write", _flaky_write)

    record = _event_bytes("will-fail")
    with pytest.raises(OSError):
        await qm.append(key, record)

    # Fragment is newline-terminated, never truncated -- the queue never
    # removes bytes it already wrote.
    assert qm._log_path(key).read_bytes() == record[:8] + b"\n"

    monkeypatch.setattr(qm_module.os, "write", real_write)
    good = _event_bytes("will-succeed")
    await qm.append(key, good)
    batch = await qm.read_batch(key, max_items=10)
    assert batch.lines == [record[:8], good]
    assert not _parses(batch.lines[0]), "malformed fragment must not parse"
    assert _parses(batch.lines[1])


async def test_partial_write_failure_logs_when_newline_terminate_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    qm = QueueManager(tmp_path)
    key = "fail-key-2"
    real_write = os.write
    calls = {"n": 0}

    def _flaky_write(fd: int, data: Any) -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            return real_write(fd, bytes(data)[:8])
        raise OSError("simulated write failure")

    monkeypatch.setattr(qm_module.os, "write", _flaky_write)

    record = _event_bytes("will-fail-hard")
    with (
        caplog.at_level(
            logging.ERROR, logger="context_intelligence_server.queue_manager"
        ),
        pytest.raises(OSError),
    ):
        await qm.append(key, record)

    errors = [r.message for r in caplog.records if r.levelno >= logging.ERROR]
    assert any("append_partial_terminate_failed" in m for m in errors), (
        "newline-terminate failure must be logged at ERROR"
    )

    # Torn tail left untouched -- queue bytes are never removed. Readers skip an
    # unterminated trailing fragment; the next append merges it into a single
    # poison line that the drainer dead-letters.
    assert qm._log_path(key).read_bytes() == record[:8]


# _discard_partial never truncates: a peer writer's committed line and this
# writer's own prior records survive a rollback.


async def test_discard_partial_never_destroys_a_peer_process_committed_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two independent processes append to the same file (writer-lease is
    detect-only -- see _write_record's own docstring, no cross-process lock).
    While this writer's partial write is rolled back, a peer process
    completes and closes its own fully-formed, already-acknowledged record.
    _discard_partial must never remove those bytes.
    """
    qm = QueueManager(tmp_path)
    key = "race-key"
    path = qm._log_path(key)
    path.write_bytes(b'{"payload":"PRIOR-COMMITTED"}\n')

    peer_can_go = threading.Event()
    peer_done = threading.Event()
    real_write = os.write

    def _flaky_write(fd: int, data: Any) -> int:
        buf = bytes(data)
        real_write(fd, buf[: len(buf) // 2])
        peer_can_go.set()
        assert peer_done.wait(_HANDSHAKE_TIMEOUT_S)
        raise OSError("simulated mid-record failure")

    def _peer_append() -> None:
        assert peer_can_go.wait(_HANDSHAKE_TIMEOUT_S)
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
        fd = os.open(path, flags, 0o644)
        try:
            real_write(fd, b'{"payload":"PEER-COMMITTED"}\n')
        finally:
            os.close(fd)
        peer_done.set()

    peer = threading.Thread(target=_peer_append)
    peer.start()

    monkeypatch.setattr(qm_module.os, "write", _flaky_write)
    record = _event_bytes("mine", filler=5000)
    with pytest.raises(OSError):
        await qm.append(key, record)
    monkeypatch.setattr(qm_module.os, "write", real_write)

    peer.join(_HANDSHAKE_TIMEOUT_S)
    assert not peer.is_alive()

    final = path.read_bytes()
    assert b"PEER-COMMITTED" in final, (
        "a peer's already-acknowledged line must never be destroyed"
    )
    assert b"PRIOR-COMMITTED" in final, (
        "pre-existing committed data must never be destroyed"
    )


async def test_discard_partial_single_writer_preserves_prior_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Single-writer case: a partial write rolled back leaves prior COMPLETE
    records intact and the fragment newline-terminated (not merged into the
    next record); a subsequent drain dead-letters the fragment rather than
    crashing.
    """
    qm = QueueManager(tmp_path)
    key = "single-writer-key"
    prior = _event_bytes("prior-committed")
    await qm.append(key, prior)

    real_write = os.write
    calls = {"n": 0}

    def _flaky_write(fd: int, data: Any) -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            return real_write(fd, bytes(data)[:8])
        if calls["n"] == 2:
            raise OSError("simulated append failure")
        return real_write(fd, bytes(data))  # rollback's newline write succeeds

    monkeypatch.setattr(qm_module.os, "write", _flaky_write)
    record = _event_bytes("torn-fragment")
    with pytest.raises(OSError):
        await qm.append(key, record)
    monkeypatch.setattr(qm_module.os, "write", real_write)

    good = _event_bytes("after-recovery")
    await qm.append(key, good)

    batch = await qm.read_batch(key, max_items=10)
    assert batch.lines == [prior, record[:8], good], (
        "no committed record lost; fragment isolated on its own line"
    )
    assert _parses(batch.lines[0])
    assert not _parses(batch.lines[1]), (
        "malformed fragment is dead-lettered, not crashed on"
    )
    assert _parses(batch.lines[2])


# ---------------------------------------------------------------------------
# delete_drained cannot race append; retains on uncommitted bytes
# ---------------------------------------------------------------------------


async def test_delete_drained_cannot_race_an_in_flight_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    qm = QueueManager(tmp_path)
    key = "race-delete-key"

    entered = threading.Event()
    proceed = threading.Event()

    def _on_chunk(fd: int, idx: int, chunk: bytes, total_len: int) -> None:
        if idx == 0 and total_len > _SMB_OP_BYTES:
            entered.set()
            assert proceed.wait(_HANDSHAKE_TIMEOUT_S)

    monkeypatch.setattr(
        qm_module.os, "write", _SplitWriteOS(os.write, on_chunk=_on_chunk)
    )

    record = _event_bytes("in-flight", filler=300 * 1024)
    task_append = asyncio.ensure_future(qm.append(key, record))
    await asyncio.to_thread(lambda: entered.wait(_HANDSHAKE_TIMEOUT_S) or None)

    guard = qm._guards[key]
    assert guard.admission.locked()

    delete_task = asyncio.ensure_future(qm.delete_drained(key))
    await _poll_until(lambda: guard.waiters == 2)  # append(1) + delete parked(1)

    proceed.set()
    await task_append
    ok = await delete_task

    # nothing committed yet, so delete_drained must retain the record fully
    # -- admission serialization blocks delete until append releases file_lock
    assert ok is False
    assert qm._log_path(key).exists()
    batch = await qm.read_batch(key, max_items=10)
    assert batch.lines == [record]
    assert _parses(batch.lines[0])


async def test_delete_drained_retains_a_log_with_uncommitted_bytes(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    qm = QueueManager(tmp_path)
    key = "uncommitted-key"
    first = _event_bytes("first")
    second = _event_bytes("second")
    await qm.append(key, first)
    batch = await qm.read_batch(key, max_items=10)
    await qm.commit(key, batch.end_offset)  # commits only `first`
    await qm.append(key, second)  # uncommitted tail

    with caplog.at_level(
        logging.WARNING, logger="context_intelligence_server.queue_manager"
    ):
        ok = await qm.delete_drained(key)

    assert ok is False
    assert qm._log_path(key).exists()
    assert qm._offset_path(key).exists()
    assert any("delete_drained_retained" in r.message for r in caplog.records)

    recoverable = await qm.recover()
    assert key in recoverable


# ---------------------------------------------------------------------------
# guard map is bounded and ABA-proof
# ---------------------------------------------------------------------------


async def test_guard_map_is_released_on_delete_drained_and_identity_checked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    qm = QueueManager(tmp_path)
    keys = [f"session-{i}" for i in range(5)]
    for k in keys:
        await qm.append(k, _event_bytes("ev"))
        batch = await qm.read_batch(k, max_items=10)
        await qm.commit(k, batch.end_offset)
    assert set(qm._guards.keys()) == set(keys)

    for k in keys:
        assert await qm.delete_drained(k) is True
    assert qm._guards == {}

    # ABA probe: swap the guard map entry for a foreign object while
    # delete_drained is mid-flight; its identity check must refuse to remove it
    key = "aba-key"
    await qm.append(key, _event_bytes("ev"))
    batch = await qm.read_batch(key, max_items=10)
    await qm.commit(key, batch.end_offset)
    g_orig = qm._guards[key]

    real_stat = Path.stat
    entered = threading.Event()
    proceed = threading.Event()

    def _paused_stat(self: Path, *a: Any, **kw: Any) -> Any:
        if self == qm._log_path(key):
            entered.set()
            assert proceed.wait(_HANDSHAKE_TIMEOUT_S)
        return real_stat(self, *a, **kw)

    monkeypatch.setattr(Path, "stat", _paused_stat)

    task = asyncio.ensure_future(qm.delete_drained(key))
    await asyncio.to_thread(lambda: entered.wait(_HANDSHAKE_TIMEOUT_S) or None)

    foreign = _KeyGuard(asyncio.Lock(), threading.Lock())
    qm._guards[key] = foreign  # simulate a swap while delete_drained is mid-flight

    proceed.set()
    ok = await task

    assert ok is True  # the unlink itself still completed against g_orig
    assert qm._guards.get(key) is foreign, (
        "foreign entry must survive the identity check"
    )
    assert g_orig is not foreign


# ---------------------------------------------------------------------------
# dead-letter path is covered and un-crashable
# ---------------------------------------------------------------------------


async def test_dead_letter_record_is_framed_under_smb_shim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    qm = QueueManager(tmp_path)
    key = "dl-key"
    monkeypatch.setattr(qm_module.os, "write", _SplitWriteOS(os.write))
    large_raw = _event_bytes("bad:record", filler=300 * 1024)
    await qm.dead_letter(key, large_raw, "simulated parse error")
    records = await qm.read_dead_letters(key)
    assert len(records) == 1
    assert records[0]["error"] == "simulated parse error"
    assert records[0]["payload"] == large_raw.decode("utf-8")


async def test_dead_letter_parsing_survives_a_malformed_line(tmp_path: Path) -> None:
    qm = QueueManager(tmp_path)
    key = "dl-malformed-key"
    dead_path = qm._dead_path(key)
    good = json.dumps({"ts": 1.0, "error": "e", "payload": "ok"})
    dead_path.write_text(good + "\n" + "{not json" + "\n", encoding="utf-8")

    records = await qm.read_dead_letters(key)  # must not raise
    assert len(records) == 1
    assert records[0]["payload"] == "ok"

    payload_set = qm._dead_payload_set(key)  # must not raise
    assert payload_set == {b"ok"}


# ---------------------------------------------------------------------------
# guard survives a delete racing a parked appender
# ---------------------------------------------------------------------------


async def test_guard_survives_a_delete_that_races_a_parked_appender(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """delete_drained racing a parked appender: the guard map entry must
    survive while appender A still holds a reference, so a subsequent
    appender B is served by the SAME guard rather than a disjoint one.
    """
    qm = QueueManager(tmp_path)
    key = "race-key"

    seed = _event_bytes("seed")
    await qm.append(key, seed)
    seed_batch = await qm.read_batch(key, max_items=10)
    await qm.commit(key, seed_batch.end_offset)  # fully drained: size == committed
    guard = qm._guards[key]

    real_stat = Path.stat
    entered_delete = threading.Event()
    proceed_delete = threading.Event()

    def _paused_stat(self: Path, *a: Any, **kw: Any) -> Any:
        if self == qm._log_path(key):
            entered_delete.set()
            assert proceed_delete.wait(_HANDSHAKE_TIMEOUT_S), (
                "delete handshake timed out"
            )
        return real_stat(self, *a, **kw)

    monkeypatch.setattr(Path, "stat", _paused_stat)

    delete_task = asyncio.ensure_future(qm.delete_drained(key))
    await asyncio.to_thread(lambda: entered_delete.wait(_HANDSHAKE_TIMEOUT_S) or None)
    assert qm._guards.get(key) is guard  # not yet removed -- delete hasn't returned

    append_a = _event_bytes("A")
    task_a = asyncio.ensure_future(qm.append(key, append_a))
    await _poll_until(lambda: guard.waiters == 2)  # delete(1, itself) + A parked(1)
    assert guard.admission.locked()

    monkeypatch.undo()  # restore Path.stat before it resumes for real
    proceed_delete.set()

    ok = await delete_task
    assert ok is True  # size == committed -> genuinely drained -> unlink succeeds
    await task_a

    append_b = _event_bytes("B")
    await qm.append(key, append_b)

    assert qm._guards.get(key) is guard, (
        "B must be served by the SAME _KeyGuard object A used -- if the "
        "guard was discarded while A held a reference (v2's identity-only "
        "removal condition), B gets a fresh guard with a disjoint "
        "file_lock, which reproduces the same torn/merged-line append corruption"
    )

    raw = qm._log_path(key).read_bytes()
    assert raw == append_a + b"\n" + append_b + b"\n"
    batch = await qm.read_batch(key, max_items=10)
    assert len(batch.lines) == 2
    assert _parses(batch.lines[0]) and _parses(batch.lines[1])


# ---------------------------------------------------------------------------
# captured production artifacts
# ---------------------------------------------------------------------------


def _seed_dir() -> Path | None:
    """Locate captured dead-letter seed files (kept outside this repo);
    walk upward so the lookup survives any checkout depth.
    """
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "docs" / "04-deadletter-artifacts" / "seeds"
        if candidate.is_dir():
            return candidate
    return None


_SEEDS = _seed_dir()
_requires_seeds = pytest.mark.skipif(
    _SEEDS is None,
    reason="captured dead-letter seeds not present",
)


@_requires_seeds
async def test_read_batch_over_a_pre_existing_merged_middle_line(
    tmp_path: Path,
) -> None:
    """A pre-existing merged middle line still fails _parse_line -- consuming
    it (dead-letter, commit past it) is the drainer's job, not this fix's.
    """
    qm = QueueManager(tmp_path)
    key = "merged-middle-key"
    assert _SEEDS is not None
    merged = (_SEEDS / "seed_corrupt_merged_line_1.0MiB.raw").read_bytes()
    good_before = _event_bytes("before")
    good_after = _event_bytes("after")
    log_path = qm._log_path(key)
    log_path.write_bytes(good_before + b"\n" + merged + b"\n" + good_after + b"\n")

    batch = await qm.read_batch(key, max_items=10)
    assert len(batch.lines) == 3
    assert batch.lines[0] == good_before
    assert batch.lines[1] == merged
    assert batch.lines[2] == good_after
    assert not _parses(merged), (
        "the framing fix does not claim to fix a pre-existing merged middle line -- the drainer consumes it"
    )
    assert _parses(good_before) and _parses(good_after)


@_requires_seeds
def test_captured_corrupt_seed_is_rejected_by_the_real_parser() -> None:
    """A real captured corrupt production line still fails _parse_line, at
    the same offset and with the same error text as the original dead-letter.
    """
    assert _SEEDS is not None
    raw = (_SEEDS / "seed_corrupt_merged_line_1.0MiB.raw").read_bytes()

    # one physical line containing two merged records, no separator between them
    assert raw.count(b"\n") == 0, "seed is a single physical line by construction"
    starts = [i for i in range(len(raw)) if raw.startswith(b'{"event":', i)]
    assert len(starts) == 2, f"expected two merged records, found {len(starts)}"
    assert starts[0] == 0
    boundary = starts[1]
    assert raw[boundary - 1 : boundary] != b"\n", (
        "record B is preceded by a newline -- that would be normal framing, not a tear"
    )

    with pytest.raises(json.JSONDecodeError) as exc:
        SessionRegistry._parse_line(raw)

    # parse fails near the merge boundary; here A tore inside a quoted string
    # so B's leading bytes are swallowed, breaking a couple bytes later
    assert boundary <= exc.value.pos <= boundary + 8, (
        f"parse failed at {exc.value.pos}, merge boundary is {boundary}"
    )
    assert exc.value.pos == 1050641, "matches the recorded dead-letter error offset"
    assert "Expecting ',' delimiter" in str(exc.value), (
        "matches the recorded dead-letter error text"
    )


@_requires_seeds
def test_captured_valid_large_event_parses_cleanly() -> None:
    """A real large event of similar size parses fine -- isolating the
    defect to framing, not payload size.
    """
    assert _SEEDS is not None
    raw = (_SEEDS / "seed_valid_large_event_1.04MiB.json").read_bytes()
    assert len(raw) > 1024 * 1024
    assert raw.count(b"\n") == 0

    event, workspace, data = SessionRegistry._parse_line(raw)
    assert event
    assert workspace
    assert isinstance(data, dict)
