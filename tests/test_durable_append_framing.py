"""Tier-1 fix test matrix for the durable append-log framing fix.

WRITE-SIDE FRAMING CORRUPTION -- THE FIX
-----------------------------------------
The durable per-session append-log used to depend on an UNSTATED filesystem
guarantee (POSIX ``O_APPEND`` write atomicity) that Azure Files/SMB does not
provide. ``QueueManager.append`` now serializes every write to one worker
key's files through a ``_KeyGuard`` whose ``file_lock`` (a ``threading.Lock``)
is acquired and released BY THE WORKER THREAD that performs the write --
never by an event-loop event, cancellation included. Correctness no longer
depends on any filesystem write-atomicity guarantee; ``O_APPEND`` is kept
only as defence in depth.

THIS FILE PINS
--------------
- ``test_control_local_o_append_is_atomic`` (T1) -- BASELINE. Passes on a
  local filesystem where ``O_APPEND`` happens to be atomic. Its passing is
  NOT evidence the code is correct -- ``test_smb_split_write_no_longer_
  merges_records`` (T2) is what proves the code itself supplies the
  guarantee, independent of the filesystem.
- T2-T18 exercise the guard, the write
  mechanism, cancellation, the torn-tail heal, dead-letter framing, and the
  v2.1 G1 fix (guard-removal-races-a-parked-appender) directly.
- ``test_captured_corrupt_seed_*`` -- the REAL production artifact fed to
  the REAL ``_parse_line``, isolating the defect to FRAMING, not payload
  size.

THE SMB MODEL
-------------
``_SplitWriteOS`` patches ``queue_manager.os.write`` (the actual write
primitive used by ``_write_record``/``_write_all``) to split any write
larger than ``_SMB_OP_BYTES`` into multiple SHORT writes and return a short
count for each -- exactly what an SMB client does with a single logical
``write()`` call, and exactly what ``_write_all``'s loop exists to handle.
Real bytes are written via the real ``os.write`` to the real fd; only the
CHUNKING and SHORT-COUNT-RETURN behaviour is injected.

NO PRODUCT CODE IS MODIFIED BY THIS FILE (except the one-time, reverted
RED/GREEN demonstration of T17 against the pre-v2.1 removal condition,
which is applied and restored via a separate script -- see the
implementation report).
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

# Bounded wait for every cross-thread handshake in this file. The tests are
# gated on threading.Event objects (deterministic), never on a bare sleep; the
# timeout exists only so a broken handshake FAILS LOUD instead of hanging.
_HANDSHAKE_TIMEOUT_S = 10.0

# Models one SMB write op. Real value is negotiated (commonly ~1 MiB), but the
# only property under test is "one logical write == MORE THAN ONE storage op",
# so a small value keeps the test fast while reproducing the same signature.
_SMB_OP_BYTES = 64 * 1024


def _event_bytes(event: str, *, filler: int = 0) -> bytes:
    """One event line exactly as POST /events persists it (main.py:913-917).

    Compact separators + a ``data`` blob, matching the real wire format. The
    ``filler`` pads ``data.payload`` to make a genuinely large record.
    ``ensure_ascii`` defaults to True (json.dumps), which is precondition P1
    No raw ``0x0A`` ever appears in the serialized bytes.
    """
    obj: dict[str, Any] = {
        "event": event,
        "workspace": WORKSPACE,
        "data": {"session_id": SESSION, "payload": "x" * filler},
        "created_by": "samueljklee",
    }
    return json.dumps(obj, separators=(",", ":")).encode("utf-8")


def _parses(raw: bytes) -> bool:
    """True iff the REAL drain-side parser accepts this line."""
    try:
        SessionRegistry._parse_line(raw)
    except Exception:  # noqa: BLE001 - mirrors registry.py:472's own broad catch
        return False
    return True


async def _poll_until(
    pred: Any, timeout: float = _HANDSHAKE_TIMEOUT_S, interval: float = 0.005
) -> None:
    """Poll an in-process predicate until true, or fail loud on timeout.

    Used for cross-task synchronization points that are not gated on a
    dedicated event (e.g. "has this OTHER already-scheduled coroutine's
    synchronous prologue run yet"). Deterministic in effect: only the WALL
    TIME is a bound, not a correctness dependency -- the predicate itself
    (an in-process counter) is exact.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s")


class _SplitWriteOS:
    """Patches ``os.write`` to split any large write into SHORT writes.

    Models Azure Files/SMB: one logical ``os.write(fd, data)`` call becomes
    N separate real ``os.write`` calls of at most ``chunk`` bytes each, with
    NO atomicity spanning them -- so another writer's bytes can land in
    between (if nothing else serializes them). Each chunk is written via the
    REAL ``os.write`` against the REAL fd; the ONLY thing this shim changes
    is the GRANULARITY and the SHORT RETURN COUNT, which is exactly the
    guarantee SMB withholds and ext4 provides -- and exactly what
    ``_write_all``'s loop exists to handle.

    ``on_chunk(fd, index, chunk, total_len)`` fires AFTER each real chunk has
    landed on disk, for deterministic test handshakes. ``index`` is 0 for the
    first op on a given fd.
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
# T1: CONTROL -- baseline, real filesystem, no shim
# ---------------------------------------------------------------------------


async def test_control_local_o_append_is_atomic(tmp_path: Path) -> None:
    """Real QueueManager, many concurrent large+small appends, NO shim.

    BASELINE ONLY. This is the ``dyad`` case: on a local filesystem
    ``O_APPEND`` happens to be atomic, so every line reads back individually
    parseable even without the guard doing any work. Passing here is NOT
    evidence the CODE is correct -- ``test_smb_split_write_no_longer_
    merges_records`` (T2) is what proves the guard supplies the guarantee,
    independent of the filesystem's own atomicity.
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
# NON-VACUITY CONTROL: the shim tears WITHOUT the gate -- proves T2 is not
# vacuous, and proves the guard (not luck) is what fixes it, all locally.
# ---------------------------------------------------------------------------


def _raw_write_no_lock(path: Path, line: bytes) -> None:
    """The OLD, pre-v2.1 unguarded write: open O_APPEND, write, close.

    No ``_KeyGuard``, no ``admission`` lock, no ``file_lock`` -- this is
    exactly the write ``_write_record``/``_write_all`` perform, MINUS the
    ``with guard.file_lock:`` that serializes them. It loops on short writes
    the same way ``_write_all`` does, so it is subject to the
    ``_SplitWriteOS`` shim exactly like the real (fixed) code path is.
    """
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
    """NON-VACUITY CONTROL for T2 -- proves the shim actually tears when
    nothing serializes it, and therefore that the ``_KeyGuard`` gate (not an
    accident of the local filesystem) is what removes the tear next door in
    ``test_smb_split_write_no_longer_merges_records``.

    The real-mount smoke test (``tests/smoke/``) confirms the ENVIRONMENTAL
    premise -- that the OLD code tears on real Azure Files/SMB/NFS -- but
    that needs a live mount this dev box does not have. THIS test needs no
    mount at all: it proves the fix's LOGIC is what matters, not the
    filesystem, by injecting non-atomicity locally (``_SplitWriteOS``) and
    showing that WITHOUT serialization the exact same shim tears on plain
    ext4. Paired with T2 (gate present -> no tear), the two together are a
    self-proving local pair: the shim faithfully models non-atomic append
    (this test) AND the guard is what prevents corruption (T2) -- no real
    SMB/NFS mount is required for either half of that proof.

    Bypasses ``QueueManager.append`` and its ``_KeyGuard`` entirely: two
    threads perform the OLD pre-v2.1 write (``_raw_write_no_lock`` --
    ``open(O_APPEND)`` / loop of raw ``os.write`` / ``close``, going through
    the shimmed ``os.write`` exactly as the real code does) against the SAME
    ``.log`` file, with no admission lock and no file_lock. A deterministic
    ``threading.Event`` handshake -- never a sleep -- parks the large writer
    after its FIRST sub-op (``idx == 0``, split by the shim because it
    exceeds ``_SMB_OP_BYTES``) and releases it only after the small
    writer's COMPLETE record has landed, forcing exactly the interleave
    ``[large-chunk-0][small-whole][large-rest]`` -- one physical line
    containing fragments of two records with no separating newline between
    them.
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
# T2: the inverted repro -- headline criterion
# ---------------------------------------------------------------------------


async def test_smb_split_write_no_longer_merges_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The framing invariant holds under a genuinely contending appender.

    Parks a large (multi-op) write mid-record via the SMB shim; while it is
    parked, a second appender for the SAME key is dispatched and PROVABLY
    contends (``guard.admission.locked()`` and ``guard.waiters == 2`` --
    the non-vacuous contention assertion). Releasing the large write lets
    it complete BEFORE the small one starts (admission is a single slot),
    so the log ends up byte-exact ``large + b"\\n" + small + b"\\n"`` and
    both lines parse.
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
# T3: concurrent appends over both key shapes, all parse (+ P1 precondition)
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

    # Precondition P1: no raw newline anywhere except
    # the terminator this module appends.
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
# T4: cancellation cannot reintroduce the tear
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
    # The exact bytes _write_all actually sees (append() adds the trailing
    # newline) -- used below to identify which record a given SHORT chunk
    # belongs to. A's first chunk is a PREFIX of line_a (it's split); B's
    # single chunk equals line_b exactly.
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

    # (b) Dispatch B now -- it must not reach os.write while A's thread
    # holds file_lock.
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
            # (d) A's bytes must already be on disk at the moment the
            # cancellation is observed here.
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
# T5: distinct keys append concurrently -- per-key parallelism preserved
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
# T6/T7: heal_torn_tails truncates and quarantines
# ---------------------------------------------------------------------------


async def test_heal_torn_tails_truncates_and_quarantines(tmp_path: Path) -> None:
    qm = QueueManager(tmp_path)
    key = "torn-key"
    log_path = qm._log_path(key)
    good = b'{"event":"a"}\n'
    torn = b'{"event":"b","data":"partial-fragment-no-terminator'
    log_path.write_bytes(good + torn)

    result = await qm.heal_torn_tails()

    assert result["files_healed"] == 1
    assert result["bytes_discarded"] == len(torn)
    assert result["files_failed"] == 0
    assert log_path.read_bytes() == good, (
        "file must end exactly at the last complete line"
    )

    sidecars = list(tmp_path.glob(f"{key}.log.torn-*.bin"))
    assert len(sidecars) == 1
    assert sidecars[0].read_bytes() == torn, "quarantined bytes must be byte-identical"

    batch = await qm.read_batch(key, max_items=10)
    assert len(batch.lines) == 1
    assert batch.lines[0] == good[:-1]
    assert _parses(batch.lines[0])


async def test_heal_torn_tails_on_empty_and_newline_free_files(tmp_path: Path) -> None:
    qm = QueueManager(tmp_path)
    empty_key = "empty-key"
    nf_key = "newline-free-key"
    qm._log_path(empty_key).write_bytes(b"")
    nf_fragment = b'{"event":"no-newline-yet"'
    qm._log_path(nf_key).write_bytes(nf_fragment)

    result = await qm.heal_torn_tails()

    assert qm._log_path(empty_key).read_bytes() == b"", "0-byte file must be untouched"
    assert not list(tmp_path.glob(f"{empty_key}.log.torn-*.bin")), (
        "no sidecar for an empty file"
    )

    assert qm._log_path(nf_key).read_bytes() == b"", (
        "newline-free file truncates to empty"
    )
    sidecars = list(tmp_path.glob(f"{nf_key}.log.torn-*.bin"))
    assert len(sidecars) == 1
    assert sidecars[0].read_bytes() == nf_fragment

    assert result["files_failed"] == 0
    # A file that was never referenced simply does not appear -- no-op, no exception.
    assert not (tmp_path / "missing-key.log").exists()


# ---------------------------------------------------------------------------
# T8/T9: partial write failure discards the record; failure is loud
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
        raise OSError("simulated append failure")

    monkeypatch.setattr(qm_module.os, "write", _flaky_write)

    record = _event_bytes("will-fail")
    with pytest.raises(OSError):
        await qm.append(key, record)

    # File is byte-identical to its pre-append state (empty) -- ftruncate discard.
    assert qm._log_path(key).read_bytes() == b""

    monkeypatch.setattr(qm_module.os, "write", real_write)
    good = _event_bytes("will-succeed")
    await qm.append(key, good)
    batch = await qm.read_batch(key, max_items=10)
    assert batch.lines == [good]
    assert _parses(batch.lines[0])


async def test_partial_write_failure_logs_when_truncate_also_fails(
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
        if calls["n"] == 2:
            raise OSError("simulated append failure")
        return real_write(fd, bytes(data))  # fallback newline write succeeds

    def _flaky_truncate(fd: int, length: int) -> None:
        raise OSError("simulated truncate failure")

    monkeypatch.setattr(qm_module.os, "write", _flaky_write)
    monkeypatch.setattr(qm_module.os, "ftruncate", _flaky_truncate)

    record = _event_bytes("will-fail-hard")
    with (
        caplog.at_level(
            logging.ERROR, logger="context_intelligence_server.queue_manager"
        ),
        pytest.raises(OSError),
    ):
        await qm.append(key, record)

    errors = [r.message for r in caplog.records if r.levelno >= logging.ERROR]
    assert any("append_partial_truncate_failed" in m for m in errors), (
        "truncate failure must be logged at ERROR"
    )
    assert not any("append_partial_terminate_failed" in m for m in errors), (
        "the fallback newline write succeeded -- no second failure expected"
    )

    # The fallback newline landed: the file now holds the partial 8-byte
    # fragment terminated by a newline. This is a SYNTACTICALLY COMPLETE line
    # (not a torn tail): read_batch accepts it and heal_torn_tails does NOT
    # quarantine it. That is safe -- the append already raised OSError to the
    # caller (event never acknowledged), so on the next drain the malformed
    # JSON fails _parse_line and is correctly dead-lettered (offset advanced).
    assert qm._log_path(key).read_bytes() == record[:8] + b"\n"


# ---------------------------------------------------------------------------
# T10/T11: delete_drained cannot race append; retains on uncommitted bytes
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

    # Nothing was ever committed for this record, so delete_drained MUST
    # retain it -- never a silent disappearance -- and the record is fully
    # present, never partial: the guard's admission serialization meant
    # delete's thread could not even begin until append's thread released
    # file_lock.
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
# T12: guard map is bounded and ABA-proof
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

    # Direct ABA probe: while delete_drained(K)'s thread is mid-flight
    # (having captured guard G_orig via its own _guard() call and about to
    # unlink), swap the map entry for a foreign object. The removal's
    # identity check must then fail, and the foreign entry must survive
    # untouched.
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
# T13/T14: dead-letter path is covered and un-crashable
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
# T17: v2.1 G1 -- the reproduced counter-example, inverted
# ---------------------------------------------------------------------------


async def test_guard_survives_a_delete_that_races_a_parked_appender(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """v2.1 G1 -- delete_drained racing a parked appender.

    MUST BE RED against v2's \u00a72.2 removal condition (identity-only, no
    ``waiters`` gate): without the gate, the map entry is deleted while
    appender A is still parked holding a reference to it, so a subsequent
    appender B gets a FRESH ``_KeyGuard`` with a DISJOINT ``file_lock`` --
    the exact mechanism that reproduces the torn/merged-line append
    corruption (``scratch/probe_guard_swap.py``). GREEN against v2.1's
    ``waiters == 1``
    gate: removal is skipped while A holds a reference, so B is served by
    the SAME guard object A used.
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
# T18: heal_torn_tails cannot crash boot
# ---------------------------------------------------------------------------


async def test_heal_torn_tails_survives_an_oserror_and_still_boots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    qm = QueueManager(tmp_path)
    good = b'{"event":"ok"}\n'
    torn = b'{"event":"torn","data":"no-terminator-yet'
    for name in ("a", "b", "c"):
        qm._log_path(name).write_bytes(good + torn)

    real_os_open = os.open
    real_os_truncate = os.truncate

    def _flaky_open(
        path: Any, flags: int, mode: int = 0o777, *a: Any, **kw: Any
    ) -> int:
        if "a.log.torn-" in str(path):
            raise OSError("simulated quarantine-copy failure for a")
        return real_os_open(path, flags, mode, *a, **kw)

    def _flaky_truncate(path: Any, length: int) -> None:
        if str(path).endswith("b.log"):
            raise OSError("simulated truncate failure for b")
        return real_os_truncate(path, length)

    monkeypatch.setattr(qm_module.os, "open", _flaky_open)
    monkeypatch.setattr(qm_module.os, "truncate", _flaky_truncate)

    with caplog.at_level(
        logging.ERROR, logger="context_intelligence_server.queue_manager"
    ):
        result = await qm.heal_torn_tails()  # MUST NOT RAISE

    assert result["files_failed"] == 2
    assert result["files_healed"] == 1

    # a: quarantine-copy raised -> file left EXACTLY as seeded, never truncated.
    assert qm._log_path("a").read_bytes() == good + torn
    assert not list(tmp_path.glob("a.log.torn-*.bin"))

    # b: copy succeeded, truncate raised -> STILL left exactly as seeded
    # (never truncated on a failed truncate call).
    assert qm._log_path("b").read_bytes() == good + torn

    # c: fully healed.
    assert qm._log_path("c").read_bytes() == good
    assert list(tmp_path.glob("c.log.torn-*.bin"))

    error_msgs = [r.message for r in caplog.records if r.levelno >= logging.ERROR]
    assert sum("torn_tail_heal_failed" in m for m in error_msgs) == 2


# ---------------------------------------------------------------------------
# T15 + the REAL captured production artifacts
# ---------------------------------------------------------------------------


def _seed_dir() -> Path | None:
    """Locate the captured dead-letter seeds.

    They live in the WORKSPACE-root ``docs/`` (this repo's ``docs/`` is product
    documentation only -- AGENTS.md:100-112), so they are intentionally NOT in
    this repo. Walk upward from this file so the lookup survives any checkout
    depth; return None when absent.
    """
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "docs" / "04-deadletter-artifacts" / "seeds"
        if candidate.is_dir():
            return candidate
    return None


_SEEDS = _seed_dir()
_requires_seeds = pytest.mark.skipif(
    _SEEDS is None,
    reason=(
        "captured dead-letter seeds not present "
        "(expected <workspace>/docs/04-deadletter-artifacts/seeds/)"
    ),
)


@_requires_seeds
async def test_read_batch_over_a_pre_existing_merged_middle_line(
    tmp_path: Path,
) -> None:
    """A pre-existing merged middle line still fails ``_parse_line`` (T15).

    This fix does NOT claim to fix damage already on disk -- consuming a
    malformed middle line (dead-letter it, commit past it) is the drainer's
    job, not the framing fix's.
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
    """The REAL 1.0 MiB corrupt line from production fails ``_parse_line``.

    Source: session 71afde0c's ``.dead.jsonl``, whose recorded error is
    ``Expecting ',' delimiter: line 1 column 1050642 (char 1050641)``. This
    asserts the real artifact still reproduces that exact rejection through the
    real parser -- the dead-letter was not a one-off environment artifact.
    """
    assert _SEEDS is not None
    raw = (_SEEDS / "seed_corrupt_merged_line_1.0MiB.raw").read_bytes()

    # Framing evidence: ONE physical line (no internal newline) that contains
    # the start of TWO distinct event records -- record B begins immediately,
    # with no separator, inside record A.
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

    # The parser dies AT the merge boundary, give or take a couple of bytes:
    # record A's truncated JSON is syntactically fine right up to where record
    # B's bytes begin. Here A was torn INSIDE a quoted string, so B's leading
    # ``{"`` is swallowed as string content and the parse only breaks on the
    # very next token -- boundary+2. That two-byte offset is itself evidence of
    # a mid-value tear rather than a record-boundary problem.
    assert boundary <= exc.value.pos <= boundary + 8, (
        f"parse failed at {exc.value.pos}, merge boundary is {boundary}"
    )
    assert exc.value.pos == 1050641, "matches the recorded dead-letter error offset"
    assert "Expecting ',' delimiter" in str(exc.value), (
        "matches the recorded dead-letter error text"
    )


@_requires_seeds
def test_captured_valid_large_event_parses_cleanly() -> None:
    """A REAL 1.04 MiB event parses fine -- isolating the defect to FRAMING.

    Same session, same magnitude, ~43 KB LARGER than the corrupt seed. Size is
    not what breaks ``_parse_line``; a torn write is.
    """
    assert _SEEDS is not None
    raw = (_SEEDS / "seed_valid_large_event_1.04MiB.json").read_bytes()
    assert len(raw) > 1024 * 1024
    assert raw.count(b"\n") == 0

    event, workspace, data = SessionRegistry._parse_line(raw)
    assert event
    assert workspace
    assert isinstance(data, dict)
