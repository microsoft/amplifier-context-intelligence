"""Concurrency-correctness proof for QueueManager.append.

Independent of filesystem append atomicity: correctness rests on
``_KeyGuard.file_lock`` holding across one whole record write. These tests
hammer real on-disk queues with high concurrency (many sessions, many
concurrent writers per session, mixed small/>1 MiB records) and prove every
record survives exactly once, complete, untorn, unmerged.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path

from context_intelligence_server.queue_manager import FileSystemQueueManager, QueueManager

_SMALL_SIZE = 64
_LARGE_SIZE = 1_500_000  # > 1 MiB, mixed in with small records
_LARGE_EVERY = 17  # every Nth record (by seq) is oversized


def _payload(size_bytes: int, session_id: str, seq: int) -> str:
    """Unique-per-record filler; random bytes hex-encoded (no control chars)."""
    random_part = os.urandom(max(size_bytes // 2, 8)).hex()
    return f"{session_id}:{seq}:{random_part}"


def _make_record(session_id: str, seq: int, *, large: bool) -> bytes:
    size = _LARGE_SIZE if large else _SMALL_SIZE
    payload = _payload(size, session_id, seq)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    obj = {"session_id": session_id, "seq": seq, "payload": payload, "sha256": digest}
    return json.dumps(obj, separators=(",", ":")).encode("utf-8")


async def _append_range(qm: QueueManager, session_id: str, count: int) -> int:
    """Concurrently append `count` uniquely-numbered records; return bytes written."""
    lines = [
        _make_record(session_id, seq, large=(seq % _LARGE_EVERY == 0))
        for seq in range(count)
    ]
    await asyncio.gather(*(qm.append(session_id, line) for line in lines))
    return sum(len(line) + 1 for line in lines)  # +1 per newline terminator


def _read_all_lines(path: Path) -> list[bytes]:
    """Split a `.log` on newlines, asserting no torn (unterminated) tail."""
    data = path.read_bytes()
    assert data.endswith(b"\n"), f"{path}: torn tail -- file does not end on \\n"
    lines = data.split(b"\n")
    assert lines[-1] == b""  # split() artifact after the trailing terminator
    return lines[:-1]


def _verify_records(lines: list[bytes]) -> set[tuple[str, int]]:
    """Parse every line as exactly one JSON record; return the (session_id, seq) set.

    A merged line (two records concatenated with no newline between them)
    fails json.loads with "Extra data"; a torn line fails with a decode
    error -- both are zero-tolerance failures here.
    """
    seen: set[tuple[str, int]] = set()
    for line in lines:
        obj = json.loads(line)
        digest = hashlib.sha256(obj["payload"].encode("utf-8")).hexdigest()
        assert digest == obj["sha256"], "payload hash mismatch -- corrupted record"
        key = (obj["session_id"], obj["seq"])
        assert key not in seen, f"duplicate record {key}"
        seen.add(key)
    return seen


async def test_concurrent_appends_many_sessions_no_tear_or_merge_or_loss(
    tmp_path: Path,
) -> None:
    """>=8 sessions x >=50 records each, all interleaved concurrently, plus
    concurrent appends to the SAME session and several >1 MiB payloads
    mixed with small ones."""
    qm = FileSystemQueueManager(queues_dir=tmp_path / "queues")
    num_sessions = 10
    records_per_session = 60

    session_ids = [f"session-{i}" for i in range(num_sessions)]
    written = await asyncio.gather(
        *(_append_range(qm, sid, records_per_session) for sid in session_ids)
    )

    total_records = 0
    total_bytes = 0
    for sid in session_ids:
        log_path = tmp_path / "queues" / f"{sid}.log"
        lines = _read_all_lines(log_path)
        seen = _verify_records(lines)
        assert seen == {(sid, seq) for seq in range(records_per_session)}
        assert len(lines) == records_per_session
        total_records += len(lines)
        total_bytes += log_path.stat().st_size

    assert total_records == num_sessions * records_per_session
    assert total_bytes == sum(written)


async def test_concurrent_appends_single_session_hammered(tmp_path: Path) -> None:
    """Worst-case contention: many concurrent tasks writing ONE session's file."""
    qm = FileSystemQueueManager(queues_dir=tmp_path / "queues")
    session_id = "hot-session"
    num_records = 300

    written = await _append_range(qm, session_id, num_records)

    log_path = tmp_path / "queues" / f"{session_id}.log"
    lines = _read_all_lines(log_path)
    seen = _verify_records(lines)

    assert seen == {(session_id, seq) for seq in range(num_records)}
    assert len(lines) == num_records
    assert log_path.stat().st_size == written
