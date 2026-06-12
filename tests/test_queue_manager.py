"""Tests for the on-disk durable queue manager (Phase B1)."""

from __future__ import annotations

import time

import pytest

from context_intelligence_server.queue_manager import Batch, QueueManager


@pytest.fixture
def qm(tmp_path):
    return QueueManager(queues_dir=tmp_path / "queues")


def test_constructor_creates_queues_dir(tmp_path):
    target = tmp_path / "nested" / "queues"
    assert not target.exists()
    QueueManager(queues_dir=target)
    assert target.is_dir()


def test_batch_holds_its_fields():
    batch = Batch(session_id="s1", lines=[b"a", b"b"], start_offset=0, end_offset=4)
    assert batch.session_id == "s1"
    assert batch.lines == [b"a", b"b"]
    assert batch.start_offset == 0
    assert batch.end_offset == 4


async def test_append_writes_line_with_trailing_newline(qm, tmp_path):
    await qm.append("s1", b'{"e":1}')
    log = tmp_path / "queues" / "s1.log"
    assert log.read_bytes() == b'{"e":1}\n'


async def test_append_does_not_double_newline(qm, tmp_path):
    await qm.append("s1", b'{"e":1}\n')
    log = tmp_path / "queues" / "s1.log"
    assert log.read_bytes() == b'{"e":1}\n'


@pytest.mark.parametrize("bad_id", ["", "a/b", "a\\b", "a\x00b"])
async def test_append_rejects_unsafe_session_id(qm, bad_id):
    with pytest.raises(ValueError):
        await qm.append(bad_id, b"x")


async def test_read_batch_returns_lines_fifo(qm):
    await qm.append("s1", b"one")
    await qm.append("s1", b"two")
    await qm.append("s1", b"three")
    batch = await qm.read_batch("s1", max_items=10)
    assert batch.session_id == "s1"
    assert batch.lines == [b"one", b"two", b"three"]
    assert batch.start_offset == 0
    assert batch.end_offset == len(b"one\ntwo\nthree\n")


async def test_read_batch_respects_max_items(qm):
    for i in range(5):
        await qm.append("s1", f"line{i}".encode())
    batch = await qm.read_batch("s1", max_items=2)
    assert batch.lines == [b"line0", b"line1"]
    assert batch.end_offset == len(b"line0\nline1\n")
    assert batch.start_offset == 0


async def test_read_batch_ignores_torn_trailing_line(qm, tmp_path):
    log = tmp_path / "queues" / "s1.log"
    log.write_bytes(b"complete1\ncomplete2\nTORN_PARTIAL")
    batch = await qm.read_batch("s1", max_items=10)
    assert batch.lines == [b"complete1", b"complete2"]
    assert batch.end_offset == len(b"complete1\ncomplete2\n")


async def test_read_batch_does_not_read_entire_tail(qm, tmp_path, monkeypatch):
    import builtins

    # ~90 KB log: 10,000 lines of 8 payload bytes + newline = 9 bytes each.
    log_path = tmp_path / "queues" / "s1.log"
    log_path.write_bytes(b"".join(b"x" * 8 + b"\n" for _ in range(10_000)))

    bytes_read = {"total": 0}
    real_open = builtins.open

    class _CountingFile:
        """Wraps a file object, tallying bytes returned by read/readline."""

        def __init__(self, wrapped):
            self._wrapped = wrapped

        def read(self, *args, **kwargs):
            data = self._wrapped.read(*args, **kwargs)
            bytes_read["total"] += len(data)
            return data

        def readline(self, *args, **kwargs):
            data = self._wrapped.readline(*args, **kwargs)
            bytes_read["total"] += len(data)
            return data

        def __getattr__(self, name):
            return getattr(self._wrapped, name)

        def __enter__(self):
            self._wrapped.__enter__()
            return self

        def __exit__(self, *exc):
            return self._wrapped.__exit__(*exc)

    def counting_open(file, *args, **kwargs):
        f = real_open(file, *args, **kwargs)
        if str(file) == str(log_path):
            return _CountingFile(f)
        return f

    monkeypatch.setattr(builtins, "open", counting_open)

    batch = await qm.read_batch("s1", max_items=100)
    assert len(batch.lines) == 100
    # Whole-tail read would pull ~90 KB; a bounded read pulls only ~100 lines.
    assert bytes_read["total"] < 50_000


async def test_read_batch_empty_for_unknown_session(qm):
    batch = await qm.read_batch("never-written", max_items=10)
    assert batch.lines == []
    assert batch.start_offset == 0
    assert batch.end_offset == 0


async def test_commit_advances_offset(qm):
    await qm.append("s1", b"a")
    await qm.append("s1", b"b")
    first = await qm.read_batch("s1", max_items=1)
    await qm.commit("s1", first.end_offset)
    await qm.append("s1", b"c")
    second = await qm.read_batch("s1", max_items=10)
    assert second.lines == [b"b", b"c"]
    assert second.start_offset == first.end_offset


async def test_commit_persists_across_a_new_instance(tmp_path):
    qdir = tmp_path / "queues"
    qm1 = QueueManager(queues_dir=qdir)
    await qm1.append("s1", b"a")
    await qm1.append("s1", b"b")
    batch = await qm1.read_batch("s1", max_items=1)
    await qm1.commit("s1", batch.end_offset)
    qm2 = QueueManager(queues_dir=qdir)  # simulate restart
    resumed = await qm2.read_batch("s1", max_items=10)
    assert resumed.lines == [b"b"]


async def test_commit_is_atomic_no_temp_leftover(qm, tmp_path):
    await qm.append("s1", b"a")
    await qm.commit("s1", 2)
    qdir = tmp_path / "queues"
    assert (qdir / "s1.offset").read_text("utf-8") == "2"
    assert list(qdir.glob("*.tmp")) == []


async def test_active_sessions_excludes_fully_committed(qm):
    await qm.append("s_active", b"x")  # appended, never committed -> undrained
    await qm.append("s_done", b"y")
    done = await qm.read_batch("s_done", max_items=10)
    await qm.commit("s_done", done.end_offset)  # drained
    active = await qm.active_sessions()
    assert active == ["s_active"]


async def test_recover_empty_dir_is_safe(qm):
    assert await qm.recover() == []


async def test_recover_reports_session_with_uncommitted_complete_line(qm, tmp_path):
    log = tmp_path / "queues" / "s1.log"
    log.write_bytes(b"a\nb\nTORN")  # two complete lines + torn tail
    assert await qm.recover() == ["s1"]
    await qm.commit("s1", 4)  # past 'a\nb\n' == 4 bytes
    assert await qm.recover() == []  # only torn tail remains -> not recoverable


async def test_dead_letter_appends_and_reads_back(qm):
    await qm.dead_letter("s1", b"poison-1", error="deadlock budget exhausted")
    await qm.dead_letter("s1", b"poison-2", error="validation failed")
    records = await qm.read_dead_letters("s1")
    assert [r["payload"] for r in records] == ["poison-1", "poison-2"]
    assert [r["error"] for r in records] == [
        "deadlock budget exhausted",
        "validation failed",
    ]
    assert all("ts" in r for r in records)
    batch = await qm.read_batch("s1", max_items=10)
    assert batch.lines == []  # main log untouched


async def test_read_dead_letters_empty_when_none(qm):
    assert await qm.read_dead_letters("nobody") == []


@pytest.mark.parametrize("bad_id", ["", "a/b", "a\\b", "a\x00b"])
async def test_read_batch_rejects_unsafe_session_id(qm, bad_id):
    with pytest.raises(ValueError):
        await qm.read_batch(bad_id, max_items=1)


@pytest.mark.parametrize("bad_id", ["", "a/b", "a\\b", "a\x00b"])
async def test_commit_rejects_unsafe_session_id(qm, bad_id):
    with pytest.raises(ValueError):
        await qm.commit(bad_id, 0)


@pytest.mark.parametrize("bad_id", ["", "a/b", "a\\b", "a\x00b"])
async def test_dead_letter_rejects_unsafe_session_id(qm, bad_id):
    with pytest.raises(ValueError):
        await qm.dead_letter(bad_id, b"x", error="e")


@pytest.mark.parametrize("bad_id", ["", "a/b", "a\\b", "a\x00b"])
async def test_read_dead_letters_rejects_unsafe_session_id(qm, bad_id):
    with pytest.raises(ValueError):
        await qm.read_dead_letters(bad_id)


async def test_delete_drained_removes_log_and_offset_keeps_dead(tmp_path) -> None:
    from context_intelligence_server.queue_manager import QueueManager

    qm = QueueManager(queues_dir=tmp_path)
    await qm.append("s", b"line")
    await qm.commit("s", 5)
    await qm.dead_letter("s", b"bad\n", "boom")

    await qm.delete_drained("s")

    assert not (tmp_path / "s.log").exists()
    assert not (tmp_path / "s.offset").exists()
    assert (tmp_path / "s.dead.jsonl").exists()  # retained
    assert len(await qm.read_dead_letters("s")) == 1


async def test_derive_all_stats_counts_pending_and_dead(qm):
    # s1: two complete pending (uncommitted) lines, no dead letters.
    await qm.append("s1", b"a")
    await qm.append("s1", b"b")
    # s2: no pending log data, one dead letter.
    await qm.dead_letter("s2", b"poison", error="boom")

    stats = await qm.derive_all_stats()

    assert stats["in_queue_total"] == 2
    assert stats["dead_total"] == 1
    assert "oldest_unflushed_age" not in stats  # deferred to C2

    by_key = {entry["worker_key"]: entry for entry in stats["per_key"]}
    assert by_key["s1"]["in_queue"] == 2
    assert by_key["s1"]["dead"] == 0
    assert by_key["s2"]["in_queue"] == 0
    assert by_key["s2"]["dead"] == 1
    for entry in stats["per_key"]:
        assert "oldest_unflushed_age" not in entry  # deferred to C2


async def test_dead_letter_keys_lists_only_keys_with_dead_files(qm):
    # 'live' has only main-log data, no dead-letter file -> excluded.
    await qm.append("live", b"x")
    # Two keys with dead-letter files; appended out of order to prove sorting.
    await qm.dead_letter("zeta", b"poison", error="boom")
    await qm.dead_letter("alpha", b"poison", error="boom")

    assert await qm.dead_letter_keys() == ["alpha", "zeta"]


async def test_purge_dead_letters_removes_file_and_returns_count(qm, tmp_path):
    await qm.dead_letter("s1", b"poison-1", error="boom")
    await qm.dead_letter("s1", b"poison-2", error="boom")

    removed = await qm.purge_dead_letters("s1")

    assert removed == 2
    assert await qm.read_dead_letters("s1") == []
    assert not (tmp_path / "queues" / "s1.dead.jsonl").exists()


async def test_purge_dead_letters_missing_file_returns_zero(qm):
    assert await qm.purge_dead_letters("nobody") == 0


@pytest.mark.parametrize("bad_id", ["", "a/b", "a\\b", "a\x00b"])
async def test_purge_dead_letters_rejects_unsafe_session_id(qm, bad_id):
    with pytest.raises(ValueError):
        await qm.purge_dead_letters(bad_id)


async def test_derive_all_stats_caches_within_ttl(qm, monkeypatch):
    await qm.append("s1", b"a")

    calls = {"n": 0}
    real = qm._all_worker_keys

    def counting():
        calls["n"] += 1
        return real()

    monkeypatch.setattr(qm, "_all_worker_keys", counting)

    await qm.derive_all_stats()
    await qm.derive_all_stats()  # within TTL -> served from cache
    assert calls["n"] == 1

    # Age the cache past the TTL; the next call must recompute.
    qm._stats_cache_at = time.monotonic() - (qm._stats_cache_ttl + 1.0)
    await qm.derive_all_stats()
    assert calls["n"] == 2
