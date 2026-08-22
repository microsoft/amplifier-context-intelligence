"""Tests for the on-disk durable queue manager."""

from __future__ import annotations

import time

import pytest
from context_intelligence_server.queue_manager import (
    Batch,
    QueueManager,
    Record,
    Verdict,
)


@pytest.fixture
def qm(tmp_path):
    return QueueManager(queues_dir=tmp_path / "queues")


def test_constructor_creates_queues_dir(tmp_path):
    target = tmp_path / "nested" / "queues"
    assert not target.exists()
    QueueManager(queues_dir=target)
    assert target.is_dir()


def test_batch_holds_its_fields():
    """``batch.lines`` is derived from ``batch.records``."""
    batch = Batch(
        session_id="s1",
        records=[Record(b"a", 0, 2), Record(b"b", 2, 4)],
        start_offset=0,
        end_offset=4,
    )
    assert batch.session_id == "s1"
    assert batch.lines == [b"a", b"b"]
    assert batch.start_offset == 0
    assert batch.end_offset == 4


# ---------------------------------------------------------------------------
# Record / Batch.records: offsets are queue-produced and read-only for callers.
# ---------------------------------------------------------------------------


async def test_read_batch_records_carry_queue_produced_offsets(qm, tmp_path):
    """Each record's start equals the previous record's end, the first/last
    records bound the batch's start/end_offset, and no record's raw payload
    has a trailing newline."""
    await qm.append("s1", b"one")
    await qm.append("s1", b"two")
    await qm.append("s1", b"three")

    batch = await qm.read_batch("s1", max_items=10)

    assert len(batch.records) == 3
    assert batch.records[0].start == batch.start_offset
    assert batch.records[-1].end == batch.end_offset
    for i in range(1, len(batch.records)):
        assert batch.records[i].start == batch.records[i - 1].end
    for rec in batch.records:
        assert not rec.raw.endswith(b"\n")
    assert [r.raw for r in batch.records] == [b"one", b"two", b"three"]


async def test_batch_lines_is_derived_from_records(qm, tmp_path):
    """``batch.lines`` always matches ``[r.raw for r in batch.records]``."""
    await qm.append("s1", b"alpha")
    await qm.append("s1", b"beta")

    batch = await qm.read_batch("s1", max_items=10)

    assert batch.lines == [r.raw for r in batch.records]


async def test_read_batch_records_survive_a_torn_trailing_line(qm, tmp_path):
    """A log ending in a partial (torn) line yields records only for the
    complete lines that precede it; end_offset stops on the line boundary."""
    log = tmp_path / "queues" / "s1.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_bytes(b"complete-one\ncomplete-two\ntorn-no-newline-yet")

    batch = await qm.read_batch("s1", max_items=10)

    assert [r.raw for r in batch.records] == [b"complete-one", b"complete-two"]
    assert batch.end_offset == len(b"complete-one\ncomplete-two\n")


async def test_committing_rec_end_advances_exactly_one_record(qm, tmp_path):
    """``commit(sid, records[0].end)`` then a fresh ``read_batch`` returns
    records ``[1:]``."""
    await qm.append("s1", b"first")
    await qm.append("s1", b"second")
    await qm.append("s1", b"third")

    batch = await qm.read_batch("s1", max_items=10)
    await qm.commit("s1", batch.records[0].end)

    remaining = await qm.read_batch("s1", max_items=10)
    assert [r.raw for r in remaining.records] == [b"second", b"third"]


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


# _read_committed_offset accepts the bare-int form and the legacy JSON offset
# document; commit() still writes bare int. A present-but-unusable offset must
# raise, never silently return 0 (0 would force a full re-drain).


async def test_read_committed_offset_accepts_legacy_json_cursor(qm):
    """A legacy JSON cursor document parses to its integer "offset" field."""
    qm._offset_path("s1").write_text(
        '{"v":1,"offset":12345,"cursor":{"dl2":{"a":1},"dl3":{}}}',
        encoding="utf-8",
    )
    assert qm._read_committed_offset("s1") == 12345


async def test_read_committed_offset_accepts_bare_int_unchanged(qm):
    """Bare-int offsets (the current write format) still parse exactly."""
    qm._offset_path("s1").write_text("980582046", encoding="utf-8")
    assert qm._read_committed_offset("s1") == 980582046


async def test_read_committed_offset_missing_file_is_zero(qm):
    assert qm._read_committed_offset("never-written") == 0


async def test_read_committed_offset_empty_file_is_zero(qm):
    qm._offset_path("s1").write_text("", encoding="utf-8")
    assert qm._read_committed_offset("s1") == 0


async def test_read_committed_offset_legacy_json_without_usable_offset_raises(qm):
    """A JSON object present but with no usable integer "offset" must raise
    ValueError -- the same as any other unparseable offset -- rather than
    silently returning 0 (which would trigger a full re-drain)."""
    qm._offset_path("s1").write_text('{"v":1,"cursor":{}}', encoding="utf-8")
    with pytest.raises(ValueError):
        qm._read_committed_offset("s1")


async def test_read_committed_offset_garbage_still_raises(qm):
    """Genuinely unparseable text (not JSON, not an int) still raises."""
    qm._offset_path("s1").write_text("not-a-number", encoding="utf-8")
    with pytest.raises(ValueError):
        qm._read_committed_offset("s1")


async def test_read_batch_drains_session_with_legacy_json_offset(qm):
    """A session with a legacy JSON-cursor .offset drains via the normal
    read path (read_batch) with no ValueError -- the fix must reach the
    hot path, not just the private helper."""
    await qm.append("s1", b"a")
    await qm.append("s1", b"b")
    qm._offset_path("s1").write_text('{"v":1,"offset":2,"cursor":{}}', encoding="utf-8")

    batch = await qm.read_batch("s1", max_items=10)

    assert batch.start_offset == 2
    assert batch.lines == [b"b"]


async def test_classify_session_with_legacy_json_offset_is_not_corrupt(qm):
    """A session with a legacy JSON-cursor .offset must not be classified
    as an unreadable/corrupt offset -- it should classify the same as an
    equivalent bare-int offset (drained, in this fully-committed case)."""
    await qm.append("s1", b"a\n" * 0 + b"a")  # single record "a"
    line = b"a\n"
    qm._offset_path("s1").write_text(
        f'{{"v":1,"offset":{len(line)},"cursor":{{}}}}', encoding="utf-8"
    )

    classification = await qm.classify_session(
        "s1", head_is_resumable=lambda _raw: True
    )

    assert classification.verdict == Verdict.DRAINED


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


# --- recovery_seed_counts: residual-0-by-construction crash-recovery seed ---


async def test_recovery_seed_counts_pending_and_committed(qm):
    # C=2 committed lines, P=1 pending line, D=0 dead. Each "x\n" is 2 bytes.
    await qm.append("s1", b"a")
    await qm.append("s1", b"b")
    await qm.append("s1", b"c")
    await qm.commit("s1", 4)  # commit the first two complete lines

    accepted, written = await qm.recovery_seed_counts()

    # written_seed = max(0, 2-0)=2; accepted_seed = 2 + 1 + 0 = 3
    assert (accepted, written) == (3, 2)


async def test_recovery_seed_counts_committed_includes_dead(qm):
    # C=1 committed, P=0 pending, D=1 dead. before-dead == 0.
    await qm.append("s2", b"a")
    await qm.commit("s2", 2)
    await qm.dead_letter("s2", b"a", error="boom")

    accepted, written = await qm.recovery_seed_counts()

    # written_seed = max(0, 1-1)=0; accepted_seed = 0 + 0 + 1 = 1
    assert (accepted, written) == (1, 0)


async def test_recovery_seed_counts_dead_only_after_log_reclaimed(qm):
    # No .log file (drained/reclaimed); only a dead-letter remains. D=1.
    await qm.dead_letter("s3", b"poison", error="boom")

    accepted, written = await qm.recovery_seed_counts()

    # before=0, pending=0, dead=1 -> written=max(0,-1)=0; accepted=0+0+1=1
    assert (accepted, written) == (1, 0)


async def test_recovery_seed_counts_residual_is_zero_mixed_shape(qm):
    # Key A: 2 committed + 1 pending, no dead.
    await qm.append("a", b"1")
    await qm.append("a", b"2")
    await qm.append("a", b"3")
    await qm.commit("a", 4)
    # Key B: 1 committed + 1 dead.
    await qm.append("b", b"x")
    await qm.commit("b", 2)
    await qm.dead_letter("b", b"x", error="boom")
    # Key C: dead-only (log reclaimed).
    await qm.dead_letter("c", b"poison", error="boom")

    accepted, written = await qm.recovery_seed_counts()
    stats = await qm.derive_all_stats()

    residual = accepted - written - stats["in_queue_total"] - stats["dead_total"]
    assert residual == 0


async def test_recovery_seed_counts_crash_window_residual_zero(qm):
    # Crash before commit advanced: the dead-lettered line is STILL pending in
    # the log (committed offset 0). C=0, P=1, D=1 -> before-dead == -1.
    # The naive formula (written=before-dead) yields written==-1 (false
    # DEGRADED). The clamp must keep written at 0 and residual at 0.
    await qm.append("s5", b"a")  # pending, never committed
    await qm.dead_letter("s5", b"a", error="boom")  # same line dead-lettered

    accepted, written = await qm.recovery_seed_counts()

    assert written == 0  # NOT -1 (the crash-window trap)
    assert accepted == 2  # written_seed(0) + pending(1) + dead(1)

    stats = await qm.derive_all_stats()
    residual = accepted - written - stats["in_queue_total"] - stats["dead_total"]
    assert residual == 0


# --- recovery_reconcile_dead: close the dead_letter->commit crash window ---


async def test_recovery_reconcile_dead_advances_past_already_dead_pending(qm):
    # Crash window: a pending (uncommitted) line that was ALREADY dead-lettered.
    # Reconcile must advance the committed offset past it so the respawned
    # drainer does not re-read and re-dead-letter it.
    await qm.append("s1", b"poison")
    await qm.dead_letter("s1", b"poison", error="boom")

    skipped = await qm.recovery_reconcile_dead()

    assert skipped == 1
    batch = await qm.read_batch("s1", max_items=10)
    assert batch.lines == []  # offset advanced past the dead-but-pending line


async def test_recovery_reconcile_dead_stops_at_first_non_dead(qm):
    # Leading poison line is dead-lettered; a healthy line follows it.
    # Reconcile skips the leading poison and STOPS at the healthy line.
    await qm.append("s1", b"poison")
    await qm.append("s1", b"good")
    await qm.dead_letter("s1", b"poison", error="boom")

    skipped = await qm.recovery_reconcile_dead()

    assert skipped == 1
    batch = await qm.read_batch("s1", max_items=10)
    assert batch.lines == [b"good"]  # healthy line is still delivered


async def test_recovery_reconcile_dead_noop_without_dead_file(qm):
    # No dead-letter file -> nothing to reconcile, line still delivered.
    await qm.append("s1", b"line")

    skipped = await qm.recovery_reconcile_dead()

    assert skipped == 0
    batch = await qm.read_batch("s1", max_items=10)
    assert batch.lines == [b"line"]  # untouched


async def test_recovery_reconcile_then_seed_keeps_residual_zero(qm):
    # Reconcile then seed then derive must leave residual == 0.
    await qm.append("s1", b"poison")
    await qm.dead_letter("s1", b"poison", error="boom")

    await qm.recovery_reconcile_dead()
    accepted, written = await qm.recovery_seed_counts()
    stats = await qm.derive_all_stats()

    residual = accepted - written - stats["in_queue_total"] - stats["dead_total"]
    assert residual == 0


async def test_recovery_seed_counts_replay_window_residual_zero(qm):
    # Replay: a committed line was dead-lettered, then re-appended for retry.
    # log = [line0 committed][line0 re-appended pending]. C=1, P=1, D=1.
    # The re-appended line is absorbed into accepted_seed (counted in P and D).
    await qm.append("s6", b"a")
    await qm.commit("s6", 2)
    await qm.dead_letter("s6", b"a", error="boom")
    await qm.append("s6", b"a")  # re-append the dead line for replay

    accepted, written = await qm.recovery_seed_counts()

    # written_seed = max(0, 1-1)=0; accepted_seed = 0 + 1 + 1 = 2
    assert (accepted, written) == (2, 0)

    stats = await qm.derive_all_stats()
    residual = accepted - written - stats["in_queue_total"] - stats["dead_total"]
    assert residual == 0


# ---------------------------------------------------------------------------
# spool_stats (Change 2): cheap, aggregate-only spool footprint for /status
# ---------------------------------------------------------------------------


async def test_spool_stats_counts_pending_session_and_bytes(qm, tmp_path):
    """A session with unconsumed log data counts as pending; total bytes
    reflects every file on disk (.log + .offset + .dead.jsonl)."""
    await qm.append("s1", b"a")
    await qm.append("s1", b"b")

    stats = await qm.spool_stats()

    assert stats["pending_sessions"] == 1
    queues_dir = tmp_path / "queues"
    expected_bytes = sum(p.stat().st_size for p in queues_dir.iterdir())
    assert stats["spool_bytes_total"] == expected_bytes
    assert expected_bytes > 0


async def test_spool_stats_fully_committed_session_not_pending(qm):
    """A session whose committed offset reaches EOF is NOT counted as
    pending, even though its .log/.offset files still occupy disk space
    (spool_bytes_total still reflects them)."""
    await qm.append("s1", b"a")
    line = b"a\n"
    await qm.commit("s1", len(line))

    stats = await qm.spool_stats()

    assert stats["pending_sessions"] == 0
    assert stats["spool_bytes_total"] > 0  # log + offset files still on disk


async def test_spool_stats_dead_letter_only_session_not_pending(qm):
    """A dead-letter-only key (no .log) contributes bytes but is never
    counted as a pending session -- pending_sessions is defined purely over
    .log files with unconsumed data."""
    await qm.dead_letter("s-dead", b"poison", error="boom")

    stats = await qm.spool_stats()

    assert stats["pending_sessions"] == 0
    assert stats["spool_bytes_total"] > 0


async def test_spool_stats_multiple_sessions_aggregate(qm):
    """pending_sessions counts sessions independently; bytes sum across all."""
    await qm.append("s1", b"a")  # pending
    await qm.append("s2", b"b")
    line = b"b\n"
    await qm.commit("s2", len(line))  # fully committed, not pending
    await qm.append("s3", b"c")  # pending

    stats = await qm.spool_stats()

    assert stats["pending_sessions"] == 2


async def test_spool_stats_returns_only_aggregate_keys_no_identifiers(qm):
    """/status is unauthenticated: spool_stats() must return ONLY the two
    aggregate integers -- no session ids, workspace names, or per-key table
    of any kind, so there's nothing to accidentally leak through /status."""
    await qm.append("my-secret-session-id", b"a")
    await qm.dead_letter("another-session-id", b"poison", error="boom")

    stats = await qm.spool_stats()

    assert set(stats.keys()) == {
        "pending_sessions",
        "spool_bytes_total",
        "corrupt_offsets",
    }
    serialized = repr(stats)
    assert "my-secret-session-id" not in serialized
    assert "another-session-id" not in serialized


async def test_spool_stats_caches_within_ttl(qm, monkeypatch):
    """Repeated calls within the TTL window are served from cache -- the
    directory is not re-scanned on every /status poll."""
    import pathlib

    await qm.append("s1", b"a")

    calls = {"n": 0}
    real_iterdir = pathlib.Path.iterdir

    # pathlib.Path instances use __slots__, so the target Path (qm._dir)
    # cannot be monkeypatched directly -- patch the class method instead,
    # counting only calls made against qm._dir (this codebase's only other
    # .iterdir() caller checked clean at write time; see grep before this
    # test was added).
    def counting_iterdir(self: pathlib.Path):
        if self == qm._dir:
            calls["n"] += 1
        return real_iterdir(self)

    monkeypatch.setattr(pathlib.Path, "iterdir", counting_iterdir)

    await qm.spool_stats()
    await qm.spool_stats()  # within TTL -> served from cache
    assert calls["n"] == 1

    # Age the cache past the TTL; the next call must recompute.
    qm._spool_cache_at = time.monotonic() - (qm._spool_cache_ttl + 1.0)
    await qm.spool_stats()
    assert calls["n"] == 2


# ---------------------------------------------------------------------------
# Streamed boot/stats scans (ci_pr73-xq2): _complete_data_end and
# _count_newlines must be bounded-memory AND numerically identical to the old
# read_bytes() + slice-count implementation, including at chunk boundaries.
# ---------------------------------------------------------------------------


def _naive_complete_data_end(data: bytes) -> int:
    last_nl = data.rfind(b"\n")
    return last_nl + 1 if last_nl != -1 else 0


def test_complete_data_end_matches_naive_and_handles_edges(qm, tmp_path):
    """Backward-scan _complete_data_end == old rfind(b'\\n')+1 for every shape:
    empty, no-newline (torn only), trailing newline, torn tail after data."""
    log = tmp_path / "queues" / "s1.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    for payload in (
        b"",  # empty
        b"no-newline-yet",  # single torn line, no complete data
        b"a\n",  # one complete line
        b"a\nb\n",  # two complete lines
        b"a\nb\ntorn-tail",  # complete data + torn trailing line
    ):
        log.write_bytes(payload)
        assert qm._complete_data_end("s1") == _naive_complete_data_end(payload)


def test_complete_data_end_missing_log_is_zero(qm):
    assert qm._complete_data_end("nope") == 0


def test_complete_data_end_newline_on_chunk_boundary(qm, tmp_path, monkeypatch):
    """The backward scan reads fixed non-overlapping windows; a newline landing
    exactly on a chunk boundary must still be found (regression guard for the
    streaming rewrite)."""
    import context_intelligence_server.queue_manager as qm_mod

    monkeypatch.setattr(qm_mod, "_SCAN_CHUNK_BYTES", 8)
    log = tmp_path / "queues" / "s1.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    # Last newline sits at index 8 (exactly one chunk from the start), followed
    # by a torn tail so complete_data_end must be 9, spanning a chunk boundary.
    data = b"01234567\ntail"  # '\n' at index 8
    log.write_bytes(data)
    assert qm._complete_data_end("s1") == _naive_complete_data_end(data) == 9


def test_count_newlines_matches_naive_across_ranges(qm, tmp_path, monkeypatch):
    """_count_newlines(start,end) == data[start:end].count(b'\\n') for arbitrary
    ranges, including across a small chunk size (multi-chunk streaming)."""
    import context_intelligence_server.queue_manager as qm_mod

    monkeypatch.setattr(qm_mod, "_SCAN_CHUNK_BYTES", 4)
    log = tmp_path / "queues" / "s1.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    data = b"aa\nbbbb\nc\n\nddddddd\n"
    log.write_bytes(data)

    n = len(data)
    for start in range(n + 1):
        # to-EOF form
        assert qm._count_newlines("s1", start) == data[start:].count(b"\n")
        for end in range(start, n + 1):
            assert qm._count_newlines("s1", start, end) == data[start:end].count(b"\n")


def test_count_newlines_missing_and_empty_range(qm):
    assert qm._count_newlines("missing") == 0
    assert qm._count_newlines("missing", 0, 0) == 0


def test_count_dead_matches_naive_and_streams(qm, tmp_path, monkeypatch):
    """_count_dead == old data.count(b'\\n') for empty / multi-record / missing,
    including a newline on a chunk boundary (streamed, not read_bytes)."""
    import context_intelligence_server.queue_manager as qm_mod

    monkeypatch.setattr(qm_mod, "_SCAN_CHUNK_BYTES", 8)
    dead = tmp_path / "queues" / "s1.dead.jsonl"
    dead.parent.mkdir(parents=True, exist_ok=True)

    assert qm._count_dead("missing") == 0

    for payload in (
        b"",  # empty -> 0
        b'{"a":1}\n',  # one record
        b'{"a":1}\n{"b":2}\n{"c":3}\n',  # three records
        b"01234567\n8\n",  # newline at index 8 == chunk boundary, 2 records
    ):
        dead.write_bytes(payload)
        assert qm._count_dead("s1") == payload.count(b"\n")


async def test_recovery_seed_counts_unchanged_under_streaming(qm):
    """End-to-end: the streamed recovery_seed_counts yields the same
    (accepted, written) baseline as the semantics it replaced."""
    # Two complete lines appended, one committed.
    await qm.append("s1", b"a")
    await qm.append("s1", b"bb")
    line1 = b"a\n"
    await qm.commit("s1", len(line1))  # 1 written, 1 still pending

    accepted, written = await qm.recovery_seed_counts()

    assert written == 1  # one committed line, no dead
    assert accepted == 2  # one written + one pending


async def test_spool_stats_empty_directory(qm):
    """An empty spool directory reports zero for both aggregates."""
    stats = await qm.spool_stats()
    assert stats == {
        "pending_sessions": 0,
        "spool_bytes_total": 0,
        "corrupt_offsets": 0,
    }


# ---------------------------------------------------------------------------
# spool_stats health-endpoint safety (regression, ci_pr73-ueh):
# /status is the unauthenticated ACA health probe and calls spool_stats()
# unconditionally. spool_stats() uses iterdir() (raises on a missing dir),
# unlike every sibling reader which uses glob() (empty on a missing dir), so
# a transiently-unavailable queue dir or a corrupt .offset MUST degrade to a
# sentinel, never raise -- an escape becomes a 500 -> failed probe -> restart.
# ---------------------------------------------------------------------------


async def test_spool_stats_missing_directory_returns_sentinel(qm, tmp_path):
    """A missing queue dir makes iterdir() raise FileNotFoundError; spool_stats
    must return the degraded sentinel {-1, -1} rather than propagate (which
    would 500 the /status health probe -- e.g. during an Azure Files remount)."""
    import shutil

    shutil.rmtree(tmp_path / "queues")
    qm._spool_cache = None  # bypass the TTL cache so the scan actually runs

    stats = await qm.spool_stats()

    assert stats == {
        "pending_sessions": -1,
        "spool_bytes_total": -1,
        "corrupt_offsets": -1,
    }


async def test_spool_stats_sentinel_is_not_cached(qm, tmp_path):
    """The degraded sentinel is NOT cached: once the directory is healthy
    again, the very next call recovers the real aggregate numbers."""
    import shutil

    queues_dir = tmp_path / "queues"
    shutil.rmtree(queues_dir)
    qm._spool_cache = None
    assert await qm.spool_stats() == {
        "pending_sessions": -1,
        "spool_bytes_total": -1,
        "corrupt_offsets": -1,
    }

    # Filesystem recovers; no manual cache reset -- the sentinel was never stored.
    queues_dir.mkdir(parents=True, exist_ok=True)
    await qm.append("s1", b"a")

    stats = await qm.spool_stats()
    assert stats["pending_sessions"] == 1
    assert stats["spool_bytes_total"] > 0


async def test_spool_stats_corrupt_offset_does_not_sink_scan(qm):
    """A corrupt/unreadable .offset for one session must not fail the whole
    scan (which would 500 /status): that file's bytes still count, only its
    pending calc is skipped."""
    await qm.append("s1", b"a")
    qm._offset_path("s1").write_text("not-a-number", encoding="utf-8")
    qm._spool_cache = None

    stats = await qm.spool_stats()

    assert stats["spool_bytes_total"] > 0
    assert isinstance(stats["pending_sessions"], int)


async def test_spool_stats_counts_corrupt_offsets(qm):
    """A non-numeric .offset is surfaced as an aggregate corrupt_offsets count
    (the ONLY visibility signal -- no logging). A healthy session contributes 0."""
    await qm.append("s-good", b"a")  # valid: no .offset yet -> committed 0
    await qm.append("s-bad", b"a")
    qm._offset_path("s-bad").write_text("not-a-number", encoding="utf-8")
    qm._spool_cache = None

    stats = await qm.spool_stats()

    assert stats["corrupt_offsets"] == 1
    assert stats["spool_bytes_total"] > 0  # corrupt file's bytes still counted


async def test_spool_stats_healthy_offsets_report_zero_corrupt(qm):
    """corrupt_offsets is 0 when every .offset is a valid integer (it must not
    fire on the normal committed-offset path)."""
    await qm.append("s1", b"a")
    line = b"a\n"
    await qm.commit("s1", len(line))  # writes a valid numeric .offset
    qm._spool_cache = None

    stats = await qm.spool_stats()

    assert stats["corrupt_offsets"] == 0
