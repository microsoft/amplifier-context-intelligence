"""Tests for the on-disk durable queue manager."""

from __future__ import annotations

import asyncio
import threading
import time

import pytest
from context_intelligence_server.queue_manager import (
    Batch,
    QueueManager,
    Record,
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


# ---------------------------------------------------------------------------
# commit(): tmp-path collision regression (incident 2026-09-10, team-shared
# production). commit() used to build a FIXED temp path
# ("{session_id}.offset.tmp"), shared by every concurrent committer of the
# same session, and takes no lock -- so two overlapping commits raced on one
# path: the loser's os.replace(tmp, final) saw FileNotFoundError (the crash,
# ``drain_worker_died``), and worse, the SURVIVING replace silently published
# the OTHER caller's offset, which could skip records forward past ones never
# actually written to Neo4j. The fix makes the tmp path unique per call (see
# queue_manager.py's commit() docstring). These tests guard both the crash
# and the silent-loss half of that incident.
# ---------------------------------------------------------------------------


async def test_commit_concurrent_calls_for_same_session_do_not_raise(
    qm, tmp_path, monkeypatch
):
    """Two overlapping commit() calls for the SAME session, forced to
    interleave so BOTH writes land before EITHER os.replace runs, must not
    raise -- and the final .offset must equal one of the two committed
    values.

    On the pre-fix source (fixed tmp name) this exact interleaving makes the
    second write clobber the first's tmp file; the second commit replaces
    first and succeeds, but the first's subsequent os.replace() then finds
    its tmp gone and raises FileNotFoundError -- this is the literal
    incident traceback. A unique-per-call tmp path makes the two commits
    independent, so neither raises no matter which write happens first.
    """
    import pathlib

    real_write_text = pathlib.Path.write_text
    first_written = threading.Event()
    second_written = threading.Event()

    def synced_write_text(self: pathlib.Path, *args, **kwargs):
        result = real_write_text(self, *args, **kwargs)
        # Only synchronize the two commits' OWN offset-tmp writes -- matches
        # both the old fixed name ("s1.offset.tmp") and the new unique one
        # ("s1.offset.<uuid>.tmp").
        if self.name.startswith("s1.offset") and self.name.endswith(".tmp"):
            if not first_written.is_set():
                first_written.set()
                # Block here so the SECOND call's write (and its own
                # os.replace, if it gets there first) happens before this
                # (first) call proceeds to its own os.replace.
                assert second_written.wait(timeout=5), (
                    "second commit's write never landed -- test is broken, "
                    "not the production code"
                )
            else:
                second_written.set()
        return result

    monkeypatch.setattr(pathlib.Path, "write_text", synced_write_text)

    results = await asyncio.gather(
        qm.commit("s1", 100), qm.commit("s1", 200), return_exceptions=True
    )

    assert results == [None, None], results
    final = (tmp_path / "queues" / "s1.offset").read_text("utf-8").strip()
    assert final in ("100", "200")


async def test_commit_leaves_no_tmp_files_after_sequential_commits(qm, tmp_path):
    """Sequential (non-overlapping) commits never leave a unique tmp file
    behind. Unlike the old fixed name (accidentally self-cleaning -- the
    next commit just overwrote it), a unique-per-call tmp is NOT
    self-cleaning, so cleanup must be explicit on every single call, not
    just correct by accident on the first one."""
    qdir = tmp_path / "queues"
    for offset in (10, 20, 30, 40, 50):
        await qm.commit("s1", offset)
    assert list(qdir.glob("*.tmp")) == []
    assert (qdir / "s1.offset").read_text("utf-8") == "50"


async def test_commit_cleans_up_tmp_when_replace_fails(qm, tmp_path, monkeypatch):
    """When os.replace() fails mid-commit, the unique tmp file must be
    unlinked and the error must still propagate. A unique tmp is garbage
    nothing else will ever reuse; without explicit cleanup on failure, every
    failed commit would leak a file into the spool that the reclaim paths
    don't know about."""
    import os

    def failing_replace(src, dst):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(os, "replace", failing_replace)

    with pytest.raises(OSError):
        await qm.commit("s1", 100)

    qdir = tmp_path / "queues"
    assert list(qdir.glob("*.tmp")) == []


async def test_commit_concurrency_at_scale_no_raise_no_leak(qm, tmp_path):
    """50 concurrent commit() calls for the same session: none raise, the
    final offset is one of the submitted values, and no tmp files survive.
    On the pre-fix source (one shared fixed tmp path, no lock), this many
    concurrent committers reliably collide."""
    offsets = list(range(1, 51))
    results = await asyncio.gather(
        *(qm.commit("s1", off) for off in offsets), return_exceptions=True
    )

    assert results == [None] * len(offsets), results
    qdir = tmp_path / "queues"
    assert list(qdir.glob("*.tmp")) == []
    final = int((qdir / "s1.offset").read_text("utf-8").strip())
    assert final in offsets


def test_source_never_reintroduces_fixed_offset_tmp_path():
    """Regression guard for the 2026-09-10 tmp-path collision incident: the
    tmp path assigned in commit()/recovery_reconcile_dead() must always be
    UNIQUE PER CALL. A fixed literal f-string like f"{session_id}.offset.tmp"
    (an interpolated identifier immediately followed by the literal
    ".offset.tmp", with no per-call uniquifier such as uuid.uuid4().hex in
    between) is exactly the bug that let two concurrent commits for the same
    session collide on one path. This checks the actual f-string ASSIGNMENT
    pattern via regex, not prose: the commit() docstring deliberately
    mentions the old name in plain backticks (no f"" prefix), which this
    pattern does not match, so the incident writeup itself can't trip it."""
    import pathlib
    import re

    import context_intelligence_server.queue_manager as qm_mod

    source = pathlib.Path(qm_mod.__file__).read_text(encoding="utf-8")
    fixed_name_pattern = re.compile(r'f"\{\w+\}\.offset\.tmp"')
    matches = fixed_name_pattern.findall(source)
    assert matches == [], (
        f"Found fixed (non-unique) offset tmp path assignment(s): {matches} "
        "-- this reintroduces the 2026-09-10 tmp-path collision bug. The "
        "tmp path must include a per-call uniquifier (e.g. uuid.uuid4().hex)."
    )


# _read_committed_offset parses the bare-int form written by commit(). A
# present-but-unusable offset must raise, never silently return 0 (0 would
# force a full re-drain).


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

    await qm.refresh_all_stats()
    stats = qm.derive_all_stats()

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


async def test_refresh_all_stats_always_scans_no_ttl_gate(qm, monkeypatch):
    """refresh_all_stats() has NO TTL gate -- every call re-scans the spool.

    INCIDENT 2026-09-10: the old derive_all_stats() scanned inline on every
    /status call, gated only by a 1-second read-side TTL -- which spares the
    SECOND caller and never the first. On a ~5000-key spool on Azure Files
    SMB that made /status time out at 60-180s while /version answered in 0s
    on the same replica. Cadence is now owned entirely by the background
    refresher loop (main.py._spool_stats_refresher), not a read-side TTL."""
    await qm.append("s1", b"a")

    calls = {"n": 0}
    real = qm._all_worker_keys

    def counting():
        calls["n"] += 1
        return real()

    monkeypatch.setattr(qm, "_all_worker_keys", counting)

    await qm.refresh_all_stats()
    await qm.refresh_all_stats()  # no TTL -- scans again every time
    assert calls["n"] == 2


def test_derive_all_stats_is_synchronous_read_only_and_never_scans(qm, monkeypatch):
    """derive_all_stats() must NEVER touch the filesystem: it is a pure,
    synchronous cache read. An empty cache returns the unavailable sentinel
    (stats_available False, zeroed aggregates, never scans, never raises); a
    populated cache returns that snapshot plus stats_available True -- this
    is the entire point of the refresh/read split that fixes /status
    stalling on a large spool (see the module docstring / incident above)."""
    import pathlib

    def _boom(self: pathlib.Path) -> None:
        raise AssertionError("derive_all_stats() touched the filesystem via glob()")

    monkeypatch.setattr(pathlib.Path, "glob", _boom)

    # Cold cache: the sentinel, no scan, no raise.
    assert qm.derive_all_stats() == {
        "per_key": [],
        "in_queue_total": 0,
        "dead_total": 0,
        "stats_available": False,
    }

    # Populated cache: returns exactly that snapshot plus stats_available,
    # still without scanning.
    qm._stats_cache = {
        "per_key": [{"worker_key": "s1", "in_queue": 2, "dead": 0}],
        "in_queue_total": 2,
        "dead_total": 0,
    }
    assert qm.derive_all_stats() == {
        "per_key": [{"worker_key": "s1", "in_queue": 2, "dead": 0}],
        "in_queue_total": 2,
        "dead_total": 0,
        "stats_available": True,
    }


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
    await qm.refresh_all_stats()
    stats = qm.derive_all_stats()

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

    await qm.refresh_all_stats()
    stats = qm.derive_all_stats()
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


async def test_recovery_reconcile_dead_leaves_no_tmp_file(qm, tmp_path):
    """recovery_reconcile_dead()'s offset rewrite uses the same
    unique-per-call tmp pattern as commit() (see queue_manager.py's
    recovery_reconcile_dead -- these two sites can collide with EACH OTHER
    since PR #99, the same incident class as commit()'s). Seed a key whose
    leading pending line is already dead-lettered so pos > committed and the
    offset is actually rewritten -- a no-op reconcile would never touch a
    tmp file at all, so this must force the rewrite branch to run."""
    await qm.append("s1", b"poison")
    await qm.dead_letter("s1", b"poison", error="boom")

    skipped = await qm.recovery_reconcile_dead()

    assert skipped == 1  # confirms the offset-rewrite branch actually ran
    qdir = tmp_path / "queues"
    assert list(qdir.glob("*.tmp")) == []


async def test_recovery_reconcile_then_seed_keeps_residual_zero(qm):
    # Reconcile then seed then derive must leave residual == 0.
    await qm.append("s1", b"poison")
    await qm.dead_letter("s1", b"poison", error="boom")

    await qm.recovery_reconcile_dead()
    accepted, written = await qm.recovery_seed_counts()
    await qm.refresh_all_stats()
    stats = qm.derive_all_stats()

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

    await qm.refresh_all_stats()
    stats = qm.derive_all_stats()
    residual = accepted - written - stats["in_queue_total"] - stats["dead_total"]
    assert residual == 0


# ---------------------------------------------------------------------------
# spool stats (Change 2, split for the /status-must-never-scan fix):
# refresh_spool_stats() does the ONE-AND-ONLY directory scan and populates
# the cache; spool_stats() is a synchronous, read-only, never-scans cache
# read for /status. See QueueManager.refresh_spool_stats/spool_stats.
# ---------------------------------------------------------------------------


async def test_refresh_spool_stats_counts_pending_session_and_bytes(qm, tmp_path):
    """A session with unconsumed log data counts as pending; total bytes
    reflects every file on disk (.log + .offset + .dead.jsonl)."""
    await qm.append("s1", b"a")
    await qm.append("s1", b"b")

    stats = await qm.refresh_spool_stats()

    assert stats["pending_sessions"] == 1
    queues_dir = tmp_path / "queues"
    expected_bytes = sum(p.stat().st_size for p in queues_dir.iterdir())
    assert stats["spool_bytes_total"] == expected_bytes
    assert expected_bytes > 0


async def test_refresh_spool_stats_fully_committed_session_not_pending(qm):
    """A session whose committed offset reaches EOF is NOT counted as
    pending, even though its .log/.offset files still occupy disk space
    (spool_bytes_total still reflects them)."""
    await qm.append("s1", b"a")
    line = b"a\n"
    await qm.commit("s1", len(line))

    stats = await qm.refresh_spool_stats()

    assert stats["pending_sessions"] == 0
    assert stats["spool_bytes_total"] > 0  # log + offset files still on disk


async def test_refresh_spool_stats_dead_letter_only_session_not_pending(qm):
    """A dead-letter-only key (no .log) contributes bytes but is never
    counted as a pending session -- pending_sessions is defined purely over
    .log files with unconsumed data."""
    await qm.dead_letter("s-dead", b"poison", error="boom")

    stats = await qm.refresh_spool_stats()

    assert stats["pending_sessions"] == 0
    assert stats["spool_bytes_total"] > 0


async def test_refresh_spool_stats_multiple_sessions_aggregate(qm):
    """pending_sessions counts sessions independently; bytes sum across all."""
    await qm.append("s1", b"a")  # pending
    await qm.append("s2", b"b")
    line = b"b\n"
    await qm.commit("s2", len(line))  # fully committed, not pending
    await qm.append("s3", b"c")  # pending

    stats = await qm.refresh_spool_stats()

    assert stats["pending_sessions"] == 2


async def test_refresh_spool_stats_returns_only_aggregate_keys_no_identifiers(qm):
    """/status is unauthenticated: the spool snapshot must carry ONLY the
    three aggregate fields -- no session ids, workspace names, or per-key
    table of any kind, so there's nothing to accidentally leak through
    /status."""
    await qm.append("my-secret-session-id", b"a")
    await qm.dead_letter("another-session-id", b"poison", error="boom")

    stats = await qm.refresh_spool_stats()

    assert set(stats.keys()) == {
        "pending_sessions",
        "spool_bytes_total",
        "corrupt_offsets",
    }
    serialized = repr(stats)
    assert "my-secret-session-id" not in serialized
    assert "another-session-id" not in serialized

    # spool_stats() (the /status read path) exposes the exact same snapshot.
    assert qm.spool_stats() == stats


async def test_refresh_spool_stats_always_scans_no_ttl_gate(qm, monkeypatch):
    """refresh_spool_stats() has NO TTL gate -- every call re-scans the
    directory. The old read-side TTL is gone; cadence is now owned entirely
    by the background refresher loop (main.py._spool_stats_refresher)."""
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

    await qm.refresh_spool_stats()
    await qm.refresh_spool_stats()  # no TTL -- scans again every time
    assert calls["n"] == 2


def test_spool_stats_is_synchronous_read_only_and_never_scans(qm, monkeypatch):
    """spool_stats() must NEVER touch the filesystem: it is a pure,
    synchronous cache read. An empty cache returns the unavailable sentinel
    (never scans, never raises); a populated cache returns exactly that
    snapshot -- this is the entire point of the refresh/read split that
    fixes /status stalling on a large spool (see the module docstring)."""
    import pathlib

    def _boom(self: pathlib.Path) -> None:
        raise AssertionError("spool_stats() touched the filesystem via iterdir()")

    monkeypatch.setattr(pathlib.Path, "iterdir", _boom)

    # Empty cache: the sentinel, no scan, no raise.
    assert qm.spool_stats() == {
        "pending_sessions": -1,
        "spool_bytes_total": -1,
        "corrupt_offsets": -1,
    }

    # Populated cache: returns exactly that snapshot, still without scanning.
    qm._spool_cache = {
        "pending_sessions": 3,
        "spool_bytes_total": 12345,
        "corrupt_offsets": 0,
    }
    assert qm.spool_stats() == {
        "pending_sessions": 3,
        "spool_bytes_total": 12345,
        "corrupt_offsets": 0,
    }


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


async def test_refresh_spool_stats_empty_directory(qm):
    """An empty spool directory reports zero for all three aggregates."""
    stats = await qm.refresh_spool_stats()
    assert stats == {
        "pending_sessions": 0,
        "spool_bytes_total": 0,
        "corrupt_offsets": 0,
    }


# ---------------------------------------------------------------------------
# refresh_spool_stats health-endpoint safety (regression, ci_pr73-ueh):
# the background refresher (main.py._spool_stats_refresher) calls
# refresh_spool_stats() unconditionally. It uses iterdir() (raises on a
# missing dir), unlike every sibling reader which uses glob() (empty on a
# missing dir), so a transiently-unavailable queue dir or a corrupt .offset
# MUST degrade to a sentinel, never raise -- an escape would kill the
# refresher loop and (pre-split) would have 500'd /status directly.
# ---------------------------------------------------------------------------


async def test_refresh_spool_stats_missing_directory_returns_sentinel(qm, tmp_path):
    """A missing queue dir makes iterdir() raise FileNotFoundError;
    refresh_spool_stats() must return the degraded sentinel {-1, -1, -1}
    rather than propagate (which would kill the background refresher loop --
    e.g. during an Azure Files remount)."""
    import shutil

    shutil.rmtree(tmp_path / "queues")

    stats = await qm.refresh_spool_stats()

    assert stats == {
        "pending_sessions": -1,
        "spool_bytes_total": -1,
        "corrupt_offsets": -1,
    }


async def test_refresh_spool_stats_sentinel_is_not_cached(qm, tmp_path):
    """A failed refresh does NOT overwrite the cache: once the directory is
    healthy again, the very next refresh recovers the real aggregate numbers
    (and, meanwhile, spool_stats() never reports a fabricated sentinel as a
    real snapshot)."""
    import shutil

    queues_dir = tmp_path / "queues"
    shutil.rmtree(queues_dir)
    assert await qm.refresh_spool_stats() == {
        "pending_sessions": -1,
        "spool_bytes_total": -1,
        "corrupt_offsets": -1,
    }
    assert qm._spool_cache is None  # the sentinel was never cached

    # Filesystem recovers.
    queues_dir.mkdir(parents=True, exist_ok=True)
    await qm.append("s1", b"a")

    stats = await qm.refresh_spool_stats()
    assert stats["pending_sessions"] == 1
    assert stats["spool_bytes_total"] > 0
    assert qm.spool_stats() == stats  # now cached, and readable without a scan


async def test_refresh_spool_stats_corrupt_offset_does_not_sink_scan(qm):
    """A corrupt/unreadable .offset for one session must not fail the whole
    scan: that file's bytes still count, only its pending calc is skipped."""
    await qm.append("s1", b"a")
    qm._offset_path("s1").write_text("not-a-number", encoding="utf-8")

    stats = await qm.refresh_spool_stats()

    assert stats["spool_bytes_total"] > 0
    assert isinstance(stats["pending_sessions"], int)


async def test_refresh_spool_stats_counts_corrupt_offsets(qm):
    """A non-numeric .offset is surfaced as an aggregate corrupt_offsets count
    (the ONLY visibility signal -- no logging). A healthy session contributes 0."""
    await qm.append("s-good", b"a")  # valid: no .offset yet -> committed 0
    await qm.append("s-bad", b"a")
    qm._offset_path("s-bad").write_text("not-a-number", encoding="utf-8")

    stats = await qm.refresh_spool_stats()

    assert stats["corrupt_offsets"] == 1
    assert stats["spool_bytes_total"] > 0  # corrupt file's bytes still counted


async def test_refresh_spool_stats_healthy_offsets_report_zero_corrupt(qm):
    """corrupt_offsets is 0 when every .offset is a valid integer (it must not
    fire on the normal committed-offset path)."""
    await qm.append("s1", b"a")
    line = b"a\n"
    await qm.commit("s1", len(line))  # writes a valid numeric .offset

    stats = await qm.refresh_spool_stats()

    assert stats["corrupt_offsets"] == 0


async def test_spool_stats_age_seconds_none_before_any_refresh(qm):
    """spool_stats_age_seconds() is None until refresh_spool_stats() has
    produced its first snapshot -- pure arithmetic, never touches disk."""
    assert qm.spool_stats_age_seconds() is None


async def test_spool_stats_age_seconds_reflects_time_since_refresh(qm):
    """After a refresh, spool_stats_age_seconds() reports elapsed time (>= 0,
    rounded to 1dp) and grows as time passes -- how /status tells a fresh
    snapshot from a stale one."""
    await qm.refresh_spool_stats()
    age0 = qm.spool_stats_age_seconds()
    assert isinstance(age0, float)
    assert age0 >= 0.0

    qm._spool_cache_at = time.monotonic() - 5.0
    age1 = qm.spool_stats_age_seconds()
    assert age1 >= 5.0


async def test_recovery_seed_counts_residual_zero_after_trimming_dead_letter_session(
    qm,
):
    """Conservation holds after Change 2's early trim: a session whose
    .log/.offset have already been reclaimed by delete_drained() (leaving
    only its .dead.jsonl) still seeds a zero residual -- the dead record is
    accounted for entirely through `dead`, not lost when its log disappears."""
    await qm.append("s1", b"a")
    line = b"a\n"
    await qm.commit("s1", len(line))  # fully drained/committed
    await qm.dead_letter("s1", b"a", error="boom")  # a dead-letter record too

    assert await qm.delete_drained("s1") is True  # log/offset reclaimed
    assert not qm._log_path("s1").exists()
    assert not qm._offset_path("s1").exists()
    assert qm._dead_path("s1").exists()

    accepted, written = await qm.recovery_seed_counts()
    await qm.refresh_all_stats()
    stats = qm.derive_all_stats()
    residual = accepted - written - stats["in_queue_total"] - stats["dead_total"]
    assert residual == 0


# ---------------------------------------------------------------------------
# reclaim_drained_orphans: reclaim .log/.offset for keys with no live worker
# (fix/trim-spool-as-processed) -- the per-session idle-branch trim only ever
# runs for a session with a live drain worker, so a session that fully
# drained and then went away keeps its files forever without this sweep.
# ---------------------------------------------------------------------------


async def test_reclaim_drained_orphans_removes_fully_drained_key(qm, tmp_path):
    """A fully-drained (committed-to-EOF) key's .log/.offset are removed, and
    the exact byte size of that log is reported back."""
    line = b"hello\n"
    await qm.append("s1", line)
    await qm.commit("s1", len(line))
    log_size = qm._log_path("s1").stat().st_size
    assert log_size == len(line)

    result = await qm.reclaim_drained_orphans()

    assert result == (1, log_size)
    assert not qm._log_path("s1").exists()
    assert not qm._offset_path("s1").exists()


async def test_reclaim_drained_orphans_keeps_dead_letters(qm):
    """A reclaimed key's .dead.jsonl survives -- only .log/.offset are removed."""
    line = b"hello\n"
    await qm.append("s1", line)
    await qm.commit("s1", len(line))
    await qm.dead_letter("s1", b"poison", error="boom")

    result = await qm.reclaim_drained_orphans()

    assert result == (1, len(line))
    assert not qm._log_path("s1").exists()
    assert qm._dead_path("s1").exists()  # retained
    assert len(await qm.read_dead_letters("s1")) == 1


async def test_reclaim_drained_orphans_skips_key_with_uncommitted_bytes(qm):
    """A key whose log has bytes beyond the committed offset is left
    entirely untouched -- delete_drained refuses it, so it is never counted."""
    await qm.append("s-pending", b"a")  # no commit() -> committed offset is 0

    result = await qm.reclaim_drained_orphans()

    assert result == (0, 0)
    assert qm._log_path("s-pending").exists()


async def test_reclaim_drained_orphans_ignores_dead_letter_only_key(qm):
    """A key with only a .dead.jsonl (no .log at all) reclaims nothing --
    delete_drained returns True for it (nothing to remove), but with a zero
    log size it must not be counted as a reclaimed key."""
    await qm.dead_letter("s-dead-only", b"poison", error="boom")

    result = await qm.reclaim_drained_orphans()

    assert result == (0, 0)
    assert qm._dead_path("s-dead-only").exists()


async def test_reclaim_drained_orphans_mixed_directory(qm):
    """Two fully-drained keys and one pending key: only the drained keys are
    reclaimed (count and byte total cover exactly those two), and the
    pending key's files are left in place."""
    line_a = b"aaa\n"
    line_b = b"bbbbb\n"
    await qm.append("s-drained-a", line_a)
    await qm.commit("s-drained-a", len(line_a))
    await qm.append("s-drained-b", line_b)
    await qm.commit("s-drained-b", len(line_b))
    await qm.append("s-pending", b"c")  # never committed

    result = await qm.reclaim_drained_orphans()

    assert result == (2, len(line_a) + len(line_b))
    assert not qm._log_path("s-drained-a").exists()
    assert not qm._offset_path("s-drained-a").exists()
    assert not qm._log_path("s-drained-b").exists()
    assert not qm._offset_path("s-drained-b").exists()
    assert qm._log_path("s-pending").exists()


async def test_reclaim_drained_orphans_per_key_failure_does_not_abort_pass(
    qm, monkeypatch
):
    """One key raising out of delete_drained() must not stop the sweep --
    the other drained key is still reclaimed and nothing propagates."""
    line_good = b"good\n"
    line_bad = b"bad\n"
    await qm.append("s-good", line_good)
    await qm.commit("s-good", len(line_good))
    await qm.append("s-bad", line_bad)
    await qm.commit("s-bad", len(line_bad))

    original_delete_drained = qm.delete_drained

    async def _flaky_delete_drained(session_id: str) -> bool:
        if session_id == "s-bad":
            raise OSError("simulated failure")
        return await original_delete_drained(session_id)

    monkeypatch.setattr(qm, "delete_drained", _flaky_delete_drained)

    result = await qm.reclaim_drained_orphans()

    assert result == (1, len(line_good))
    assert not qm._log_path("s-good").exists()
    assert qm._log_path("s-bad").exists()  # left untouched after the failure
