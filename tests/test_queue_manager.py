"""Tests for the on-disk durable queue manager (Phase B1)."""

from __future__ import annotations

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
