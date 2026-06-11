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
