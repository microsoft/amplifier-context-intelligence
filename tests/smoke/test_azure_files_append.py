"""Real-mount smoke test -- proves the fix on ACTUAL storage.

WHY THIS FILE EXISTS
---------------------
``tests/test_durable_append_framing.py`` proves the fix's serialization
logic is correct, but it runs on tmpfs/ext4, where POSIX ``O_APPEND`` already
happens to be atomic. Those tests therefore pass for the code's actual reason
AND for the filesystem's incidental reason simultaneously -- they cannot, on
their own, rule out "this only works because ext4 is forgiving." Azure
Files/SMB (and the new NFS share) do NOT provide atomic ``O_APPEND``; that gap
is exactly what produces torn/merged-line append corruption. This file
closes that gap by running against the REAL backend.

THIS IS A MANUAL SHIP GATE, NOT A CI TEST
------------------------------------------
CI has no Azure Files / NFS mount, so this test is **skipped unless**
``CI_SMOKE_QUEUES_DIR`` is set to a writable directory ON THE MOUNT UNDER
TEST. Run it once by hand against each backend and paste BOTH outputs into
the PR (success criterion 12)::

    CI_SMOKE_QUEUES_DIR=/mnt/azurefiles/smoke-$(date +%s) \\
        uv run pytest tests/smoke/test_azure_files_append.py -q -s

    CI_SMOKE_QUEUES_DIR=/mnt/nfs-share/smoke-$(date +%s) \\
        uv run pytest tests/smoke/test_azure_files_append.py -q -s

TWO HALVES, CONTROL FIRST (non-vacuity)
-----------------------------------------
1. **Control (evaluated FIRST)** -- a two-OS-PROCESS race using the PRE-FIX
   writer (a bare, unguarded ``open(path, "ab").write(line)``, exactly what
   production did before the fix). This is the load-bearing half: if the mount
   under test cannot reproduce a torn/merged line with the OLD, unguarded
   writer, then the mount is not exhibiting the non-atomic-append condition
   that makes this corruption possible, and a "PASS" from the real fix below would be
   proving nothing -- passing for the wrong reason all over again. In that
   case this test reports ``SKIPPED: environment did not reproduce
   tearing``, NEVER a green pass.
2. **The fix, on the real mount** -- only reached if (1) proved the mount can
   tear. 50 concurrent appends of mixed sizes (4 KiB .. 1.5 MiB) to one key,
   through the REAL, shipped ``QueueManager`` (the v2.1 ``_guard`` fix).
   Every line must parse via the REAL ``SessionRegistry._parse_line``, and
   the byte-multiset of what comes back must equal the byte-multiset of what
   went in -- zero loss, zero duplication, zero merge.

LOCAL-TMPFS/EXT4 LIMITATION (why this self-skips on a dev box)
-----------------------------------------------------------------
On a local filesystem where ``O_APPEND`` is already atomic (ext4, tmpfs,
most local disks), the control in half (1) above CANNOT tear, by
construction -- the kernel gives atomicity the pre-fix writer here happens
to rely on accidentally. On such a filesystem this whole test module
therefore reports SKIPPED, not PASSED, when ``CI_SMOKE_QUEUES_DIR`` is
pointed at a purely local directory. That is the correct, honest outcome --
it is NOT evidence the fix is broken, and it is NOT a substitute for running
this against the real SMB/NFS mount.

Both halves run inside ONE test function so that no test-selection flag
(``-k``, ``-m``) can accidentally run half (2) without first proving half (1)
reproduced tearing.
"""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
import random
import uuid
from pathlib import Path
from typing import Any

import pytest

from context_intelligence_server.queue_manager import QueueManager
from context_intelligence_server.registry import SessionRegistry

_QUEUES_DIR_ENV = "CI_SMOKE_QUEUES_DIR"

if not os.environ.get(_QUEUES_DIR_ENV):
    # Module-level skip: aborts the REST of this module's execution, so no
    # test function below is even defined. This is the primary safety net --
    # it fires regardless of any -k/-m selection aimed at this file.
    pytest.skip(
        f"set {_QUEUES_DIR_ENV} to a real mounted share to run "
        "(this is a manual ship gate, not a CI test)",
        allow_module_level=True,
    )

# Marker for explicit selection/deselection (``-m smoke`` / ``-m "not smoke"``).
# Deliberately NOT added to pyproject.toml's [tool.pytest.ini_options] markers
# list -- that file is out of scope for this change, and the repo does not
# set `--strict-markers` / `filterwarnings = ["error"]` (confirmed:
# pyproject.toml has no `addopts`), so this produces at most a
# PytestUnknownMarkWarning, never a collection error. The env-var gate above
# is the real, load-bearing skip mechanism; this marker is a convenience.
pytestmark = pytest.mark.smoke


# --------------------------------------------------------------------------
# Shared record helpers
# --------------------------------------------------------------------------


def _record(event: str, *, size: int) -> bytes:
    """One event record, same shape the real ingest path serializes.

    ``json.dumps(..., ensure_ascii=True)`` (the default) is precondition P1:
    the serialized bytes never contain a raw ``0x0A`` except
    the terminator ``QueueManager.append`` adds. Any newline that DOES show
    up mid-record downstream is therefore proof of framing corruption, not
    payload content -- which is exactly what makes "does every returned line
    parse as one clean JSON object" a valid tear detector.

    Returned WITHOUT a trailing newline -- ``QueueManager.append`` adds
    exactly one, and ``read_batch`` strips exactly one on the way out, so
    comparing this raw value against ``read_batch``'s output is a direct
    byte-multiset comparison.
    """
    filler = "x" * max(0, size - 200)
    obj = {"event": event, "workspace": "-smoke-tear", "data": {"payload": filler}}
    return json.dumps(obj, separators=(",", ":")).encode("utf-8")


def _parses(raw: bytes) -> bool:
    """True iff the REAL drain-side parser accepts this line."""
    try:
        SessionRegistry._parse_line(raw)  # the real parser, not a copy
    except Exception:  # noqa: BLE001 - mirrors registry.py's own broad catch
        return False
    return True


# --------------------------------------------------------------------------
# Half 2 -- the non-vacuity control (pre-fix, unguarded writer)
# --------------------------------------------------------------------------

# Comfortably above the 256 KiB floor the task calls for, to maximize the
# window a non-atomic-append backend has to interleave two writers' bytes.
_CONTROL_RECORD_SIZE = 384 * 1024
_CONTROL_RECORDS_PER_PROCESS = 10


def _prefix_unguarded_append(
    path_str: str,
    records: list[bytes],
    ready_evt: Any,
    start_evt: Any,
) -> None:
    """PRE-FIX writer: exactly what production did before the fix.

    No ``_KeyGuard``, no ``threading.Lock``, no admission gate -- a bare
    ``open(path, "ab").write(line)`` per record. Run this from TWO separate
    OS PROCESSES against the SAME path with no synchronization between them,
    and a non-atomic-append backend (Azure Files/SMB, NFS) can interleave
    the two processes' bytes into one torn/merged line. A backend with
    atomic ``O_APPEND`` (ext4, tmpfs) cannot -- which is the whole point of
    this control.
    """
    ready_evt.set()
    start_evt.wait(timeout=30)
    path = Path(path_str)
    for rec in records:
        line = rec if rec.endswith(b"\n") else rec + b"\n"
        with open(path, "ab") as f:  # deliberately the PRE-FIX primitive
            f.write(line)


def _run_control_race(target_path: Path) -> tuple[int, int]:
    """Race two unguarded OS processes appending to ``target_path``.

    Returns ``(lines_scanned, lines_that_failed_to_parse)``. A backend that
    can tear will produce at least one line that fails ``_parse_line`` --
    two records' bytes concatenated without a clean JSON boundary between
    them (or, symmetrically, a record split so its tail lands elsewhere).
    """
    if target_path.exists():
        target_path.unlink()

    records_a = [
        _record(f"control-a-{i}", size=_CONTROL_RECORD_SIZE)
        for i in range(_CONTROL_RECORDS_PER_PROCESS)
    ]
    records_b = [
        _record(f"control-b-{i}", size=_CONTROL_RECORD_SIZE)
        for i in range(_CONTROL_RECORDS_PER_PROCESS)
    ]

    ctx = multiprocessing.get_context("spawn")
    ready_a, ready_b, start = ctx.Event(), ctx.Event(), ctx.Event()
    proc_a = ctx.Process(
        target=_prefix_unguarded_append,
        args=(str(target_path), records_a, ready_a, start),
    )
    proc_b = ctx.Process(
        target=_prefix_unguarded_append,
        args=(str(target_path), records_b, ready_b, start),
    )
    proc_a.start()
    proc_b.start()
    assert ready_a.wait(timeout=30), "control process A never signalled ready"
    assert ready_b.wait(timeout=30), "control process B never signalled ready"
    start.set()  # release both processes at (as close as we can get to) once
    proc_a.join(timeout=120)
    proc_b.join(timeout=120)
    assert not proc_a.is_alive(), "control process A did not finish in time"
    assert not proc_b.is_alive(), "control process B did not finish in time"
    assert proc_a.exitcode == 0, f"control process A failed: exitcode={proc_a.exitcode}"
    assert proc_b.exitcode == 0, f"control process B failed: exitcode={proc_b.exitcode}"

    raw = target_path.read_bytes()
    # Mirror QueueManager.read_batch's own framing rule: a line only counts
    # if it is newline-terminated. A trailing unterminated fragment (the
    # process could have been mid-write at the very end) is not scored --
    # it is neither a proven-good nor a proven-torn line.
    segments = raw.split(b"\n")
    complete = segments[:-1]
    scanned = 0
    failed = 0
    for seg in complete:
        if not seg:
            continue
        scanned += 1
        if not _parses(seg):
            failed += 1
    return scanned, failed


# --------------------------------------------------------------------------
# Half 1 -- the fix, on the real mount
# --------------------------------------------------------------------------

_FIX_APPEND_COUNT = 50
_FIX_SIZE_MIN = 4 * 1024
_FIX_SIZE_MAX = int(1.5 * 1024 * 1024)


async def _run_fix_holds(queues_dir: Path) -> None:
    """50 concurrent appends of mixed sizes through the REAL, shipped code."""
    rng = random.Random(90042)  # deterministic mix of sizes across runs
    records = [
        _record(f"fix-{i}", size=rng.randint(_FIX_SIZE_MIN, _FIX_SIZE_MAX))
        for i in range(_FIX_APPEND_COUNT)
    ]
    key = f"smoke-fix-{uuid.uuid4()}"

    qm = QueueManager(queues_dir)
    await asyncio.gather(*(qm.append(key, rec) for rec in records))

    batch = await qm.read_batch(key, max_items=_FIX_APPEND_COUNT + 10)

    assert len(batch.lines) == len(records), (
        f"expected {len(records)} complete lines back, got {len(batch.lines)} "
        "-- possible loss, duplication, or a merge that produced fewer lines "
        "than records written"
    )
    for line in batch.lines:
        # Raises loud (test fails) on the first line that is not one clean,
        # complete JSON object -- i.e. any merged/torn line.
        SessionRegistry._parse_line(line)  # the real drain-side parser

    # Byte-multiset equality: zero loss, zero duplication, zero merge. Every
    # record appears back exactly once, byte-for-byte, in any order.
    assert sorted(batch.lines) == sorted(records), (
        "byte-multiset mismatch between appended records and read-back "
        "lines -- order-independent proof of loss/dup/merge"
    )


# --------------------------------------------------------------------------
# The gate
# --------------------------------------------------------------------------


async def test_azure_files_append_fix_holds_on_real_mount() -> None:
    """Control first (non-vacuity), then the fix on the real mount.

    Reports SKIPPED -- never a green PASS -- if the control does not
    reproduce tearing, because that means the mount under test did not
    exhibit the non-atomic-append condition the fix exists for, and a pass
    on half 1 in that case would prove nothing (see module docstring).
    """
    queues_dir = Path(os.environ[_QUEUES_DIR_ENV])
    queues_dir.mkdir(parents=True, exist_ok=True)

    # ---- Half 2 (control) runs FIRST and gates the verdict ----
    control_path = queues_dir / f"smoke-control-{uuid.uuid4()}.log"
    scanned, failed = _run_control_race(control_path)
    print(  # this output is the evidence pasted into the PR
        f"[smoke control] lines_scanned={scanned} lines_failed_to_parse={failed} "
        f"path={control_path}"
    )
    if failed == 0:
        pytest.skip(
            "environment did not reproduce tearing; smoke result inconclusive "
            f"(control wrote {scanned} complete lines with the PRE-FIX unguarded "
            "writer and every one still parsed cleanly -- this mount's "
            "O_APPEND is atomic, e.g. local ext4/tmpfs; this does NOT confirm "
            "the fix and must be re-run against the real Azure Files SMB / NFS "
            "mount)"
        )

    # ---- Half 1: the control proved the mount CAN tear -- now prove the ----
    # ---- real, shipped fix prevents it under the same class of contention.
    await _run_fix_holds(queues_dir)
    print(  # evidence for the PR
        f"[smoke fix] {_FIX_APPEND_COUNT} concurrent appends "
        f"({_FIX_SIZE_MIN}..{_FIX_SIZE_MAX} bytes) all parsed clean, "
        "byte-multiset preserved"
    )
