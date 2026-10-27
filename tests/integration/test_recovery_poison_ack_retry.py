"""A recovery retry must never be satisfied by a poison drainer's old acknowledgement.

The race (PR #126 merge blocker): the poison drainer publishes the receipt as
``quarantined`` and only then acknowledges the old queue line (an offset
rename).  An admin retry that lands in between archives the dead letter,
resets the receipt, and REUSES the still-queued old line.  When the old
acknowledgement then lands, reconciliation reads "enqueued + committed" as
proof of a graph write and marks the receipt ``written`` with no flush.

Everything below runs the real admission/admin routes, a really spawned and
draining recovery worker, SQLite receipts and queue files.  Only Neo4j is
replaced (an in-memory graph that records which flushes actually completed a
write).  The spies delegate to the real code and change no behavior; they
only record ordering and signal events.  Coordination is event based: the old
acknowledgement is parked on a ``threading.Event`` inside the offset rename,
and every wait is bounded by ``BOUND``.

What each test catches:

* ``test_sequential_...``  control: with the old ack landed first, a retry
  appends a fresh line and ``written`` requires a completed flush.  Fails if
  the harness (or the ordinary retry path) is broken.
* ``test_retry_waits_...`` the retry may not archive or reset while the old
  acknowledgement is in flight; once it lands the retry redelivers a fresh
  line.  Fails if quarantine+ack and archive+reset stop being serialized.
* ``test_retry_never_marks_written_...``  whatever the mechanism, a receipt is
  never requested ``written`` before a graph write completed.  Fails if the
  old ack can again be read as proof of the retried delivery.
* ``test_retry_stays_blocked_...``  cancelling the delayed-ack drainer must
  not release the retry while the rename is still in flight.  Fails if the
  serialization is dropped the moment the drainer's await is cancelled.

The second interleaving (lease mode, real static-key authentication): two
identical FIRST submissions both classify ``new``; the second is admitted as a
``duplicate`` and its repair snapshots the receipt as ``enqueued`` with no dead
letter yet, then stalls.  Meanwhile the poison is dead-lettered, quarantined
and acknowledged, and an operator retry archives it and resets the receipt to
``retry_pending``, then waits for the outbox.  When the stalled repair resumes
it sees a committed line and marks the receipt ``written`` with no flush, and
the retry then finds nothing left to bridge.

Two paths are valid for how the second submission meets the first.  Either both
classify ``new`` (the stale-repair path above), or admission is serialized
before classification, so the second is already waiting on the held shared lock
and later classifies ``duplicate``.  The scenario resolves which from positive
signals, never from timing.  Either way the retry must be accepted, append a
fresh line, and ``written`` must follow a completed flush.

* ``test_sequential_duplicate_...``  control: the duplicate repair finishes
  before the poison publishes; a retry then redelivers normally.
* ``test_duplicate_repair_...``  whatever the mechanism, that repair may never
  request ``written`` before a graph write completed.
* ``test_retry_after_duplicate_...``  the designed outcome: the retry appends a
  fresh line and ``written`` follows a completed flush.
"""

from __future__ import annotations

import asyncio
import contextvars
import copy
import hashlib
import io
import os
import sys
import threading
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal
from unittest.mock import MagicMock

import httpx
import pytest

from context_intelligence_server import main as main_module
from context_intelligence_server import registry as registry_module
from context_intelligence_server.services import GraphState
from tests.test_native_recovery import (
    _admission_body,
    _recovery_body,
    _source_headers,
    _write_lease,
)
from tests.test_recovery_review_fixes import SESSION, _lease_settings, _legacy_body

BOUND = 5.0  # hard upper bound for every wait; nothing waits unbounded
# Scheduler turns (not wall-clock) given to a cancelled drainer to unwind and
# wake anything waiting on a lock it held.
LOOP_TURNS = 50

# Obviously fake credentials for the authenticated lease scenario (tests only).
FAKE_DATA_TOKEN = "fake-data-token-for-duplicate-repair-test"
FAKE_ADMIN_TOKEN = "fake-admin-token-for-duplicate-repair-test"
FAKE_CONTRIBUTOR = "duplicate-repair-contributor"

Mode = Literal["permit", "lease"]
MODES = pytest.mark.parametrize("mode", ["permit", "lease"])

_in_retry: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "in_retry", default=False
)
# Request tags, set inside the request's own context so they survive the child
# task that ``_await_uninterrupted`` creates for the final admission step (task
# identity would not).  ``_request_tag``: order in which the first two
# submissions entered the route.  ``_classified_new``: the index at which this
# request's classification came back ``new``.
_request_tag: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "request_tag", default=None
)
_classified_new: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "classified_new", default=None
)


def _will_wait(lock: asyncio.Lock) -> bool:
    """Whether ``lock.acquire()`` called now would actually WAIT.

    This is the negation of asyncio's fast path (``not locked and no live
    waiters``).  ``locked()`` alone misses the FIFO hand-off: right after a
    release the lock reports ``locked() == False`` while a woken, not yet
    resumed waiter still has priority, so a new ``acquire()`` queues behind it.
    asyncio has no public waiter API, so this reads ``_waiters`` (instrumentation
    only; a missing attribute counts as no waiters).
    """
    waiters = getattr(lock, "_waiters", None) or ()
    return lock.locked() or any(not w.cancelled() for w in waiters)


# ---------------------------------------------------------------------------
# Test doubles: Neo4j stand-in
# ---------------------------------------------------------------------------


@dataclass
class FlushProbe:
    """What the graph stand-in saw, and an optional hold on the next flush."""

    hold: bool = False
    entered: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    entered_count: int = 0
    completed_writes: int = 0  # flushes that made at least one new node durable
    graphs: list[ProbeGraph] = field(default_factory=list)

    def committed_start_events(self) -> list[dict[str, Any]]:
        return [
            node
            for graph in self.graphs
            for node in graph.committed.values()
            if node.get("event_name") == "session:start"
            and node.get("session_id") == SESSION
        ]


class ProbeGraph(GraphState):
    """In-memory graph: writes are visible at once (like the real store's
    buffer); ``committed`` is what a flush that COMPLETED made durable."""

    def __init__(self, probe: FlushProbe, **_ignored: Any) -> None:
        super().__init__()
        self._probe = probe
        self.committed: dict[str, dict[str, Any]] = {}
        probe.graphs.append(self)

    async def flush(self) -> None:
        probe = self._probe
        probe.entered_count += 1
        probe.entered.set()
        if probe.hold:
            await probe.release.wait()
        await super().flush()
        fresh = set(self._nodes) - set(self.committed)
        self.committed = copy.deepcopy(self._nodes)
        if fresh:
            probe.completed_writes += 1


# ---------------------------------------------------------------------------
# The environment under test
# ---------------------------------------------------------------------------


def _set_event() -> asyncio.Event:
    event = asyncio.Event()
    event.set()
    return event


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _authenticated_lease_settings(tmp_path: Path) -> Any:
    """Lease-mode app with REAL static auth: a data key and a distinct admin key."""
    from context_intelligence_server.config import Settings

    return Settings(
        auth_mode="static",
        api_keys={_digest(FAKE_DATA_TOKEN): {"id": FAKE_CONTRIBUTOR}},
        admin_api_key_sha256=_digest(FAKE_ADMIN_TOKEN),
        api_keys_store_path=str(tmp_path / "keys.json"),
        entra_identities_store_path=str(tmp_path / "entra.json"),
        queues_path=str(tmp_path / "live"),
        recovery={  # type: ignore[arg-type]
            "enabled": True,
            "permit_required": False,
            "guard_lease_path": str(tmp_path / "lease.json"),
            "receipt_store_path": str(tmp_path / "receipts.sqlite3"),
            "queues_path": str(tmp_path / "recovery"),
        },
    )


@dataclass
class Env:
    mode: Mode
    app: Any
    client: httpx.AsyncClient
    recovery_registry: Any
    queue: Any
    receipts: Any
    recovery_dir: Path
    lease_path: Path
    probe: FlushProbe
    ack: SimpleNamespace  # threading events around the old offset rename
    trace: list[str] = field(default_factory=list)
    owned: list[asyncio.Future[Any]] = field(default_factory=list)
    poison: bool = True
    origin: dict[str, Any] = field(default_factory=dict)
    written_requests: list[tuple[int, int]] = field(default_factory=list)
    quarantined: asyncio.Event = field(default_factory=asyncio.Event)
    written: asyncio.Event = field(default_factory=asyncio.Event)
    retry_entered: asyncio.Event = field(default_factory=asyncio.Event)
    retry_parked: asyncio.Event = field(default_factory=asyncio.Event)
    retry_resumed: asyncio.Event = field(default_factory=asyncio.Event)
    admin_headers: dict[str, str] = field(default_factory=dict)
    # Duplicate-repair scenario; all inert unless hold_duplicate_repair=True.
    classified_new: int = 0
    # Explicit "this request started" barrier: the first two submissions entering
    # the route (before any lock or classification), tagged by arrival order.
    request_seq: int = 0
    both_started: asyncio.Event = field(default_factory=asyncio.Event)
    # Where the other submission is while the first one is held at classification:
    # also classifying new (baseline), or waiting on the held shared lock (when
    # admission is serialized before classification).
    first_classified: asyncio.Event = field(default_factory=asyncio.Event)
    parked_tag: int | None = None
    pending_shared: dict[int, int] = field(default_factory=dict)
    second_blocked: asyncio.Event = field(default_factory=asyncio.Event)
    enqueued: asyncio.Event = field(default_factory=asyncio.Event)
    classify_release: list[asyncio.Event] = field(
        default_factory=lambda: [asyncio.Event(), asyncio.Event()]
    )
    both_classified: asyncio.Event = field(default_factory=asyncio.Event)
    poison_go: asyncio.Event = field(default_factory=_set_event)
    dup_parked: asyncio.Event = field(default_factory=asyncio.Event)
    dup_release: asyncio.Event = field(default_factory=asyncio.Event)
    drainer_parked: asyncio.Event = field(default_factory=asyncio.Event)
    landed: asyncio.Event = field(default_factory=asyncio.Event)

    def log_lines(self) -> int:
        try:
            data = (self.recovery_dir / f"{SESSION}.log").read_bytes()
        except FileNotFoundError:
            return 0
        return sum(1 for raw in data.split(b"\n") if raw.strip())

    async def admit(self) -> None:
        if self.mode == "permit":
            body = _recovery_body()
            permit = await self.client.post(
                "/recovery/admissions", json=_admission_body(body)
            )
            assert permit.status_code == 201, permit.text
            accepted = await self.client.post(
                "/recovery/events",
                json=body,
                headers={"X-Recovery-Permit": permit.json()["permit"]},
            )
        else:
            _write_lease(self.lease_path)
            accepted = await self.client.post("/recovery/events", json=_legacy_body())
        assert accepted.status_code == 202, accepted.text

    async def reconcile(self) -> None:
        """The next real recovery request reconciles the outbox first."""
        if self.mode == "permit":
            await self.client.post(
                "/recovery/admissions", json=_admission_body(_recovery_body())
            )
        else:
            _write_lease(self.lease_path, "lease-reconcile")
            await self.client.post("/recovery/events", json=_legacy_body())

    def start_retry(self) -> asyncio.Task[httpx.Response]:
        if self.mode == "lease":
            _write_lease(self.lease_path, "lease-retry")  # flush needs a live lease
        self.poison = False  # the redelivery is healthy
        task = asyncio.create_task(
            self.client.post(
                "/admin/recovery/retry-quarantined", headers=self.admin_headers
            ),
            name="test-retry",
        )
        self.owned.append(task)
        return task

    def submit_first(self) -> asyncio.Task[httpx.Response]:
        """One real lease-mode first submission of the poison record."""
        task = asyncio.create_task(
            self.client.post("/recovery/events", json=_legacy_body()),
            name="test-submit",
        )
        self.owned.append(task)
        return task


async def _event(event: asyncio.Event, what: str) -> None:
    try:
        await asyncio.wait_for(event.wait(), BOUND)
    except TimeoutError:
        raise AssertionError(f"timed out ({BOUND}s) waiting for {what}") from None


async def _thread_event(event: threading.Event, what: str) -> None:
    if not await asyncio.to_thread(event.wait, BOUND):
        raise AssertionError(f"timed out ({BOUND}s) waiting for {what}")


async def _first_of(env: Env, retry: asyncio.Future[Any]) -> set[str]:
    """Wait until the retry has either FINISHED or BLOCKED on a held lock.

    The old acknowledgement stays parked until this returns, so the retry's
    behavior while it is in flight is fully observed.  Releasing it earlier
    (for example on the first archive call) would let the ack land mid-retry
    and mask the race.  Without serialization the retry finishes on its own;
    with it the retry parks and waits for the ack."""
    return await _wait_first(
        env,
        "the retry to finish or block on a held lock",
        events={"retry_parked": env.retry_parked},
        futures={"retry_finished": retry},
    )


async def _wait_first(
    env: Env,
    what: str,
    *,
    events: dict[str, asyncio.Event] | None = None,
    futures: dict[str, asyncio.Future[Any]] | None = None,
) -> set[str]:
    """Wait (bounded) until any event is set or any future is done; say which."""
    events = events or {}
    futures = futures or {}
    waiters = {name: asyncio.ensure_future(e.wait()) for name, e in events.items()}
    try:
        await asyncio.wait(
            [*waiters.values(), *futures.values()],
            timeout=BOUND,
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        for waiter in waiters.values():
            waiter.cancel()
        if waiters:
            await asyncio.wait(list(waiters.values()), timeout=BOUND)
    hit = {name for name, e in events.items() if e.is_set()}
    hit |= {name for name, f in futures.items() if f.done()}
    assert hit, f"timed out ({BOUND}s) waiting for {what}. trace={env.trace}"
    return hit


async def _finished(env: Env, task: asyncio.Future[Any], what: str) -> Any:
    """Bounded wait for a task, then its result (a task error is a test failure)."""
    done, _ = await asyncio.wait([task], timeout=BOUND)
    assert done, f"{what} did not finish within {BOUND}s. trace={env.trace}"
    return task.result()


def _describe_stuck(task: asyncio.Future[Any]) -> str:
    out = io.StringIO()
    if isinstance(task, asyncio.Task):
        task.print_stack(file=out)
    return f"{task!r}\n{out.getvalue()}"


async def _stop_tasks(tasks: Sequence[asyncio.Future[Any]], stuck: list[str]) -> None:
    """Cancel and wait, bounded; record the stack of anything that will not stop.

    ``asyncio.wait`` never waits past its timeout on a task that refuses to be
    cancelled (``gather`` and ``wait_for`` do), so teardown cannot hang."""
    pending = [t for t in tasks if not t.done()]
    for task in pending:
        task.cancel()
    if pending:
        _, still_running = await asyncio.wait(pending, timeout=BOUND)
        stuck.extend(_describe_stuck(t) for t in still_running)


@asynccontextmanager
async def recovery_env(
    mode: Mode,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    park_ack: bool,
    authenticated: bool = False,
    hold_duplicate_repair: bool = False,
) -> AsyncIterator[Env]:
    from context_intelligence_server.routers.admin import require_admin

    loop = asyncio.get_running_loop()
    baseline = asyncio.all_tasks()
    probe = FlushProbe()
    ack = SimpleNamespace(
        seen=False,
        reached=threading.Event(),
        release=threading.Event(),
        landed=threading.Event(),
    )
    trace: list[str] = []
    retry_events: dict[str, asyncio.Event] = {}
    holder: dict[str, Env] = {}  # the lock spy exists before the Env does

    # --- spy on asyncio.Lock BEFORE the app builds its locks/conditions ------
    real_acquire = asyncio.Lock.acquire

    async def acquire_spy(self: asyncio.Lock) -> Any:
        waits = _will_wait(self)  # evaluated once, at call time
        contended = _in_retry.get() and waits
        if contended:
            state = main_module.app.state
            locks = {
                "outbox": getattr(state, "recovery_outbox_lock", None),
                "quarantine_retry": getattr(
                    state, "recovery_quarantine_retry_lock", None
                ),
            }
            which = next(
                (name for name, lock in locks.items() if lock is self), "other"
            )
            trace.append("retry_parked")
            trace.append(f"retry_waits_on:{which}")
            retry_events["parked"].set()
        task = asyncio.current_task()
        if (
            task is not None
            and task.get_name().startswith("drain-")
            and waits
            and not retry_events["drainer_parked"].is_set()
        ):
            trace.append("drainer_parked")
            retry_events["drainer_parked"].set()
        # A tagged submission about to WAIT on the held shared lock.  Counted
        # while pending, so a wait that already resolved is never mistaken for a
        # persistent block (see Env.second_blocked).
        tag = _request_tag.get()
        current: Env | None = holder.get("env")
        if (
            current is not None
            and tag is not None
            and waits
            and self
            is getattr(main_module.app.state, "recovery_quarantine_retry_lock", None)
        ):
            current.pending_shared[tag] = current.pending_shared.get(tag, 0) + 1
            trace.append(f"request_waits_on_shared:{tag}")
            if current.parked_tag is not None and tag != current.parked_tag:
                trace.append("other_request_blocked_on_shared")
                current.second_blocked.set()
            try:
                result = await real_acquire(self)
            finally:
                current.pending_shared[tag] -= 1
        else:
            result = await real_acquire(self)
        if contended:
            trace.append("retry_resumed")
            retry_events["resumed"].set()
        return result

    monkeypatch.setattr(asyncio.Lock, "acquire", acquire_spy)

    if mode == "permit":
        from context_intelligence_server.config import Settings

        settings = Settings(
            api_key=None,
            api_keys=None,
            api_keys_store_path=str(tmp_path / "keys.json"),
            allow_unauthenticated=True,
            queues_path=str(tmp_path / "live"),
            recovery={  # type: ignore[arg-type]
                "enabled": True,
                "receipt_store_path": str(tmp_path / "receipts.sqlite3"),
                "queues_path": str(tmp_path / "recovery"),
            },
        )
        headers = _source_headers()
    elif authenticated:
        settings = _authenticated_lease_settings(tmp_path)
        headers = {"Authorization": f"Bearer {FAKE_DATA_TOKEN}"}
    else:
        settings = _lease_settings(tmp_path)
        headers = {}
    assert not (authenticated and mode == "permit"), "authenticated is lease-only"
    asgi_app = main_module.create_asgi_app(settings=settings)
    app = main_module.app
    recovery_registry = app.state.recovery_registry
    queue = recovery_registry.queue_manager
    receipts = app.state.recovery_receipts
    recovery_dir = tmp_path / "recovery"

    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi_app),
        base_url="http://test",
        headers=headers,
    )
    env = Env(
        mode=mode,
        app=app,
        client=client,
        recovery_registry=recovery_registry,
        queue=queue,
        receipts=receipts,
        recovery_dir=recovery_dir,
        lease_path=tmp_path / "lease.json",
        probe=probe,
        ack=ack,
        trace=trace,
    )
    retry_events["parked"] = env.retry_parked
    retry_events["resumed"] = env.retry_resumed
    retry_events["drainer_parked"] = env.drainer_parked
    holder["env"] = env
    if authenticated:
        env.admin_headers = {"Authorization": f"Bearer {FAKE_ADMIN_TOKEN}"}

    # --- no Neo4j, no network ------------------------------------------------
    monkeypatch.setattr(
        registry_module, "Neo4jGraphStore", lambda **kw: ProbeGraph(probe, **kw)
    )
    monkeypatch.setattr(
        registry_module, "build_bounded_neo4j_driver", lambda *a, **k: MagicMock()
    )

    # --- deterministic poison on the first delivery; real handlers after -----
    real_process_event = registry_module.process_event

    async def process_event(
        worker: Any, event: str, data: Any, handlers: Any, **kw: Any
    ) -> None:
        if env.poison:
            await env.poison_go.wait()  # pre-set unless a scenario holds the poison
            raise ValueError("deterministic poison")
        return await real_process_event(worker, event, data, handlers, **kw)

    monkeypatch.setattr(registry_module, "process_event", process_event)

    # --- the old acknowledgement: the first offset rename in the recovery dir
    real_replace = os.replace

    def replace_spy(src: Any, dst: Any, *a: Any, **k: Any) -> None:
        target = Path(os.fspath(dst))
        if (
            target.parent == recovery_dir
            and target.name.endswith(".offset")
            and not ack.seen
        ):
            ack.seen = True
            trace.append("ack_reached")
            ack.reached.set()
            if park_ack and not ack.release.wait(BOUND):
                raise TimeoutError("old ack was never released")
            real_replace(src, dst, *a, **k)
            trace.append("ack_landed")
            ack.landed.set()
            loop.call_soon_threadsafe(env.landed.set)
            return None
        return real_replace(src, dst, *a, **k)

    monkeypatch.setattr(os, "replace", replace_spy)

    # --- recording spies (all delegate to the real implementation) ----------
    real_mark = receipts.mark

    async def mark_spy(origin: Any, state: str, *a: Any, **k: Any) -> Any:
        if state == "written":
            # What had actually completed when this transition was REQUESTED.
            env.written_requests.append(
                (probe.completed_writes, len(probe.committed_start_events()))
            )
        ok = await real_mark(origin, state, *a, **k)
        trace.append(f"mark:{state}")
        if ok and state == "enqueued":
            env.enqueued.set()
        if ok and state == "quarantined":
            env.quarantined.set()
        if ok and state == "written":
            env.written.set()
        return ok

    monkeypatch.setattr(receipts, "mark", mark_spy)

    real_requeue = receipts.requeue_quarantined

    async def requeue_spy(*a: Any, **k: Any) -> Any:
        trace.append("requeue_called")
        return await real_requeue(*a, **k)

    monkeypatch.setattr(receipts, "requeue_quarantined", requeue_spy)

    real_archive = queue.archive_recovery_dead_letters

    async def archive_spy(origin: Any) -> Any:
        trace.append("archive_called")
        return await real_archive(origin)

    monkeypatch.setattr(queue, "archive_recovery_dead_letters", archive_spy)

    real_append = queue.append

    async def append_spy(session_id: str, raw: bytes) -> None:
        await real_append(session_id, raw)
        trace.append("append")

    monkeypatch.setattr(queue, "append", append_spy)

    real_retry = main_module._retry_quarantined_native_recovery

    async def retry_spy(app_instance: Any) -> bool:
        token = _in_retry.set(True)
        trace.append("retry_entered")
        env.retry_entered.set()
        try:
            return await real_retry(app_instance)
        finally:
            _in_retry.reset(token)

    monkeypatch.setattr(main_module, "_retry_quarantined_native_recovery", retry_spy)

    if hold_duplicate_repair:
        # Tag the first two submissions as they ENTER the route: the route-level
        # reconcile is the first thing a recovery request awaits, before any lock
        # or classification.  That entry is the explicit "request started"
        # barrier, and the tag (a context variable set in the request's own
        # context) follows the request into the child task that
        # ``_await_uninterrupted`` creates for the final admission step.
        real_reconcile = main_module._reconcile_native_recovery_outbox

        async def reconcile_spy(app_instance: Any) -> None:
            if env.request_seq < 2 and _request_tag.get() is None:
                tag = env.request_seq
                env.request_seq += 1
                _request_tag.set(tag)
                trace.append(f"request_started:{tag}")
                if env.request_seq == 2:
                    env.both_started.set()
            return await real_reconcile(app_instance)

        monkeypatch.setattr(
            main_module, "_reconcile_native_recovery_outbox", reconcile_spy
        )

        # Hold each request AFTER classify_source says "new" and park the one
        # that becomes the duplicate inside its repair, right after
        # origin_is_dead came back False.  Keyed on the context variable set
        # at classification, so it holds across child tasks; the route-level
        # reconcile runs before classification and can never match, and neither
        # can any worker or retry.  When admission is serialized before
        # classification only one request ever classifies "new": the other
        # waits on the held shared lock (see ``second_blocked``).
        real_classify = receipts.classify_source

        async def classify_spy(*a: Any, **k: Any) -> Any:
            result = await real_classify(*a, **k)
            if result == "new" and env.classified_new < 2:
                index = env.classified_new
                env.classified_new += 1
                _classified_new.set(index)
                tag = _request_tag.get()
                trace.append(f"classify_new:{index}")
                if index == 0:
                    env.parked_tag = tag
                    env.first_classified.set()
                    # The other request may already be waiting on the shared
                    # lock this one holds (it began waiting before this parked).
                    if tag is not None and env.pending_shared.get(1 - tag, 0) > 0:
                        trace.append("other_request_blocked_on_shared")
                        env.second_blocked.set()
                if env.classified_new == 2:
                    env.both_classified.set()
                await env.classify_release[index].wait()
                trace.append(f"classify_released:{index}")
            return result

        monkeypatch.setattr(receipts, "classify_source", classify_spy)

        real_is_dead = queue.recovery_origin_is_dead

        async def is_dead_spy(origin: Any) -> bool:
            result = await real_is_dead(origin)
            if _classified_new.get() == 1 and not env.dup_parked.is_set():
                trace.append(f"duplicate_origin_is_dead:{result}")
                env.dup_parked.set()
                await env.dup_release.wait()
                trace.append("duplicate_repair_released")
            return result

        monkeypatch.setattr(queue, "recovery_origin_is_dead", is_dead_spy)
        env.poison_go.clear()  # the poison waits until the scenario lets it publish

    if not authenticated:
        app.dependency_overrides[require_admin] = lambda: None
    stuck: list[str] = []
    try:
        yield env
    finally:
        # Unconditionally release every barrier, then stop everything we own.
        ack.release.set()
        probe.release.set()
        env.poison_go.set()
        env.dup_release.set()
        for gate in env.classify_release:
            gate.set()
        await _stop_tasks(env.owned, stuck)
        for registry in (recovery_registry, app.state.registry):
            shutdown = asyncio.ensure_future(registry.shutdown_workers())
            _, still_shutting_down = await asyncio.wait([shutdown], timeout=BOUND)
            if still_shutting_down:
                shutdown.cancel()  # the stragglers pass below records the stacks
        stragglers = [
            t
            for t in asyncio.all_tasks() - baseline - {asyncio.current_task()}
            if not t.done()
        ]
        await _stop_tasks(stragglers, stuck)
        app.dependency_overrides.pop(require_admin, None)
        await client.aclose()
        if stuck:
            report = "tasks did not stop within bound:\n" + "\n".join(stuck)
            in_flight = sys.exc_info()[1]
            if in_flight is not None:
                in_flight.add_note(report)  # keep the test's own failure primary
            else:
                raise AssertionError(report)


# ---------------------------------------------------------------------------
# Shared scenario steps
# ---------------------------------------------------------------------------


async def _admit_and_quarantine(env: Env) -> None:
    """Admit one poison record; wait until its quarantine is durable and visible."""
    await env.admit()
    await _thread_event(env.ack.reached, "the poison drainer to reach its ack")
    await _event(env.quarantined, "the receipt to become quarantined")
    quarantined = await env.receipts.next_quarantined()
    assert quarantined is not None
    env.origin = {
        "source_handle": quarantined["source_handle"],
        **quarantined["origin"],
    }


async def _assert_retry_cannot_proceed_while_ack_in_flight(
    env: Env, retry: asyncio.Future[Any]
) -> None:
    """The retry has started; it must neither archive nor reset the receipt."""
    await _event(env.retry_entered, "the retry request to start")
    await _first_of(env, retry)
    premature = [e for e in env.trace if e in ("archive_called", "requeue_called")]
    assert not premature, (
        "retry archived/reset while the old poison acknowledgement was still in "
        f"flight (offset rename not released). trace={env.trace}"
    )
    assert env.retry_parked.is_set(), (
        f"retry neither waited for the old acknowledgement nor ran. trace={env.trace}"
    )
    assert await env.receipts.status() == {"quarantined": 1}
    assert await env.queue.recovery_origin_commit_state(env.origin) == "queued"
    assert not env.ack.landed.is_set()


async def _assert_redelivery_needs_a_completed_flush(
    env: Env, retry: asyncio.Future[Any]
) -> None:
    """After the old ack landed: a fresh line, held flush => pending, then written."""
    done, _ = await asyncio.wait([retry], timeout=BOUND)
    assert done, f"retry did not answer within {BOUND}s. trace={env.trace}"
    assert retry.result().json() == {"retried": 1}

    await _event(env.probe.entered, "the redelivery to enter its graph flush")
    # While the flush is held the receipt is pending, never written.
    assert await env.receipts.status() == {"enqueued": 1}
    assert [p["state"] for p in await env.receipts.pending_receipts()] == ["enqueued"]
    assert env.probe.completed_writes == 0
    assert env.written_requests == []
    # A fresh, still-uncommitted line was appended AFTER the old ack landed.
    assert env.log_lines() == 2
    appends = [i for i, entry in enumerate(env.trace) if entry == "append"]
    assert len(appends) == 2, env.trace
    assert env.trace.index("ack_landed") < appends[1], env.trace
    assert await env.queue.recovery_origin_commit_state(env.origin) == "queued"

    env.probe.release.set()
    await _event(env.written, "the receipt to be marked written")
    # 'written' was requested only after a COMPLETED write made the event durable.
    assert env.written_requests, env.trace
    assert all(
        completed >= 1 and observed >= 1 for completed, observed in env.written_requests
    ), f"written requested before a completed graph write: {env.written_requests}"
    assert env.probe.completed_writes == 1
    assert len(env.probe.committed_start_events()) == 1
    assert await env.receipts.status() == {"written": 1}
    assert await env.receipts.pending_receipts() == []


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@MODES
async def test_sequential_old_ack_then_retry_delivers_fresh_line_and_writes_after_flush(
    mode: Mode, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with recovery_env(mode, tmp_path, monkeypatch, park_ack=False) as env:
        await _admit_and_quarantine(env)
        await _thread_event(env.ack.landed, "the old ack to land first")
        env.probe.hold = True

        retry = env.start_retry()
        await _assert_redelivery_needs_a_completed_flush(env, retry)


@pytest.mark.asyncio
@MODES
async def test_retry_waits_for_inflight_poison_ack_then_redelivers_with_a_real_flush(
    mode: Mode, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with recovery_env(mode, tmp_path, monkeypatch, park_ack=True) as env:
        await _admit_and_quarantine(env)

        retry = env.start_retry()
        await _assert_retry_cannot_proceed_while_ack_in_flight(env, retry)

        env.probe.hold = True
        env.ack.release.set()
        await _thread_event(env.ack.landed, "the old ack to land after release")
        await _assert_redelivery_needs_a_completed_flush(env, retry)


@pytest.mark.asyncio
@MODES
async def test_retry_never_marks_written_without_a_completed_graph_write(
    mode: Mode, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with recovery_env(mode, tmp_path, monkeypatch, park_ack=True) as env:
        await _admit_and_quarantine(env)

        retry = env.start_retry()
        await _event(env.retry_entered, "the retry request to start")
        await _first_of(env, retry)  # either way, the ack is still parked here

        env.ack.release.set()
        await _thread_event(env.ack.landed, "the old ack to land after release")
        done, _ = await asyncio.wait([retry], timeout=BOUND)
        assert done, f"retry did not answer within {BOUND}s. trace={env.trace}"
        if retry.result().json() == {"retried": 1}:
            await env.reconcile()  # restart/sweep/next request all do this
            await _event(env.written, "the retried receipt to be marked written")

        forged = [r for r in env.written_requests if r[0] < 1 or r[1] < 1]
        assert not forged, (
            "FORGED WRITE PROOF: receipt requested 'written' with "
            f"(completed_writes, durable_events)={forged} "
            f"retry={retry.result().json()} trace={env.trace}"
        )


@pytest.mark.asyncio
@MODES
async def test_retry_stays_blocked_when_the_delayed_ack_drainer_is_cancelled(
    mode: Mode, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with recovery_env(mode, tmp_path, monkeypatch, park_ack=True) as env:
        await _admit_and_quarantine(env)

        retry = env.start_retry()
        await _assert_retry_cannot_proceed_while_ack_in_flight(env, retry)

        # Shutdown / idle reap cancels the drainer while its rename is in flight.
        (worker,) = env.recovery_registry.workers()
        assert worker.task is not None
        worker.task.cancel()
        for _ in range(LOOP_TURNS):
            await asyncio.sleep(0)
        assert not env.retry_resumed.is_set(), (
            "retry resumed after the drainer was cancelled although the old "
            f"offset rename was still in flight. trace={env.trace}"
        )
        assert "archive_called" not in env.trace, env.trace

        env.ack.release.set()
        await _thread_event(env.ack.landed, "the old ack to land after release")
        done, _ = await asyncio.wait([retry], timeout=BOUND)
        assert done, f"retry did not answer within {BOUND}s. trace={env.trace}"
        assert retry.result().json() == {"retried": 1}
        appends = [i for i, entry in enumerate(env.trace) if entry == "append"]
        assert len(appends) == 2, env.trace
        assert env.trace.index("ack_landed") < appends[1], env.trace


# ---------------------------------------------------------------------------
# Duplicate first-submission repair vs. an operator retry (lease mode)
# ---------------------------------------------------------------------------


@dataclass
class DuplicateRepairRace:
    retry: asyncio.Future[Any]
    # How the other submission met the first while the first was held at
    # classification: "classified_new" (both classified new: the stale-repair
    # path) or "blocked_on_shared_lock" (admission serialized before
    # classification, so the other one could not classify yet).
    position: str
    statuses: list[str]  # the two submissions' response statuses, sorted
    stale_repair_exercised: bool  # the duplicate repair evaluated origin_is_dead
    at_release: dict[str, Any]  # what the world looked like when it was released


async def _race_duplicate_repair_against_retry(
    env: Env, *, release_duplicate_early: bool = False
) -> DuplicateRepairRace:
    """Drive the interleaving; every step waits for a positive signal.

    With ``release_duplicate_early`` the duplicate repair is released right
    after it parked, BEFORE the poison publishes: the same machinery with no bug
    interleaving, used as the control.

    1. Two identical FIRST submissions are started (explicit barrier: both have
       entered the route).  One classifies ``new`` and is held.  The other is
       either also held after classifying ``new`` (baseline), or already waiting
       on the shared lock the first one holds (admission serialized before
       classification).  Both are positive signals; neither is guessed.
    2. The first is released and admitted; its poison is held at process_event.
    3. Baseline: the second is released, admitted as a duplicate, its repair
       snapshots the receipt ``enqueued``, finds no dead letter, and parks
       (holding the outbox).  Serialized: the second simply proceeds, classifies
       ``duplicate`` and returns.
    4. The poison is released: dead-letter, quarantine, acknowledge.
    5. The real admin retry runs until it finishes or blocks on a held lock.
    6. Anything still parked is released and everything is awaited.
    """
    _write_lease(env.lease_path)
    first, second = env.submit_first(), env.submit_first()
    submissions = asyncio.gather(first, second, return_exceptions=True)
    env.owned.append(submissions)
    await _event(env.both_started, "both identical submissions to enter the route")
    await _wait_first(
        env,
        "one submission to classify new",
        events={"first_classified": env.first_classified},
    )
    await _wait_first(
        env,
        "the other submission to classify new or to wait on the held shared lock",
        events={
            "both_classified": env.both_classified,
            "second_blocked": env.second_blocked,
        },
    )
    position = (
        "classified_new" if env.both_classified.is_set() else "blocked_on_shared_lock"
    )
    env.trace.append(f"position:{position}")

    env.classify_release[0].set()
    await _event(env.enqueued, "the first admission to be durable")
    pending = await env.receipts.pending_outbox()
    assert len(pending) == 1, pending
    # Real user context: the receipt is attributed to the authenticated contributor.
    assert pending[0]["actor"] == FAKE_CONTRIBUTOR, pending[0]["actor"]
    env.origin = {"source_handle": pending[0]["source_handle"], **pending[0]["origin"]}

    env.classify_release[1].set()  # releases the second only if it classified new
    await _wait_first(
        env,
        "the duplicate repair to park or both submissions to finish",
        events={"dup_parked": env.dup_parked},
        futures={"both_finished": submissions},
    )
    if position == "classified_new":
        # Both classified new, so the stale-repair path must really have run;
        # if it did not, this test no longer exercises what it claims to.
        assert env.dup_parked.is_set(), (
            "both submissions classified new but the duplicate repair never "
            f"evaluated origin_is_dead; revisit this test. trace={env.trace}"
        )
    if release_duplicate_early:
        env.dup_release.set()
        await _finished(env, submissions, "both submissions (released early)")

    env.poison_go.set()
    await _wait_first(
        env,
        "the poison drainer to acknowledge or block",
        events={"ack_landed": env.landed, "drainer_parked": env.drainer_parked},
    )

    retry = env.start_retry()
    await _event(env.retry_entered, "the retry request to start")
    await _first_of(env, retry)
    at_release = {"trace": list(env.trace), "receipts": await env.receipts.status()}

    env.dup_release.set()
    responses = await _finished(env, submissions, "both submissions once released")
    await _finished(env, retry, "the retry")
    assert all(isinstance(r, httpx.Response) for r in responses), responses
    statuses = sorted(r.json()["status"] for r in responses)
    assert statuses == ["duplicate", "queued"], (
        f"exactly one submission is admitted and the other is a duplicate: {statuses}"
    )
    return DuplicateRepairRace(
        retry=retry,
        position=position,
        statuses=statuses,
        stale_repair_exercised=env.dup_parked.is_set(),
        at_release=at_release,
    )


@pytest.mark.asyncio
async def test_sequential_duplicate_admission_then_retry_delivers_fresh_line_and_writes_after_flush(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Control: same machinery, but the duplicate repair finishes before the poison
    publishes.  Fails if the new harness (held classification, duplicate
    admission, real auth, poison gate) rather than the race is what is broken."""
    async with recovery_env(
        "lease",
        tmp_path,
        monkeypatch,
        park_ack=False,
        authenticated=True,
        hold_duplicate_repair=True,
    ) as env:
        env.probe.hold = True
        race = await _race_duplicate_repair_against_retry(
            env, release_duplicate_early=True
        )
        await _assert_redelivery_needs_a_completed_flush(env, race.retry)


@pytest.mark.asyncio
async def test_duplicate_repair_cannot_forge_a_written_receipt_for_a_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with recovery_env(
        "lease",
        tmp_path,
        monkeypatch,
        park_ack=False,
        authenticated=True,
        hold_duplicate_repair=True,
    ) as env:
        race = await _race_duplicate_repair_against_retry(env)
        retried = race.retry.result().json()
        if retried == {"retried": 1} and not env.written.is_set():
            await env.reconcile()  # restart/sweep/next request all do this
            await _event(env.written, "the retried receipt to be marked written")

        forged = [r for r in env.written_requests if r[0] < 1 or r[1] < 1]
        status = await env.receipts.status()
        context = (
            f"position={race.position} stale_repair_exercised="
            f"{race.stale_repair_exercised} statuses={race.statuses} retry={retried} "
            f"receipts={status} queue_lines={env.log_lines()} "
            f"at_release={race.at_release}"
        )
        assert not forged, (
            "FORGED WRITE PROOF (duplicate repair): receipt requested 'written' "
            f"with (completed_writes, durable_events)={forged}; {context}"
        )
        # Safe is not enough, and a changed path must not skip the positive
        # proof: the retry is accepted and 'written' follows a completed flush
        # that made the event durable.
        assert retried == {"retried": 1}, f"retry was not accepted: {context}"
        assert status == {"written": 1}, f"retried receipt not written: {context}"
        assert env.probe.completed_writes == 1, context
        assert len(env.probe.committed_start_events()) == 1, context


@pytest.mark.asyncio
async def test_retry_after_duplicate_admission_redelivers_a_fresh_line_and_writes_after_flush(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with recovery_env(
        "lease",
        tmp_path,
        monkeypatch,
        park_ack=False,
        authenticated=True,
        hold_duplicate_repair=True,
    ) as env:
        env.probe.hold = True
        race = await _race_duplicate_repair_against_retry(env)

        status = await env.receipts.status()
        assert env.log_lines() == 2, (
            "retry reported success without redelivering: expected a fresh queue "
            f"line (2 lines), got {env.log_lines()}; position={race.position} "
            f"stale_repair_exercised={race.stale_repair_exercised} "
            f"statuses={race.statuses} retry={race.retry.result().json()} "
            f"receipts={status} "
            f"graph_writes_completed={env.probe.completed_writes} "
            f"at_release={race.at_release}"
        )
        await _assert_redelivery_needs_a_completed_flush(env, race.retry)
