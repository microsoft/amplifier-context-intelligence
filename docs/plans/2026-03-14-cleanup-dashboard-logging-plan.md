# Context Intelligence Server — Worker Cleanup, Log Persistence & Dashboard Enhancement

> **Execution:** Use the subagent-driven-development workflow to implement this plan.

**Goal:** Fix the worker memory/connection leak after session:end, persist server operational logs to a Docker volume, and enhance the dashboard with completed session history, live Neo4j queries, and a real-time SSE log viewer.

**Architecture:** Self-terminating drain workers write a CompletedSession summary before closing their Neo4j driver and deregistering. A RotatingFileHandler writes structured JSON logs alongside stdout. GET /logs/stream tails the log file over SSE. The dashboard polls /status for session data and connects an EventSource for live log streaming.

**Tech Stack:** Python 3.11, FastAPI, asyncio, aiofiles, RotatingFileHandler, SSE (text/event-stream), Docker Compose named volumes

**Design doc:** `docs/plans/2026-03-14-cleanup-dashboard-logging-design.md`

---

## Conventions Reference

Before implementing, note these codebase conventions (verified from source):

- **pytest-asyncio auto mode** — configured in `pyproject.toml` (`asyncio_mode = "auto"`). Module-level async test functions don't need `@pytest.mark.asyncio`; class methods DO need it.
- **conftest.py** — has `reset_registry` autouse fixture that clears `registry._workers`. After Task 3 adds `_completed`, this fixture must also clear it.
- **Test factories** — `make_record(...)` pattern in `tests/test_dashboard.py`. Follow this for new factories.
- **HTTP test client** — `httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")` via the `client` fixture in conftest.
- **Logger name** — `logging.getLogger("context_intelligence_server")` in registry.py and main.py.
- **Log format** — `'{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'` defined as `_LOG_FORMAT` in main.py.
- **All paths are relative to** `amplifier-context-intelligence/` (the inner project root).

---

## Task 1: Add `log_path` to Settings

**Files:**
- Modify: `context_intelligence_server/config.py`
- Modify: `tests/test_config.py`

**Step 1: Write the failing test**

Add this test to the end of `tests/test_config.py`:

```python
def test_settings_log_path_default():
    """Settings.log_path defaults to /data/logs/server.jsonl."""
    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.log_path == "/data/logs/server.jsonl"
```

**Step 2: Run test to verify it fails**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_config.py::test_settings_log_path_default -xvs
```

Expected: FAIL — `AttributeError: 'Settings' object has no attribute 'log_path'`

**Step 3: Add `log_path` field to Settings**

In `context_intelligence_server/config.py`, add one line after `log_level: str = "INFO"` (line 19):

```python
    log_path: str = "/data/logs/server.jsonl"
```

**Step 4: Run test to verify it passes**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_config.py -xvs
```

Expected: ALL PASS (including existing tests — `test_settings_defaults` does not break because it doesn't assert the absence of `log_path`).

**Step 5: Commit**

```bash
cd amplifier-context-intelligence && git add context_intelligence_server/config.py tests/test_config.py && git commit -m "feat(config): add log_path setting for log file persistence"
```

---

## Task 2: Create `logging_config.py`

**Files:**
- Create: `context_intelligence_server/logging_config.py`
- Create: `tests/test_logging_config.py`
- Modify: `pyproject.toml` (no dependency needed — `RotatingFileHandler` is stdlib)

**Step 1: Write the failing tests**

Create `tests/test_logging_config.py`:

```python
"""Tests for logging_config.setup_logging."""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from unittest.mock import patch

import pytest


class TestSetupLogging:
    """setup_logging configures stdout + rotating file handlers."""

    def test_setup_logging_adds_stream_handler(self, tmp_path: Path) -> None:
        """setup_logging attaches a StreamHandler to the root logger."""
        from context_intelligence_server.logging_config import setup_logging

        log_file = tmp_path / "test.jsonl"
        with patch(
            "context_intelligence_server.logging_config.get_settings"
        ) as mock_settings:
            mock_settings.return_value.log_path = str(log_file)
            mock_settings.return_value.log_level = "INFO"
            setup_logging()

        root = logging.getLogger()
        stream_handlers = [
            h for h in root.handlers if isinstance(h, logging.StreamHandler)
            and not isinstance(h, RotatingFileHandler)
        ]
        assert len(stream_handlers) >= 1

        # Cleanup: remove handlers added by setup_logging
        for h in list(root.handlers):
            root.removeHandler(h)

    def test_setup_logging_adds_rotating_file_handler(self, tmp_path: Path) -> None:
        """setup_logging attaches a RotatingFileHandler to the root logger."""
        from context_intelligence_server.logging_config import setup_logging

        log_file = tmp_path / "test.jsonl"
        with patch(
            "context_intelligence_server.logging_config.get_settings"
        ) as mock_settings:
            mock_settings.return_value.log_path = str(log_file)
            mock_settings.return_value.log_level = "INFO"
            setup_logging()

        root = logging.getLogger()
        file_handlers = [
            h for h in root.handlers if isinstance(h, RotatingFileHandler)
        ]
        assert len(file_handlers) >= 1

        # Cleanup
        for h in list(root.handlers):
            root.removeHandler(h)

    def test_setup_logging_creates_parent_directory(self, tmp_path: Path) -> None:
        """setup_logging creates the parent directory of log_path if it doesn't exist."""
        from context_intelligence_server.logging_config import setup_logging

        log_file = tmp_path / "nested" / "dir" / "test.jsonl"
        with patch(
            "context_intelligence_server.logging_config.get_settings"
        ) as mock_settings:
            mock_settings.return_value.log_path = str(log_file)
            mock_settings.return_value.log_level = "INFO"
            setup_logging()

        assert log_file.parent.exists()

        # Cleanup
        root = logging.getLogger()
        for h in list(root.handlers):
            root.removeHandler(h)

    def test_rotating_file_handler_maxbytes_and_backups(self, tmp_path: Path) -> None:
        """RotatingFileHandler has maxBytes=10MB and backupCount=5."""
        from context_intelligence_server.logging_config import setup_logging

        log_file = tmp_path / "test.jsonl"
        with patch(
            "context_intelligence_server.logging_config.get_settings"
        ) as mock_settings:
            mock_settings.return_value.log_path = str(log_file)
            mock_settings.return_value.log_level = "INFO"
            setup_logging()

        root = logging.getLogger()
        file_handlers = [
            h for h in root.handlers if isinstance(h, RotatingFileHandler)
        ]
        handler = file_handlers[0]
        assert handler.maxBytes == 10 * 1024 * 1024  # 10 MB
        assert handler.backupCount == 5

        # Cleanup
        for h in list(root.handlers):
            root.removeHandler(h)

    def test_setup_logging_writes_json_to_file(self, tmp_path: Path) -> None:
        """A log message written after setup_logging appears as JSON in the log file."""
        import json

        from context_intelligence_server.logging_config import setup_logging

        log_file = tmp_path / "test.jsonl"
        with patch(
            "context_intelligence_server.logging_config.get_settings"
        ) as mock_settings:
            mock_settings.return_value.log_path = str(log_file)
            mock_settings.return_value.log_level = "INFO"
            setup_logging()

        test_logger = logging.getLogger("context_intelligence_server.test")
        test_logger.info("hello from test")

        # Flush handlers
        for h in logging.getLogger().handlers:
            h.flush()

        content = log_file.read_text()
        assert content.strip() != ""
        line = json.loads(content.strip().split("\n")[-1])
        assert "time" in line
        assert "level" in line
        assert "message" in line

        # Cleanup
        root = logging.getLogger()
        for h in list(root.handlers):
            root.removeHandler(h)
```

**Step 2: Run tests to verify they fail**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_logging_config.py -xvs
```

Expected: FAIL — `ModuleNotFoundError: No module named 'context_intelligence_server.logging_config'`

**Step 3: Create the logging_config module**

Create `context_intelligence_server/logging_config.py`:

```python
"""Structured JSON logging configuration with stdout + rotating file handlers."""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from context_intelligence_server.config import get_settings

_LOG_FORMAT = (
    '{"time": "%(asctime)s", "level": "%(levelname)s", '
    '"logger": "%(name)s", "message": "%(message)s"}'
)

_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
_BACKUP_COUNT = 5


def setup_logging() -> None:
    """Configure root logger with stdout + rotating file handlers.

    Reads ``log_path`` and ``log_level`` from application settings.
    Creates the parent directory of ``log_path`` if it does not exist.
    Both handlers use the same structured JSON format.
    """
    settings = get_settings()
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    formatter = logging.Formatter(_LOG_FORMAT)

    root = logging.getLogger()
    root.setLevel(level)

    # Stdout handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    # Rotating file handler
    log_path = Path(settings.log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    file_handler = RotatingFileHandler(
        filename=str(log_path),
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
    )
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)
```

**Step 4: Run tests to verify they pass**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_logging_config.py -xvs
```

Expected: ALL PASS

**Step 5: Commit**

```bash
cd amplifier-context-intelligence && git add context_intelligence_server/logging_config.py tests/test_logging_config.py && git commit -m "feat(logging): add logging_config module with stdout + rotating file handlers"
```

---

## Task 3: Add `CompletedSession` dataclass + `_completed` ring to `SessionRegistry`

**Files:**
- Modify: `context_intelligence_server/registry.py`
- Modify: `tests/test_registry.py`
- Modify: `tests/conftest.py` (clear `_completed` in `reset_registry`)

**Step 1: Write the failing tests**

Add to the end of `tests/test_registry.py`:

```python
from context_intelligence_server.registry import CompletedSession


def make_completed(
    session_id: str = "sess-1",
    workspace: str = "/ws",
    started_at: float = 1000.0,
    ended_at: float = 1010.0,
    events_processed: int = 5,
    error_count: int = 0,
    duration_seconds: float = 10.0,
) -> CompletedSession:
    return CompletedSession(
        session_id=session_id,
        workspace=workspace,
        started_at=started_at,
        ended_at=ended_at,
        events_processed=events_processed,
        error_count=error_count,
        duration_seconds=duration_seconds,
    )


class TestCompletedSession:
    """CompletedSession dataclass and _completed ring buffer on SessionRegistry."""

    def test_completed_session_is_dataclass(self) -> None:
        """CompletedSession is a dataclass with expected fields."""
        cs = make_completed()
        assert cs.session_id == "sess-1"
        assert cs.workspace == "/ws"
        assert cs.started_at == 1000.0
        assert cs.ended_at == 1010.0
        assert cs.events_processed == 5
        assert cs.error_count == 0
        assert cs.duration_seconds == 10.0

    def test_registry_has_completed_deque(self) -> None:
        """SessionRegistry._completed is a deque with maxlen=100."""
        reg = SessionRegistry()
        assert hasattr(reg, "_completed")
        assert reg._completed.maxlen == 100

    def test_completed_sessions_returns_list(self) -> None:
        """completed_sessions() returns a list copy of _completed."""
        reg = SessionRegistry()
        cs = make_completed()
        reg._completed.appendleft(cs)
        result = reg.completed_sessions()
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0] is cs

    def test_completed_ring_overflow(self) -> None:
        """Pushing 101 completions retains only 100; oldest is evicted."""
        reg = SessionRegistry()
        for i in range(101):
            reg._completed.appendleft(make_completed(session_id=f"sess-{i}"))
        result = reg.completed_sessions()
        assert len(result) == 100
        # Most recent is first (appendleft)
        assert result[0].session_id == "sess-100"
        # Oldest (sess-0) was evicted
        ids = {cs.session_id for cs in result}
        assert "sess-0" not in ids

    def test_session_worker_has_started_at_field(self) -> None:
        """SessionWorker has a started_at field defaulting to a time.time() value."""
        import time

        before = time.time()
        worker = SessionWorker(
            session_id="test",
            workspace="/ws",
            services=HookStateService(workspace="/ws"),
        )
        after = time.time()
        assert before <= worker.started_at <= after

    def test_session_worker_has_error_count_field(self) -> None:
        """SessionWorker has an error_count field defaulting to 0."""
        worker = SessionWorker(
            session_id="test",
            workspace="/ws",
            services=HookStateService(workspace="/ws"),
        )
        assert worker.error_count == 0
```

**Step 2: Run tests to verify they fail**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_registry.py::TestCompletedSession -xvs
```

Expected: FAIL — `ImportError: cannot import name 'CompletedSession' from 'context_intelligence_server.registry'`

**Step 3: Add CompletedSession, _completed ring, started_at, error_count**

In `context_intelligence_server/registry.py`:

1. Add `from collections import deque` to the imports (line 1 area, alongside existing imports).

2. Add `CompletedSession` dataclass after the existing `SessionWorker` dataclass (after line 28):

```python
@dataclass
class CompletedSession:
    """Summary of a session that has finished processing."""

    session_id: str
    workspace: str
    started_at: float
    ended_at: float
    events_processed: int
    error_count: int
    duration_seconds: float
```

3. Add `started_at` and `error_count` fields to `SessionWorker` (after `events_processed: int = 0`, line 27):

```python
    started_at: float = field(default_factory=time.time)
    error_count: int = 0
```

4. In `SessionRegistry.__init__`, add after `self._workers` line:

```python
        self._completed: deque[CompletedSession] = deque(maxlen=100)
```

5. Add `completed_sessions` method to `SessionRegistry` (after `active_sessions`, near end of class):

```python
    def completed_sessions(self) -> list[CompletedSession]:
        """Return the completed sessions ring as a list."""
        return list(self._completed)
```

**Step 4: Update conftest.py to clear `_completed`**

In `tests/conftest.py`, inside the `reset_registry` fixture, add `registry._completed.clear()` in both the setup and teardown sections. The fixture should become:

```python
@pytest.fixture(autouse=True)
def reset_registry() -> Generator[None, None, None]:
    """Ensure each test starts with a clean session registry."""
    registry._workers.clear()
    if hasattr(registry, "_completed"):
        registry._completed.clear()
    yield
    # Explicitly cancel running drain tasks before clearing so teardown intent is clear
    for w in list(registry._workers.values()):
        if w.task and not w.task.done():
            w.task.cancel()
    registry._workers.clear()
    if hasattr(registry, "_completed"):
        registry._completed.clear()
```

**Step 5: Run tests to verify they pass**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_registry.py -xvs
```

Expected: ALL PASS

**Step 6: Run full test suite to check for regressions**

```bash
cd amplifier-context-intelligence && python -m pytest -x --timeout=30
```

Expected: ALL PASS

**Step 7: Commit**

```bash
cd amplifier-context-intelligence && git add context_intelligence_server/registry.py tests/test_registry.py tests/conftest.py && git commit -m "feat(registry): add CompletedSession dataclass and _completed ring buffer"
```

---

## Task 4: Add `_deregister` method to `SessionRegistry`

**Files:**
- Modify: `context_intelligence_server/registry.py`
- Modify: `tests/test_registry.py`

**Step 1: Write the failing tests**

Add to `tests/test_registry.py`:

```python
class TestDeregister:
    """_deregister removes worker from registry WITHOUT cancelling its task."""

    @pytest.mark.asyncio
    async def test_deregister_removes_from_workers(self) -> None:
        """_deregister removes the session_id from _workers dict."""
        reg = SessionRegistry()
        worker = reg.get_or_create("sess-1", "/ws")
        assert reg.active_count() == 1

        reg._deregister("sess-1")
        assert reg.active_count() == 0

    @pytest.mark.asyncio
    async def test_deregister_does_not_cancel_task(self) -> None:
        """_deregister does NOT cancel the worker's asyncio task."""
        reg = SessionRegistry()
        worker = reg.get_or_create("sess-1", "/ws")
        task = worker.task
        assert task is not None
        assert not task.done()

        reg._deregister("sess-1")
        # Task should still be running (not cancelled)
        assert not task.cancelled()

        # Cleanup: cancel the orphaned task
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_deregister_nonexistent_is_noop(self) -> None:
        """_deregister on a missing session_id does not raise."""
        reg = SessionRegistry()
        reg._deregister("nonexistent")  # must not raise
        assert reg.active_count() == 0

    @pytest.mark.asyncio
    async def test_remove_cancels_task_but_deregister_does_not(self) -> None:
        """remove() cancels the task; _deregister() does not."""
        reg = SessionRegistry()

        # Worker for remove() test
        w1 = reg.get_or_create("sess-remove", "/ws")
        t1 = w1.task
        reg.remove("sess-remove")
        assert t1 is not None
        assert t1.cancelled()

        # Worker for _deregister() test
        w2 = reg.get_or_create("sess-dereg", "/ws")
        t2 = w2.task
        reg._deregister("sess-dereg")
        assert t2 is not None
        assert not t2.cancelled()

        # Cleanup
        t2.cancel()
        try:
            await t2
        except asyncio.CancelledError:
            pass
```

**Step 2: Run tests to verify they fail**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_registry.py::TestDeregister -xvs
```

Expected: FAIL — `AttributeError: 'SessionRegistry' object has no attribute '_deregister'`

**Step 3: Add `_deregister` method**

In `context_intelligence_server/registry.py`, add this method to `SessionRegistry` right after the existing `remove` method:

```python
    def _deregister(self, session_id: str) -> None:
        """Remove worker from registry WITHOUT cancelling its task.

        Called by the drain loop when self-terminating after session:end.
        """
        self._workers.pop(session_id, None)
```

**Step 4: Run tests to verify they pass**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_registry.py::TestDeregister -xvs
```

Expected: ALL PASS

**Step 5: Commit**

```bash
cd amplifier-context-intelligence && git add context_intelligence_server/registry.py tests/test_registry.py && git commit -m "feat(registry): add _deregister method for non-cancelling worker removal"
```

---

## Task 5: Self-termination in `drain_worker` after `session:end`

**Files:**
- Modify: `context_intelligence_server/registry.py`
- Modify: `tests/test_registry.py`

**Step 1: Write the failing tests**

Add to `tests/test_registry.py`:

```python
class TestWorkerSelfTermination:
    """drain_worker self-terminates after processing session:end."""

    @pytest.mark.asyncio
    async def test_session_end_removes_worker_from_registry(self) -> None:
        """After session:end the worker is removed from _workers."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/ws",
            services=HookStateService(workspace="/ws"),
        )
        worker.services.graph.close = AsyncMock()
        reg._workers["test-session"] = worker

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            task = asyncio.create_task(reg.drain_worker(worker))
            await worker.queue.put(("session:end", "/ws", {"session_id": "test-session"}))
            # Wait for the drain loop to process and self-terminate
            await asyncio.wait_for(task, timeout=5.0)

        assert reg.active_count() == 0

    @pytest.mark.asyncio
    async def test_session_end_writes_completed_session(self) -> None:
        """After session:end a CompletedSession is appended to _completed."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/ws",
            services=HookStateService(workspace="/ws"),
        )
        worker.services.graph.close = AsyncMock()
        reg._workers["test-session"] = worker

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            task = asyncio.create_task(reg.drain_worker(worker))
            # Send a normal event first, then session:end
            await worker.queue.put(("tool_call", "/ws", {"session_id": "test-session"}))
            await worker.queue.put(("session:end", "/ws", {"session_id": "test-session"}))
            await asyncio.wait_for(task, timeout=5.0)

        completed = reg.completed_sessions()
        assert len(completed) == 1
        cs = completed[0]
        assert cs.session_id == "test-session"
        assert cs.workspace == "/ws"
        assert cs.events_processed >= 1
        assert cs.duration_seconds >= 0

    @pytest.mark.asyncio
    async def test_session_end_calls_graph_close(self) -> None:
        """After session:end the worker's graph store is closed."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/ws",
            services=HookStateService(workspace="/ws"),
        )
        worker.services.graph.close = AsyncMock()
        reg._workers["test-session"] = worker

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            task = asyncio.create_task(reg.drain_worker(worker))
            await worker.queue.put(("session:end", "/ws", {"session_id": "test-session"}))
            await asyncio.wait_for(task, timeout=5.0)

        worker.services.graph.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_session_end_drains_tail_events(self) -> None:
        """Events queued after session:end are still processed before termination."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/ws",
            services=HookStateService(workspace="/ws"),
        )
        worker.services.graph.close = AsyncMock()
        reg._workers["test-session"] = worker

        call_log: list[str] = []

        async def track_process(w: Any, event: str, data: Any, handlers: Any) -> None:
            call_log.append(event)

        with patch(
            "context_intelligence_server.registry.process_event",
            side_effect=track_process,
        ):
            # Pre-fill queue: session:end followed by a tail event
            await worker.queue.put(("session:end", "/ws", {"session_id": "test-session"}))
            await worker.queue.put(("tail_event", "/ws", {"session_id": "test-session"}))
            task = asyncio.create_task(reg.drain_worker(worker))
            await asyncio.wait_for(task, timeout=5.0)

        assert "session:end" in call_log
        assert "tail_event" in call_log

    @pytest.mark.asyncio
    async def test_session_end_graph_close_error_still_deregisters(self) -> None:
        """Even if graph.close() raises, the worker is deregistered."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/ws",
            services=HookStateService(workspace="/ws"),
        )
        worker.services.graph.close = AsyncMock(side_effect=RuntimeError("Neo4j down"))
        reg._workers["test-session"] = worker

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            task = asyncio.create_task(reg.drain_worker(worker))
            await worker.queue.put(("session:end", "/ws", {"session_id": "test-session"}))
            await asyncio.wait_for(task, timeout=5.0)

        assert reg.active_count() == 0
        assert len(reg.completed_sessions()) == 1

    @pytest.mark.asyncio
    async def test_error_count_incremented_on_process_event_failure(self) -> None:
        """worker.error_count increments when process_event raises."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id="test-session",
            workspace="/ws",
            services=HookStateService(workspace="/ws"),
        )
        worker.services.graph.close = AsyncMock()
        reg._workers["test-session"] = worker

        call_count = 0

        async def fail_then_succeed(w: Any, event: str, data: Any, handlers: Any) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("simulated error")

        with patch(
            "context_intelligence_server.registry.process_event",
            side_effect=fail_then_succeed,
        ):
            task = asyncio.create_task(reg.drain_worker(worker))
            await worker.queue.put(("bad_event", "/ws", {"session_id": "test-session"}))
            await asyncio.sleep(0.05)
            await worker.queue.put(("session:end", "/ws", {"session_id": "test-session"}))
            await asyncio.wait_for(task, timeout=5.0)

        assert worker.error_count == 1
        cs = reg.completed_sessions()[0]
        assert cs.error_count == 1
```

**Step 2: Run tests to verify they fail**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_registry.py::TestWorkerSelfTermination -xvs
```

Expected: FAIL — tests will time out or fail because `drain_worker` doesn't handle `session:end`.

**Step 3: Modify `drain_worker` in `registry.py`**

Replace the `drain_worker` method in `context_intelligence_server/registry.py` with:

```python
    async def drain_worker(
        self, worker: SessionWorker, flush_timeout: float = 30.0
    ) -> None:
        """Background coroutine that drains the session's event queue.

        Initializes handlers once, then loops:
        - Dequeues events with a timeout of *flush_timeout* seconds.
        - Dispatches each event via process_event.
        - On TimeoutError (no events for flush_timeout seconds): calls graph.flush
          as a periodic fallback for disconnected sessions.
        - On CancelledError (shutdown): flushes once then exits cleanly.
        - On session:end: drains tail events, writes CompletedSession summary,
          closes graph, deregisters, and returns.
        """
        handlers = setup_handlers(worker.services)

        while True:
            try:
                event_tuple = await asyncio.wait_for(
                    worker.queue.get(), timeout=flush_timeout
                )
                event, _workspace, data = event_tuple
                result = "ok"
                error = ""
                try:
                    await process_event(worker, event, data, handlers)
                    worker.last_event = event
                    worker.last_event_time = time.time()
                    worker.events_processed += 1
                except Exception as exc:
                    result = "error"
                    error = str(exc)
                    worker.error_count += 1
                finally:
                    if event:
                        ring_buffer.add(
                            EventRecord(
                                timestamp=time.time(),
                                event=event,
                                session_id=data.get("session_id", ""),
                                workspace=worker.workspace,
                                result=result,
                                error=error,
                            )
                        )
                    worker.queue.task_done()

                # Self-termination after session:end
                if event == "session:end":
                    # Drain any remaining tail events
                    while not worker.queue.empty():
                        try:
                            tail_event, tail_ws, tail_data = worker.queue.get_nowait()
                            await process_event(worker, tail_event, tail_data, handlers)
                            worker.events_processed += 1
                            worker.queue.task_done()
                        except asyncio.QueueEmpty:
                            break

                    # Write completion summary
                    now = time.time()
                    self._completed.appendleft(CompletedSession(
                        session_id=worker.session_id,
                        workspace=worker.workspace,
                        started_at=worker.started_at,
                        ended_at=now,
                        events_processed=worker.events_processed,
                        error_count=worker.error_count,
                        duration_seconds=now - worker.started_at,
                    ))

                    # Close Neo4j driver (final flush + driver close)
                    try:
                        await worker.services.graph.close()
                    except Exception:
                        logger.exception(
                            "drain_worker: graph.close() failed during cleanup",
                            extra={"session_id": worker.session_id},
                        )

                    # Remove from registry without cancelling this task
                    self._deregister(worker.session_id)
                    break

            except asyncio.TimeoutError:
                # Periodic fallback flush for disconnected sessions
                await worker.services.graph.flush()

            except asyncio.CancelledError:
                # Shutdown: flush any buffered writes before exiting
                await worker.services.graph.flush()
                break
```

**Step 4: Run tests to verify they pass**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_registry.py -xvs
```

Expected: ALL PASS

**Step 5: Run full test suite**

```bash
cd amplifier-context-intelligence && python -m pytest -x --timeout=30
```

Expected: ALL PASS

**Step 6: Commit**

```bash
cd amplifier-context-intelligence && git add context_intelligence_server/registry.py tests/test_registry.py && git commit -m "feat(registry): self-terminating drain_worker after session:end"
```

---

## Task 6: Add `CompletedSession` to `build_status_response` + `error_count_last_hour`

**Files:**
- Modify: `context_intelligence_server/dashboard.py`
- Modify: `tests/test_dashboard.py`

**Step 1: Write the failing tests**

Add to `tests/test_dashboard.py`:

```python
from context_intelligence_server.dashboard import error_count_last_hour


class TestErrorCountLastHour:
    """error_count_last_hour scans the ring buffer for recent errors."""

    def test_no_errors_returns_zero(self) -> None:
        """No error records means count is 0."""
        buf = EventRingBuffer()
        buf.add(make_record(result="ok"))
        assert error_count_last_hour(buf) == 0

    def test_counts_recent_errors(self) -> None:
        """Errors within the last hour are counted."""
        buf = EventRingBuffer()
        buf.add(make_record(result="error", timestamp=time.time()))
        buf.add(make_record(result="error", timestamp=time.time()))
        buf.add(make_record(result="ok", timestamp=time.time()))
        assert error_count_last_hour(buf) == 2

    def test_ignores_old_errors(self) -> None:
        """Errors older than 1 hour are not counted."""
        buf = EventRingBuffer()
        old_time = time.time() - 7200  # 2 hours ago
        buf.add(make_record(result="error", timestamp=old_time))
        buf.add(make_record(result="error", timestamp=time.time()))
        assert error_count_last_hour(buf) == 1

    def test_empty_buffer_returns_zero(self) -> None:
        """Empty buffer returns 0."""
        buf = EventRingBuffer()
        assert error_count_last_hour(buf) == 0


class TestBuildStatusResponseWithCompleted:
    """build_status_response includes completed_sessions and error_count_last_hour."""

    def setup_method(self) -> None:
        """Clear the module-level ring_buffer before each test."""
        ring_buffer._buffer.clear()

    def test_includes_completed_sessions_key(self) -> None:
        """Response contains completed_sessions list."""
        registry = SessionRegistry()
        response = build_status_response(registry, time.time())
        assert "completed_sessions" in response
        assert isinstance(response["completed_sessions"], list)

    def test_includes_error_count_last_hour_key(self) -> None:
        """Response contains error_count_last_hour integer."""
        registry = SessionRegistry()
        response = build_status_response(registry, time.time())
        assert "error_count_last_hour" in response
        assert isinstance(response["error_count_last_hour"], int)

    def test_completed_sessions_populated(self) -> None:
        """Completed sessions from registry appear in response."""
        from context_intelligence_server.registry import CompletedSession

        registry = SessionRegistry()
        cs = CompletedSession(
            session_id="sess-done",
            workspace="/ws",
            started_at=1000.0,
            ended_at=1010.0,
            events_processed=42,
            error_count=1,
            duration_seconds=10.0,
        )
        registry._completed.appendleft(cs)

        response = build_status_response(registry, time.time())
        assert len(response["completed_sessions"]) == 1
        completed = response["completed_sessions"][0]
        assert completed["session_id"] == "sess-done"
        assert completed["events_processed"] == 42
        assert completed["error_count"] == 1
```

**Step 2: Run tests to verify they fail**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_dashboard.py::TestErrorCountLastHour -xvs
```

Expected: FAIL — `ImportError: cannot import name 'error_count_last_hour'`

**Step 3: Implement changes in `dashboard.py`**

In `context_intelligence_server/dashboard.py`:

1. Add `error_count_last_hour` function after the `ring_buffer` singleton (after line 55):

```python
def error_count_last_hour(ring: EventRingBuffer) -> int:
    """Count error records in the ring buffer from the last hour."""
    cutoff = time.time() - 3600
    return sum(1 for r in ring.recent() if r.result == "error" and r.timestamp >= cutoff)
```

2. Update `build_status_response` to include `completed_sessions` and `error_count_last_hour`. Add `import dataclasses` at the top (already imported). The return dict becomes:

```python
    return {
        "status": "ok",
        "uptime_seconds": time.time() - start_time,
        "active_sessions": registry.active_count(),
        "sessions": sessions,
        "recent_events": [dataclasses.asdict(rec) for rec in ring_buffer.recent()],
        "completed_sessions": [
            dataclasses.asdict(s) for s in registry.completed_sessions()
        ],
        "error_count_last_hour": error_count_last_hour(ring_buffer),
    }
```

**Step 4: Run tests to verify they pass**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_dashboard.py -xvs
```

Expected: ALL PASS

**Step 5: Run full test suite**

```bash
cd amplifier-context-intelligence && python -m pytest -x --timeout=30
```

Expected: ALL PASS (the existing `test_main.py::test_status_body` still passes because it only asserts on `status`, `uptime_seconds`, and `active_sessions` — the new keys are additive).

**Step 6: Commit**

```bash
cd amplifier-context-intelligence && git add context_intelligence_server/dashboard.py tests/test_dashboard.py && git commit -m "feat(dashboard): add completed_sessions + error_count_last_hour to status response"
```

---

## Task 7: Call `setup_logging()` in lifespan, update `GET /status`

**Files:**
- Modify: `context_intelligence_server/main.py`
- Modify: `tests/test_main.py`

**Step 1: Write the failing tests**

Add to `tests/test_main.py`:

```python
async def test_status_includes_completed_sessions(client: httpx.AsyncClient) -> None:
    """GET /status response includes completed_sessions list."""
    response = await client.get("/status")
    data = response.json()
    assert "completed_sessions" in data
    assert isinstance(data["completed_sessions"], list)


async def test_status_includes_error_count_last_hour(client: httpx.AsyncClient) -> None:
    """GET /status response includes error_count_last_hour integer."""
    response = await client.get("/status")
    data = response.json()
    assert "error_count_last_hour" in data
    assert isinstance(data["error_count_last_hour"], int)
```

**Step 2: Run tests to verify they fail (or pass — the dashboard change may already flow through)**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_main.py::test_status_includes_completed_sessions tests/test_main.py::test_status_includes_error_count_last_hour -xvs
```

Expected: These should PASS already since Task 6 modified `build_status_response`. If they do, great — the test is confirming the integration works end-to-end.

**Step 3: Update `main.py` to call `setup_logging()` and remove old logging setup**

In `context_intelligence_server/main.py`:

1. Add the import near the top (after line 11):

```python
from context_intelligence_server.logging_config import setup_logging
```

2. Remove the old module-level logging setup. Delete these lines (currently around lines 25-27):

```python
_LOG_FORMAT = '{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'

logging.basicConfig(level=_settings.log_level, format=_LOG_FORMAT)
```

3. Call `setup_logging()` at the top of the lifespan function, before the Neo4j driver creation:

```python
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application lifespan: configure logging and create shared Neo4j driver."""
    setup_logging()
    logger.info("lifespan_startup: creating Neo4j driver url=%s", _settings.neo4j_url)
    # ... rest unchanged
```

**Step 4: Run tests to verify they pass**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_main.py -xvs
```

Expected: ALL PASS

**Step 5: Commit**

```bash
cd amplifier-context-intelligence && git add context_intelligence_server/main.py tests/test_main.py && git commit -m "feat(main): integrate setup_logging in lifespan, verify completed_sessions in /status"
```

---

## Task 8: Add `GET /logs/stream` SSE endpoint

**Files:**
- Modify: `context_intelligence_server/main.py`
- Modify: `pyproject.toml` (add `aiofiles` dependency)
- Modify: `tests/test_main.py`

**Step 1: Add `aiofiles` dependency**

In `pyproject.toml`, add `"aiofiles>=24.0.0"` to the `dependencies` list:

```toml
dependencies = [
    "fastapi>=0.115.0",
    "uvicorn[standard]>=0.30.0",
    "pydantic-settings>=2.0.0",
    "neo4j>=5.0",
    "aiofiles>=24.0.0",
]
```

Install the new dependency:

```bash
cd amplifier-context-intelligence && pip install -e ".[dev]"
```

**Step 2: Write the failing tests**

Add to `tests/test_main.py`:

```python
async def test_logs_stream_returns_200_event_stream(
    client: httpx.AsyncClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /logs/stream returns 200 with text/event-stream content type."""
    log_file = tmp_path / "server.jsonl"
    log_file.write_text("")
    monkeypatch.setattr(main_module._settings, "log_path", str(log_file))

    async with client.stream("GET", "/logs/stream") as response:
        assert response.status_code == 200
        assert "text/event-stream" in response.headers["content-type"]


async def test_logs_stream_backfills_existing_lines(
    client: httpx.AsyncClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /logs/stream sends existing log lines as SSE data on connect."""
    log_file = tmp_path / "server.jsonl"
    lines = [f'{{"time": "t{i}", "level": "INFO", "message": "line {i}"}}\n' for i in range(5)]
    log_file.write_text("".join(lines))
    monkeypatch.setattr(main_module._settings, "log_path", str(log_file))

    received: list[str] = []
    async with client.stream("GET", "/logs/stream") as response:
        async for chunk in response.aiter_text():
            for line in chunk.split("\n"):
                if line.startswith("data: "):
                    received.append(line[6:])
            if len(received) >= 5:
                break

    assert len(received) >= 5
    assert '"line 0"' in received[0]
    assert '"line 4"' in received[4]
```

**Step 3: Run tests to verify they fail**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_main.py::test_logs_stream_returns_200_event_stream -xvs
```

Expected: FAIL — 404 (endpoint doesn't exist yet)

**Step 4: Implement the SSE endpoint**

In `context_intelligence_server/main.py`:

1. Add imports near the top:

```python
import asyncio
from pathlib import Path

import aiofiles
from fastapi.responses import StreamingResponse
```

Note: `asyncio` is already imported transitively but add it explicitly if not present. `StreamingResponse` needs to be added to the existing `from fastapi.responses import ...` line.

2. Add the endpoint after the existing `post_cypher` endpoint:

```python
@app.get("/logs/stream")
async def stream_logs(request: Request) -> StreamingResponse:
    """Stream server logs as Server-Sent Events with backfill."""
    settings = get_settings()

    async def event_generator():
        log_path = Path(settings.log_path)
        # Backfill: last 200 lines
        if log_path.exists():
            lines = log_path.read_text().splitlines()
            for line in lines[-200:]:
                yield f"data: {line}\n\n"
        # Tail new lines
        if log_path.exists():
            async with aiofiles.open(log_path, mode="r") as f:
                await f.seek(0, 2)  # seek to end
                while True:
                    if await request.is_disconnected():
                        break
                    line = await f.readline()
                    if line:
                        yield f"data: {line.rstrip()}\n\n"
                    else:
                        await asyncio.sleep(0.2)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
```

**Step 5: Run tests to verify they pass**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_main.py::test_logs_stream_returns_200_event_stream tests/test_main.py::test_logs_stream_backfills_existing_lines -xvs --timeout=10
```

Expected: ALL PASS

**Step 6: Run full test suite**

```bash
cd amplifier-context-intelligence && python -m pytest -x --timeout=30
```

Expected: ALL PASS

**Step 7: Commit**

```bash
cd amplifier-context-intelligence && git add context_intelligence_server/main.py pyproject.toml && git commit -m "feat(main): add GET /logs/stream SSE endpoint with backfill and tail"
```

---

## Task 9: Update `_DASHBOARD_HTML` — Completed Sessions table + Neo4j expand + error badge

**Files:**
- Modify: `context_intelligence_server/main.py`

**Step 1: No separate test — verify via existing dashboard test**

The existing `test_dashboard_returns_html` test checks for `Context Intelligence Server` and `setInterval` in the HTML. We'll add specific assertions below.

Add to `tests/test_main.py`:

```python
async def test_dashboard_html_includes_completed_sessions_section(
    client: httpx.AsyncClient,
) -> None:
    """Dashboard HTML contains completed sessions section."""
    response = await client.get("/")
    body = response.text
    assert "completed-body" in body
    assert "Completed Sessions" in body


async def test_dashboard_html_includes_error_badge(
    client: httpx.AsyncClient,
) -> None:
    """Dashboard HTML contains error badge element."""
    response = await client.get("/")
    body = response.text
    assert "error-badge" in body
```

**Step 2: Run tests to verify they fail**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_main.py::test_dashboard_html_includes_completed_sessions_section tests/test_main.py::test_dashboard_html_includes_error_badge -xvs
```

Expected: FAIL — the current HTML doesn't contain these elements.

**Step 3: Update `_DASHBOARD_HTML`**

Replace the entire `_DASHBOARD_HTML` string in `context_intelligence_server/main.py` with:

```python
_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Context Intelligence Server</title>
  <style>
    body {
      background: #1a1a2e;
      color: #e0e0e0;
      font-family: monospace;
      margin: 0;
      padding: 20px;
    }
    h1 { color: #a0c4ff; }
    h2 { color: #9fb3c8; margin-top: 24px; }
    .metrics { display: flex; gap: 32px; margin: 16px 0; flex-wrap: wrap; }
    .metric { background: #16213e; padding: 12px 20px; border-radius: 6px; }
    .metric-label { font-size: 0.8em; color: #888; }
    .metric-value { font-size: 1.4em; color: #a0c4ff; }
    table { width: 100%; border-collapse: collapse; margin-top: 8px; }
    th { background: #16213e; padding: 8px 12px; text-align: left; color: #9fb3c8; }
    td { padding: 6px 12px; border-bottom: 1px solid #2a2a4a; }
    tr:hover td { background: #1e2a3a; }
    tr.clickable { cursor: pointer; }
    .error-badge {
      display: inline-block;
      background: #c0392b;
      color: #fff;
      border-radius: 10px;
      padding: 2px 8px;
      font-size: 0.8em;
      margin-left: 8px;
    }
    .error-badge.hidden { display: none; }
    .detail-row td {
      background: #0d1b2a;
      padding: 8px 16px;
      font-size: 0.9em;
      color: #8ecae6;
    }
  </style>
</head>
<body>
  <h1>Context Intelligence Server</h1>
  <div class="metrics">
    <div class="metric">
      <div class="metric-label">Uptime (s)</div>
      <div class="metric-value"><span id="uptime">-</span></div>
    </div>
    <div class="metric">
      <div class="metric-label">Active Sessions</div>
      <div class="metric-value"><span id="active_sessions">-</span></div>
    </div>
    <div class="metric">
      <div class="metric-label">Errors (1h)</div>
      <div class="metric-value"><span id="error_count">0</span><span id="error-badge" class="error-badge hidden">!</span></div>
    </div>
  </div>

  <h2>Sessions</h2>
  <table>
    <thead>
      <tr>
        <th>Session</th>
        <th>Workspace</th>
        <th>Queue</th>
        <th>Last Event</th>
        <th>Processed</th>
      </tr>
    </thead>
    <tbody id="sessions-body"></tbody>
  </table>

  <h2>Completed Sessions</h2>
  <table>
    <thead>
      <tr>
        <th>Session</th>
        <th>Workspace</th>
        <th>Duration</th>
        <th>Events</th>
        <th>Errors</th>
        <th>Ended</th>
      </tr>
    </thead>
    <tbody id="completed-body"></tbody>
  </table>

  <h2>Recent Events</h2>
  <table>
    <thead>
      <tr>
        <th>Time</th>
        <th>Event</th>
        <th>Session</th>
        <th>Workspace</th>
        <th>Result</th>
      </tr>
    </thead>
    <tbody id="events-body"></tbody>
  </table>

  <script>
    function timeAgo(ts) {
      var diff = (Date.now() / 1000) - ts;
      if (diff < 60) return Math.round(diff) + 's ago';
      if (diff < 3600) return Math.round(diff / 60) + 'm ago';
      return Math.round(diff / 3600) + 'h ago';
    }

    function truncate(s, n) {
      return s.length > n ? s.substring(0, n) + '\\u2026' : s;
    }

    function toggleDetail(sessionId, workspace, row) {
      var existing = document.getElementById('detail-' + sessionId);
      if (existing) { existing.remove(); return; }
      var detailRow = document.createElement('tr');
      detailRow.id = 'detail-' + sessionId;
      detailRow.className = 'detail-row';
      detailRow.innerHTML = '<td colspan="6">Loading...</td>';
      row.parentNode.insertBefore(detailRow, row.nextSibling);
      fetch('/cypher', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          query: "MATCH (n {workspace: $workspace}) WHERE n.node_id CONTAINS $sid RETURN labels(n)[0] as type, count(n) as cnt ORDER BY cnt DESC",
          params: {sid: sessionId, ws: workspace},
          workspace: '*'
        })
      })
      .then(function(r) { return r.json(); })
      .then(function(data) {
        var text = (data.results || []).map(function(r) {
          return r.type + ': ' + r.cnt;
        }).join(', ') || 'No graph nodes found';
        detailRow.innerHTML = '<td colspan="6">' + text + '</td>';
      })
      .catch(function(err) {
        detailRow.innerHTML = '<td colspan="6" style="color:#e74c3c;">Query failed: ' + err + '</td>';
      });
    }

    function refresh() {
      fetch('/status')
        .then(function(r) { return r.json(); })
        .then(function(data) {
          document.getElementById('uptime').textContent = data.uptime_seconds.toFixed(1);
          document.getElementById('active_sessions').textContent = data.active_sessions;

          var errCount = data.error_count_last_hour || 0;
          document.getElementById('error_count').textContent = errCount;
          var badge = document.getElementById('error-badge');
          if (errCount > 0) { badge.classList.remove('hidden'); }
          else { badge.classList.add('hidden'); }

          var sb = document.getElementById('sessions-body');
          sb.innerHTML = (data.sessions || []).map(function(s) {
            return '<tr><td>' + s.session_id + '</td><td>' + s.workspace + '</td><td>' +
              s.queue_depth + '</td><td>' + (s.last_event || '-') + '</td><td>' +
              s.events_processed + '</td></tr>';
          }).join('');

          var cb = document.getElementById('completed-body');
          cb.innerHTML = (data.completed_sessions || []).map(function(s) {
            return '<tr class="clickable" onclick="toggleDetail(\\'' + s.session_id + '\\', \\'' + s.workspace + '\\', this)"><td>' +
              truncate(s.session_id, 8) + '</td><td>' + s.workspace + '</td><td>' +
              s.duration_seconds.toFixed(1) + 's</td><td>' +
              s.events_processed + '</td><td>' + s.error_count + '</td><td>' +
              timeAgo(s.ended_at) + '</td></tr>';
          }).join('');

          var eb = document.getElementById('events-body');
          eb.innerHTML = (data.recent_events || []).map(function(e) {
            var t = new Date(e.timestamp * 1000).toISOString();
            return '<tr><td>' + t + '</td><td>' + e.event + '</td><td>' +
              e.session_id + '</td><td>' + e.workspace + '</td><td>' + e.result + '</td></tr>';
          }).join('');
        });
    }
    refresh();
    setInterval(refresh, 3000);
  </script>
</body>
</html>"""
```

**Step 4: Run tests to verify they pass**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_main.py::test_dashboard_returns_html tests/test_main.py::test_dashboard_html_includes_completed_sessions_section tests/test_main.py::test_dashboard_html_includes_error_badge -xvs
```

Expected: ALL PASS

**Step 5: Commit**

```bash
cd amplifier-context-intelligence && git add context_intelligence_server/main.py tests/test_main.py && git commit -m "feat(dashboard): add completed sessions table, Neo4j row expand, error badge"
```

---

## Task 10: Update `_DASHBOARD_HTML` — log viewer panel + SSE client JS

**Files:**
- Modify: `context_intelligence_server/main.py`
- Modify: `tests/test_main.py`

**Step 1: Write the failing test**

Add to `tests/test_main.py`:

```python
async def test_dashboard_html_includes_log_viewer(
    client: httpx.AsyncClient,
) -> None:
    """Dashboard HTML contains log viewer panel with SSE EventSource."""
    response = await client.get("/")
    body = response.text
    assert "log-container" in body
    assert "EventSource" in body
    assert "/logs/stream" in body
```

**Step 2: Run test to verify it fails**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_main.py::test_dashboard_html_includes_log_viewer -xvs
```

Expected: FAIL — log viewer elements not in HTML yet.

**Step 3: Add log viewer to `_DASHBOARD_HTML`**

In `context_intelligence_server/main.py`, within the `_DASHBOARD_HTML` string:

1. Add CSS for the log viewer — insert these rules inside the `<style>` block (before `</style>`):

```css
    #log-panel { margin-top: 24px; }
    #log-controls { display: flex; gap: 12px; align-items: center; margin-bottom: 8px; }
    #log-filter { background: #16213e; color: #e0e0e0; border: 1px solid #2a2a4a; padding: 6px 10px; border-radius: 4px; flex: 1; font-family: monospace; }
    #log-toggle { background: #16213e; color: #a0c4ff; border: 1px solid #2a2a4a; padding: 6px 14px; border-radius: 4px; cursor: pointer; font-family: monospace; }
    #log-container { background: #0d1b2a; border-radius: 6px; padding: 8px; max-height: 400px; overflow-y: auto; font-size: 0.85em; }
    .log-line { padding: 2px 4px; white-space: pre-wrap; word-break: break-all; }
    .log-INFO { color: #888; }
    .log-WARNING { color: #f0ad4e; }
    .log-ERROR { color: #e74c3c; }
    .log-DEBUG { color: #5dade2; }
    .log-error-badge { display: inline-block; background: #c0392b; color: #fff; border-radius: 10px; padding: 2px 8px; font-size: 0.8em; margin-left: 8px; }
    .log-error-badge.hidden { display: none; }
```

2. Add the log viewer HTML — insert before `</body>`:

```html
  <div id="log-panel">
    <h2>Server Logs <span id="log-error-badge" class="log-error-badge hidden">0</span></h2>
    <div id="log-controls">
      <input type="text" id="log-filter" placeholder="Filter logs..." oninput="filterLogs()" />
      <button id="log-toggle" onclick="togglePause()">Pause</button>
    </div>
    <div id="log-container"></div>
  </div>
```

3. Add the SSE JS — insert before `</script>`:

```javascript
    // Log viewer SSE
    var logContainer = document.getElementById('log-container');
    var logFilter = document.getElementById('log-filter');
    var logToggle = document.getElementById('log-toggle');
    var logErrorBadge = document.getElementById('log-error-badge');
    var logErrorCount = 0;
    var isPaused = false;
    var pauseBuffer = [];
    var autoScroll = true;

    logContainer.addEventListener('scroll', function() {
      var atBottom = logContainer.scrollHeight - logContainer.clientHeight <= logContainer.scrollTop + 5;
      autoScroll = atBottom;
    });

    function appendLogLine(text) {
      var level = 'INFO';
      try {
        var parsed = JSON.parse(text);
        level = parsed.level || 'INFO';
      } catch(e) {}

      if (level === 'ERROR') {
        logErrorCount++;
        logErrorBadge.textContent = logErrorCount;
        logErrorBadge.classList.remove('hidden');
      }

      var div = document.createElement('div');
      div.className = 'log-line log-' + level;
      div.textContent = text;

      var filterVal = logFilter.value.toLowerCase();
      if (filterVal && !text.toLowerCase().includes(filterVal)) {
        div.style.display = 'none';
      }

      logContainer.appendChild(div);

      // Cap at 2000 lines
      while (logContainer.children.length > 2000) {
        logContainer.removeChild(logContainer.firstChild);
      }

      if (autoScroll) {
        logContainer.scrollTop = logContainer.scrollHeight;
      }
    }

    function filterLogs() {
      var filterVal = logFilter.value.toLowerCase();
      var lines = logContainer.getElementsByClassName('log-line');
      for (var i = 0; i < lines.length; i++) {
        lines[i].style.display = (!filterVal || lines[i].textContent.toLowerCase().includes(filterVal)) ? '' : 'none';
      }
    }

    function togglePause() {
      isPaused = !isPaused;
      logToggle.textContent = isPaused ? 'Resume' : 'Pause';
      if (!isPaused) {
        pauseBuffer.forEach(appendLogLine);
        pauseBuffer = [];
      }
    }

    var evtSource = new EventSource('/logs/stream');
    evtSource.onmessage = function(e) {
      if (isPaused) {
        pauseBuffer.push(e.data);
      } else {
        appendLogLine(e.data);
      }
    };
```

**Step 4: Run tests to verify they pass**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_main.py::test_dashboard_html_includes_log_viewer tests/test_main.py::test_dashboard_returns_html -xvs
```

Expected: ALL PASS

**Step 5: Commit**

```bash
cd amplifier-context-intelligence && git add context_intelligence_server/main.py tests/test_main.py && git commit -m "feat(dashboard): add live log viewer panel with SSE, filter, pause/resume"
```

---

## Task 11: Add `log_data` volume to `docker-compose.yml`

**Files:**
- Modify: `docker-compose.yml`

**Step 1: No unit test — this is infrastructure configuration**

**Step 2: Add volume mount to server service**

In `docker-compose.yml`, in the `context-intelligence-server` service, add the log volume mount. The `volumes:` section currently has:

```yaml
    volumes:
      - blob_data:/data/blobs
```

Change it to:

```yaml
    volumes:
      - blob_data:/data/blobs
      - log_data:/data/logs
```

**Step 3: Add volume to top-level volumes section**

The `volumes:` section currently has:

```yaml
volumes:
  blob_data:
  neo4j_data:
```

Change it to:

```yaml
volumes:
  blob_data:
  neo4j_data:
  log_data:
```

**Step 4: Validate the compose file**

```bash
cd amplifier-context-intelligence && docker compose config --quiet 2>&1 || echo "Compose validation failed"
```

Expected: No output (clean validation) or compose config printed without errors.

**Step 5: Commit**

```bash
cd amplifier-context-intelligence && git add docker-compose.yml && git commit -m "infra(docker): add log_data volume for persistent server logs"
```

---

## Task 12: Integration test — cleanup + SSE + session history

**Files:**
- Modify: `tests/integration/test_event_pipeline.py`

**Step 1: Write the integration tests**

Add these imports to the top of `tests/integration/test_event_pipeline.py` (alongside existing imports):

```python
import asyncio
from unittest.mock import AsyncMock, patch

from context_intelligence_server.registry import SessionRegistry, SessionWorker
from context_intelligence_server.services import HookStateService
```

Add the following test classes at the end of the file:

```python
# ===========================================================================
# TestSessionEndWorkerCleanup
# ===========================================================================


class TestSessionEndWorkerCleanup:
    """Session:end causes worker self-termination and moves to completed ring."""

    async def test_session_end_removes_worker_from_registry(self) -> None:
        """After session:end the registry has zero active workers."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id=SESSION_ID,
            workspace=WORKSPACE,
            services=HookStateService(workspace=WORKSPACE),
        )
        worker.services.graph.close = AsyncMock()
        reg._workers[SESSION_ID] = worker

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            task = asyncio.create_task(reg.drain_worker(worker))

            # Feed a realistic event sequence ending with session:end
            events = [
                ("session:start", WORKSPACE, {"session_id": SESSION_ID, "timestamp": T0}),
                ("prompt:submit", WORKSPACE, {"session_id": SESSION_ID, "timestamp": T1}),
                ("session:end", WORKSPACE, {"session_id": SESSION_ID, "timestamp": T6}),
            ]
            for evt in events:
                await worker.queue.put(evt)

            await asyncio.wait_for(task, timeout=5.0)

        assert reg.active_count() == 0

    async def test_session_end_populates_completed_ring(self) -> None:
        """After session:end a CompletedSession exists in _completed."""
        reg = SessionRegistry()
        worker = SessionWorker(
            session_id=SESSION_ID,
            workspace=WORKSPACE,
            services=HookStateService(workspace=WORKSPACE),
        )
        worker.services.graph.close = AsyncMock()
        reg._workers[SESSION_ID] = worker

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            task = asyncio.create_task(reg.drain_worker(worker))
            await worker.queue.put(
                ("session:start", WORKSPACE, {"session_id": SESSION_ID, "timestamp": T0})
            )
            await worker.queue.put(
                ("session:end", WORKSPACE, {"session_id": SESSION_ID, "timestamp": T6})
            )
            await asyncio.wait_for(task, timeout=5.0)

        completed = reg.completed_sessions()
        assert len(completed) == 1
        assert completed[0].session_id == SESSION_ID
        assert completed[0].events_processed > 0


# ===========================================================================
# TestStatusIncludesCompletedSessions
# ===========================================================================


class TestStatusIncludesCompletedSessions:
    """GET /status includes completed_sessions after session:end."""

    async def test_status_has_completed_sessions_after_drain(self) -> None:
        """After session:end + drain, build_status_response includes the session."""
        from context_intelligence_server.dashboard import build_status_response

        reg = SessionRegistry()
        worker = SessionWorker(
            session_id=SESSION_ID,
            workspace=WORKSPACE,
            services=HookStateService(workspace=WORKSPACE),
        )
        worker.services.graph.close = AsyncMock()
        reg._workers[SESSION_ID] = worker

        with patch(
            "context_intelligence_server.registry.process_event",
            new_callable=AsyncMock,
        ):
            task = asyncio.create_task(reg.drain_worker(worker))
            await worker.queue.put(
                ("session:start", WORKSPACE, {"session_id": SESSION_ID, "timestamp": T0})
            )
            await worker.queue.put(
                ("session:end", WORKSPACE, {"session_id": SESSION_ID, "timestamp": T6})
            )
            await asyncio.wait_for(task, timeout=5.0)

        import time

        response = build_status_response(reg, time.time() - 60)
        assert "completed_sessions" in response
        assert len(response["completed_sessions"]) == 1
        cs = response["completed_sessions"][0]
        assert cs["session_id"] == SESSION_ID
        assert cs["events_processed"] > 0
```

**Step 2: Run integration tests**

```bash
cd amplifier-context-intelligence && python -m pytest tests/integration/test_event_pipeline.py -xvs --timeout=30
```

Expected: ALL PASS (both new and existing tests)

**Step 3: Commit**

```bash
cd amplifier-context-intelligence && git add tests/integration/test_event_pipeline.py && git commit -m "test(integration): add cleanup + session history integration tests"
```

---

## Task 13: Final test run + commit all

**Files:** None new — verification only.

**Step 1: Run the complete test suite**

```bash
cd amplifier-context-intelligence && python -m pytest -x --timeout=30 -v
```

Expected: ALL PASS

**Step 2: Run linting and type checks**

```bash
cd amplifier-context-intelligence && python -m ruff check context_intelligence_server/ tests/
```

Fix any issues found.

```bash
cd amplifier-context-intelligence && python -m ruff format --check context_intelligence_server/ tests/
```

Fix any formatting issues:

```bash
cd amplifier-context-intelligence && python -m ruff format context_intelligence_server/ tests/
```

**Step 3: Final commit if any lint fixes were needed**

```bash
cd amplifier-context-intelligence && git add -A && git status
```

If there are changes:

```bash
cd amplifier-context-intelligence && git commit -m "chore: lint and format fixes"
```

**Step 4: Verify git log shows clean task progression**

```bash
cd amplifier-context-intelligence && git log --oneline -15
```

Expected: Commits from each task visible in order.
