# Session `last_updated` Implementation Plan

> **Execution:** Use the subagent-driven-development workflow to implement this plan.

**Goal:** Add a `last_updated` property to `:Session` nodes, updated on every event, with child/sub-session activity propagating upward to all ancestor sessions.

**Architecture:** After every handler dispatch in `pipeline.py`, call `services.touch_session(session_id, timestamp)`. The method updates `last_updated` on the target session node (if the new timestamp is strictly newer) and recursively walks `parent_id` links to propagate the timestamp to all ancestor sessions.

**Tech Stack:** Python 3.12, pytest (asyncio_mode=auto), `GraphState` in-memory test double, `HookStateService`

**Design document:** [`docs/plans/2026-04-06-session-last-updated-design.md`](./2026-04-06-session-last-updated-design.md)

---

## Conventions observed from codebase

- `pyproject.toml` sets `asyncio_mode = "auto"` — bare `async def test_...` functions work without `@pytest.mark.asyncio`
- Pipeline tests (`tests/test_pipeline.py`) are bare async functions (not in classes), using `MagicMock`/`AsyncMock` fixtures
- Service tests (`tests/test_services.py`) use class-based grouping with `services` fixture from `conftest.py`
- `conftest.py` fixture `services` returns `HookStateService(workspace="test-workspace")` backed by default `GraphState`
- `GraphState.upsert_node` requires `"labels": ["Session"]` in the data dict for Session nodes
- `GraphState.get_node` returns a shallow copy (safe to inspect without side effects)
- `services.py` currently has **no** `logging` import — must be added
- `pipeline.py` already extracts `timestamp` at lines 146-148 as a local variable

---

### Task 1: Create `test_touch_session.py` with 3 direct-session tests (RED)

**Files:**
- Create: `tests/test_touch_session.py`

**Step 1: Write 3 failing tests**

Create `tests/test_touch_session.py` with this content:

```python
"""Tests for HookStateService.touch_session — last_updated propagation."""

from __future__ import annotations

from context_intelligence_server.services import HookStateService


# ===========================================================================
# Direct session — last_updated on the target session itself
# ===========================================================================


async def test_touch_session_sets_last_updated_when_null(
    services: HookStateService,
) -> None:
    """First event on a session sets last_updated from NULL."""
    await services.graph.upsert_node(
        "s1", {"labels": ["Session"], "session_id": "s1"}
    )

    await services.touch_session("s1", "2026-01-01T00:00:01Z")

    node = await services.graph.get_node("s1")
    assert node is not None
    assert node.get("last_updated") == "2026-01-01T00:00:01Z", (
        f"Expected last_updated to be set, got {node.get('last_updated')!r}"
    )


async def test_touch_session_advances_with_newer_timestamp(
    services: HookStateService,
) -> None:
    """A newer timestamp advances last_updated."""
    await services.graph.upsert_node(
        "s1",
        {"labels": ["Session"], "session_id": "s1", "last_updated": "2026-01-01T00:00:01Z"},
    )

    await services.touch_session("s1", "2026-01-01T00:00:05Z")

    node = await services.graph.get_node("s1")
    assert node is not None
    assert node["last_updated"] == "2026-01-01T00:00:05Z", (
        f"Expected last_updated to advance, got {node['last_updated']!r}"
    )


async def test_touch_session_ignores_older_timestamp(
    services: HookStateService,
) -> None:
    """An older timestamp must NOT regress last_updated."""
    await services.graph.upsert_node(
        "s1",
        {"labels": ["Session"], "session_id": "s1", "last_updated": "2026-01-01T00:00:05Z"},
    )

    await services.touch_session("s1", "2026-01-01T00:00:01Z")

    node = await services.graph.get_node("s1")
    assert node is not None
    assert node["last_updated"] == "2026-01-01T00:00:05Z", (
        f"Expected last_updated to remain unchanged, got {node['last_updated']!r}"
    )
```

**Step 2: Run tests to verify they fail**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_touch_session.py -v
```

Expected: All 3 tests FAIL with `AttributeError: 'HookStateService' object has no attribute 'touch_session'`

---

### Task 2: Implement `touch_session` — direct session only (GREEN)

**Files:**
- Modify: `context_intelligence_server/services.py`

**Step 1: Add `logging` import to `services.py`**

At the top of `context_intelligence_server/services.py`, after the existing `import fnmatch` line (line 10), add:

```python
import logging
```

Then, after the `from context_intelligence_server.handlers.data_layer_2.state import DataLayer2State` line (line 13), add:

```python

logger = logging.getLogger(__name__)
```

**Step 2: Add `touch_session` method (without ancestor walk)**

At the end of `context_intelligence_server/services.py` (after line 262, the last line of `ensure_session_node`), add:

```python

    async def touch_session(self, session_id: str, timestamp: str) -> None:
        """Update last_updated on this session and all ancestor sessions.

        Uses the event timestamp as the new value only if it is strictly
        greater than the current last_updated (or last_updated is absent).
        Propagates upward through parent_id links so child-session activity
        keeps ancestor sessions alive.

        Never raises — errors are logged at WARNING level.
        """
        try:
            node = await self.graph.get_node(session_id)
            if node is None:
                return

            current = node.get("last_updated")
            if current is None or timestamp > current:
                await self.graph.upsert_node(
                    session_id,
                    {"labels": ["Session"], "last_updated": timestamp},
                )
        except Exception:
            logger.warning(
                "touch_session failed for %s @ %s",
                session_id,
                timestamp,
                exc_info=True,
            )
```

Note: No ancestor walk yet — that comes in Task 4.

**Step 3: Run tests to verify they pass**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_touch_session.py -v
```

Expected: All 3 tests PASS

**Step 4: Commit**

```bash
cd amplifier-context-intelligence && git add context_intelligence_server/services.py tests/test_touch_session.py && git commit -m "feat: add touch_session to HookStateService (direct session only)"
```

---

### Task 3: Add 2 ancestor propagation tests (RED)

**Files:**
- Modify: `tests/test_touch_session.py`

**Step 1: Append 2 ancestor propagation tests**

Add these tests at the bottom of `tests/test_touch_session.py`:

```python


# ===========================================================================
# Ancestor propagation — child activity updates parent and grandparent
# ===========================================================================


async def test_touch_session_propagates_to_parent(
    services: HookStateService,
) -> None:
    """Child session event advances parent's last_updated via parent_id link."""
    # Create parent and child session nodes
    await services.graph.upsert_node(
        "parent", {"labels": ["Session"], "session_id": "parent"}
    )
    await services.graph.upsert_node(
        "child",
        {"labels": ["Session"], "session_id": "child", "parent_id": "parent"},
    )

    await services.touch_session("child", "2026-01-01T00:00:10Z")

    parent_node = await services.graph.get_node("parent")
    assert parent_node is not None
    assert parent_node.get("last_updated") == "2026-01-01T00:00:10Z", (
        f"Expected parent last_updated to be set, got {parent_node.get('last_updated')!r}"
    )


async def test_touch_session_propagates_to_grandparent(
    services: HookStateService,
) -> None:
    """Grandchild event propagates last_updated through the full ancestor chain."""
    # Create grandparent → parent → grandchild chain
    await services.graph.upsert_node(
        "grandparent", {"labels": ["Session"], "session_id": "grandparent"}
    )
    await services.graph.upsert_node(
        "parent",
        {"labels": ["Session"], "session_id": "parent", "parent_id": "grandparent"},
    )
    await services.graph.upsert_node(
        "grandchild",
        {"labels": ["Session"], "session_id": "grandchild", "parent_id": "parent"},
    )

    await services.touch_session("grandchild", "2026-01-01T00:00:20Z")

    # Grandchild itself
    gc_node = await services.graph.get_node("grandchild")
    assert gc_node is not None
    assert gc_node["last_updated"] == "2026-01-01T00:00:20Z"

    # Parent
    parent_node = await services.graph.get_node("parent")
    assert parent_node is not None
    assert parent_node["last_updated"] == "2026-01-01T00:00:20Z", (
        f"Expected parent last_updated to propagate, got {parent_node.get('last_updated')!r}"
    )

    # Grandparent
    gp_node = await services.graph.get_node("grandparent")
    assert gp_node is not None
    assert gp_node["last_updated"] == "2026-01-01T00:00:20Z", (
        f"Expected grandparent last_updated to propagate, got {gp_node.get('last_updated')!r}"
    )
```

**Step 2: Run tests to verify the 2 new tests fail**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_touch_session.py -v
```

Expected: 3 PASS, 2 FAIL (the parent/grandparent assertions fail because the ancestor walk is not yet implemented)

---

### Task 4: Add recursive ancestor walk to `touch_session` (GREEN)

**Files:**
- Modify: `context_intelligence_server/services.py`

**Step 1: Add ancestor walk**

In `context_intelligence_server/services.py`, inside the `touch_session` method, after the `await self.graph.upsert_node(...)` call, add the ancestor walk. The complete try block should now read:

```python
        try:
            node = await self.graph.get_node(session_id)
            if node is None:
                return

            current = node.get("last_updated")
            if current is None or timestamp > current:
                await self.graph.upsert_node(
                    session_id,
                    {"labels": ["Session"], "last_updated": timestamp},
                )

                # Propagate to ancestors via parent_id chain
                parent_id = (node.get("parent_id") or "").strip()
                if parent_id:
                    await self.touch_session(parent_id, timestamp)
```

The key addition is the 3 lines starting with `# Propagate to ancestors`. Note the ancestor walk is **inside** the `if current is None or timestamp > current` guard — if this session's timestamp didn't advance, ancestors don't need updating either.

**Step 2: Run tests to verify all pass**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_touch_session.py -v
```

Expected: All 5 tests PASS

**Step 3: Commit**

```bash
cd amplifier-context-intelligence && git add context_intelligence_server/services.py tests/test_touch_session.py && git commit -m "feat: add ancestor propagation to touch_session"
```

---

### Task 5: Add 2 edge-case tests — verify they pass GREEN immediately

**Files:**
- Modify: `tests/test_touch_session.py`

**Step 1: Append edge-case tests**

Add these tests at the bottom of `tests/test_touch_session.py`:

```python


# ===========================================================================
# Edge cases — no-op and error isolation
# ===========================================================================


async def test_touch_session_noop_when_session_absent(
    services: HookStateService,
) -> None:
    """touch_session is a silent no-op when the session node does not exist."""
    # No session node created — call should not raise
    await services.touch_session("nonexistent", "2026-01-01T00:00:01Z")
    node = await services.graph.get_node("nonexistent")
    assert node is None, "No node should be created for a missing session"


async def test_touch_session_swallows_graph_exception(
    services: HookStateService,
) -> None:
    """Exceptions from the graph store are swallowed (logged, not raised)."""
    from unittest.mock import AsyncMock

    # Sabotage get_node to raise
    services.graph.get_node = AsyncMock(side_effect=RuntimeError("db down"))  # type: ignore[method-assign]

    # Must NOT raise
    await services.touch_session("s1", "2026-01-01T00:00:01Z")
```

**Step 2: Run tests to verify all pass**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_touch_session.py -v
```

Expected: All 7 tests PASS (these edge cases are already handled by the existing implementation)

**Step 3: Commit**

```bash
cd amplifier-context-intelligence && git add tests/test_touch_session.py && git commit -m "test: add edge-case tests for touch_session"
```

---

### Task 6: Add pipeline call site tests (RED)

**Files:**
- Modify: `tests/test_pipeline.py`

**Step 1: Add `touch_session` to the `mock_worker` fixture**

In `tests/test_pipeline.py`, find the `mock_worker` fixture (lines 75-82). Add a `touch_session` mock. Replace the fixture with:

```python
@pytest.fixture
def mock_worker() -> MagicMock:
    worker = MagicMock()
    worker.services = MagicMock()
    worker.services.ensure_session_node = AsyncMock()
    worker.services.touch_session = AsyncMock()
    worker.services.graph = MagicMock()
    worker.services.graph.flush = AsyncMock()
    worker.services.blob_store = None
    return worker
```

The only change is adding line `worker.services.touch_session = AsyncMock()`.

**Step 2: Append 2 pipeline call site tests**

Add these tests at the bottom of `tests/test_pipeline.py`:

```python


# ===========================================================================
# process_event — touch_session call site
# ===========================================================================


async def test_process_event_calls_touch_session(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
) -> None:
    """process_event calls touch_session with session_id and timestamp."""
    from context_intelligence_server.pipeline import process_event

    data = {"session_id": "sess-123", "timestamp": "2026-01-01T00:00:01Z"}
    await process_event(mock_worker, "some:event", data, pipeline_handlers)

    mock_worker.services.touch_session.assert_called_once_with(
        "sess-123", "2026-01-01T00:00:01Z"
    )


async def test_process_event_skips_touch_session_without_timestamp(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
) -> None:
    """process_event does NOT call touch_session when timestamp is missing."""
    from context_intelligence_server.pipeline import process_event

    data: dict[str, Any] = {"session_id": "sess-123"}  # no timestamp
    await process_event(mock_worker, "some:event", data, pipeline_handlers)

    mock_worker.services.touch_session.assert_not_called()


async def test_process_event_skips_touch_session_without_session_id(
    mock_worker: MagicMock,
    pipeline_handlers: Any,
) -> None:
    """process_event does NOT call touch_session when session_id is missing."""
    from context_intelligence_server.pipeline import process_event

    data = {"timestamp": "2026-01-01T00:00:01Z"}  # no session_id
    await process_event(mock_worker, "some:event", data, pipeline_handlers)

    mock_worker.services.touch_session.assert_not_called()
```

**Step 3: Run tests to verify the new tests fail**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_pipeline.py::test_process_event_calls_touch_session tests/test_pipeline.py::test_process_event_skips_touch_session_without_timestamp tests/test_pipeline.py::test_process_event_skips_touch_session_without_session_id -v
```

Expected: All 3 FAIL — `touch_session` is never called in the pipeline yet, so `assert_called_once_with` fails.

---

### Task 7: Add `touch_session` call in `pipeline.py` (GREEN)

**Files:**
- Modify: `context_intelligence_server/pipeline.py`

**Step 1: Insert the call**

In `context_intelligence_server/pipeline.py`, after the enricher dispatch loop (line 167: `await enricher(event, data)`) and **before** the terminal flush check (line 170: `if event in TERMINAL_EVENTS:`), insert:

```python

        # Step 5b — update last_updated on session and ancestors
        if session_id and timestamp:
            await worker.services.touch_session(session_id, timestamp)
```

The `timestamp` variable already exists (extracted at lines 146-148). The `session_id` variable already exists (extracted at line 139).

After insertion, lines 164-175 should read:

```python
        # Step 5 — call matching enrichers additionally
        for enricher in handlers.enrichers:
            if event in enricher.handled_events:
                await enricher(event, data)

        # Step 5b — update last_updated on session and ancestors
        if session_id and timestamp:
            await worker.services.touch_session(session_id, timestamp)

        # Step 6 — terminal flush
        if event in TERMINAL_EVENTS:
            await worker.services.graph.flush()
```

**Step 2: Run new pipeline tests to verify they pass**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_pipeline.py::test_process_event_calls_touch_session tests/test_pipeline.py::test_process_event_skips_touch_session_without_timestamp tests/test_pipeline.py::test_process_event_skips_touch_session_without_session_id -v
```

Expected: All 3 PASS

**Step 3: Run all pipeline tests to check for regressions**

```bash
cd amplifier-context-intelligence && python -m pytest tests/test_pipeline.py -v
```

Expected: All tests PASS (existing tests unaffected because `mock_worker.services.touch_session` is now an `AsyncMock` that does nothing)

**Step 4: Commit**

```bash
cd amplifier-context-intelligence && git add context_intelligence_server/pipeline.py tests/test_pipeline.py && git commit -m "feat: call touch_session from pipeline after handler dispatch"
```

---

### Task 8: Full suite + final commit

**Files:**
- No new modifications

**Step 1: Run the full test suite**

```bash
cd amplifier-context-intelligence && python -m pytest tests/ -v
```

Expected: All tests PASS with zero failures. Confirm no regressions.

**Step 2: Run quality checks**

```bash
cd amplifier-context-intelligence && python -m ruff check context_intelligence_server/services.py context_intelligence_server/pipeline.py tests/test_touch_session.py tests/test_pipeline.py
```

Expected: No lint errors.

**Step 3: Squash-commit if needed, or tag the final state**

If all intermediate commits were already made, no additional commit is needed. Verify the git log shows the feature progression:

```bash
git log --oneline -5
```

Expected output (most recent first):
```
<hash> feat: call touch_session from pipeline after handler dispatch
<hash> test: add edge-case tests for touch_session
<hash> feat: add ancestor propagation to touch_session
<hash> feat: add touch_session to HookStateService (direct session only)
<hash> docs: add last_updated session node design
```

---

## Summary of changes

| File | Change |
|---|---|
| `context_intelligence_server/services.py` | Add `import logging`, `logger`, and `touch_session()` method to `HookStateService` |
| `context_intelligence_server/pipeline.py` | Add 3-line `touch_session` call after enricher dispatch |
| `tests/test_touch_session.py` | New file: 7 tests for `touch_session` (3 direct, 2 ancestor, 2 edge-case) |
| `tests/test_pipeline.py` | Add `touch_session` mock to fixture + 3 pipeline call site tests |