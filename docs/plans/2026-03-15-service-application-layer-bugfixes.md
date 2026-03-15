# Service Application Layer Bugfixes

> **Execution:** Use the subagent-driven-development workflow to implement this plan.

**Goal:** Fix 9 bugs found during code audit that would prevent the intelligence service from working in production.
**Architecture:** Targeted fixes to config, Docker files, session manager, app lifecycle, and WebSocket error handling. No new modules — only modifications to existing files.
**Tech Stack:** Python 3.13, FastAPI, pydantic-settings, pytest, Docker

---

**Working directory:** `/home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence`
**Branch:** `feat/exploration-system`
**Run tests:** `.venv/bin/pytest tests/intelligence_service/ -v`
**Current baseline:** 88 tests passing

---

### Task 1: Fix config field name and env var prefix mismatch

The `config.py` field `ingestion_server_url` maps to env var `INTEL_SERVICE_INGESTION_SERVER_URL`, but `docker-compose.yml` sets `INTEL_SERVICE_INGESTION_URL`. The field name must match what Docker sets. Also, `docker-compose.yml` sets `BUNDLE_PATH`, `ROUTING_MATRIX` without the `INTEL_SERVICE_` prefix, so they are silently ignored and the service always runs in stub mode.

**Files:**
- Modify: `intelligence_service/config.py`
- Modify: `docker-compose.yml`
- Modify: `Dockerfile.intelligence`
- Modify: `tests/intelligence_service/test_config.py`

**Step 1: Write the failing test**

In `tests/intelligence_service/test_config.py`, add a test that asserts the field is named `ingestion_url` (not `ingestion_server_url`):

```python
def test_settings_ingestion_url_field_name() -> None:
    """Config field is 'ingestion_url' (maps to INTEL_SERVICE_INGESTION_URL)."""
    settings = Settings()

    assert hasattr(settings, "ingestion_url")
    assert settings.ingestion_url == "http://context-intelligence-server:8000"
```

Append this test to the end of `tests/intelligence_service/test_config.py`.

**Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/intelligence_service/test_config.py::test_settings_ingestion_url_field_name -v`
Expected: FAIL with `AttributeError: 'Settings' object has no attribute 'ingestion_url'`

**Step 3: Fix config.py — rename field**

In `intelligence_service/config.py`, change line 19:

```python
# OLD:
    ingestion_server_url: str = "http://context-intelligence-server:8000"

# NEW:
    ingestion_url: str = "http://context-intelligence-server:8000"
```

**Step 4: Update the existing defaults test**

In `tests/intelligence_service/test_config.py`, in `test_settings_defaults()`, change line 14:

```python
# OLD:
    assert settings.ingestion_server_url == "http://context-intelligence-server:8000"

# NEW:
    assert settings.ingestion_url == "http://context-intelligence-server:8000"
```

**Step 5: Fix docker-compose.yml env vars**

In `docker-compose.yml`, replace the `intelligence-service` environment block (lines 43-46):

```yaml
# OLD:
    environment:
      AMPLIFIER_HOME: /data/context-intelligence-service
      BUNDLE_PATH: /app/bundles/context-intelligence-server/bundle.md
      ROUTING_MATRIX: balanced
      INTEL_SERVICE_INGESTION_URL: http://context-intelligence-server:8000

# NEW:
    environment:
      AMPLIFIER_HOME: /data/context-intelligence-service
      INTEL_SERVICE_AMPLIFIER_HOME: /data/context-intelligence-service
      INTEL_SERVICE_BUNDLE_PATH: /app/bundles/context-intelligence-server/bundle.md
      INTEL_SERVICE_ROUTING_MATRIX: balanced
      INTEL_SERVICE_INGESTION_URL: http://context-intelligence-server:8000
```

Note: `AMPLIFIER_HOME` (unprefixed) is kept because the amplifier library reads it directly from `os.environ`. The `INTEL_SERVICE_*` versions are for pydantic-settings.

**Step 6: Fix Dockerfile.intelligence env vars**

In `Dockerfile.intelligence`, replace lines 28-29:

```dockerfile
# OLD:
ENV AMPLIFIER_HOME=/data/context-intelligence-service \
    BUNDLE_PATH=/app/bundles/context-intelligence-server/bundle.md

# NEW:
ENV AMPLIFIER_HOME=/data/context-intelligence-service \
    INTEL_SERVICE_AMPLIFIER_HOME=/data/context-intelligence-service \
    INTEL_SERVICE_BUNDLE_PATH=/app/bundles/context-intelligence-server/bundle.md \
    INTEL_SERVICE_ROUTING_MATRIX=balanced
```

**Step 7: Run all config tests to verify pass**

Run: `.venv/bin/pytest tests/intelligence_service/test_config.py -v`
Expected: All tests PASS (9 tests)

**Step 8: Grep for stale references to ingestion_server_url**

Run: `grep -rn "ingestion_server_url" intelligence_service/ tests/`

If any hits remain, update them to `ingestion_url`. The field is not currently referenced anywhere outside `config.py` and `test_config.py`, so no other files should need changes.

**Step 9: Commit**

```bash
git add intelligence_service/config.py tests/intelligence_service/test_config.py docker-compose.yml Dockerfile.intelligence
git commit -m "fix: env var prefix mismatch — rename ingestion_server_url, add INTEL_SERVICE_ prefixes to Docker"
```

---

### Task 2: Fix Dockerfile healthcheck endpoint and add PYTHONUNBUFFERED

The Dockerfile HEALTHCHECK hits `/status` but the app only exposes `/health`. Also missing `PYTHONUNBUFFERED=1` for proper log flushing in containers.

**Files:**
- Modify: `Dockerfile.intelligence`

**Step 1: Fix healthcheck URL**

In `Dockerfile.intelligence`, change line 35:

```dockerfile
# OLD:
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8100/status')"

# NEW:
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8100/health')"
```

**Step 2: Add PYTHONUNBUFFERED**

In `Dockerfile.intelligence`, add `PYTHONUNBUFFERED=1` to the ENV block. After Task 1's changes, the ENV block should become:

```dockerfile
ENV AMPLIFIER_HOME=/data/context-intelligence-service \
    INTEL_SERVICE_AMPLIFIER_HOME=/data/context-intelligence-service \
    INTEL_SERVICE_BUNDLE_PATH=/app/bundles/context-intelligence-server/bundle.md \
    INTEL_SERVICE_ROUTING_MATRIX=balanced \
    PYTHONUNBUFFERED=1
```

**Step 3: Verify Dockerfile is correct**

Run: `grep -n 'status\|health\|PYTHONUNBUFFERED' Dockerfile.intelligence`
Expected: `/health` in healthcheck, `PYTHONUNBUFFERED=1` in ENV, no `/status` references.

**Step 4: Commit**

```bash
git add Dockerfile.intelligence
git commit -m "fix: Dockerfile healthcheck /status -> /health, add PYTHONUNBUFFERED=1"
```

---

### Task 3: Fix workspace root path in AmplifierSessionManager

`amplifier_session_manager.py` hardcodes `_WORKSPACE_ROOT = "/data/workspace"` but no volume is mounted there. The session_cwd should use `amplifier_home` (which maps to the mounted `/data/context-intelligence-service` volume).

**Files:**
- Modify: `intelligence_service/amplifier_session_manager.py`
- Modify: `intelligence_service/app.py`
- Modify: `tests/intelligence_service/test_amplifier_session_manager.py`

**Step 1: Write the failing test**

In `tests/intelligence_service/test_amplifier_session_manager.py`, add a test that verifies `session_cwd` uses `amplifier_home`:

```python
async def test_create_session_uses_amplifier_home_for_cwd() -> None:
    """create_session() builds session_cwd from amplifier_home, not a hardcoded path."""
    from intelligence_service.amplifier_session_manager import AmplifierSessionManager

    mock_app = MagicMock()
    mock_session = MagicMock()
    mock_app.prepared.create_session = AsyncMock(return_value=mock_session)

    manager = AmplifierSessionManager(
        amplifier_app=mock_app,
        workspace="myproject",
        amplifier_home="/custom/data/dir",
    )
    session_id = await manager.create_session()

    mock_app.prepared.create_session.assert_called_once_with(
        session_id=session_id,
        session_cwd="/custom/data/dir/myproject",
    )
```

Append this test to the end of `tests/intelligence_service/test_amplifier_session_manager.py`.

**Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/intelligence_service/test_amplifier_session_manager.py::test_create_session_uses_amplifier_home_for_cwd -v`
Expected: FAIL with `TypeError: AmplifierSessionManager.__init__() got an unexpected keyword argument 'amplifier_home'`

**Step 3: Update AmplifierSessionManager**

In `intelligence_service/amplifier_session_manager.py`:

1. Remove the module-level constant `_WORKSPACE_ROOT = "/data/workspace"` (line 14).

2. Add `amplifier_home` parameter to `__init__`:

```python
# OLD:
    def __init__(
        self,
        *,
        amplifier_app: Any,
        workspace: str,
    ) -> None:
        self._amplifier_app = amplifier_app
        self._workspace = workspace
        self._sessions: dict[str, Any] = {}

# NEW:
    def __init__(
        self,
        *,
        amplifier_app: Any,
        workspace: str,
        amplifier_home: str,
    ) -> None:
        self._amplifier_app = amplifier_app
        self._workspace = workspace
        self._amplifier_home = amplifier_home
        self._sessions: dict[str, Any] = {}
```

3. Update `create_session` to use `self._amplifier_home`:

```python
# OLD:
        session = await self._amplifier_app.prepared.create_session(
            session_id=session_id,
            session_cwd=f"{_WORKSPACE_ROOT}/{self._workspace}",
        )

# NEW:
        session = await self._amplifier_app.prepared.create_session(
            session_id=session_id,
            session_cwd=f"{self._amplifier_home}/{self._workspace}",
        )
```

**Step 4: Update app.py to pass amplifier_home**

In `intelligence_service/app.py`, update the `AmplifierSessionManager` constructor call (line 47-50):

```python
# OLD:
        application.state.session_manager = AmplifierSessionManager(
            amplifier_app=amplifier_app,
            workspace=settings.workspace,
        )

# NEW:
        application.state.session_manager = AmplifierSessionManager(
            amplifier_app=amplifier_app,
            workspace=settings.workspace,
            amplifier_home=settings.amplifier_home,
        )
```

**Step 5: Update existing test that asserts old cwd path**

In `tests/intelligence_service/test_amplifier_session_manager.py`, the test `test_create_session_delegates_to_prepared` (line 49) currently asserts `session_cwd="/data/workspace/myproject"`. Update the constructor call and assertion:

```python
# OLD (test_create_session_delegates_to_prepared):
    manager = AmplifierSessionManager(amplifier_app=mock_app, workspace="myproject")
    session_id = await manager.create_session()

    mock_app.prepared.create_session.assert_called_once_with(
        session_id=session_id,
        session_cwd="/data/workspace/myproject",
    )

# NEW:
    manager = AmplifierSessionManager(
        amplifier_app=mock_app, workspace="myproject", amplifier_home="/data/home"
    )
    session_id = await manager.create_session()

    mock_app.prepared.create_session.assert_called_once_with(
        session_id=session_id,
        session_cwd="/data/home/myproject",
    )
```

**Step 6: Update ALL other test constructors to pass amplifier_home**

Every test in `test_amplifier_session_manager.py` that creates an `AmplifierSessionManager` needs the new `amplifier_home` kwarg. Update each constructor call:

```python
# OLD pattern (appears in tests 1-13):
    manager = AmplifierSessionManager(amplifier_app=mock_app, workspace="myproject")

# NEW pattern:
    manager = AmplifierSessionManager(
        amplifier_app=mock_app, workspace="myproject", amplifier_home="/data/home"
    )
```

Apply this change to ALL of these tests:
- `test_protocol_conformance` (line 20)
- `test_create_session_returns_string_id` (line 36)
- `test_create_session_increments_active_count` (line 78)
- `test_destroy_session_decrements_count` (line 99)
- `test_destroy_nonexistent_session_is_noop` (line 117)
- `test_reset_session_returns_new_id_with_same_count` (line 137)
- `test_get_session_returns_metadata` (line 159)
- `test_get_session_returns_none_for_unknown` (line 179)
- `test_execute_calls_session_execute` (line 201)
- `test_execute_returns_text_and_a2ui` (line 224)
- `test_execute_unknown_session_raises_key_error` (line 242)
- `test_close_all_clears_all_sessions` (line 260)

**Step 7: Run all session manager tests**

Run: `.venv/bin/pytest tests/intelligence_service/test_amplifier_session_manager.py -v`
Expected: All 14 tests PASS

**Step 8: Run full suite to check for regressions**

Run: `.venv/bin/pytest tests/intelligence_service/ -v`
Expected: All tests PASS (89 total — 88 original + 1 new)

**Step 9: Commit**

```bash
git add intelligence_service/amplifier_session_manager.py intelligence_service/app.py tests/intelligence_service/test_amplifier_session_manager.py
git commit -m "fix: use amplifier_home for session_cwd instead of hardcoded /data/workspace"
```

---

### Task 4: Set AMPLIFIER_HOME env var in AmplifierApp.startup()

`amplifier_app.py` stores `_amplifier_home` but never passes it to the amplifier library. The library reads `AMPLIFIER_HOME` from `os.environ`.

**Files:**
- Modify: `intelligence_service/amplifier_app.py`
- Modify: `tests/intelligence_service/test_amplifier_app.py`

**Step 1: Write the failing test**

In `tests/intelligence_service/test_amplifier_app.py`, add:

```python
import os


async def test_startup_sets_amplifier_home_env_var(
    mock_bundle_chain: tuple,
) -> None:
    """startup() sets AMPLIFIER_HOME env var before loading the bundle."""
    app = make_app()
    await app.startup()

    assert os.environ.get("AMPLIFIER_HOME") == "/data/home"
```

Append this test to the end of the file. Also add `import os` at the top of the file (after the existing imports, before the PATCH_TARGET line).

**Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/intelligence_service/test_amplifier_app.py::test_startup_sets_amplifier_home_env_var -v`
Expected: FAIL with `AssertionError: assert None == '/data/home'` (or a different existing value)

**Step 3: Implement the fix**

In `intelligence_service/amplifier_app.py`, add `import os` at the top (after `from typing import Any`, before the try block):

```python
from typing import Any

import os
```

Then update the `startup` method:

```python
# OLD:
    async def startup(self) -> None:
        """Load, compose, and prepare the bundle."""
        self._prepared = await self._load_and_prepare()

# NEW:
    async def startup(self) -> None:
        """Load, compose, and prepare the bundle."""
        os.environ["AMPLIFIER_HOME"] = self._amplifier_home
        self._prepared = await self._load_and_prepare()
```

**Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/intelligence_service/test_amplifier_app.py::test_startup_sets_amplifier_home_env_var -v`
Expected: PASS

**Step 5: Run all amplifier_app tests**

Run: `.venv/bin/pytest tests/intelligence_service/test_amplifier_app.py -v`
Expected: All 8 tests PASS

**Step 6: Commit**

```bash
git add intelligence_service/amplifier_app.py tests/intelligence_service/test_amplifier_app.py
git commit -m "fix: set AMPLIFIER_HOME env var in AmplifierApp.startup()"
```

---

### Task 5: Add resource cleanup to close, reload, and destroy_session

Three related cleanup issues:
- `amplifier_app.py` `close()` doesn't call cleanup on PreparedBundle
- `amplifier_app.py` `reload()` abandons old PreparedBundle without closing
- `amplifier_session_manager.py` `destroy_session()` doesn't close the Amplifier session

**Files:**
- Modify: `intelligence_service/amplifier_app.py`
- Modify: `intelligence_service/amplifier_session_manager.py`
- Modify: `tests/intelligence_service/test_amplifier_app.py`
- Modify: `tests/intelligence_service/test_amplifier_session_manager.py`

**Step 1: Write failing test — close() calls prepared.close()**

In `tests/intelligence_service/test_amplifier_app.py`, add:

```python
async def test_close_calls_prepared_close(
    mock_bundle_chain: tuple,
) -> None:
    """close() calls close() on the PreparedBundle if it has one."""
    _, _, _, mock_prepared = mock_bundle_chain
    mock_prepared.close = AsyncMock()

    app = make_app()
    await app.startup()
    await app.close()

    mock_prepared.close.assert_called_once()
    assert app.prepared is None
```

**Step 2: Write failing test — reload() closes old prepared**

In `tests/intelligence_service/test_amplifier_app.py`, add:

```python
async def test_reload_closes_old_prepared(
    mock_bundle_chain: tuple,
) -> None:
    """reload() closes the old PreparedBundle after successful swap."""
    _, _, mock_composed, first_prepared = mock_bundle_chain
    first_prepared.close = AsyncMock()
    second_prepared = MagicMock()
    mock_composed.prepare = AsyncMock(side_effect=[first_prepared, second_prepared])

    app = make_app()
    await app.startup()
    await app.reload()

    first_prepared.close.assert_called_once()
    assert app.prepared is second_prepared
```

**Step 3: Write failing test — destroy_session() closes session**

In `tests/intelligence_service/test_amplifier_session_manager.py`, add:

```python
async def test_destroy_session_closes_amplifier_session() -> None:
    """destroy_session() calls close() on the Amplifier session if available."""
    from intelligence_service.amplifier_session_manager import AmplifierSessionManager

    mock_session = MagicMock()
    mock_session.close = AsyncMock()

    mock_app = MagicMock()
    mock_app.prepared.create_session = AsyncMock(return_value=mock_session)

    manager = AmplifierSessionManager(
        amplifier_app=mock_app, workspace="myproject", amplifier_home="/data/home"
    )
    session_id = await manager.create_session()

    await manager.destroy_session(session_id)

    mock_session.close.assert_called_once()
```

**Step 4: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/intelligence_service/test_amplifier_app.py::test_close_calls_prepared_close tests/intelligence_service/test_amplifier_app.py::test_reload_closes_old_prepared tests/intelligence_service/test_amplifier_session_manager.py::test_destroy_session_closes_amplifier_session -v`
Expected: All 3 FAIL

**Step 5: Implement cleanup in amplifier_app.py**

In `intelligence_service/amplifier_app.py`, update `close()`:

```python
# OLD:
    async def close(self) -> None:
        """Clear the prepared bundle."""
        self._prepared = None

# NEW:
    async def close(self) -> None:
        """Close and clear the prepared bundle."""
        if self._prepared is not None and hasattr(self._prepared, "close"):
            await self._prepared.close()
        self._prepared = None
```

Update `reload()`:

```python
# OLD:
    async def reload(self) -> None:
        """Reload the bundle, atomically swapping the PreparedBundle on success.

        If loading or preparation fails, the old PreparedBundle remains active
        and the exception is re-raised.
        """
        old_prepared = self._prepared
        try:
            self._prepared = await self._load_and_prepare()
        except Exception:
            self._prepared = old_prepared
            raise

# NEW:
    async def reload(self) -> None:
        """Reload the bundle, atomically swapping the PreparedBundle on success.

        If loading or preparation fails, the old PreparedBundle remains active
        and the exception is re-raised.  On success the old bundle is closed.
        """
        old_prepared = self._prepared
        try:
            self._prepared = await self._load_and_prepare()
        except Exception:
            self._prepared = old_prepared
            raise
        if old_prepared is not None and hasattr(old_prepared, "close"):
            await old_prepared.close()
```

**Step 6: Implement cleanup in amplifier_session_manager.py**

In `intelligence_service/amplifier_session_manager.py`, update `destroy_session()`:

```python
# OLD:
    async def destroy_session(self, session_id: str) -> None:
        """Remove the session with *session_id*.  No-op if not found."""
        self._sessions.pop(session_id, None)

# NEW:
    async def destroy_session(self, session_id: str) -> None:
        """Remove the session with *session_id*.  No-op if not found."""
        session = self._sessions.pop(session_id, None)
        if session is not None and hasattr(session, "close"):
            await session.close()
```

**Step 7: Run all tests to verify pass**

Run: `.venv/bin/pytest tests/intelligence_service/test_amplifier_app.py tests/intelligence_service/test_amplifier_session_manager.py -v`
Expected: All tests PASS

**Step 8: Commit**

```bash
git add intelligence_service/amplifier_app.py intelligence_service/amplifier_session_manager.py tests/intelligence_service/test_amplifier_app.py tests/intelligence_service/test_amplifier_session_manager.py
git commit -m "fix: add resource cleanup to close(), reload(), and destroy_session()"
```

---

### Task 6: Add error handling around execute() in WebSocket handler

If `session_manager.execute()` raises (LLM timeout, API error), the exception propagates uncaught in the WS loop. The WebSocket disconnects silently instead of sending an error message back.

**Files:**
- Modify: `intelligence_service/app.py`
- Modify: `tests/intelligence_service/test_app.py`

**Step 1: Write the failing test**

In `tests/intelligence_service/test_app.py`, add this test. It patches `StubSessionManager.execute` to raise, then verifies the client receives an error message and the WS stays open:

```python
from unittest.mock import AsyncMock, patch


def test_ws_execute_error_sends_error_and_keeps_connection() -> None:
    """When execute() raises, the client receives an error message and the WS stays open."""
    with TestClient(app) as client:
        with patch.object(
            app.state.session_manager,
            "execute",
            new_callable=AsyncMock,
            side_effect=RuntimeError("LLM timeout"),
        ):
            with client.websocket_connect("/ws") as ws:
                ws.receive_json()  # consume session_created
                ws.send_json({"type": "message", "text": "hello"})
                data = ws.receive_json()

                assert data["type"] == "error"
                assert "LLM timeout" in data["message"]

                # WS is still open — send another message
                ws.send_json({"type": "action", "componentId": "test-1"})
                ack = ws.receive_json()
                assert ack["type"] == "action_ack"
```

Append this test to the end of `tests/intelligence_service/test_app.py`.

**Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/intelligence_service/test_app.py::test_ws_execute_error_sends_error_and_keeps_connection -v`
Expected: FAIL (the exception kills the WS loop, test receives disconnect instead of error message)

**Step 3: Wrap execute() call in try/except**

In `intelligence_service/app.py`, replace the `elif msg.msg_type == "message":` block (lines 122-127):

```python
# OLD:
            elif msg.msg_type == "message":
                text = msg.payload.get("text", "")
                result = await session_manager.execute(session_id, text)  # type: ignore[attr-defined]
                await websocket.send_json(format_response(session_id, result["text"]))
                for a2ui_msg in result.get("a2ui", []):
                    await websocket.send_json(a2ui_msg)

# NEW:
            elif msg.msg_type == "message":
                text = msg.payload.get("text", "")
                try:
                    result = await session_manager.execute(session_id, text)  # type: ignore[attr-defined]
                    await websocket.send_json(
                        format_response(session_id, result["text"])
                    )
                    for a2ui_msg in result.get("a2ui", []):
                        await websocket.send_json(a2ui_msg)
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "execute failed for session %s", session_id
                    )
                    await websocket.send_json(
                        format_error(session_id, str(exc))
                    )
```

**Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/intelligence_service/test_app.py::test_ws_execute_error_sends_error_and_keeps_connection -v`
Expected: PASS

**Step 5: Run all app tests to check regressions**

Run: `.venv/bin/pytest tests/intelligence_service/test_app.py -v`
Expected: All tests PASS (12 tests — 11 original + 1 new)

**Step 6: Commit**

```bash
git add intelligence_service/app.py tests/intelligence_service/test_app.py
git commit -m "fix: catch execute() errors in WS handler, send error message instead of dropping connection"
```

---

### Task 7: Fill test gaps — startup failure, routing overlay content, create_session guard

Add targeted tests for uncovered edge cases:
1. AmplifierApp: verify routing overlay hook content (module name + matrix value)
2. AmplifierApp: startup() failure propagates exception and leaves prepared as None
3. AmplifierSessionManager: create_session when `prepared` is None
4. AmplifierSessionManager: execute() failure from underlying session propagates

**Files:**
- Modify: `tests/intelligence_service/test_amplifier_app.py`
- Modify: `tests/intelligence_service/test_amplifier_session_manager.py`

**Step 1: Add routing overlay content test**

In `tests/intelligence_service/test_amplifier_app.py`, add:

```python
async def test_startup_routing_overlay_has_correct_hook_content(
    mock_bundle_chain: tuple,
) -> None:
    """Routing overlay hook has module='hooks-routing' and config with the matrix."""
    _, mock_loaded, _, _ = mock_bundle_chain

    app = make_app()
    await app.startup()

    overlay = mock_loaded.compose.call_args[0][0]
    assert len(overlay.hooks) == 1
    hook = overlay.hooks[0]
    assert hook["module"] == "hooks-routing"
    assert hook["config"]["default_matrix"] == "balanced"
```

**Step 2: Add startup failure test**

In `tests/intelligence_service/test_amplifier_app.py`, add:

```python
async def test_startup_failure_leaves_prepared_none(
    mock_load_bundle: AsyncMock,
) -> None:
    """When load_bundle raises during startup, prepared remains None."""
    mock_load_bundle.side_effect = RuntimeError("bundle not found")

    app = make_app()

    with pytest.raises(RuntimeError, match="bundle not found"):
        await app.startup()

    assert app.prepared is None
```

**Step 3: Add create_session guard test**

In `tests/intelligence_service/test_amplifier_session_manager.py`, add:

```python
async def test_create_session_when_prepared_is_none_raises() -> None:
    """create_session() raises AttributeError when prepared is None."""
    from intelligence_service.amplifier_session_manager import AmplifierSessionManager

    mock_app = MagicMock()
    mock_app.prepared = None

    manager = AmplifierSessionManager(
        amplifier_app=mock_app, workspace="myproject", amplifier_home="/data/home"
    )

    with pytest.raises(AttributeError):
        await manager.create_session()
```

**Step 4: Add execute failure propagation test**

In `tests/intelligence_service/test_amplifier_session_manager.py`, add:

```python
async def test_execute_propagates_session_error() -> None:
    """When the underlying session.execute() raises, the error propagates."""
    from intelligence_service.amplifier_session_manager import AmplifierSessionManager

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(side_effect=RuntimeError("LLM timeout"))

    mock_app = MagicMock()
    mock_app.prepared.create_session = AsyncMock(return_value=mock_session)

    manager = AmplifierSessionManager(
        amplifier_app=mock_app, workspace="myproject", amplifier_home="/data/home"
    )
    session_id = await manager.create_session()

    with pytest.raises(RuntimeError, match="LLM timeout"):
        await manager.execute(session_id, "hello")
```

**Step 5: Run all new tests**

Run: `.venv/bin/pytest tests/intelligence_service/test_amplifier_app.py tests/intelligence_service/test_amplifier_session_manager.py -v`
Expected: All tests PASS (no implementation changes needed — these test existing behavior)

**Step 6: Commit**

```bash
git add tests/intelligence_service/test_amplifier_app.py tests/intelligence_service/test_amplifier_session_manager.py
git commit -m "test: fill coverage gaps — routing hook content, startup failure, session guards"
```

---

### Task 8: Full test suite verification

Run the complete test suite and verify no regressions.

**Step 1: Run intelligence_service tests**

Run: `.venv/bin/pytest tests/intelligence_service/ -v`
Expected: All tests PASS. Expected count: ~97 tests (88 original + 1 config + 1 cwd + 3 cleanup + 1 WS error + 4 gap fills = ~98, minus any overlap)

**Step 2: Run full project test suite**

Run: `.venv/bin/pytest tests/ -v`
Expected: All tests PASS with 0 failures

**Step 3: Run linter**

Run: `.venv/bin/ruff check intelligence_service/ tests/intelligence_service/`
Expected: No errors

**Step 4: Run formatter**

Run: `.venv/bin/ruff format --check intelligence_service/ tests/intelligence_service/`
Expected: All files formatted correctly

**Step 5: Commit (if any formatting fixes needed)**

```bash
# Only if ruff format --check fails:
.venv/bin/ruff format intelligence_service/ tests/intelligence_service/
git add -u
git commit -m "style: format after bugfix pass"
```
