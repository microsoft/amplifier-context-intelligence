# Service Application Layer Implementation Plan

> **Execution:** Use the subagent-driven-development workflow to implement this plan.

**Goal:** Revise the intelligence_service/ package from Phase 1 CLI-based stubs to a real Amplifier integration using the programmatic API (load_bundle → compose → prepare → create_session → execute).

**Architecture:** The Intelligence Service becomes a bespoke Amplifier application. `AmplifierApp` manages the `PreparedBundle` lifecycle (load, compose, prepare, reload). `AmplifierSessionManager` creates real Amplifier sessions per WebSocket connection via `PreparedBundle.create_session()`. A routing overlay selects the `balanced` matrix at startup. The service captures its own telemetry via `hook-context-intelligence` with workspace `"context-intelligence-service"`.

**Tech Stack:** Python 3.13, FastAPI, uvicorn, amplifier-foundation, amplifier-core, 7 provider modules, uv for dependency management, Docker, pytest with pytest-asyncio.

**Design doc:** `docs/plans/2026-03-15-context-intelligence-service-application-layer-design.md`

---

## Working Directory

All file paths are relative to: `amplifier-context-intelligence/`

All commands run from: `cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence`

Branch: `feat/exploration-system`

---

### Task 1: Update pyproject.toml with Amplifier Dependencies

**Files:**
- Modify: `intelligence_service/pyproject.toml`

**Step 1: Replace pyproject.toml content**

Replace the entire content of `intelligence_service/pyproject.toml` with:

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "context-intelligence-service"
version = "0.1.0"
description = "AI-powered graph exploration service (bespoke Amplifier application)"
requires-python = ">=3.11"
dependencies = [
    # Web framework
    "fastapi>=0.115.0",
    "uvicorn[standard]>=0.30.0",
    "websockets>=13.0",
    "pydantic-settings>=2.0.0",

    # Amplifier core (programmatic API)
    "amplifier-core @ git+https://github.com/microsoft/amplifier-core@main",
    "amplifier-foundation @ git+https://github.com/microsoft/amplifier-foundation@main",

    # All 7 providers
    "amplifier-module-provider-anthropic @ git+https://github.com/microsoft/amplifier-module-provider-anthropic@main",
    "amplifier-module-provider-openai @ git+https://github.com/microsoft/amplifier-module-provider-openai@main",
    "amplifier-module-provider-gemini @ git+https://github.com/microsoft/amplifier-module-provider-gemini@main",
    "amplifier-module-provider-azure-openai @ git+https://github.com/microsoft/amplifier-module-provider-azure-openai@main",
    "amplifier-module-provider-github-copilot @ git+https://github.com/microsoft/amplifier-module-provider-github-copilot@main",
    "amplifier-module-provider-ollama @ git+https://github.com/microsoft/amplifier-module-provider-ollama@main",
    "amplifier-module-provider-vllm @ git+https://github.com/microsoft/amplifier-module-provider-vllm@main",

    # Orchestrator + context
    "amplifier-module-loop-basic @ git+https://github.com/microsoft/amplifier-module-loop-basic@main",
    "amplifier-module-context-simple @ git+https://github.com/microsoft/amplifier-module-context-simple@main",
]

[dependency-groups]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.24.0",
    "httpx>=0.27.0",
]

[tool.hatch.build.targets.wheel]
include = ["intelligence_service/**"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

Key changes from Phase 1:
- Project name: `context-intelligence-service` (was `intelligence-service`)
- Added `websockets>=13.0` dependency
- Added all amplifier-foundation, amplifier-core, 7 providers, orchestrator, context module
- Dev deps moved from `[project.optional-dependencies]` to `[dependency-groups]` (uv-native)

**Step 2: Generate the lock file**

```bash
cd intelligence_service && uv lock
```

Expected: Creates `intelligence_service/uv.lock` with pinned versions. This may take 1-2 minutes as it resolves the git dependencies.

If `uv lock` fails because the git repositories are not accessible, that's OK for now — the lock file will be generated during Docker build. In that case, skip the lock step and note it in the commit message.

**Step 3: Verify existing tests still pass**

```bash
cd intelligence_service && uv run pytest tests/ -v
```

Expected: All 26 existing tests pass. The new dependencies are not imported yet, so they don't need to be installed for tests to pass.

**Step 4: Commit**

```bash
git add intelligence_service/pyproject.toml intelligence_service/uv.lock
git commit -m "build: add amplifier dependencies to pyproject.toml

Add amplifier-foundation, amplifier-core, all 7 provider modules,
loop-basic orchestrator, and context-simple as direct dependencies.
Move dev deps to dependency-groups (uv-native). Add websockets dep.
Rename project to context-intelligence-service."
```

---

### Task 2: Add New Config Settings

**Files:**
- Modify: `intelligence_service/config.py`
- Modify: `tests/intelligence_service/test_config.py`

**Step 1: Write the failing tests**

Add these tests to the bottom of `tests/intelligence_service/test_config.py`:

```python
def test_settings_amplifier_home_default() -> None:
    """amplifier_home defaults to /data/context-intelligence-service."""
    settings = Settings()

    assert settings.amplifier_home == "/data/context-intelligence-service"


def test_settings_bundle_path_default() -> None:
    """bundle_path defaults to empty string (stub mode when unset)."""
    settings = Settings()

    assert settings.bundle_path == ""


def test_settings_routing_matrix_default() -> None:
    """routing_matrix defaults to 'balanced'."""
    settings = Settings()

    assert settings.routing_matrix == "balanced"


def test_settings_workspace_default() -> None:
    """workspace defaults to 'context-intelligence-service'."""
    settings = Settings()

    assert settings.workspace == "context-intelligence-service"


def test_settings_amplifier_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """INTEL_SERVICE_ prefixed env vars override amplifier settings."""
    monkeypatch.setenv("INTEL_SERVICE_AMPLIFIER_HOME", "/custom/home")
    monkeypatch.setenv("INTEL_SERVICE_BUNDLE_PATH", "/custom/bundle.md")
    monkeypatch.setenv("INTEL_SERVICE_ROUTING_MATRIX", "anthropic")
    monkeypatch.setenv("INTEL_SERVICE_WORKSPACE", "my-workspace")

    settings = Settings()

    assert settings.amplifier_home == "/custom/home"
    assert settings.bundle_path == "/custom/bundle.md"
    assert settings.routing_matrix == "anthropic"
    assert settings.workspace == "my-workspace"
```

**Step 2: Run tests to verify they fail**

```bash
cd intelligence_service && uv run pytest tests/intelligence_service/test_config.py -v
```

Expected: 3 existing tests PASS, 5 new tests FAIL with `AttributeError` (fields don't exist on Settings yet).

**Step 3: Add the new fields to Settings**

In `intelligence_service/config.py`, add 4 new fields to the `Settings` class, after the existing `log_level` field:

```python
    log_level: str = "INFO"

    # Amplifier integration
    amplifier_home: str = "/data/context-intelligence-service"
    bundle_path: str = ""
    routing_matrix: str = "balanced"
    workspace: str = "context-intelligence-service"
```

**Step 4: Run tests to verify they pass**

```bash
cd intelligence_service && uv run pytest tests/intelligence_service/test_config.py -v
```

Expected: All 8 tests PASS.

**Step 5: Also update test_settings_defaults to cover new fields**

In `tests/intelligence_service/test_config.py`, update the existing `test_settings_defaults` test to also assert the new fields. Add these lines at the bottom of that test function:

```python
    assert settings.amplifier_home == "/data/context-intelligence-service"
    assert settings.bundle_path == ""
    assert settings.routing_matrix == "balanced"
    assert settings.workspace == "context-intelligence-service"
```

**Step 6: Run full test suite**

```bash
cd intelligence_service && uv run pytest tests/ -v
```

Expected: All tests pass (26 existing + 5 new = 31).

**Step 7: Commit**

```bash
git add intelligence_service/config.py tests/intelligence_service/test_config.py
git commit -m "feat(config): add amplifier_home, bundle_path, routing_matrix, workspace settings

New INTEL_SERVICE_ prefixed settings for Amplifier integration:
- amplifier_home: runtime data directory (default: /data/context-intelligence-service)
- bundle_path: path to pre-baked server bundle (default: '' for stub mode)
- routing_matrix: which matrix to use (default: balanced)
- workspace: self-telemetry identity (default: context-intelligence-service)"
```

---

### Task 3: Create AmplifierApp (Bundle Lifecycle Manager)

**Files:**
- Create: `intelligence_service/amplifier_app.py`
- Create: `tests/intelligence_service/test_amplifier_app.py`

This is the core new module. `AmplifierApp` encapsulates the entire `PreparedBundle` lifecycle: load → compose → prepare → reload → close. It keeps `app.py` thin.

**Step 1: Write the failing tests**

Create `tests/intelligence_service/test_amplifier_app.py`:

```python
"""Tests for the AmplifierApp bundle lifecycle manager.

All Amplifier APIs are mocked — no real bundle loading, composition, or preparation.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from intelligence_service.amplifier_app import AmplifierApp


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_amplifier_app_stores_config() -> None:
    """AmplifierApp stores bundle_path, routing_matrix, and amplifier_home."""
    app = AmplifierApp(
        bundle_path="/app/bundles/server/bundle.md",
        routing_matrix="balanced",
        amplifier_home="/data/amplifier",
    )

    assert app.bundle_path == "/app/bundles/server/bundle.md"
    assert app.routing_matrix == "balanced"
    assert app.amplifier_home == "/data/amplifier"


def test_amplifier_app_prepared_is_none_before_startup() -> None:
    """prepared property is None before startup() is called."""
    app = AmplifierApp(
        bundle_path="/app/bundles/server/bundle.md",
        routing_matrix="balanced",
        amplifier_home="/data/amplifier",
    )

    assert app.prepared is None


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------


@patch("intelligence_service.amplifier_app.load_bundle", new_callable=AsyncMock)
async def test_startup_calls_load_bundle_with_path(mock_load: AsyncMock) -> None:
    """startup() calls load_bundle with the configured bundle_path."""
    mock_bundle = MagicMock()
    mock_composed = MagicMock()
    mock_prepared = MagicMock()

    mock_load.return_value = mock_bundle
    mock_bundle.compose.return_value = mock_composed
    mock_composed.prepare = AsyncMock(return_value=mock_prepared)

    app = AmplifierApp(
        bundle_path="/app/bundles/server/bundle.md",
        routing_matrix="balanced",
        amplifier_home="/data/amplifier",
    )
    await app.startup()

    mock_load.assert_awaited_once_with("/app/bundles/server/bundle.md")


@patch("intelligence_service.amplifier_app.load_bundle", new_callable=AsyncMock)
async def test_startup_composes_routing_overlay(mock_load: AsyncMock) -> None:
    """startup() composes the loaded bundle with a routing overlay."""
    mock_bundle = MagicMock()
    mock_composed = MagicMock()
    mock_prepared = MagicMock()

    mock_load.return_value = mock_bundle
    mock_bundle.compose.return_value = mock_composed
    mock_composed.prepare = AsyncMock(return_value=mock_prepared)

    app = AmplifierApp(
        bundle_path="/app/bundles/server/bundle.md",
        routing_matrix="balanced",
        amplifier_home="/data/amplifier",
    )
    await app.startup()

    mock_bundle.compose.assert_called_once()
    # The overlay argument should be a Bundle with hooks-routing config
    overlay = mock_bundle.compose.call_args[0][0]
    assert overlay.name == "routing-config"


@patch("intelligence_service.amplifier_app.load_bundle", new_callable=AsyncMock)
async def test_startup_calls_prepare(mock_load: AsyncMock) -> None:
    """startup() calls prepare() on the composed bundle."""
    mock_bundle = MagicMock()
    mock_composed = MagicMock()
    mock_prepared = MagicMock()

    mock_load.return_value = mock_bundle
    mock_bundle.compose.return_value = mock_composed
    mock_composed.prepare = AsyncMock(return_value=mock_prepared)

    app = AmplifierApp(
        bundle_path="/app/bundles/server/bundle.md",
        routing_matrix="balanced",
        amplifier_home="/data/amplifier",
    )
    await app.startup()

    mock_composed.prepare.assert_awaited_once()
    assert app.prepared is mock_prepared


# ---------------------------------------------------------------------------
# Reload
# ---------------------------------------------------------------------------


@patch("intelligence_service.amplifier_app.load_bundle", new_callable=AsyncMock)
async def test_reload_swaps_prepared_bundle(mock_load: AsyncMock) -> None:
    """reload() replaces the PreparedBundle with a freshly loaded one."""
    mock_bundle = MagicMock()
    mock_composed = MagicMock()
    mock_prepared_old = MagicMock()
    mock_prepared_new = MagicMock()

    mock_load.return_value = mock_bundle
    mock_bundle.compose.return_value = mock_composed
    mock_composed.prepare = AsyncMock(side_effect=[mock_prepared_old, mock_prepared_new])

    app = AmplifierApp(
        bundle_path="/app/bundles/server/bundle.md",
        routing_matrix="balanced",
        amplifier_home="/data/amplifier",
    )
    await app.startup()
    assert app.prepared is mock_prepared_old

    await app.reload()
    assert app.prepared is mock_prepared_new


@patch("intelligence_service.amplifier_app.load_bundle", new_callable=AsyncMock)
async def test_reload_keeps_old_prepared_on_failure(mock_load: AsyncMock) -> None:
    """If reload fails, the old PreparedBundle remains in place."""
    mock_bundle = MagicMock()
    mock_composed = MagicMock()
    mock_prepared_old = MagicMock()

    mock_load.return_value = mock_bundle
    mock_bundle.compose.return_value = mock_composed
    mock_composed.prepare = AsyncMock(
        side_effect=[mock_prepared_old, RuntimeError("prepare failed")]
    )

    app = AmplifierApp(
        bundle_path="/app/bundles/server/bundle.md",
        routing_matrix="balanced",
        amplifier_home="/data/amplifier",
    )
    await app.startup()

    with pytest.raises(RuntimeError, match="prepare failed"):
        await app.reload()

    assert app.prepared is mock_prepared_old


# ---------------------------------------------------------------------------
# Close
# ---------------------------------------------------------------------------


def test_close_clears_prepared() -> None:
    """close() sets prepared to None."""
    app = AmplifierApp(
        bundle_path="/app/bundles/server/bundle.md",
        routing_matrix="balanced",
        amplifier_home="/data/amplifier",
    )
    # Manually set a fake prepared to verify close clears it
    app._prepared = MagicMock()
    assert app.prepared is not None

    app.close()

    assert app.prepared is None
```

**Step 2: Run tests to verify they fail**

```bash
cd intelligence_service && uv run pytest tests/intelligence_service/test_amplifier_app.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'intelligence_service.amplifier_app'`

**Step 3: Write the implementation**

Create `intelligence_service/amplifier_app.py`:

```python
"""Bundle lifecycle manager for the Intelligence Service.

AmplifierApp encapsulates load → compose → prepare → reload → close.
It is the single owner of the PreparedBundle singleton.
"""

import logging
from typing import Any

from amplifier_foundation import load_bundle
from amplifier_foundation.bundle import Bundle

logger = logging.getLogger(__name__)


class AmplifierApp:
    """Manage the PreparedBundle lifecycle.

    Usage::

        app = AmplifierApp(bundle_path="...", routing_matrix="balanced", amplifier_home="...")
        await app.startup()          # load → compose → prepare
        session = await app.prepared.create_session(...)
        await app.reload()           # hot-reload: re-load → re-compose → re-prepare
        app.close()                  # cleanup
    """

    def __init__(
        self,
        *,
        bundle_path: str,
        routing_matrix: str,
        amplifier_home: str,
    ) -> None:
        self.bundle_path = bundle_path
        self.routing_matrix = routing_matrix
        self.amplifier_home = amplifier_home
        self._prepared: Any | None = None

    @property
    def prepared(self) -> Any | None:
        """Return the current PreparedBundle, or None if not started."""
        return self._prepared

    async def startup(self) -> None:
        """Load bundle, compose routing overlay, and prepare."""
        logger.info("Loading bundle from %s", self.bundle_path)
        self._prepared = await self._load_and_prepare()
        logger.info("Bundle prepared successfully")

    async def reload(self) -> None:
        """Hot-reload: re-load, re-compose, re-prepare, swap PreparedBundle.

        On failure the old PreparedBundle remains in place.
        """
        logger.info("Reloading bundle from %s", self.bundle_path)
        new_prepared = await self._load_and_prepare()
        self._prepared = new_prepared
        logger.info("Bundle reloaded successfully")

    def close(self) -> None:
        """Clear the PreparedBundle reference."""
        self._prepared = None
        logger.info("AmplifierApp closed")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _build_routing_overlay(self) -> Bundle:
        """Build a Bundle overlay that configures the routing matrix."""
        return Bundle(
            name="routing-config",
            hooks=[{
                "module": "hooks-routing",
                "config": {
                    "default_matrix": self.routing_matrix,
                },
            }],
        )

    async def _load_and_prepare(self) -> Any:
        """Load bundle from path, compose with routing overlay, and prepare."""
        bundle = await load_bundle(self.bundle_path)
        overlay = self._build_routing_overlay()
        composed = bundle.compose(overlay)
        prepared = await composed.prepare()
        return prepared
```

**Step 4: Run tests to verify they pass**

```bash
cd intelligence_service && uv run pytest tests/intelligence_service/test_amplifier_app.py -v
```

Expected: All 7 tests PASS.

**Note:** If `from amplifier_foundation import load_bundle` fails because the package isn't installed in the dev environment, use this workaround: add a `try/except ImportError` block at the top of `amplifier_app.py` that stubs the imports. However, the tests mock these imports anyway, so they should pass as long as the module can be imported. If the imports fail at module level before mocking takes effect, restructure the imports to be late-bound or use `importlib`. The tests use `@patch("intelligence_service.amplifier_app.load_bundle")` which requires the name to exist in the module namespace. If `amplifier_foundation` is not installed, change the import to a conditional:

```python
try:
    from amplifier_foundation import load_bundle
    from amplifier_foundation.bundle import Bundle
except ImportError:  # pragma: no cover
    load_bundle = None  # type: ignore[assignment]
    Bundle = None  # type: ignore[assignment,misc]
```

The `@patch` decorator will replace these with mocks during testing.

**Step 5: Commit**

```bash
git add intelligence_service/amplifier_app.py tests/intelligence_service/test_amplifier_app.py
git commit -m "feat: AmplifierApp bundle lifecycle manager

Encapsulates load_bundle → compose(routing overlay) → prepare → reload.
Single owner of the PreparedBundle singleton. On reload failure the old
PreparedBundle remains in place. All Amplifier APIs mocked in tests."
```

---

### Task 4: Create AmplifierSessionManager

**Files:**
- Create: `intelligence_service/amplifier_session_manager.py`
- Create: `tests/intelligence_service/test_amplifier_session_manager.py`

The `AmplifierSessionManager` implements the existing `SessionManager` protocol AND adds an `execute` method for running prompts through real Amplifier sessions.

**Step 1: Write the failing tests**

Create `tests/intelligence_service/test_amplifier_session_manager.py`:

```python
"""Tests for AmplifierSessionManager.

All Amplifier APIs are mocked — no real sessions are created.
"""

from unittest.mock import AsyncMock, MagicMock

from intelligence_service.amplifier_session_manager import AmplifierSessionManager
from intelligence_service.session_manager import SessionManager


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_amplifier_session_manager_satisfies_protocol() -> None:
    """AmplifierSessionManager is a runtime-checkable SessionManager."""
    mock_amplifier_app = MagicMock()
    manager = AmplifierSessionManager(
        amplifier_app=mock_amplifier_app, workspace="test-workspace"
    )

    assert isinstance(manager, SessionManager)


# ---------------------------------------------------------------------------
# Create session
# ---------------------------------------------------------------------------


async def test_create_session_returns_string_id() -> None:
    """create_session() returns a non-empty string session ID."""
    mock_prepared = MagicMock()
    mock_session = MagicMock()
    mock_prepared.create_session = AsyncMock(return_value=mock_session)

    mock_amplifier_app = MagicMock()
    mock_amplifier_app.prepared = mock_prepared

    manager = AmplifierSessionManager(
        amplifier_app=mock_amplifier_app, workspace="test-workspace"
    )
    session_id = await manager.create_session()

    assert isinstance(session_id, str)
    assert len(session_id) > 0


async def test_create_session_calls_prepared_create_session() -> None:
    """create_session() delegates to amplifier_app.prepared.create_session()."""
    mock_prepared = MagicMock()
    mock_session = MagicMock()
    mock_prepared.create_session = AsyncMock(return_value=mock_session)

    mock_amplifier_app = MagicMock()
    mock_amplifier_app.prepared = mock_prepared

    manager = AmplifierSessionManager(
        amplifier_app=mock_amplifier_app, workspace="test-workspace"
    )
    await manager.create_session()

    mock_prepared.create_session.assert_awaited_once()


async def test_create_session_increments_active_count() -> None:
    """active_count goes 0 → 1 → 2 as sessions are created."""
    mock_prepared = MagicMock()
    mock_prepared.create_session = AsyncMock(return_value=MagicMock())

    mock_amplifier_app = MagicMock()
    mock_amplifier_app.prepared = mock_prepared

    manager = AmplifierSessionManager(
        amplifier_app=mock_amplifier_app, workspace="test-workspace"
    )

    assert manager.active_count == 0
    await manager.create_session()
    assert manager.active_count == 1
    await manager.create_session()
    assert manager.active_count == 2


# ---------------------------------------------------------------------------
# Destroy session
# ---------------------------------------------------------------------------


async def test_destroy_session_decrements_active_count() -> None:
    """Destroying a session decrements active_count."""
    mock_prepared = MagicMock()
    mock_prepared.create_session = AsyncMock(return_value=MagicMock())

    mock_amplifier_app = MagicMock()
    mock_amplifier_app.prepared = mock_prepared

    manager = AmplifierSessionManager(
        amplifier_app=mock_amplifier_app, workspace="test-workspace"
    )
    session_id = await manager.create_session()
    assert manager.active_count == 1

    await manager.destroy_session(session_id)
    assert manager.active_count == 0


async def test_destroy_nonexistent_session_is_noop() -> None:
    """Destroying an unknown session ID does not raise."""
    mock_amplifier_app = MagicMock()
    manager = AmplifierSessionManager(
        amplifier_app=mock_amplifier_app, workspace="test-workspace"
    )

    await manager.destroy_session("nonexistent")
    assert manager.active_count == 0


# ---------------------------------------------------------------------------
# Reset session
# ---------------------------------------------------------------------------


async def test_reset_session_returns_new_id() -> None:
    """reset_session() destroys old session and creates new one with different ID."""
    mock_prepared = MagicMock()
    mock_prepared.create_session = AsyncMock(return_value=MagicMock())

    mock_amplifier_app = MagicMock()
    mock_amplifier_app.prepared = mock_prepared

    manager = AmplifierSessionManager(
        amplifier_app=mock_amplifier_app, workspace="test-workspace"
    )
    old_id = await manager.create_session()
    new_id = await manager.reset_session(old_id)

    assert new_id != old_id
    assert manager.active_count == 1


# ---------------------------------------------------------------------------
# Get session
# ---------------------------------------------------------------------------


async def test_get_session_returns_metadata() -> None:
    """get_session() returns dict with session_id and status."""
    mock_prepared = MagicMock()
    mock_prepared.create_session = AsyncMock(return_value=MagicMock())

    mock_amplifier_app = MagicMock()
    mock_amplifier_app.prepared = mock_prepared

    manager = AmplifierSessionManager(
        amplifier_app=mock_amplifier_app, workspace="test-workspace"
    )
    session_id = await manager.create_session()
    metadata = await manager.get_session(session_id)

    assert metadata is not None
    assert metadata["session_id"] == session_id
    assert metadata["status"] == "active"


async def test_get_session_returns_none_for_unknown() -> None:
    """get_session() returns None for unknown session ID."""
    mock_amplifier_app = MagicMock()
    manager = AmplifierSessionManager(
        amplifier_app=mock_amplifier_app, workspace="test-workspace"
    )

    assert await manager.get_session("unknown") is None


# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------


async def test_execute_calls_session_execute() -> None:
    """execute() delegates to the Amplifier session's execute method."""
    mock_session = MagicMock()
    mock_session.execute = AsyncMock(return_value="AI response text")

    mock_prepared = MagicMock()
    mock_prepared.create_session = AsyncMock(return_value=mock_session)

    mock_amplifier_app = MagicMock()
    mock_amplifier_app.prepared = mock_prepared

    manager = AmplifierSessionManager(
        amplifier_app=mock_amplifier_app, workspace="test-workspace"
    )
    session_id = await manager.create_session()
    result = await manager.execute(session_id, "What is the graph structure?")

    mock_session.execute.assert_awaited_once_with("What is the graph structure?")
    assert result["text"] == "AI response text"
    assert result["a2ui"] == []


async def test_execute_unknown_session_raises_key_error() -> None:
    """execute() raises KeyError for unknown session ID."""
    mock_amplifier_app = MagicMock()
    manager = AmplifierSessionManager(
        amplifier_app=mock_amplifier_app, workspace="test-workspace"
    )

    import pytest as _pytest

    with _pytest.raises(KeyError):
        await manager.execute("nonexistent", "hello")


# ---------------------------------------------------------------------------
# Close all
# ---------------------------------------------------------------------------


async def test_close_all_clears_all_sessions() -> None:
    """close_all() removes all sessions and resets active_count to 0."""
    mock_prepared = MagicMock()
    mock_prepared.create_session = AsyncMock(return_value=MagicMock())

    mock_amplifier_app = MagicMock()
    mock_amplifier_app.prepared = mock_prepared

    manager = AmplifierSessionManager(
        amplifier_app=mock_amplifier_app, workspace="test-workspace"
    )
    await manager.create_session()
    await manager.create_session()
    assert manager.active_count == 2

    await manager.close_all()
    assert manager.active_count == 0
```

**Step 2: Run tests to verify they fail**

```bash
cd intelligence_service && uv run pytest tests/intelligence_service/test_amplifier_session_manager.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'intelligence_service.amplifier_session_manager'`

**Step 3: Write the implementation**

Create `intelligence_service/amplifier_session_manager.py`:

```python
"""Amplifier-backed session manager.

Creates real Amplifier sessions via PreparedBundle.create_session().
Implements the SessionManager protocol from session_manager.py.
"""

import logging
import uuid
from typing import Any

from intelligence_service.a2ui_bridge import extract_a2ui_from_response

logger = logging.getLogger(__name__)


class AmplifierSessionManager:
    """Map conversation IDs to real Amplifier sessions.

    Implements the SessionManager protocol and adds execute() for
    running prompts through the Amplifier agentic loop.
    """

    def __init__(self, *, amplifier_app: Any, workspace: str) -> None:
        self._amplifier_app = amplifier_app
        self._workspace = workspace
        self._sessions: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # SessionManager protocol
    # ------------------------------------------------------------------

    @property
    def active_count(self) -> int:
        """Return the number of currently active sessions."""
        return len(self._sessions)

    async def create_session(self) -> str:
        """Create a new Amplifier session and return its ID."""
        session_id = str(uuid.uuid4())
        session = await self._amplifier_app.prepared.create_session(
            session_id=session_id,
            session_cwd=f"/data/workspace/{self._workspace}",
        )
        self._sessions[session_id] = session
        logger.info("Created Amplifier session %s", session_id)
        return session_id

    async def destroy_session(self, session_id: str) -> None:
        """Remove session. No-op if not found."""
        self._sessions.pop(session_id, None)

    async def reset_session(self, session_id: str) -> str:
        """Destroy old session and create a replacement."""
        await self.destroy_session(session_id)
        return await self.create_session()

    async def get_session(self, session_id: str) -> dict[str, str] | None:
        """Return session metadata, or None if not found."""
        if session_id not in self._sessions:
            return None
        return {"session_id": session_id, "status": "active"}

    # ------------------------------------------------------------------
    # Extended API (beyond SessionManager protocol)
    # ------------------------------------------------------------------

    async def execute(self, session_id: str, prompt: str) -> dict[str, Any]:
        """Execute a prompt in the given session and return the result.

        Returns dict with 'text' (response string) and 'a2ui' (list of
        A2UI payloads extracted from tool results, empty until agent design).

        Raises KeyError if session_id is not found.
        """
        session = self._sessions[session_id]
        response = await session.execute(prompt)
        a2ui_messages = extract_a2ui_from_response(response)
        return {"text": response, "a2ui": a2ui_messages}

    async def close_all(self) -> None:
        """Close and remove all active sessions."""
        self._sessions.clear()
        logger.info("All sessions closed")
```

**Step 4: Run tests to verify they pass**

```bash
cd intelligence_service && uv run pytest tests/intelligence_service/test_amplifier_session_manager.py -v
```

Expected: FAIL — `ImportError: cannot import name 'extract_a2ui_from_response' from 'intelligence_service.a2ui_bridge'`

This is expected! `extract_a2ui_from_response` doesn't exist yet. We'll create it in Task 5. For now, to unblock this task, add a temporary stub to `a2ui_bridge.py` (we'll TDD the real version in Task 5):

Add to the bottom of `intelligence_service/a2ui_bridge.py`:

```python


def extract_a2ui_from_response(response: Any) -> list[dict[str, Any]]:
    """Extract A2UI payloads from an Amplifier session response.

    Returns a list of A2UI message dicts found in tool results.
    Currently returns an empty list — the extraction logic depends on
    agent tool design (deferred to agent design phase).
    """
    return []
```

Now run again:

```bash
cd intelligence_service && uv run pytest tests/intelligence_service/test_amplifier_session_manager.py -v
```

Expected: All 13 tests PASS.

**Step 5: Commit**

```bash
git add intelligence_service/amplifier_session_manager.py \
      tests/intelligence_service/test_amplifier_session_manager.py \
      intelligence_service/a2ui_bridge.py
git commit -m "feat: AmplifierSessionManager with real Amplifier sessions

Implements SessionManager protocol plus execute() for running prompts
through Amplifier's agentic loop. Maps conversation IDs to Amplifier
sessions created via PreparedBundle.create_session(). Includes stub
for extract_a2ui_from_response (full implementation in next task)."
```

---

### Task 5: TDD extract_a2ui_from_response

**Files:**
- Modify: `intelligence_service/a2ui_bridge.py` (stub already added in Task 4)
- Modify: `tests/intelligence_service/test_a2ui_bridge.py`

The stub was added in Task 4. Now we add proper tests to lock down the interface and behavior.

**Step 1: Write the tests**

Add these tests to the bottom of `tests/intelligence_service/test_a2ui_bridge.py`:

```python
from intelligence_service.a2ui_bridge import extract_a2ui_from_response


# ---------------------------------------------------------------------------
# extract_a2ui_from_response
# ---------------------------------------------------------------------------


def test_extract_a2ui_from_string_response_returns_empty_list() -> None:
    """A plain string response has no A2UI payloads."""
    result = extract_a2ui_from_response("Here is the analysis of the graph.")

    assert result == []


def test_extract_a2ui_from_none_returns_empty_list() -> None:
    """None response returns empty list."""
    result = extract_a2ui_from_response(None)

    assert result == []


def test_extract_a2ui_return_type_is_list() -> None:
    """Return type is always a list."""
    result = extract_a2ui_from_response("anything")

    assert isinstance(result, list)
```

**Step 2: Run tests to verify they pass**

```bash
cd intelligence_service && uv run pytest tests/intelligence_service/test_a2ui_bridge.py -v
```

Expected: All 11 tests PASS (8 existing + 3 new). The stub from Task 4 already returns `[]` for any input, so these tests pass immediately. This is intentional — we're locking down the interface contract now so the real implementation (deferred to agent design phase) can't break it.

**Step 3: Run the full a2ui_bridge test suite**

```bash
cd intelligence_service && uv run pytest tests/intelligence_service/test_a2ui_bridge.py -v
```

Expected: All 11 tests PASS.

**Step 4: Commit**

```bash
git add tests/intelligence_service/test_a2ui_bridge.py
git commit -m "test: lock down extract_a2ui_from_response interface contract

Tests for string, None, and return type. The function currently returns
empty list for all inputs — real extraction logic depends on agent tool
design (deferred to agent design phase)."
```

---

### Task 6: Revise app.py — Lifespan and Reload Endpoint

**Files:**
- Modify: `intelligence_service/app.py`

This is the integration task. The lifespan handler creates either `AmplifierApp + AmplifierSessionManager` (production, when `BUNDLE_PATH` is set) or `StubSessionManager` (dev/test mode). The reload endpoint becomes POST and calls `amplifier_app.reload()`.

**Step 1: Replace app.py content**

Replace the entire content of `intelligence_service/app.py` with:

```python
"""Intelligence Service FastAPI application."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from intelligence_service.a2ui_bridge import (
    format_action_ack,
    format_error,
    format_response,
    format_session_created,
    parse_incoming,
)
from intelligence_service.config import get_settings
from intelligence_service.drain import DrainManager
from intelligence_service.session_manager import SessionManager, StubSessionManager

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application startup and shutdown lifecycle."""
    settings = get_settings()
    logger.info("Intelligence Service starting up")

    application.state.drain = DrainManager(
        timeout_seconds=settings.drain_timeout_seconds
    )
    application.state.amplifier_app = None

    if settings.bundle_path:
        # Production mode: real Amplifier integration
        from intelligence_service.amplifier_app import AmplifierApp
        from intelligence_service.amplifier_session_manager import (
            AmplifierSessionManager,
        )

        amplifier_app = AmplifierApp(
            bundle_path=settings.bundle_path,
            routing_matrix=settings.routing_matrix,
            amplifier_home=settings.amplifier_home,
        )
        await amplifier_app.startup()
        application.state.amplifier_app = amplifier_app
        application.state.session_manager = AmplifierSessionManager(
            amplifier_app=amplifier_app,
            workspace=settings.workspace,
        )
        logger.info("Amplifier integration active (matrix=%s)", settings.routing_matrix)
    else:
        # Dev/test mode: stub session manager
        application.state.session_manager = StubSessionManager()
        logger.info("Stub mode (no BUNDLE_PATH set)")

    try:
        yield
    finally:
        logger.info("Intelligence Service shutting down")
        await application.state.drain.start_drain()
        if hasattr(application.state.session_manager, "close_all"):
            await application.state.session_manager.close_all()
        if application.state.amplifier_app is not None:
            application.state.amplifier_app.close()


app = FastAPI(title="Intelligence Service", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    """Return service health status."""
    return {"status": "ok"}


@app.post("/admin/reload-bundle")
async def reload_bundle() -> dict[str, str]:
    """Hot-reload the Amplifier bundle without restarting the container."""
    amplifier_app = app.state.amplifier_app
    if amplifier_app is None:
        return {"status": "skipped", "message": "No bundle configured (stub mode)."}

    try:
        await amplifier_app.reload()
        return {"status": "reloaded"}
    except Exception as exc:
        logger.exception("Bundle reload failed")
        return {"status": "error", "message": str(exc)}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """WebSocket endpoint wiring session manager, A2UI bridge, and drain."""
    drain: DrainManager = websocket.app.state.drain
    session_manager: SessionManager = websocket.app.state.session_manager

    if not drain.accepting:
        await websocket.close(code=1013)
        return

    await websocket.accept()

    session_id = await session_manager.create_session()
    drain.register(session_id)

    try:
        await websocket.send_json(format_session_created(session_id))

        while True:
            data = await websocket.receive_json()
            msg = parse_incoming(data)

            if msg.msg_type == "new_session":
                old_session_id = session_id
                session_id = await session_manager.reset_session(old_session_id)
                drain.unregister(old_session_id)
                drain.register(session_id)
                await websocket.send_json(format_session_created(session_id))

            elif msg.msg_type == "message":
                text = msg.payload.get("text", "")
                if hasattr(session_manager, "execute"):
                    result = await session_manager.execute(session_id, text)
                    await websocket.send_json(
                        format_response(session_id, result["text"])
                    )
                    for a2ui_msg in result.get("a2ui", []):
                        await websocket.send_json(a2ui_msg)
                else:
                    await websocket.send_json(format_response(session_id, text))

            elif msg.msg_type == "action":
                component_id = msg.payload.get("componentId", "")
                await websocket.send_json(
                    format_action_ack(session_id, component_id)
                )

            else:
                await websocket.send_json(
                    format_error(
                        session_id,
                        f"Unknown message type: {msg.msg_type}",
                    )
                )

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for session %s", session_id)
    finally:
        drain.unregister(session_id)
        await session_manager.destroy_session(session_id)
```

Key changes:
- Lifespan branches on `settings.bundle_path`: real Amplifier or stub
- Imports of `AmplifierApp` / `AmplifierSessionManager` are **lazy** (inside the if-block) so dev/test mode never needs amplifier packages installed
- `reload_bundle` is now `POST` (was `GET`), calls `amplifier_app.reload()`
- Message handler checks `hasattr(session_manager, "execute")` for Amplifier vs stub mode
- Shutdown calls `close_all()` and `amplifier_app.close()` when available

**Step 2: Run existing tests to verify no regressions**

```bash
cd intelligence_service && uv run pytest tests/intelligence_service/test_app.py -v
```

Expected: The existing WebSocket tests continue to pass because `BUNDLE_PATH` is not set — the lifespan creates a `StubSessionManager` (same as before). **However**, the `test_reload_bundle_returns_stub_response` test will fail because the endpoint changed from GET to POST and the response changed. We need to update that test in Task 7.

For now, expect 6 of 7 tests to pass, 1 to fail (the reload test). That's OK — Task 7 fixes it.

**Step 3: Commit**

```bash
git add intelligence_service/app.py
git commit -m "feat(app): real Amplifier integration in lifespan + POST reload

Lifespan branches on bundle_path: AmplifierApp + AmplifierSessionManager
when set, StubSessionManager when unset. Lazy imports keep dev mode
lightweight. reload-bundle becomes POST with error handling. Message
handler dispatches through execute() when available."
```

---

### Task 7: Revise App Tests

**Files:**
- Modify: `tests/intelligence_service/test_app.py`

Update existing tests for the POST reload endpoint and add a test for reload in stub mode.

**Step 1: Replace test_app.py content**

Replace the entire content of `tests/intelligence_service/test_app.py` with:

```python
"""Tests for the Intelligence Service FastAPI application.

All tests run in stub mode (no BUNDLE_PATH set), so StubSessionManager
is used. Amplifier integration is tested via mocked AmplifierApp in
test_amplifier_app.py and test_amplifier_session_manager.py.
"""

import httpx
from fastapi.testclient import TestClient

from intelligence_service.app import app


# ---------------------------------------------------------------------------
# HTTP endpoint tests
# ---------------------------------------------------------------------------


async def test_health_returns_200_with_status_ok(client: httpx.AsyncClient) -> None:
    """GET /health returns 200 with {'status': 'ok'}."""
    response = await client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"


async def test_reload_bundle_stub_mode_returns_skipped(
    client: httpx.AsyncClient,
) -> None:
    """POST /admin/reload-bundle in stub mode returns 'skipped'."""
    response = await client.post("/admin/reload-bundle")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "skipped"


async def test_reload_bundle_get_not_allowed(client: httpx.AsyncClient) -> None:
    """GET /admin/reload-bundle returns 405 (method changed to POST)."""
    response = await client.get("/admin/reload-bundle")

    assert response.status_code == 405


# ---------------------------------------------------------------------------
# WebSocket tests — use Starlette TestClient (sync) for WS support
# ---------------------------------------------------------------------------


def test_ws_connect_receives_session_created() -> None:
    """Connecting to /ws immediately receives a session_created message with a session_id."""
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            data = ws.receive_json()

    assert data["type"] == "session_created"
    assert "session_id" in data


def test_ws_message_receives_response() -> None:
    """Sending a message yields a response message whose content echoes the text."""
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            ws.receive_json()  # consume session_created
            ws.send_json({"type": "message", "text": "hello"})
            data = ws.receive_json()

    assert data["type"] == "response"
    assert "hello" in data["content"]


def test_ws_new_session_returns_different_id() -> None:
    """Sending new_session yields a session_created with a different session_id."""
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            first = ws.receive_json()  # initial session_created
            original_id = first["session_id"]
            ws.send_json({"type": "new_session"})
            data = ws.receive_json()

    assert data["type"] == "session_created"
    assert data["session_id"] != original_id


def test_ws_action_receives_ack() -> None:
    """Sending an action message yields an action_ack with the matching component_id."""
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            ws.receive_json()  # consume session_created
            ws.send_json({"type": "action", "componentId": "graph-1"})
            data = ws.receive_json()

    assert data["type"] == "action_ack"
    assert data["component_id"] == "graph-1"


def test_ws_unknown_type_receives_error() -> None:
    """Sending an unrecognised message type yields an error containing the type name."""
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            ws.receive_json()  # consume session_created
            ws.send_json({"type": "invalid_type"})
            data = ws.receive_json()

    assert data["type"] == "error"
    assert "invalid_type" in data["message"]


def test_ws_new_session_no_drain_leak() -> None:
    """After new_session and disconnect, drain has zero active sessions.

    Regression: the old session_id must be unregistered from drain when a
    new_session reset occurs.
    """
    with TestClient(app) as client:
        drain = app.state.drain
        with client.websocket_connect("/ws") as ws:
            ws.receive_json()  # initial session_created
            ws.send_json({"type": "new_session"})
            ws.receive_json()  # new session_created
        assert drain.active_count == 0
```

**Step 2: Run tests to verify they pass**

```bash
cd intelligence_service && uv run pytest tests/intelligence_service/test_app.py -v
```

Expected: All 10 tests PASS (was 7, now 10 — added reload POST, GET 405, removed old stub GET test).

**Step 3: Run the full test suite**

```bash
cd intelligence_service && uv run pytest tests/ -v
```

Expected: All tests pass. Count should be approximately 44+ (31 from Task 2 + 7 amplifier_app + 13 amplifier_session_manager + 3 a2ui extract - 7 old app tests + 10 new app tests = ~57).

**Step 4: Commit**

```bash
git add tests/intelligence_service/test_app.py
git commit -m "test(app): update tests for POST reload and stub mode

Reload endpoint changed from GET to POST. Add test for GET returning
405. Reload in stub mode returns 'skipped'. All WebSocket tests
unchanged — they use StubSessionManager (no BUNDLE_PATH set)."
```

---

### Task 8: Rewrite Dockerfile.intelligence

**Files:**
- Modify: `Dockerfile.intelligence`

**Step 1: Replace Dockerfile content**

Replace the entire content of `Dockerfile.intelligence` with:

```dockerfile
FROM python:3.13-slim

# Install uv (Amplifier ecosystem standard)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Build tools for amplifier-core's Rust bindings
RUN apt-get update && apt-get install -y --no-install-recommends \
    git build-essential pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy dependency manifests first (layer caching: deps change rarely)
COPY intelligence_service/pyproject.toml intelligence_service/uv.lock \
     /app/intelligence_service/
WORKDIR /app/intelligence_service
RUN uv sync --frozen --no-dev --no-install-project

# Copy service source code and install the project itself
COPY intelligence_service/ /app/intelligence_service/
RUN uv sync --frozen --no-dev

# Pre-bake the server bundle (read-only, local path reference)
COPY amplifier-bundle-context-intelligence-server/ \
     /app/bundles/context-intelligence-server/

# Runtime config
ENV AMPLIFIER_HOME=/data/context-intelligence-service
ENV BUNDLE_PATH=/app/bundles/context-intelligence-server/bundle.md
EXPOSE 8100

# Cold start: prepare() downloads modules on first run (up to 3 min)
# Warm start: cache hit from AMPLIFIER_HOME volume (5-15s)
HEALTHCHECK --interval=10s --timeout=5s --retries=60 --start-period=180s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8100/health')"

CMD ["uv", "run", "uvicorn", "intelligence_service.app:app", \
     "--host", "0.0.0.0", "--port", "8100"]
```

Key changes from Phase 1:
- No `uv tool install amplifier` — no CLI
- No `entrypoint.sh` — pure Python startup via uvicorn
- Two-stage `uv sync`: deps first (cached), then source (fast rebuild)
- `AMPLIFIER_HOME` and `BUNDLE_PATH` env vars set
- 180s `start_period` for cold starts (prepare downloads modules)
- `build-essential`, `pkg-config`, `libssl-dev` for amplifier-core Rust bindings

**Step 2: Validate Dockerfile syntax**

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence && docker build --check -f Dockerfile.intelligence . 2>&1 || echo "Docker check not available — visual review OK"
```

Expected: No syntax errors (or the `--check` flag isn't supported, in which case visual review is sufficient).

**Step 3: Commit**

```bash
git add Dockerfile.intelligence
git commit -m "build: rewrite Dockerfile.intelligence for programmatic Amplifier

Pure Python startup via uvicorn, no CLI or entrypoint script.
Two-stage uv sync for layer caching. Build tools for Rust bindings.
AMPLIFIER_HOME and BUNDLE_PATH env vars. 180s start_period for
cold start (prepare downloads modules)."
```

---

### Task 9: Update docker-compose.yml

**Files:**
- Modify: `docker-compose.yml`

**Step 1: Replace docker-compose.yml content**

Replace the entire content of `docker-compose.yml` with:

```yaml
services:
  context-intelligence-server:
    build: .
    ports:
      - "8000:8000"
    environment:
      CI_SERVER_NEO4J_URL: neo4j://neo4j:7687
      CI_SERVER_BLOB_PATH: /data/blobs
      PYTHONUNBUFFERED: "1"
    volumes:
      - blob_data:/data/blobs
      - log_data:/data/logs
    depends_on:
      neo4j:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/status"]
      interval: 10s
      timeout: 5s
      retries: 3
    restart: unless-stopped
    labels:
      com.context-intelligence.component: server
    networks:
      - context-intelligence

  intelligence-service:
    build:
      context: .
      dockerfile: Dockerfile.intelligence
    ports:
      - "8100:8100"
    depends_on:
      context-intelligence-server:
        condition: service_healthy
    volumes:
      - context_intelligence_service_data:/data/context-intelligence-service
      - blob_data:/data/blobs:ro
    env_file: config/secrets.env
    environment:
      AMPLIFIER_HOME: /data/context-intelligence-service
      BUNDLE_PATH: /app/bundles/context-intelligence-server/bundle.md
      ROUTING_MATRIX: balanced
      INTEL_SERVICE_INGESTION_URL: http://context-intelligence-server:8000
      PYTHONUNBUFFERED: "1"
    networks:
      - context-intelligence
    labels:
      com.context-intelligence.component: intelligence
    healthcheck:
      test: ["CMD", "python", "-c",
        "import urllib.request; urllib.request.urlopen('http://localhost:8100/health')"]
      interval: 10s
      timeout: 5s
      retries: 60
      start_period: 180s

  neo4j:
    image: neo4j:5.26.22-community
    environment:
      NEO4J_AUTH: none
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - neo4j_data:/data
    restart: unless-stopped
    labels:
      com.context-intelligence.component: neo4j
    healthcheck:
      test: ["CMD", "wget", "-O", "-", "http://localhost:7474"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 15s
    networks:
      - context-intelligence

volumes:
  blob_data:
  neo4j_data:
  log_data:
  context_intelligence_service_data:

networks:
  context-intelligence:
    driver: bridge
```

Changes from Phase 1:
- Added `intelligence-service` service with correct Dockerfile, volumes, env vars
- Added `context_intelligence_service_data` named volume
- `blob_data` mounted read-only in intelligence-service
- `env_file: config/secrets.env` for API keys
- `AMPLIFIER_HOME`, `BUNDLE_PATH`, `ROUTING_MATRIX=balanced`, `INTEL_SERVICE_INGESTION_URL`
- 180s `start_period` healthcheck for cold starts
- `depends_on: context-intelligence-server` with `service_healthy`

**Note:** The `frontend` service is NOT added here — it was already handled in Phase 2 (exploration-system-phase2-frontend). This compose file only adds the intelligence-service and its volume. The frontend service will be added when the frontend Dockerfile is ready.

**Step 2: Validate compose syntax**

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence && docker compose config --quiet 2>&1 || echo "Compose validation requires Docker — visual review OK"
```

**Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "infra: add intelligence-service to docker-compose

New service with Amplifier volumes (context_intelligence_service_data),
env vars (AMPLIFIER_HOME, BUNDLE_PATH, ROUTING_MATRIX=balanced),
read-only blob access, 180s start_period healthcheck, and
depends_on context-intelligence-server."
```

---

### Task 10: Full Test Suite Verification

**Files:** None (verification only)

**Step 1: Run the complete test suite**

```bash
cd intelligence_service && uv run pytest tests/ -v --tb=short
```

Expected: All tests pass. Approximate count:

| Test file | Count |
|-----------|-------|
| `test_config.py` | 8 |
| `test_drain.py` | 9 |
| `test_session_manager.py` | 8 |
| `test_a2ui_bridge.py` | 11 |
| `test_amplifier_app.py` | 7 |
| `test_amplifier_session_manager.py` | 13 |
| `test_app.py` | 10 |
| **Total** | **~66** |

**Step 2: Verify no existing tests were broken**

Specifically confirm:
- `test_session_manager.py`: All 8 StubSessionManager tests still pass (file was not modified)
- `test_drain.py`: All 9 drain tests still pass (file was not modified)
- `test_a2ui_bridge.py`: Original 8 tests still pass, plus 3 new ones

**Step 3: Verify file organization is clean**

```bash
ls -la intelligence_service/*.py
```

Expected files:
- `__init__.py` — package marker (unchanged)
- `__main__.py` — CLI entry point (unchanged)
- `a2ui_bridge.py` — message bridge + extract_a2ui_from_response
- `amplifier_app.py` — **NEW**: bundle lifecycle manager
- `amplifier_session_manager.py` — **NEW**: Amplifier-backed session manager
- `app.py` — FastAPI app with Amplifier lifespan
- `config.py` — settings with Amplifier fields
- `drain.py` — graceful shutdown (unchanged)
- `session_manager.py` — Protocol + StubSessionManager (unchanged)

Each file has ONE responsibility. No bloated files. No code duplication.

**Step 4: Final commit (if any uncommitted changes)**

```bash
git status
```

If clean: done. If stray changes: commit them with an appropriate message.

---

## Summary of Files Changed

| File | Action | Lines (approx) |
|------|--------|-----------------|
| `intelligence_service/pyproject.toml` | Modified | 28 → 47 |
| `intelligence_service/config.py` | Modified | 30 → 35 |
| `intelligence_service/a2ui_bridge.py` | Modified | 63 → 75 |
| `intelligence_service/amplifier_app.py` | **Created** | ~80 |
| `intelligence_service/amplifier_session_manager.py` | **Created** | ~75 |
| `intelligence_service/app.py` | Modified | 103 → ~115 |
| `Dockerfile.intelligence` | Modified | 23 → 30 |
| `docker-compose.yml` | Modified | 55 → 72 |
| `tests/intelligence_service/test_config.py` | Modified | 40 → 70 |
| `tests/intelligence_service/test_a2ui_bridge.py` | Modified | 88 → 110 |
| `tests/intelligence_service/test_amplifier_app.py` | **Created** | ~120 |
| `tests/intelligence_service/test_amplifier_session_manager.py` | **Created** | ~160 |
| `tests/intelligence_service/test_app.py` | Modified | 107 → 100 |

**Unchanged files:** `session_manager.py`, `drain.py`, `__init__.py`, `__main__.py`, `conftest.py`, `test_session_manager.py`, `test_drain.py`
