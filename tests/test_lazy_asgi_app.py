"""Tests for lazy ASGI app construction (bug: CLI crashed on --help/--version).

Bug summary: ``context_intelligence_server.main`` used to call
``create_asgi_app()`` unconditionally at MODULE IMPORT time (module-level
``asgi_app = create_asgi_app()``). Since the console-script entry point
imports ``main`` to reach ``main()``, even ``--help`` constructed the whole
ASGI app and hit create_asgi_app()'s startup guards (e.g. the neo4j
"deployed profile must declare clients explicitly" invariant) -- an operator
with a broken/absent config could not even ask the binary for --help.

The fix makes construction lazy (PEP 562 module ``__getattr__`` +
``get_asgi_app()``), triggered on first access instead of at import. These
tests prove:

  1. Importing the module never constructs the app (no side effects, no
     guard evaluation) -- verified via a real subprocess so we exercise a
     genuinely fresh import, not one already cached by another test in this
     session.
  2. ``--help`` works via subprocess even when a startup guard WOULD fail
     (proves the guard is not evaluated merely by importing/parsing args).
  3. Actually building the app (accessing ``asgi_app`` / calling
     ``get_asgi_app()`` / invoking ``serve``) still runs create_asgi_app()
     and therefore still enforces its guards -- fails loud exactly as
     before, just later. The guard logic itself is UNCHANGED; only the
     timing moved.
  4. ``get_asgi_app()`` / the ``asgi_app`` module attribute construct
     exactly once and return the SAME cached instance (idempotent lazy
     singleton), matching the pre-existing get_api_key_store() /
     get_entra_identity_store() accessor pattern in this module.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent


# Wall-clock budget for a subprocess that is expected to exit almost
# immediately (`--help`, `--version`, an import). Generous already: these
# never reach gunicorn.
_FAST_CLI_TIMEOUT_S = 15

# Budget for a `serve` invocation that must fail its startup guard. This one
# pays for spawn + a cold import of the whole server package (fastapi, neo4j,
# pydantic) + gunicorn arbiter start + fork + worker load() before the guard
# can raise at all -- and it pays it on a shared CI runner with a cold page
# cache, not on a warm dev box.
#
# CI FLAKE 2026-09-10 (run 34419935174, main @ 7c021bc): this test tripped a
# 15s timeout and was reported as a guard regression. It was not. The same
# tree had passed CI twice on its branch (e58bbcd, 1f60669), and locally the
# guard fires in 0.74s with returncode 3 and a full traceback. The subprocess
# had only reached "Starting gunicorn" -- it never got to worker boot, so the
# guard never had the chance to fire. A tight wall-clock budget on a cold
# runner is not evidence about the guard.
#
# Raising this costs NOTHING on a healthy run (the process exits in under a
# second) and only spends real time when something is genuinely wrong -- which
# is exactly when you want to wait and capture the output rather than SIGKILL
# it and lose the buffered stderr.
_SERVE_CLI_TIMEOUT_S = 90


def _run_cli(
    args: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    timeout: int = _FAST_CLI_TIMEOUT_S,
) -> subprocess.CompletedProcess:
    """Invoke the CLI's main() in a fresh subprocess (genuinely fresh import).

    On timeout, re-raises with the partial stdout/stderr attached to the
    message. A bare ``TimeoutExpired`` says only "15 seconds passed", which
    tells the next reader nothing about WHERE the process was stuck -- and the
    SIGKILL discards whatever the child had buffered. Surfacing the partial
    output is the difference between "the guard regressed" and "the runner was
    slow and never reached worker boot".
    """
    code = (
        "import sys\n"
        "from context_intelligence_server.main import main\n"
        "try:\n"
        "    main(sys.argv[1:])\n"
        "except SystemExit as e:\n"
        "    sys.exit(e.code if e.code is not None else 0)\n"
    )
    try:
        return subprocess.run(
            [sys.executable, "-c", code, *args],
            cwd=str(cwd),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise AssertionError(
            f"CLI subprocess {args!r} did not exit within {timeout}s.\n"
            f"This is a WALL-CLOCK timeout, NOT proof that a guard failed to "
            f"fire -- check the partial output below for how far it got "
            f"before it was killed.\n"
            f"--- partial stdout ---\n{exc.stdout or '<empty>'}\n"
            f"--- partial stderr ---\n{exc.stderr or '<empty>'}"
        ) from exc


def _clean_env(tmp_path: Path) -> dict[str, str]:
    """A minimal env with NO server-config.yaml and NO auth/config overrides.

    Explicitly points AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE at a
    path that does not exist, so this is immune to a stray server-config.yaml
    in the real repo root (YamlConfigSettingsSource silently skips a missing
    file -- see its docstring).
    """
    return {
        "PATH": "/usr/bin:/bin",
        "HOME": str(tmp_path),
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE": str(
            tmp_path / "does-not-exist.yaml"
        ),
    }


class TestImportDoesNotConstructApp:
    """`import context_intelligence_server.main` must never raise."""

    def test_bare_import_succeeds_with_no_config(self, tmp_path: Path) -> None:
        result = subprocess.run(
            [sys.executable, "-c", "import context_intelligence_server.main"],
            cwd=str(PROJECT_ROOT),
            env=_clean_env(tmp_path),
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        assert result.returncode == 0, (
            f"bare import must not raise; stderr:\n{result.stderr}"
        )

    def test_bare_import_succeeds_even_when_a_startup_guard_would_fail(
        self, tmp_path: Path
    ) -> None:
        """Import must not evaluate create_asgi_app()'s guards at all.

        neo4j_require_explicit_clients=True with no structured `neo4j` block
        is a real, deterministic create_asgi_app() guard (see
        _assert_neo4j_clients_explicit). If import still constructed the app,
        this would raise RuntimeError here exactly as the original bug did.
        """
        env = _clean_env(tmp_path)
        env["AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_REQUIRE_EXPLICIT_CLIENTS"] = (
            "true"
        )
        result = subprocess.run(
            [sys.executable, "-c", "import context_intelligence_server.main"],
            cwd=str(PROJECT_ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        assert result.returncode == 0, (
            f"import must not construct the app / evaluate its guards; "
            f"stderr:\n{result.stderr}"
        )


class TestHelpAndVersionWorkWithoutConfig:
    """--help (and any pure-argparse invocation) must work with no config."""

    def test_help_works_with_no_config(self, tmp_path: Path) -> None:
        result = _run_cli(["--help"], cwd=PROJECT_ROOT, env=_clean_env(tmp_path))
        assert result.returncode == 0, result.stderr
        assert "usage:" in result.stdout

    def test_help_works_even_when_a_startup_guard_would_fail(
        self, tmp_path: Path
    ) -> None:
        """--help must succeed even under a config that WOULD fail a real serve."""
        env = _clean_env(tmp_path)
        env["AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_REQUIRE_EXPLICIT_CLIENTS"] = (
            "true"
        )
        result = _run_cli(["--help"], cwd=PROJECT_ROOT, env=env)
        assert result.returncode == 0, result.stderr
        assert "usage:" in result.stdout

    def test_unrecognized_version_flag_fails_via_argparse_not_via_auth_guard(
        self, tmp_path: Path
    ) -> None:
        """--version is not an implemented flag; confirm the CLI still reaches
        argparse's own error handling (exit code 2, "unrecognized arguments")
        instead of crashing on app construction (the original bug: ANY
        invocation, including an unrecognized flag, crashed at import time
        with a RuntimeError before argparse ever ran).
        """
        result = _run_cli(["--version"], cwd=PROJECT_ROOT, env=_clean_env(tmp_path))
        assert result.returncode == 2
        assert "unrecognized arguments" in result.stderr
        assert "RuntimeError" not in result.stderr


class TestAuthGuardStillFiresWhenServing:
    """Guard logic inside create_asgi_app() is unweakened -- only deferred."""

    def test_create_asgi_app_still_raises_for_a_real_misconfiguration(self) -> None:
        from context_intelligence_server.config import Settings
        from context_intelligence_server.main import create_asgi_app

        bad_settings = Settings(neo4j_require_explicit_clients=True)
        try:
            create_asgi_app(settings=bad_settings)
        except RuntimeError as exc:
            assert "neo4j_require_explicit_clients" in str(exc)
        else:
            raise AssertionError(
                "create_asgi_app() must still raise for this misconfiguration"
            )

    def test_serve_still_fails_loud_for_a_real_misconfiguration(
        self, tmp_path: Path
    ) -> None:
        """A real subprocess `serve` invocation under a bad config still
        crashes with the guard's RuntimeError (proves the guard fires at
        serve time even though it no longer fires at import time)."""
        env = _clean_env(tmp_path)
        env["AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_REQUIRE_EXPLICIT_CLIENTS"] = (
            "true"
        )
        # A high, unlikely-to-collide port -- this must fail during app
        # construction (inside the worker's load()), before ever binding
        # matters for the assertion.
        env["AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_SERVER_PORT"] = "18321"
        result = _run_cli(
            ["serve"], cwd=PROJECT_ROOT, env=env, timeout=_SERVE_CLI_TIMEOUT_S
        )
        assert result.returncode != 0
        assert "neo4j_require_explicit_clients" in result.stdout + result.stderr


class TestAsgiAppLazySingleton:
    """get_asgi_app() / the asgi_app module attribute construct exactly once."""

    def test_asgi_app_attribute_and_get_asgi_app_return_same_cached_instance(
        self,
    ) -> None:
        from context_intelligence_server import main as main_module

        first = main_module.get_asgi_app()
        second = main_module.asgi_app
        third = main_module.get_asgi_app()
        assert first is second is third

    def test_unknown_module_attribute_still_raises_attribute_error(self) -> None:
        from context_intelligence_server import main as main_module

        try:
            main_module.this_attribute_does_not_exist  # noqa: B018
        except AttributeError:
            pass
        else:
            raise AssertionError("expected AttributeError for an unknown attribute")
