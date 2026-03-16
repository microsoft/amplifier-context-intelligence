"""Tests validating intelligence_service/pyproject.toml configuration.

These tests ensure the pyproject.toml contains required Amplifier dependencies
and follows the expected structure (dependency-groups for dev deps, etc.).
"""

import tomllib
from pathlib import Path

import pytest

PYPROJECT_PATH = (
    Path(__file__).parent.parent.parent / "intelligence_service" / "pyproject.toml"
)

# All Amplifier packages that must appear as git+https dependencies.
AMPLIFIER_PACKAGES = [
    "amplifier-core",
    "amplifier-foundation",
    "provider-anthropic",
    "provider-openai",
    "provider-gemini",
    "provider-azure-openai",
    "provider-github-copilot",
    "provider-ollama",
    "provider-vllm",
    "loop-basic",
    "context-simple",
]


def _load_config() -> dict:
    with open(PYPROJECT_PATH, "rb") as f:
        return tomllib.load(f)


def test_project_name_is_context_intelligence_service() -> None:
    """Project name must be 'context-intelligence-service'."""
    config = _load_config()
    assert config["project"]["name"] == "context-intelligence-service"


def test_websockets_dependency_present() -> None:
    """websockets>=13.0 must be listed in project dependencies."""
    config = _load_config()
    deps = config["project"]["dependencies"]
    assert any("websockets" in dep for dep in deps), (
        f"websockets not found in dependencies: {deps}"
    )


@pytest.mark.parametrize("package", AMPLIFIER_PACKAGES)
def test_amplifier_package_present(package: str) -> None:
    """Each Amplifier package must appear in project dependencies as a git+https source."""
    config = _load_config()
    deps = config["project"]["dependencies"]
    assert any(package in dep for dep in deps), f"{package} not found in: {deps}"
    assert any("git+https" in dep for dep in deps if package in dep), (
        f"{package} must be a git+https dependency"
    )


def test_dev_deps_use_dependency_groups() -> None:
    """Dev dependencies must use [dependency-groups] format (uv-native), not optional-dependencies."""
    config = _load_config()
    # Must have [dependency-groups] with dev
    assert "dependency-groups" in config, (
        "[dependency-groups] section missing from pyproject.toml"
    )
    assert "dev" in config["dependency-groups"], (
        "dev group missing from [dependency-groups]"
    )
    # Must NOT have dev in [project.optional-dependencies]
    optional_deps = config.get("project", {}).get("optional-dependencies", {})
    assert "dev" not in optional_deps, (
        "dev dependencies should be in [dependency-groups], not [project.optional-dependencies]"
    )


def test_build_system_uses_hatchling() -> None:
    """Build system must use hatchling."""
    config = _load_config()
    assert config["build-system"]["build-backend"] == "hatchling.build"
    assert "hatchling" in config["build-system"]["requires"]
