"""AmplifierIntelligenceRuntime - 6-phase bundle composition pipeline.

Implements the composition pipeline:
    Phase 1: Set AMPLIFIER_HOME env var, create BundleRegistry
    Phase 2: Load amplifier-dev bundle
    Phase 3: Load context-intelligence-server, compose onto base
    Phase 4: Load context-intelligence (telemetry hook), compose onto result
    Phase 5: Create runtime-config Bundle with hooks-routing, compose onto result
    Phase 6: Call prepare() on fully composed bundle
"""

from __future__ import annotations

import logging
import os
import re
import yaml
from pathlib import Path
from typing import Any

from intelligence_service._async_utils import close_if_async

try:
    from amplifier_foundation import Bundle, BundleRegistry, load_bundle  # type: ignore[import]
except ImportError:
    from dataclasses import dataclass, field as _field

    @dataclass
    class Bundle:  # type: ignore[no-redef]
        """Fallback Bundle stub for environments without amplifier_foundation."""

        name: str = ""
        providers: list = _field(default_factory=list)
        hooks: list = _field(default_factory=list)

    @dataclass
    class BundleRegistry:  # type: ignore[no-redef]
        """Fallback BundleRegistry stub for environments without amplifier_foundation."""

        def register(self, bundles: dict) -> None:
            """Register a dict of bundle names to sources."""

    async def load_bundle(name: str, *, registry: Any = None) -> Any:  # type: ignore[misc]
        """Fallback load_bundle stub for environments without amplifier_foundation."""
        raise NotImplementedError("amplifier_foundation is not installed")


# Well-known bundle catalog
WELL_KNOWN_BUNDLES: dict[str, str] = {
    "foundation": "git+https://github.com/microsoft/amplifier-foundation@main",
    "amplifier-dev": "git+https://github.com/microsoft/amplifier-foundation@main#subdirectory=bundles/amplifier-dev.yaml",
    "context-intelligence-server": "git+https://github.com/colombod/amplifier-bundle-context-intelligence-server@main",
    "context-intelligence": "git+https://github.com/microsoft/amplifier-bundle-context-intelligence@main",
}

_logger = logging.getLogger("intelligence_service.runtime")

# ---------------------------------------------------------------------------
# Provider registry (canonical, keyed by short name)
# ---------------------------------------------------------------------------
PROVIDERS: dict[str, dict[str, str]] = {
    "anthropic": {
        "env_var": "ANTHROPIC_API_KEY",
        "module": "provider-anthropic",
        "source": "git+https://github.com/microsoft/amplifier-module-provider-anthropic@main",
    },
    "gemini": {
        "env_var": "GOOGLE_API_KEY",
        "module": "provider-gemini",
        "source": "git+https://github.com/microsoft/amplifier-module-provider-gemini@main",
    },
    "openai": {
        "env_var": "OPENAI_API_KEY",
        "module": "provider-openai",
        "source": "git+https://github.com/microsoft/amplifier-module-provider-openai@main",
    },
    "azure-openai": {
        "env_var": "AZURE_OPENAI_API_KEY",
        "module": "provider-azure-openai",
        "source": "git+https://github.com/microsoft/amplifier-module-provider-azure-openai@main",
    },
    "github-copilot": {
        "env_var": "GITHUB_TOKEN",
        "module": "provider-github-copilot",
        "source": "git+https://github.com/microsoft/amplifier-module-provider-github-copilot@main",
    },
}


def _get_available_providers() -> set[str]:
    """Return the set of provider short names whose env var is set and non-empty."""
    return {name for name, info in PROVIDERS.items() if os.environ.get(info["env_var"])}


# ---------------------------------------------------------------------------
# Routing roles: maps each model role to an ordered list of provider candidates
# ---------------------------------------------------------------------------
# Multiple roles intentionally share the same candidate (anthropic/claude-sonnet-*);
# per-role differentiation is reserved for future routing logic.
ROUTING_ROLES: dict[str, list[dict[str, Any]]] = {
    "general": [
        {
            "provider": "anthropic",
            "model": "claude-sonnet-*",
            "default_model": "claude-sonnet-4-5",
        },
    ],
    "fast": [
        {
            "provider": "gemini",
            "model": "gemini-*-flash",
            "default_model": "gemini-2.5-flash",
        },
        {
            "provider": "anthropic",
            "model": "claude-haiku-*",
            "default_model": "claude-haiku-4-5",
        },
    ],
    "coding": [
        {
            "provider": "anthropic",
            "model": "claude-sonnet-*",
            "default_model": "claude-sonnet-4-5",
        },
    ],
    "reasoning": [
        {
            "provider": "anthropic",
            "model": "claude-sonnet-*",
            "default_model": "claude-sonnet-4-5",
            "config": {"reasoning_effort": "high"},
        },
    ],
    "critique": [
        {
            "provider": "anthropic",
            "model": "claude-sonnet-*",
            "default_model": "claude-sonnet-4-5",
            "config": {"reasoning_effort": "high"},
        },
    ],
    "creative": [
        {
            "provider": "anthropic",
            "model": "claude-sonnet-*",
            "default_model": "claude-sonnet-4-5",
        },
    ],
    "writing": [
        {
            "provider": "anthropic",
            "model": "claude-sonnet-*",
            "default_model": "claude-sonnet-4-5",
        },
    ],
    "research": [
        {
            "provider": "anthropic",
            "model": "claude-sonnet-*",
            "default_model": "claude-sonnet-4-5",
        },
    ],
    "vision": [
        {
            "provider": "gemini",
            "model": "gemini-*-flash",
            "default_model": "gemini-2.5-flash",
        },
        {
            "provider": "anthropic",
            "model": "claude-sonnet-*",
            "default_model": "claude-sonnet-4-5",
        },
    ],
    "image-gen": [
        {
            "provider": "gemini",
            # exact match — no wildcard variants exist for image generation
            "model": "gemini-2.0-flash-preview-image-generation",
            "default_model": "gemini-2.0-flash-preview-image-generation",
        },
    ],
    "critical-ops": [
        {
            "provider": "anthropic",
            "model": "claude-sonnet-*",
            "default_model": "claude-sonnet-4-5",
        },
    ],
}


def _is_version(seg: str) -> bool:
    """True when a segment is a plain integer or decimal version number."""
    try:
        float(seg)
        return True
    except ValueError:
        return False


def _model_suffix(model: str) -> str:
    """Extract a short suffix from a model name for use in instance IDs.

    Strips known prefixes (claude-, gemini-, gpt-), skips leading version
    segments (e.g. ``2.5``, ``4``), keeps consecutive alphabetic segments,
    and stops when a trailing version segment is encountered.

    Examples::

        claude-sonnet-4-5                          -> sonnet
        claude-haiku-4-5                           -> haiku
        gemini-2.5-flash                           -> flash
        gemini-2.0-flash-preview-image-generation  -> flash-preview-image-generation
        gpt-4o                                     -> 4o  (fallback: no pure-alpha segment)
    """
    # Strip a known model-family prefix
    for prefix in ("claude-", "gemini-", "gpt-"):
        if model.startswith(prefix):
            model = model[len(prefix) :]
            break

    segments = model.split("-")

    # Skip any leading version segments (e.g. "2.5", "4")
    i = 0
    while i < len(segments) and _is_version(segments[i]):
        i += 1

    # Collect consecutive alphabetic segments; stop at the first version segment
    result: list[str] = []
    while i < len(segments):
        seg = segments[i]
        if seg.isalpha():
            result.append(seg)
        else:
            break
        i += 1

    return "-".join(result) if result else model


def _build_provider_instances(available: set[str]) -> list[dict[str, Any]]:
    """Build deduplicated provider instance configs from ROUTING_ROLES.

    Walks every role in ROUTING_ROLES, collects unique ``(provider,
    default_model)`` pairs, filters to providers present in *available*, and
    returns a list of instance dicts ready for bundle composition.

    Each returned dict has the keys:
        * ``module``      – the provider module ID (from PROVIDERS)
        * ``instance_id`` – ``"{provider}-{suffix}"`` (lowercase, no spaces)
        * ``source``      – the provider source URL (from PROVIDERS)
        * ``config``      – ``{"default_model": <default_model>}``

    Args:
        available: Set of provider short names that have a valid API key.

    Returns:
        Deduplicated list of provider instance config dicts.
    """
    seen: set[tuple[str, str]] = set()
    instances: list[dict[str, Any]] = []

    for candidates in ROUTING_ROLES.values():
        for candidate in candidates:
            provider = candidate["provider"]
            default_model = candidate["default_model"]

            if provider not in available:
                continue

            key = (provider, default_model)
            if key in seen:
                continue
            seen.add(key)

            provider_info = PROVIDERS[provider]
            suffix = _model_suffix(default_model)
            instance_id = f"{provider}-{suffix}"

            instances.append(
                {
                    "module": provider_info["module"],
                    "instance_id": instance_id,
                    "source": provider_info["source"],
                    "config": {"default_model": default_model},
                }
            )

    return instances


def _build_matrix_dict(available: set[str]) -> dict[str, Any]:
    """Build a routing matrix dict for the intelligence service.

    Walks ROUTING_ROLES, filters candidates to available providers, and returns
    a dict matching the standard matrix YAML schema.

    Roles with zero available candidates are omitted entirely.  Within each
    role, only candidates whose provider is in *available* are kept, preserving
    their order from ROUTING_ROLES.  The ``config`` key is included in a
    candidate entry only when present in the ROUTING_ROLES entry.

    Args:
        available: Set of provider short names that have a valid API key.

    Returns:
        Dict with keys: name, description, updated, roles.
    """
    from datetime import date

    roles: dict[str, Any] = {}

    for role_name, candidates in ROUTING_ROLES.items():
        filtered: list[dict[str, Any]] = []
        for candidate in candidates:
            if candidate["provider"] not in available:
                continue
            entry: dict[str, Any] = {
                "provider": candidate["provider"],
                "model": candidate["model"],
            }
            if "config" in candidate:
                entry["config"] = candidate["config"]
            filtered.append(entry)
        if filtered:
            roles[role_name] = {"candidates": filtered}

    return {
        "name": "intelligence-service",
        "description": "Auto-generated routing matrix for the intelligence service.",
        "updated": str(date.today()),
        "roles": roles,
    }


def _write_routing_matrix(matrix: dict[str, Any], bundle_root: Path, name: str) -> None:
    """Serialize a routing matrix dict to {bundle_root}/routing/{name}.yaml.

    Args:
        matrix: The routing matrix dict to serialize.
        bundle_root: Root path of the bundle directory.
        name: Base filename (without extension) for the output YAML file.
    """
    routing_dir = bundle_root / "routing"
    routing_dir.mkdir(parents=True, exist_ok=True)
    output_path = routing_dir / f"{name}.yaml"
    with output_path.open("w") as f:
        yaml.dump(matrix, f, default_flow_style=False, sort_keys=False)


# ---------------------------------------------------------------------------
# Provider detection from environment
# ---------------------------------------------------------------------------
_PROVIDER_MAP: list[tuple[str, str, str, str]] = [
    # (env_var, module_id, source, default_model)
    (
        "GOOGLE_API_KEY",
        "provider-gemini",
        "git+https://github.com/microsoft/amplifier-module-provider-gemini@main",
        "gemini-2.5-flash",
    ),
    (
        "ANTHROPIC_API_KEY",
        "provider-anthropic",
        "git+https://github.com/microsoft/amplifier-module-provider-anthropic@main",
        "claude-sonnet-4-5",
    ),
    (
        "OPENAI_API_KEY",
        "provider-openai",
        "git+https://github.com/microsoft/amplifier-module-provider-openai@main",
        "gpt-4o",
    ),
    (
        "AZURE_OPENAI_API_KEY",
        "provider-azure-openai",
        "git+https://github.com/microsoft/amplifier-module-provider-azure-openai@main",
        "gpt-4o",
    ),
    (
        "GITHUB_TOKEN",
        "provider-github-copilot",
        "git+https://github.com/microsoft/amplifier-module-provider-github-copilot@main",
        "gpt-4o",
    ),
]


def _detect_providers() -> list[dict[str, Any]]:
    """Build provider configs for each API key found in the environment."""
    providers: list[dict[str, Any]] = []
    for env_var, module_id, source, default_model in _PROVIDER_MAP:
        if os.environ.get(env_var):
            providers.append(
                {
                    "module": module_id,
                    "source": source,
                    "config": {"default_model": default_model},
                }
            )
    return providers


# ---------------------------------------------------------------------------
# Env-var expansion (replicates amplifier_app_cli expand_env_vars logic)
# ---------------------------------------------------------------------------
_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")


def _expand_env_vars(value: Any) -> Any:
    """Expand ``${VAR}`` and ``${VAR:default}`` in nested config values.

    The Amplifier CLI normally handles this during config loading, but the
    intelligence service uses ``amplifier_foundation`` directly (bypassing
    the CLI layer), so we replicate the expansion here.
    """
    if isinstance(value, str):

        def _replace(m: re.Match[str]) -> str:
            var_name = m.group(1)
            default = m.group(2)
            return os.environ.get(var_name, default if default is not None else "")

        return _ENV_PATTERN.sub(_replace, value)
    if isinstance(value, dict):
        return {k: _expand_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env_vars(item) for item in value]
    return value


def _expand_bundle_hook_configs(bundle: Any) -> None:
    """Expand env-var templates in a bundle's hook configs in-place."""
    hooks = getattr(bundle, "hooks", None)
    if not hooks:
        return
    for hook in hooks:
        if isinstance(hook, dict) and "config" in hook:
            hook["config"] = _expand_env_vars(hook["config"])


class AmplifierIntelligenceRuntime:
    """6-phase bundle composition pipeline for the Intelligence Service."""

    def __init__(
        self,
        *,
        routing_matrix: str,
        runtime_state_path: str,
    ) -> None:
        self._routing_matrix = routing_matrix
        self._runtime_state_path = runtime_state_path
        self._prepared: Any = None
        self._file_handler: logging.FileHandler | None = None

    @property
    def prepared(self) -> Any:
        """Return the current PreparedBundle or None."""
        return self._prepared

    def _setup_file_logging(self) -> None:
        """Add a FileHandler to the 'intelligence_service.runtime' logger."""
        try:
            log_dir = Path(self._runtime_state_path)
            log_dir.mkdir(parents=True, exist_ok=True)
            log_path = log_dir / "runtime.log"
            handler = logging.FileHandler(str(log_path))
            _logger.addHandler(handler)
            self._file_handler = handler
        except OSError:
            _logger.warning(
                "Could not set up file logging at %s", self._runtime_state_path
            )

    async def startup(self) -> None:
        """Run the 6-phase bundle composition pipeline."""
        # Phase 1: Set env var, set up file logging, create registry
        os.environ["AMPLIFIER_HOME"] = self._runtime_state_path
        self._setup_file_logging()
        _logger.debug("Creating bundle registry with well-known bundles")
        registry = BundleRegistry()
        registry.register(WELL_KNOWN_BUNDLES)
        _logger.debug("Bundle registry ready with %d entries", len(WELL_KNOWN_BUNDLES))

        # Phase 2: Load amplifier-dev bundle
        _logger.debug("Loading amplifier-dev bundle")
        base_bundle = await load_bundle("amplifier-dev", registry=registry)

        # Phase 3: Load context-intelligence-server and compose onto base
        _logger.debug("Loading context-intelligence-server bundle")
        server_bundle = await load_bundle(
            "context-intelligence-server", registry=registry
        )
        _expand_bundle_hook_configs(server_bundle)
        composed_with_server = base_bundle.compose(server_bundle)

        # Phase 4: Load context-intelligence (telemetry hook)
        _logger.debug("Loading context-intelligence bundle (telemetry)")
        telemetry_bundle = await load_bundle("context-intelligence", registry=registry)
        _expand_bundle_hook_configs(telemetry_bundle)
        composed_with_telemetry = composed_with_server.compose(telemetry_bundle)

        # Phase 5: Create runtime-config Bundle with providers + hooks-routing
        providers = _detect_providers()
        if providers:
            _logger.info(
                "Detected %d provider(s): %s",
                len(providers),
                ", ".join(p["module"] for p in providers),
            )
        else:
            _logger.warning("No provider API keys found in environment")

        runtime_config = Bundle(
            name="runtime-config",
            providers=providers,
            hooks=[
                {
                    "module": "hooks-routing",
                    "config": {"default_matrix": self._routing_matrix},
                }
            ],
        )
        composed_with_config = composed_with_telemetry.compose(runtime_config)

        # Phase 6: Prepare the fully composed bundle
        _logger.debug("Calling prepare() on fully composed bundle")
        self._prepared = await composed_with_config.prepare()

    async def close(self) -> None:
        """Close the prepared bundle and clean up resources."""
        if self._prepared is not None:
            await close_if_async(self._prepared)
        self._prepared = None

        if self._file_handler is not None:
            _logger.removeHandler(self._file_handler)
            self._file_handler.close()
            self._file_handler = None
