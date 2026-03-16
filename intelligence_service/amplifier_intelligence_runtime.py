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
        composed_with_server = base_bundle.compose(server_bundle)

        # Phase 4: Load context-intelligence (telemetry hook)
        _logger.debug("Loading context-intelligence bundle (telemetry)")
        telemetry_bundle = await load_bundle("context-intelligence", registry=registry)
        composed_with_telemetry = composed_with_server.compose(telemetry_bundle)

        # Phase 5: Create runtime-config Bundle with hooks-routing and compose
        runtime_config = Bundle(
            name="runtime-config",
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
