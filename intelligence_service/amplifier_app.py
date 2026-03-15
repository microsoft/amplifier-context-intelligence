"""AmplifierApp - Bundle Lifecycle Manager for the Intelligence Service.

Encapsulates the entire PreparedBundle lifecycle:
    load -> compose -> prepare -> reload -> close
"""

from __future__ import annotations

import inspect
import os
from typing import Any

try:
    from amplifier_foundation import Bundle, load_bundle  # type: ignore[import]
except ImportError:
    from dataclasses import dataclass, field as _field

    @dataclass
    class Bundle:  # type: ignore[no-redef]
        """Fallback Bundle stub for environments without amplifier_foundation."""

        name: str = ""
        hooks: list = _field(default_factory=list)

    async def load_bundle(path: str) -> Any:  # type: ignore[misc]
        """Fallback load_bundle stub for environments without amplifier_foundation."""
        raise NotImplementedError("amplifier_foundation is not installed")


class AmplifierApp:
    """Manages the PreparedBundle lifecycle for the Intelligence Service."""

    def __init__(
        self,
        *,
        bundle_path: str,
        routing_matrix: str,
        amplifier_home: str,
    ) -> None:
        self._bundle_path = bundle_path
        self._routing_matrix = routing_matrix
        self._amplifier_home = (
            amplifier_home  # reserved for amplifier_foundation.configure(home=...)
        )
        self._prepared = None

    @property
    def prepared(self) -> Any:
        """Return the current PreparedBundle or None."""
        return self._prepared

    async def _load_and_prepare(self) -> Any:
        """Load the bundle, compose the routing overlay, and prepare it."""
        loaded = await load_bundle(self._bundle_path)
        routing_overlay = Bundle(
            name="routing-config",
            hooks=[
                {
                    "module": "hooks-routing",
                    "config": {"default_matrix": self._routing_matrix},
                }
            ],
        )
        return await loaded.compose(routing_overlay).prepare()

    async def startup(self) -> None:
        """Load, compose, and prepare the bundle."""
        os.environ["AMPLIFIER_HOME"] = self._amplifier_home
        self._prepared = await self._load_and_prepare()

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
        if old_prepared is not None and inspect.iscoroutinefunction(
            getattr(old_prepared, "close", None)
        ):
            await old_prepared.close()

    async def close(self) -> None:
        """Close and clear the prepared bundle."""
        if self._prepared is not None and inspect.iscoroutinefunction(
            getattr(self._prepared, "close", None)
        ):
            await self._prepared.close()
        self._prepared = None
