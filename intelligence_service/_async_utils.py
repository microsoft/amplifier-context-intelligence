"""Shared async utilities for the Intelligence Service."""

from __future__ import annotations

import inspect
from typing import Any


async def close_if_async(obj: Any) -> None:
    """Call obj.close() if it is an async coroutine method."""
    if inspect.iscoroutinefunction(getattr(obj, "close", None)):
        await obj.close()
