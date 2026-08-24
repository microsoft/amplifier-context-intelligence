"""Backend selection for the writer-lease store.

One backend today (filesystem). A future backend is added here and nowhere
else -- the detector never learns which one it got.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from context_intelligence_server.lease_store.filesystem import FileSystemLeaseStore
from context_intelligence_server.lease_store.protocol import LeaseStore


def create_lease_store(dir_source: Callable[[], Path]) -> LeaseStore:
    """Build the lease store. *dir_source* is resolved lazily per operation, so
    nothing is constructed and no path is read at build time."""
    return FileSystemLeaseStore(dir_source)
