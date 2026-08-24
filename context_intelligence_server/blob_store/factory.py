"""Config-driven BlobStore factory \u2014 the ONLY place a blob-store backend is selected.

This is the single seam through which the concrete backend is chosen. Adding
a new backend (e.g. Azure) means: one new module implementing
:class:`~.protocol.BlobStore`, one new branch here, and a config value \u2014
zero changes to :mod:`context_intelligence_server.registry` or any consumer.

This module (and :mod:`~context_intelligence_server.config`) are the only
places ``settings.blob_path`` is read \u2014 the on-disk root is a filesystem-
backend concern, resolved here and handed to the concrete backend at
construction time. Callers only ever see the :class:`~.protocol.BlobStore`
Protocol.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .filesystem import FileSystemBlobStore
from .protocol import BlobStore

if TYPE_CHECKING:
    from context_intelligence_server.config import Settings


def create_blob_store(settings: Settings) -> BlobStore:
    """Build the configured :class:`~.protocol.BlobStore` backend.

    Reads ``settings.blob_backend`` (default ``"filesystem"``) to select the
    implementation:

    - ``"filesystem"``: :class:`~.filesystem.FileSystemBlobStore` rooted at
      ``settings.blob_path``.
    - ``"azure"``: not yet implemented.
    - anything else: rejected as an unknown backend.

    Raises:
        NotImplementedError: If ``blob_backend == "azure"`` (not yet built).
        ValueError: If ``blob_backend`` names an unknown backend.
    """
    backend = settings.blob_backend
    if backend == "filesystem":
        return FileSystemBlobStore(root=settings.blob_path)
    if backend == "azure":
        raise NotImplementedError("azure blob backend not yet implemented")
    raise ValueError(f"Unknown blob_backend: {backend!r}")
