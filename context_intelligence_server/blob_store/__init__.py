"""blob_store \u2014 session-scoped, URI-addressable blob storage.

The public surface is the backend-neutral :class:`BlobStore` Protocol plus
:class:`BlobReference` / :class:`BlobNotFoundError` and the
:func:`create_blob_store` factory. Consumers should depend on these, never on
a concrete backend class.

Package layout:
    protocol.py    BlobStore Protocol, BlobReference, BlobNotFoundError \u2014 the
                   backend-neutral seam (no filesystem imports).
    filesystem.py  FileSystemBlobStore \u2014 the disk-backed implementation.
    factory.py     create_blob_store(settings) \u2014 the ONLY place a backend is
                   selected and the ONLY place (besides config.py) that reads
                   settings.blob_path.
"""

from __future__ import annotations

from .factory import create_blob_store
from .filesystem import FileSystemBlobStore
from .protocol import BlobNotFoundError, BlobReference, BlobStore

__all__ = [
    "BlobNotFoundError",
    "BlobReference",
    "BlobStore",
    "FileSystemBlobStore",
    "create_blob_store",
]
