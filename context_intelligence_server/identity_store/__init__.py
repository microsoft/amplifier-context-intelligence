"""identity_store -- durable, write-through key -> contributor-identity map.

The public surface is the backend-neutral :class:`IdentityStore` Protocol plus
the :func:`create_identity_store` factory. Consumers should depend on these,
never on a concrete backend class.

Package layout:
    protocol.py    IdentityStore Protocol -- the backend-neutral seam (no
                   filesystem imports). AUTH-CRITICAL commit-order and
                   fail-closed-load guarantees are documented here.
    filesystem.py  FileSystemIdentityStore -- the JSON-file-backed
                   implementation.
    factory.py     create_identity_store(settings, kind) -- the ONLY place a
                   backend is selected and the ONLY place (besides config.py)
                   that reads settings.entra_identities_store_path /
                   settings.api_keys_store_path.
"""

from __future__ import annotations

from .factory import create_identity_store
from .filesystem import FileSystemIdentityStore
from .protocol import IdentityStore

__all__ = [
    "FileSystemIdentityStore",
    "IdentityStore",
    "create_identity_store",
]
