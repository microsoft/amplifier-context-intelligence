"""Config-driven IdentityStore factory -- the ONLY place a backend is selected.

This is the single seam through which the concrete backend is chosen. Adding
a new backend (e.g. Azure) means: one new module implementing
:class:`~.protocol.IdentityStore`, one new branch here -- zero changes to
:mod:`context_intelligence_server.main` or any other consumer.

This module (and :mod:`~context_intelligence_server.config`) are the only
places ``settings.entra_identities_store_path`` / ``settings.api_keys_store_path``
are read -- the on-disk location is a filesystem-backend concern, resolved
here and handed to the concrete backend at construction time. Callers only
ever see the :class:`~.protocol.IdentityStore` Protocol.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from .filesystem import FileSystemIdentityStore
from .protocol import IdentityStore

if TYPE_CHECKING:
    from context_intelligence_server.config import Settings


def create_identity_store(settings: Settings, kind: str) -> IdentityStore:
    """Build an :class:`~.protocol.IdentityStore` rooted at the configured path for *kind*.

    Construction only -- callers are responsible for ``load()`` / ``seed()``
    and any auth-mode wiring (this mirrors
    ``blob_store.factory.create_blob_store``: a single-backend, config-reading
    seam that keeps the store paths out of consumers such as ``main.py``).

    Args:
        settings: The active ``Settings``.
        kind: ``"entra"`` for the OID identity map, ``"api_key"`` for the
            SHA-256 digest keystore.

    Raises:
        ValueError: If *kind* is neither ``"entra"`` nor ``"api_key"``.
    """
    if kind == "entra":
        path = settings.entra_identities_store_path
    elif kind == "api_key":
        path = settings.api_keys_store_path
    else:
        raise ValueError(f"Unknown identity store kind: {kind!r}")
    return FileSystemIdentityStore(Path(path))
