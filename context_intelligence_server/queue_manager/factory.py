"""Config-driven QueueManager factory — the ONLY place a queue backend is selected.

This is the single seam through which the concrete backend is chosen. Adding
a new backend (e.g. Azure) means: one new module implementing
:class:`~.protocol.QueueManager`, one new branch here, and a config value —
zero changes to :mod:`context_intelligence_server.registry` or any consumer.

This module (and :mod:`~context_intelligence_server.config`) are the only
places ``settings.queues_path`` is read — the on-disk root is a filesystem-
backend concern, resolved here and handed to the concrete backend at
construction time. Callers only ever see the :class:`~.protocol.QueueManager`
Protocol.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from .filesystem import FileSystemQueueManager
from .protocol import QueueManager

if TYPE_CHECKING:
    from context_intelligence_server.config import Settings


def create_queue_manager(settings: Settings) -> QueueManager:
    """Build the durable ``QueueManager`` from config.

    Single backend today (on-disk), so this is a thin config-reading seam
    rather than a multi-backend dispatcher — but it keeps ``settings.queues_path``
    out of consumers (mirrors ``blob_store.factory.create_blob_store`` and
    ``identity_store.factory.create_identity_store``).
    """
    return FileSystemQueueManager(queues_dir=Path(settings.queues_path))
