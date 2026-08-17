"""queue_manager — durable, per-session append-only queue for the event-write pipeline.

The public surface is the backend-neutral :class:`QueueManager` Protocol plus
:class:`Batch` and the :func:`create_queue_manager` factory. Consumers should
depend on these, never on a concrete backend class.

Package layout:
    protocol.py    QueueManager Protocol, Batch — the backend-neutral seam (no
                   filesystem imports).
    filesystem.py  FileSystemQueueManager — the on-disk implementation.
    factory.py     create_queue_manager(settings) — the ONLY place a backend is
                   selected and the ONLY place (besides config.py) that reads
                   settings.queues_path.
"""

from __future__ import annotations

from .factory import create_queue_manager
from .filesystem import FileSystemQueueManager
from .protocol import Batch, QueueManager

__all__ = [
    "Batch",
    "FileSystemQueueManager",
    "QueueManager",
    "create_queue_manager",
]
