"""Tripwire: the queue on-disk location stays inside the queue_manager package.

Locks the isolation boundary established by the QueueManager refactor
(mirrors ``tests/test_blob_isolation_tripwire.py`` and
``tests/test_identity_isolation_tripwire.py``). These tests fail loudly if a
future change lets any module other than the ``queue_manager/`` package
locate the durable queue's backing directory, or resurrects a concrete-class
reference at a consumer site -- i.e. if a direct-FS queue leak is
reintroduced.

Two invariants:
  1. Only the ``queue_manager/`` package (specifically ``factory.py``, the
     single construction site) and the config field declaration (config.py)
     may reference ``settings.queues_path`` -- a caller that cannot locate
     the queue root physically cannot do queue filesystem I/O directly.
     ``registry.py`` goes through ``create_queue_manager(settings)`` and
     never sees the path itself.
  2. The concrete ``FileSystemQueueManager`` must appear ONLY inside the
     ``queue_manager/`` package (production consumers depend on the
     ``QueueManager`` Protocol and obtain instances via
     ``create_queue_manager(settings)``). Tests are permitted to construct a
     concrete backend directly.
"""

from __future__ import annotations

import pathlib

PKG = pathlib.Path(__file__).resolve().parents[1] / "context_intelligence_server"


def _code_lines(path: pathlib.Path):
    """Yield (lineno, stripped) for real code lines, skipping comments and
    rst-doc lines (``...`` backtick spans) so docstring prose never trips the
    guard."""
    for i, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        s = raw.strip()
        if not s or s.startswith("#") or "``" in raw:
            continue
        yield i, s


def test_queues_path_locatable_only_at_construction_site() -> None:
    """`settings.queues_path` -- the only way to find the on-disk queue root
    -- is referenced solely inside the queue_manager/ package (which owns
    the layout) and where the config field is declared (config.py).
    registry.py goes through create_queue_manager(settings) and never sees
    the path itself."""
    queue_manager_pkg = PKG / "queue_manager"
    allowed_top_level = {"config.py"}
    offenders: list[str] = []
    for p in PKG.rglob("*.py"):
        # Anything inside the queue_manager/ package owns the layout -- allowed.
        if queue_manager_pkg in p.parents:
            continue
        if p.name in allowed_top_level:
            continue
        for lineno, line in _code_lines(p):
            if "queues_path" in line:
                offenders.append(f"{p.relative_to(PKG)}:{lineno}: {line}")
    assert not offenders, (
        "queue root re-derived outside the queue_manager/ package/config -- a "
        "caller that can locate the queue root can bypass QueueManager and "
        "touch disk directly:\n" + "\n".join(offenders)
    )


def test_concrete_impl_referenced_only_inside_the_package() -> None:
    """The concrete ``FileSystemQueueManager`` must appear ONLY inside the
    ``queue_manager/`` package. Production consumers depend on the
    ``QueueManager`` Protocol and obtain instances via
    ``create_queue_manager(settings)`` -- never by naming a concrete backend
    -- so a disk->Azure swap touches only the package. (Tests are permitted
    to construct a concrete backend directly.)"""
    queue_manager_pkg = PKG / "queue_manager"
    offenders: list[str] = []
    for p in PKG.rglob("*.py"):
        if queue_manager_pkg in p.parents:
            continue
        for lineno, line in _code_lines(p):
            if "FileSystemQueueManager" in line:
                offenders.append(f"{p.relative_to(PKG)}:{lineno}: {line}")
    assert not offenders, (
        "concrete FileSystemQueueManager named outside the queue_manager/ "
        "package -- consumers must use the QueueManager Protocol + "
        "create_queue_manager(settings), not a concrete backend:\n"
        + "\n".join(offenders)
    )
