"""Tripwire: the blob-store on-disk layout stays inside blob_store.py.

Locks the isolation boundary established by the BlobStore refactor
(see docs/blob-store-abstraction.md). These tests fail loudly if a future
change lets any module other than ``blob_store.py`` locate the blob root or
resurrects the old direct-filesystem reclaim implementation -- i.e. if a
direct-FS blob leak is reintroduced.

Two invariants:
  1. Only the single construction site (registry.py) and the config field
     declaration (config.py) may reference ``settings.blob_path`` -- a caller
     that cannot locate the blob root physically cannot do blob filesystem I/O.
  2. The pre-refactor direct-disk reclaim symbols must never reappear.
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


def test_blob_root_locatable_only_at_construction_site() -> None:
    """`settings.blob_path` -- the only way to find the on-disk blob root -- is
    referenced solely where the store is constructed (registry.py) and where the
    config field is declared (config.py). blob_store.py owns the layout itself."""
    allowed = {"registry.py", "config.py", "blob_store.py"}
    offenders: list[str] = []
    for p in PKG.rglob("*.py"):
        if p.name in allowed:
            continue
        for lineno, line in _code_lines(p):
            if "blob_path" in line:
                offenders.append(f"{p.relative_to(PKG)}:{lineno}: {line}")
    assert not offenders, (
        "blob root re-derived outside the single construction site "
        "(registry.py)/config -- a caller that can locate the blob root can "
        "bypass BlobStore and touch disk directly:\n" + "\n".join(offenders)
    )


def test_old_direct_disk_reclaim_symbols_are_gone() -> None:
    """The pre-refactor direct-FS reclaim implementation (a private on-disk blob
    dataclass + a filesystem scan helper in routers/admin.py) must not reappear
    anywhere in the package."""
    banned = ("_OnDiskBlob", "_scan_disk_blobs")
    offenders: list[str] = []
    for p in PKG.rglob("*.py"):
        for lineno, line in _code_lines(p):
            for token in banned:
                if token in line:
                    offenders.append(f"{p.relative_to(PKG)}:{lineno}: {token}")
    assert not offenders, (
        "old direct-disk blob-reclaim symbols resurfaced (the reclaim GC must "
        "go through BlobStore.scan()/delete()):\n" + "\n".join(offenders)
    )
