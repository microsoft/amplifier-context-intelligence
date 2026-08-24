"""Best-effort AST tripwire for the storage-agnosticism boundary.

Storage artifacts (blobs, durable queues, identity stores) are reached ONLY
through their backend Protocols. No module OUTSIDE the three backend packages
may enumerate, stat, or unlink a storage artifact, nor read a storage root
path from settings -- otherwise a second backend (e.g. Azure) could not be
dropped in without editing consumers.

This guard walks the AST of every non-storage module and fails on the file
operations and settings reads that would reach around a Protocol. It is a
TRIPWIRE, not a proof: a determined caller can defeat any static check
(dynamic import, getattr, os.system, a C-extension). The real guarantee is
that each consumer is positively verified protocol-only by reading. This test
exists to catch the accidental reintroduction of a KNOWN leak shape, and to
fail loudly the moment one lands.
"""

from __future__ import annotations

import ast
from pathlib import Path

# The three storage backend packages -- the ONLY place a raw file operation on
# a storage artifact is allowed to live.
_STORAGE_PACKAGES = {"blob_store", "queue_manager", "identity_store", "lease_store"}

# Attribute calls that mutate or enumerate a storage artifact on disk.
_BANNED_CALLS = {
    ("os", "unlink"),
    ("os", "remove"),
    ("os", "removedirs"),
    ("os", "rmdir"),
    ("os", "scandir"),
    ("os", "listdir"),
    ("os", "walk"),
}
# Attribute names that are banned regardless of the receiver (glob on any Path,
# any shutil operation, Path.unlink()).
_BANNED_METHODS = {"glob", "rglob", "iterdir"}
_BANNED_MODULE_PREFIXES = {"shutil"}

# settings.*_path reads that leak a storage root into a consumer. The factory
# and config own these; nobody else reads them.
_BANNED_SETTINGS_ATTRS = {"blob_path", "queues_path"}

# The single human-approved exception (workspace AGENTS.md): the WriterLease
# boot detector resolves the queue directory WITHOUT constructing a
# QueueManager, so registry.queues_dir_path reads settings.queues_path.
_APPROVED_EXCEPTIONS = {
    ("context_intelligence_server/registry.py", "queues_path"),
}

_SERVER_ROOT = Path(__file__).resolve().parent.parent / "context_intelligence_server"
_SCRIPTS_ROOT = Path(__file__).resolve().parent.parent / "scripts"


def _iter_guarded_files() -> list[Path]:
    files: list[Path] = []
    for root in (_SERVER_ROOT, _SCRIPTS_ROOT):
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            parts = set(path.relative_to(root.parent).parts)
            if parts & _STORAGE_PACKAGES:
                continue  # backend packages are the sanctioned home
            files.append(path)
    return files


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(_SERVER_ROOT.parent))
    except ValueError:
        # A path outside the repo (e.g. the planted-leak self-test's tmp file).
        return str(path)


def _violations(path: Path) -> list[str]:
    tree = ast.parse(path.read_text("utf-8"), filename=str(path))
    rel = _rel(path)
    found: list[str] = []

    for node in ast.walk(tree):
        # os.unlink(...) / os.walk(...) / shutil.rmtree(...) etc.
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            attr = node.func.attr
            recv = node.func.value
            if isinstance(recv, ast.Name):
                if (recv.id, attr) in _BANNED_CALLS:
                    found.append(f"{rel}:{node.lineno} {recv.id}.{attr}(...)")
                if recv.id in _BANNED_MODULE_PREFIXES:
                    found.append(f"{rel}:{node.lineno} {recv.id}.{attr}(...)")
            if attr in _BANNED_METHODS:
                found.append(f"{rel}:{node.lineno} .{attr}(...)")
            if attr == "unlink":  # Path(...).unlink()
                found.append(f"{rel}:{node.lineno} .unlink(...)")

        # settings.blob_path / settings.queues_path reads
        if isinstance(node, ast.Attribute) and node.attr in _BANNED_SETTINGS_ATTRS:
            if (rel, node.attr) in _APPROVED_EXCEPTIONS:
                continue
            found.append(f"{rel}:{node.lineno} settings.{node.attr}")

    return found


def test_no_storage_file_ops_outside_backend_packages() -> None:
    """No consumer reaches around a storage Protocol with a raw file op."""
    violations: list[str] = []
    for path in _iter_guarded_files():
        violations.extend(_violations(path))

    assert not violations, (
        "storage-agnosticism boundary breached -- storage artifacts must be "
        "reached only through their backend Protocol. Offending sites:\n  "
        + "\n  ".join(sorted(violations))
    )


def test_guard_actually_detects_a_planted_leak(tmp_path: Path) -> None:
    """The tripwire is armed: a planted os.unlink is caught (red-on-violation)."""
    leak = tmp_path / "context_intelligence_server" / "routers" / "leaky.py"
    leak.parent.mkdir(parents=True)
    leak.write_text("import os\n\n\ndef f(p):\n    os.unlink(p)\n", "utf-8")
    assert _violations(leak), "guard failed to detect a planted os.unlink leak"
