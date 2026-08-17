"""Tripwire: identity-store on-disk location stays inside the identity_store package.

Locks the isolation boundary established by the IdentityStore refactor
(mirrors ``tests/test_blob_isolation_tripwire.py`` for the blob store). These
tests fail loudly if a future change lets any module other than the
``identity_store/`` package locate an identity-store's backing file, or
resurrects a concrete-class reference at a consumer site -- i.e. if a
direct-FS identity leak is reintroduced.

Two invariants:
  1. Only the ``identity_store/`` package (specifically ``factory.py``, the
     single construction site) and the config field declarations (config.py)
     may reference ``settings.entra_identities_store_path`` /
     ``settings.api_keys_store_path`` -- a caller that cannot locate the
     backing path physically cannot do identity-store filesystem I/O
     directly. ``main.py`` and ``routers/admin.py`` go through
     ``create_identity_store(settings, kind)`` and never see the paths.
  2. The concrete ``FileSystemIdentityStore`` must appear ONLY inside the
     ``identity_store/`` package (production consumers depend on the
     ``IdentityStore`` Protocol and obtain instances via
     ``create_identity_store(settings, kind)``). Tests are permitted to
     construct a concrete backend directly.
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


def test_identity_store_paths_locatable_only_at_construction_site() -> None:
    """`settings.entra_identities_store_path` / `settings.api_keys_store_path` --
    the only way to find an identity store's backing file -- are referenced
    solely inside the identity_store/ package (which owns the on-disk layout)
    and where the config fields are declared (config.py). main.py and
    routers/admin.py go through create_identity_store(settings, kind) and
    never see the paths themselves."""
    identity_store_pkg = PKG / "identity_store"
    allowed_top_level = {"config.py"}
    banned = ("entra_identities_store_path", "api_keys_store_path")
    offenders: list[str] = []
    for p in PKG.rglob("*.py"):
        # Anything inside the identity_store/ package owns the layout -- allowed.
        if identity_store_pkg in p.parents:
            continue
        if p.name in allowed_top_level:
            continue
        for lineno, line in _code_lines(p):
            for token in banned:
                if token in line:
                    offenders.append(f"{p.relative_to(PKG)}:{lineno}: {line}")
    assert not offenders, (
        "identity-store backing path re-derived outside the identity_store/ "
        "package/config -- a caller that can locate the path can bypass "
        "IdentityStore and touch disk directly:\n" + "\n".join(offenders)
    )


def test_concrete_impl_referenced_only_inside_the_package() -> None:
    """The concrete ``FileSystemIdentityStore`` must appear ONLY inside the
    ``identity_store/`` package. Production consumers depend on the
    ``IdentityStore`` Protocol and obtain instances via
    ``create_identity_store(settings, kind)`` -- never by naming a concrete
    backend -- so a disk->Azure swap touches only the package. (Tests are
    permitted to construct a concrete backend directly.)"""
    identity_store_pkg = PKG / "identity_store"
    offenders: list[str] = []
    for p in PKG.rglob("*.py"):
        if identity_store_pkg in p.parents:
            continue
        for lineno, line in _code_lines(p):
            if "FileSystemIdentityStore" in line:
                offenders.append(f"{p.relative_to(PKG)}:{lineno}: {line}")
    assert not offenders, (
        "concrete FileSystemIdentityStore named outside the identity_store/ "
        "package -- consumers must use the IdentityStore Protocol + "
        "create_identity_store(settings, kind), not a concrete backend:\n"
        + "\n".join(offenders)
    )
