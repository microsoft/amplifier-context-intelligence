"""WS-3a sec 3a-5 tripwire: no shipped doc instructs `uvicorn ...main:app`.

`main:app` is the bare FastAPI app -- no BearerTokenMiddleware, so /admin/*
(including /admin/maintenance) is UNAUTHENTICATED under that command. The
correct entrypoint for a real run is `main:asgi_app`. This test asserts the
*command shape* (`uvicorn ...main:app`), not the bare token `main:app` --
`docs/auth-troubleshooting-and-upgrades.md` legitimately mentions `main:app`
in prose *about* this exact bug, and must not trip the tripwire.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent

# Matches a uvicorn invocation targeting the un-middlewared bare app, e.g.:
#   uvicorn context_intelligence_server.main:app --host ...
#   uv run uvicorn context_intelligence_server.main:app --reload
# but NOT `context_intelligence_server.main:asgi_app` (asgi_app doesn't end
# in a boundary right after `:app`).
_BARE_APP_TARGET_RE = re.compile(r"uvicorn\s+context_intelligence_server\.main:app\b")


def _shipped_docs() -> list[Path]:
    """README.md plus every markdown file under docs/ (product docs only)."""
    paths = [_REPO_ROOT / "README.md"]
    docs_dir = _REPO_ROOT / "docs"
    if docs_dir.is_dir():
        paths.extend(sorted(docs_dir.rglob("*.md")))
    return [p for p in paths if p.is_file()]


def test_no_shipped_doc_instructs_bare_main_app() -> None:
    """No README.md/docs/*.md tells an operator to run the un-middlewared app.

    docs/auth-troubleshooting-and-upgrades.md is allowed to keep discussing
    the bug in prose (it does not contain an actual `uvicorn ...main:app`
    command), so this test does not special-case any file -- it just checks
    for the dangerous COMMAND SHAPE everywhere.
    """
    offenders: dict[str, list[str]] = {}
    for path in _shipped_docs():
        text = path.read_text(encoding="utf-8")
        matches = _BARE_APP_TARGET_RE.findall(text)
        if matches:
            offenders[str(path.relative_to(_REPO_ROOT))] = matches

    assert not offenders, (
        "Found a documented `uvicorn ...main:app` command (the bare, "
        "un-middlewared app -- /admin/* is UNAUTHENTICATED under it). Use "
        "`main:asgi_app` instead. Offending files: "
        f"{offenders!r}"
    )


def test_auth_troubleshooting_doc_still_mentions_the_bug_in_prose() -> None:
    """Sanity check for the test above: the discussion doc still exists and
    still names `main:app` in prose (proving the regex correctly did NOT
    flag it, rather than the file having been silently deleted/rewritten)."""
    doc = _REPO_ROOT / "docs" / "auth-troubleshooting-and-upgrades.md"
    assert doc.is_file()
    text = doc.read_text(encoding="utf-8")
    assert "main:app" in text
    assert not _BARE_APP_TARGET_RE.search(text)
