"""Guard the retired production PyJWT/JWKS path and SDK-independent static mode."""

import ast
import inspect
import subprocess
import sys
from pathlib import Path


def test_auth_module_no_longer_imports_or_uses_pyjwt_or_jwks() -> None:
    from context_intelligence_server import auth

    tree = ast.parse(inspect.getsource(auth))
    imports: set[str] = set()
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.split(".")[0])
        elif isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
    assert "jwt" not in imports
    assert not names.intersection({"jwt", "PyJWKClient", "JWKS_CACHE_LIFESPAN_SECONDS"})


def test_static_import_and_construction_need_neither_mise_nor_pyjwt(
    tmp_path: Path,
) -> None:
    """Fresh-process import guard, not fake SDK installation or token validation."""
    code = """
import hashlib
import http.client
import sys
from importlib.abc import MetaPathFinder

attempts = []
http_attempts = []
def reject_connection(*args, **kwargs):
    http_attempts.append(True)
    raise AssertionError("Static mode must not connect to a sidecar")
http.client.HTTPConnection = reject_connection
class RejectAuthenticitySdkImport(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in {"mise", "jwt"}:
            attempts.append(fullname)
            raise AssertionError("Static mode must not load authenticity SDKs")

sys.meta_path.insert(0, RejectAuthenticitySdkImport())
from context_intelligence_server.auth import StaticKeyResolver
from context_intelligence_server.config import Settings
from context_intelligence_server.main import create_asgi_app

digest = hashlib.sha256(b"static-import-handle").hexdigest()
settings = Settings(
    auth_mode="static",
    api_keys={digest: {"id": "static-contributor"}},
    api_keys_store_path=sys.argv[1],
    allow_unauthenticated=False,
)
wrapped = create_asgi_app(settings=settings)
assert isinstance(wrapped.resolver, StaticKeyResolver)
assert wrapped.resolver.resolve("static-import-handle") == (
    "static-contributor", [], False
)
assert attempts == []
assert http_attempts == []
assert "mise" not in sys.modules
assert "jwt" not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(tmp_path / "api-keys.json")],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
