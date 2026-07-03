"""Tests for admin-key storage-at-rest alignment (admin_api_key_sha256).

Mirrors the ``api_keys`` digest-at-rest contract for the admin credential:
the RECOMMENDED ``admin_api_key_sha256`` stores a SHA-256 digest at rest, while
the legacy raw ``admin_api_key`` still works (hashed at load) but is deprecated.
See docs/managing-api-keys.md and config.resolve_admin_api_key_digest().
"""

import hashlib

import pytest
from pydantic import ValidationError

# A real token and its digest, derived exactly the way the server does.
_TOKEN = "kuD8xSnjKC4QTa-4kuqlw9uLA1EonsFntMvVmU8DAjo"
_DIGEST = hashlib.sha256(_TOKEN.encode()).hexdigest()


def test_admin_api_key_sha256_resolves_verbatim():
    """A configured digest is returned verbatim (leak-safe: no raw token at rest)."""
    from context_intelligence_server.config import Settings

    s = Settings(admin_api_key_sha256=_DIGEST)
    assert s.admin_api_key is None
    assert s.resolve_admin_api_key_digest() == _DIGEST


def test_admin_api_key_sha256_uppercase_is_normalized():
    """An UPPERCASE digest is normalized to lowercase to match hexdigest()."""
    from context_intelligence_server.config import Settings

    s = Settings(admin_api_key_sha256=_DIGEST.upper())
    assert s.admin_api_key_sha256 == _DIGEST
    assert s.resolve_admin_api_key_digest() == _DIGEST


def test_admin_api_key_sha256_invalid_fails_closed():
    """A non-64-hex digest is a hard startup error (fail-closed), not a silent 401."""
    from context_intelligence_server.config import Settings

    with pytest.raises(ValidationError):
        Settings(admin_api_key_sha256="not-a-valid-digest")
    with pytest.raises(ValidationError):
        Settings(admin_api_key_sha256="abc123")  # too short


def test_admin_api_key_sha256_empty_string_is_none():
    """Empty string normalizes to None (mirrors admin_api_key)."""
    from context_intelligence_server.config import Settings

    s = Settings(admin_api_key_sha256="")
    assert s.admin_api_key_sha256 is None
    assert s.resolve_admin_api_key_digest() is None


def test_legacy_raw_admin_api_key_still_hashed():
    """Back-compat: legacy raw admin_api_key is hashed to the same digest."""
    from context_intelligence_server.config import Settings

    s = Settings(admin_api_key=_TOKEN)
    assert s.admin_api_key_sha256 is None
    assert s.resolve_admin_api_key_digest() == _DIGEST


def test_digest_field_wins_when_both_set():
    """When both are set, the digest field wins and the raw field is ignored."""
    from context_intelligence_server.config import Settings

    other_digest = hashlib.sha256(b"a-different-token").hexdigest()
    s = Settings(admin_api_key="some-raw-token", admin_api_key_sha256=other_digest)
    assert s.resolve_admin_api_key_digest() == other_digest


def test_no_admin_key_resolves_none():
    """Neither field set -> admin API disabled (None digest)."""
    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.resolve_admin_api_key_digest() is None


def test_resolved_digest_matches_bearer_token_flow():
    """The digest stored at rest equals sha256 of the raw bearer token a client sends.

    This is what lets a client keep the raw token while the server stores only
    the one-way digest -- the same guarantee api_keys provides.
    """
    from context_intelligence_server.config import Settings

    s = Settings(admin_api_key_sha256=_DIGEST)
    presented_token_digest = hashlib.sha256(_TOKEN.encode()).hexdigest()
    assert s.resolve_admin_api_key_digest() == presented_token_digest


def test_env_var_configures_digest(monkeypatch):
    """The digest can be set via the SERVER_-prefixed env var (pydantic-settings)."""
    monkeypatch.setenv(
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ADMIN_API_KEY_SHA256", _DIGEST
    )
    from context_intelligence_server.config import Settings

    s = Settings()
    assert s.resolve_admin_api_key_digest() == _DIGEST
