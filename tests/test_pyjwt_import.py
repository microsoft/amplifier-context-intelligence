"""T1 smoke tests: pyjwt[crypto] is installed and the crypto extra is available.

These tests exist purely to verify that the runtime dependency added by T1 is
present and that the ``cryptography`` extra (RSA key operations) was pulled in.
They contain no production logic — they are intentionally trivial so any CI
failure is unambiguous: the package is missing, not the test logic.
"""


def test_jwt_package_importable() -> None:
    """``import jwt`` succeeds and the package is a real pyjwt install."""
    import jwt  # noqa: PLC0415

    # PyJWT exposes __version__; absence means a wrong package is installed.
    assert hasattr(jwt, "__version__"), "pyjwt must expose __version__"


def test_jwt_crypto_extra_available() -> None:
    """The ``cryptography`` extra is installed (pyjwt[crypto] dependency).

    ``jwt.algorithms.RSAAlgorithm`` is only available when the ``cryptography``
    package is present.  A bare ``pyjwt`` install without the extra would raise
    ``ImportError`` here.
    """
    from jwt.algorithms import RSAAlgorithm  # noqa: PLC0415

    # RSAAlgorithm.from_jwk is the crypto-backed call required by PyJWKClient.
    assert callable(RSAAlgorithm.from_jwk), (
        "RSAAlgorithm.from_jwk must be callable (cryptography extra present)"
    )
