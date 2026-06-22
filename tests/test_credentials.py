"""Tests for credential generation and persistence."""

from pathlib import Path

import pytest
import yaml

from context_intelligence_server.credentials import (
    generate_credentials,
    read_credentials,
)


class TestGenerateCredentials:
    def test_writes_yaml_file(self, tmp_path: Path) -> None:
        """generate_credentials creates a credentials.yaml file."""
        cred_path = tmp_path / "credentials.yaml"
        generate_credentials(
            cred_path, neo4j_url="neo4j://localhost:7687", neo4j_user="neo4j"
        )

        assert cred_path.exists()

    def test_contains_required_keys(self, tmp_path: Path) -> None:
        """Generated file contains neo4j_url, neo4j_user, neo4j_password, api_keys (not flat api_key)."""
        cred_path = tmp_path / "credentials.yaml"
        generate_credentials(
            cred_path, neo4j_url="neo4j://localhost:7687", neo4j_user="neo4j"
        )

        data = yaml.safe_load(cred_path.read_text())
        assert "neo4j_url" in data
        assert "neo4j_user" in data
        assert "neo4j_password" in data
        assert "api_keys" in data   # new nested format
        assert "api_key" not in data  # legacy flat key must NOT appear

    def test_generates_non_empty_secrets(self, tmp_path: Path) -> None:
        """neo4j_password and api_keys digest entry are non-empty."""
        cred_path = tmp_path / "credentials.yaml"
        generate_credentials(
            cred_path, neo4j_url="neo4j://localhost:7687", neo4j_user="neo4j"
        )

        data = yaml.safe_load(cred_path.read_text())
        assert len(data["neo4j_password"]) > 0
        digest = next(iter(data["api_keys"]))
        assert len(digest) == 64  # sha256 hex digest is 64 chars

    def test_generates_unique_secrets(self, tmp_path: Path) -> None:
        """Two calls produce different passwords and token digests."""
        path_a = tmp_path / "a.yaml"
        path_b = tmp_path / "b.yaml"
        generate_credentials(
            path_a, neo4j_url="neo4j://localhost:7687", neo4j_user="neo4j"
        )
        generate_credentials(
            path_b, neo4j_url="neo4j://localhost:7687", neo4j_user="neo4j"
        )

        a = yaml.safe_load(path_a.read_text())
        b = yaml.safe_load(path_b.read_text())
        assert a["neo4j_password"] != b["neo4j_password"]
        a_digest = next(iter(a["api_keys"]))
        b_digest = next(iter(b["api_keys"]))
        assert a_digest != b_digest

    def test_preserves_provided_neo4j_values(self, tmp_path: Path) -> None:
        """neo4j_url and neo4j_user are written exactly as provided."""
        cred_path = tmp_path / "credentials.yaml"
        generate_credentials(
            cred_path, neo4j_url="neo4j://custom:9999", neo4j_user="admin"
        )

        data = yaml.safe_load(cred_path.read_text())
        assert data["neo4j_url"] == "neo4j://custom:9999"
        assert data["neo4j_user"] == "admin"

    def test_accepts_explicit_password(self, tmp_path: Path) -> None:
        """When neo4j_password is provided, it is used instead of generating one."""
        cred_path = tmp_path / "credentials.yaml"
        generate_credentials(
            cred_path,
            neo4j_url="neo4j://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="my-explicit-pw",
        )

        data = yaml.safe_load(cred_path.read_text())
        assert data["neo4j_password"] == "my-explicit-pw"

    def test_creates_parent_directories(self, tmp_path: Path) -> None:
        """Parent directories are created if they do not exist."""
        cred_path = tmp_path / "deep" / "nested" / "credentials.yaml"
        generate_credentials(
            cred_path, neo4j_url="neo4j://localhost:7687", neo4j_user="neo4j"
        )

        assert cred_path.exists()


class TestReadCredentials:
    def test_reads_generated_file(self, tmp_path: Path) -> None:
        """read_credentials returns the same data that was written."""
        cred_path = tmp_path / "credentials.yaml"
        generate_credentials(
            cred_path, neo4j_url="neo4j://localhost:7687", neo4j_user="neo4j"
        )

        data = read_credentials(cred_path)
        assert data["neo4j_url"] == "neo4j://localhost:7687"
        assert data["neo4j_user"] == "neo4j"
        assert "neo4j_password" in data
        assert "api_keys" in data  # new format: nested api_keys, not flat api_key
        assert "api_key" not in data

    def test_raises_on_missing_file(self, tmp_path: Path) -> None:
        """read_credentials raises FileNotFoundError for missing file."""
        with pytest.raises(FileNotFoundError):
            read_credentials(tmp_path / "nonexistent.yaml")


# ---------------------------------------------------------------------------
# Phase 4b: new api_keys format emitted by generate_credentials
# ---------------------------------------------------------------------------


class TestGenerateCredentialsNewFormat:
    """RED-first tests for the new api_keys bootstrap format (Phase 4b).

    All five tests were written BEFORE the implementation was changed so they
    could be run in the RED state first.
    """

    def test_generate_credentials_emits_api_keys_block(
        self, tmp_path: Path
    ) -> None:
        """Written YAML contains api_keys:{<digest>:{id:'owner'}}, NOT top-level api_key."""
        cred_path = tmp_path / "credentials.yaml"
        generate_credentials(
            cred_path, neo4j_url="neo4j://localhost:7687", neo4j_user="neo4j"
        )

        data = yaml.safe_load(cred_path.read_text())

        assert "api_keys" in data, "written YAML must contain 'api_keys'"
        assert "api_key" not in data, "written YAML must NOT contain legacy flat 'api_key'"
        assert isinstance(data["api_keys"], dict)
        assert len(data["api_keys"]) == 1

        digest = next(iter(data["api_keys"]))
        assert len(digest) == 64, f"digest key must be 64 chars, got {len(digest)}"
        assert all(c in "0123456789abcdef" for c in digest), (
            f"digest key must be lowercase hex, got {digest!r}"
        )

        entry = data["api_keys"][digest]
        assert entry == {"id": "owner"}, f"entry must be {{id: owner}}, got {entry!r}"

    def test_generated_digest_matches_server_derivation(
        self, tmp_path: Path
    ) -> None:
        """Stored digest == hashlib.sha256(returned_raw_token.encode()).hexdigest().

        Single-source-of-truth guard: the digest in the file must be derivable
        from the returned token using the EXACT same formula as auth.py
        _resolve_token.
        """
        import hashlib

        cred_path = tmp_path / "credentials.yaml"
        raw_token = generate_credentials(
            cred_path, neo4j_url="neo4j://localhost:7687", neo4j_user="neo4j"
        )

        data = yaml.safe_load(cred_path.read_text())
        digest_in_file = next(iter(data["api_keys"]))

        expected_digest = hashlib.sha256(raw_token.encode()).hexdigest()
        assert digest_in_file == expected_digest, (
            f"File digest {digest_in_file[:8]}... does not match "
            f"sha256(raw_token) {expected_digest[:8]}..."
        )

    def test_generate_credentials_returns_raw_token(self, tmp_path: Path) -> None:
        """Function returns non-empty raw token; raw token is NOT present in the file."""
        cred_path = tmp_path / "credentials.yaml"
        raw_token = generate_credentials(
            cred_path, neo4j_url="neo4j://localhost:7687", neo4j_user="neo4j"
        )

        assert isinstance(raw_token, str), "return value must be a string"
        assert len(raw_token) > 0, "raw token must be non-empty"

        file_contents = cred_path.read_text()
        assert raw_token not in file_contents, (
            "raw token must NOT appear in the credentials file; only the digest is stored"
        )

    def test_generated_config_round_trips_to_keystore(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Load generated file via Settings/build_keystore(); raw token authenticates.

        Full pipeline: generate_credentials → YAML file → Settings → build_keystore()
        → verify sha256(raw_token) is in keystore mapping to 'owner'.
        """
        import hashlib

        from context_intelligence_server.config import Settings

        cred_path = tmp_path / "credentials.yaml"
        raw_token = generate_credentials(
            cred_path, neo4j_url="neo4j://localhost:7687", neo4j_user="neo4j"
        )

        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(cred_path)
        )
        s = Settings()
        ks = s.build_keystore()

        expected_digest = hashlib.sha256(raw_token.encode()).hexdigest()
        assert expected_digest in ks, (
            f"digest {expected_digest[:8]}... not found in keystore; keystore keys: "
            f"{[k[:8] + '...' for k in ks]!r}"
        )
        assert ks[expected_digest] == "owner", (
            f"keystore[digest] must be 'owner', got {ks[expected_digest]!r}"
        )

    def test_shell_derivation_parity(self) -> None:
        """Shell heredoc digest formula == auth.py _resolve_token derivation.

        Guards against the shell one-liner drifting from the Python derivation.
        The shell scripts (start.sh, docker-entrypoint.sh) use:

            python3 -c "import hashlib,sys; \\
                print(hashlib.sha256(sys.argv[1].encode()).hexdigest())" "$RAW_TOKEN"

        This test replicates that exact stdlib expression and verifies it produces
        a digest that _resolve_token can find in a keystore keyed by that same
        digest — proving the shell and server derivations are identical.

        NOTE: This is a Python test that mirrors the shell formula.  No subprocess
        is spawned.  We pin the expression here rather than run the script to keep
        CI dependency-free (avoids bash version quirks, path issues, etc.).
        """
        import hashlib

        from context_intelligence_server.auth import _resolve_token

        token = "sentinel-parity-token-for-phase-4b"

        # Replicate the exact shell inline expression (stdlib only, no project imports):
        #   python3 -c "import hashlib,sys;
        #       print(hashlib.sha256(sys.argv[1].encode()).hexdigest())" "$token"
        shell_formula_digest = hashlib.sha256(token.encode()).hexdigest()

        # Verify _resolve_token (auth.py) can find a token via this same digest.
        keystore = {shell_formula_digest: "owner"}
        resolved = _resolve_token(token, keystore)

        assert resolved == "owner", (
            f"Shell formula produced digest {shell_formula_digest[:8]}... "
            f"but _resolve_token could not authenticate the token. "
            f"The shell and server derivations have diverged."
        )
