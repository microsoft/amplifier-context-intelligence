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
        """Generated file contains neo4j_url, neo4j_user, neo4j_password, api_key."""
        cred_path = tmp_path / "credentials.yaml"
        generate_credentials(
            cred_path, neo4j_url="neo4j://localhost:7687", neo4j_user="neo4j"
        )

        data = yaml.safe_load(cred_path.read_text())
        assert "neo4j_url" in data
        assert "neo4j_user" in data
        assert "neo4j_password" in data
        assert "api_key" in data

    def test_generates_non_empty_secrets(self, tmp_path: Path) -> None:
        """neo4j_password and api_key are non-empty strings."""
        cred_path = tmp_path / "credentials.yaml"
        generate_credentials(
            cred_path, neo4j_url="neo4j://localhost:7687", neo4j_user="neo4j"
        )

        data = yaml.safe_load(cred_path.read_text())
        assert len(data["neo4j_password"]) > 0
        assert len(data["api_key"]) > 0

    def test_generates_unique_secrets(self, tmp_path: Path) -> None:
        """Two calls produce different passwords and tokens."""
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
        assert a["api_key"] != b["api_key"]

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
        assert "api_key" in data

    def test_raises_on_missing_file(self, tmp_path: Path) -> None:
        """read_credentials raises FileNotFoundError for missing file."""
        with pytest.raises(FileNotFoundError):
            read_credentials(tmp_path / "nonexistent.yaml")
