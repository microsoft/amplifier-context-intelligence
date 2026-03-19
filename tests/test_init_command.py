"""Tests for the context-intelligence-server-init CLI command."""

from pathlib import Path

import yaml

from context_intelligence_server.init_command import run_init


class TestRunInit:
    def test_writes_config_file(self, tmp_path: Path) -> None:
        """run_init writes a server-config.yaml file."""
        config_path = tmp_path / "server-config.yaml"
        run_init(
            config_path=config_path,
            neo4j_url="neo4j://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="test-password",
        )

        assert config_path.exists()

    def test_config_contains_all_keys(self, tmp_path: Path) -> None:
        """Written config contains neo4j_url, neo4j_user, neo4j_password, api_key."""
        config_path = tmp_path / "server-config.yaml"
        run_init(
            config_path=config_path,
            neo4j_url="neo4j://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="test-password",
        )

        data = yaml.safe_load(config_path.read_text())
        assert data["neo4j_url"] == "neo4j://localhost:7687"
        assert data["neo4j_user"] == "neo4j"
        assert data["neo4j_password"] == "test-password"
        assert "api_key" in data
        assert len(data["api_key"]) > 0

    def test_auto_generates_api_key(self, tmp_path: Path) -> None:
        """api_key is auto-generated and non-empty."""
        config_path = tmp_path / "server-config.yaml"
        run_init(
            config_path=config_path,
            neo4j_url="neo4j://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="pw",
        )

        data = yaml.safe_load(config_path.read_text())
        assert isinstance(data["api_key"], str)
        assert len(data["api_key"]) >= 32

    def test_returns_api_key(self, tmp_path: Path) -> None:
        """run_init returns the generated api_key so the caller can print it."""
        config_path = tmp_path / "server-config.yaml"
        result = run_init(
            config_path=config_path,
            neo4j_url="neo4j://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="pw",
        )

        assert "api_key" in result
        data = yaml.safe_load(config_path.read_text())
        assert result["api_key"] == data["api_key"]

    def test_overwrites_existing_config(self, tmp_path: Path) -> None:
        """Running init again overwrites the existing config file."""
        config_path = tmp_path / "server-config.yaml"

        run_init(
            config_path=config_path,
            neo4j_url="neo4j://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="pw1",
        )
        first = yaml.safe_load(config_path.read_text())

        run_init(
            config_path=config_path,
            neo4j_url="neo4j://localhost:9999",
            neo4j_user="admin",
            neo4j_password="pw2",
        )
        second = yaml.safe_load(config_path.read_text())

        assert second["neo4j_url"] == "neo4j://localhost:9999"
        assert second["neo4j_user"] == "admin"
        assert second["neo4j_password"] == "pw2"
        assert first["api_key"] != second["api_key"]

    def test_preserves_existing_fields_not_overwritten(self, tmp_path: Path) -> None:
        """Existing config fields (e.g. blob_path) are preserved when init writes."""
        config_path = tmp_path / "server-config.yaml"
        config_path.write_text("blob_path: /custom/blobs\nlog_level: DEBUG\n")

        run_init(
            config_path=config_path,
            neo4j_url="neo4j://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="pw",
        )

        data = yaml.safe_load(config_path.read_text())
        assert data["blob_path"] == "/custom/blobs"
        assert data["log_level"] == "DEBUG"
        assert "api_key" in data


class TestCliEntryPoint:
    def test_cli_with_flags(self, tmp_path: Path) -> None:
        """CLI entry point accepts --neo4j-url, --neo4j-user, --neo4j-password flags."""
        import subprocess
        import sys

        config_path = tmp_path / "server-config.yaml"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "context_intelligence_server.init_command",
                "--config-path",
                str(config_path),
                "--neo4j-url",
                "neo4j://localhost:7687",
                "--neo4j-user",
                "neo4j",
                "--neo4j-password",
                "test-pw",
            ],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent.parent),
        )

        assert result.returncode == 0
        assert config_path.exists()
        assert "api_key" in result.stdout or "config" in result.stdout.lower()
