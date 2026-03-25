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

    def test_neo4j_browser_url_absent_when_not_provided(self, tmp_path: Path) -> None:
        """neo4j_browser_url is NOT written to config when not provided — it is optional."""
        config_path = tmp_path / "server-config.yaml"
        run_init(
            config_path=config_path,
            neo4j_url="bolt://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="pw",
        )
        data = yaml.safe_load(config_path.read_text())
        assert "neo4j_browser_url" not in data

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


class TestRunInitNewParams:
    def test_blob_path_written_and_expanded(self, tmp_path: Path) -> None:
        """--blob-path is written to config with ~ expanded."""
        config_path = tmp_path / "server-config.yaml"
        run_init(
            config_path=config_path,
            neo4j_url="bolt://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="pw",
            blob_path="~/.local/share/context-intelligence/blobs",
            log_path="~/.local/share/context-intelligence/logs/server.jsonl",
            cursor_path="~/.local/share/context-intelligence/cursors",
            server_host="0.0.0.0",
            server_port=8000,
        )

        data = yaml.safe_load(config_path.read_text())
        # ~ should be expanded to the actual home directory
        assert "blob_path" in data
        assert "~" not in data["blob_path"]
        assert data["blob_path"].endswith("context-intelligence/blobs")

    def test_log_path_written(self, tmp_path: Path) -> None:
        """--log-path is written to config."""
        config_path = tmp_path / "server-config.yaml"
        run_init(
            config_path=config_path,
            neo4j_url="bolt://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="pw",
            blob_path="/tmp/blobs",
            log_path="~/.local/share/context-intelligence/logs/server.jsonl",
            cursor_path="/tmp/cursors",
            server_host="0.0.0.0",
            server_port=8000,
        )

        data = yaml.safe_load(config_path.read_text())
        assert "log_path" in data
        assert "~" not in data["log_path"]
        assert data["log_path"].endswith("logs/server.jsonl")

    def test_cursor_path_written(self, tmp_path: Path) -> None:
        """--cursor-path is written to config."""
        config_path = tmp_path / "server-config.yaml"
        run_init(
            config_path=config_path,
            neo4j_url="bolt://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="pw",
            blob_path="/tmp/blobs",
            log_path="/tmp/server.jsonl",
            cursor_path="~/.local/share/context-intelligence/cursors",
            server_host="0.0.0.0",
            server_port=8000,
        )

        data = yaml.safe_load(config_path.read_text())
        assert "cursor_path" in data
        assert "~" not in data["cursor_path"]
        assert data["cursor_path"].endswith("context-intelligence/cursors")

    def test_server_host_written(self, tmp_path: Path) -> None:
        """--server-host is written to config."""
        config_path = tmp_path / "server-config.yaml"
        run_init(
            config_path=config_path,
            neo4j_url="bolt://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="pw",
            blob_path="/tmp/blobs",
            log_path="/tmp/server.jsonl",
            cursor_path="/tmp/cursors",
            server_host="127.0.0.1",
            server_port=8000,
        )

        data = yaml.safe_load(config_path.read_text())
        assert data["server_host"] == "127.0.0.1"

    def test_server_port_written_as_int(self, tmp_path: Path) -> None:
        """--server-port is written to config as an integer."""
        config_path = tmp_path / "server-config.yaml"
        run_init(
            config_path=config_path,
            neo4j_url="bolt://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="pw",
            blob_path="/tmp/blobs",
            log_path="/tmp/server.jsonl",
            cursor_path="/tmp/cursors",
            server_host="0.0.0.0",
            server_port=9090,
        )

        data = yaml.safe_load(config_path.read_text())
        assert data["server_port"] == 9090
        assert isinstance(data["server_port"], int)

    def test_neo4j_browser_url_custom(self, tmp_path: Path) -> None:
        """--neo4j-browser-url is written verbatim — supports remote hosts."""
        config_path = tmp_path / "server-config.yaml"
        run_init(
            config_path=config_path,
            neo4j_url="bolt://remotehost:37687",
            neo4j_user="neo4j",
            neo4j_password="pw",
            neo4j_browser_url="http://remotehost:37474",
        )
        data = yaml.safe_load(config_path.read_text())
        assert data["neo4j_browser_url"] == "http://remotehost:37474"

    def test_neo4j_browser_url_not_written_when_omitted(self, tmp_path: Path) -> None:
        """neo4j_browser_url is NOT written to config when not provided — it is optional."""
        config_path = tmp_path / "server-config.yaml"
        run_init(
            config_path=config_path,
            neo4j_url="bolt://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="pw",
        )
        data = yaml.safe_load(config_path.read_text())
        assert "neo4j_browser_url" not in data

    def test_all_flags_produce_complete_config(self, tmp_path: Path) -> None:
        """Providing all flags at once produces a complete config with all keys."""
        config_path = tmp_path / "server-config.yaml"
        run_init(
            config_path=config_path,
            neo4j_url="bolt://localhost:7687",
            neo4j_user="admin",
            neo4j_password="secret",
            blob_path="/data/blobs",
            log_path="/data/logs/server.jsonl",
            cursor_path="/data/cursors",
            server_host="0.0.0.0",
            server_port=8080,
            neo4j_browser_url="http://localhost:9474",
        )

        data = yaml.safe_load(config_path.read_text())
        assert data["neo4j_url"] == "bolt://localhost:7687"
        assert data["neo4j_user"] == "admin"
        assert data["neo4j_password"] == "secret"
        assert "api_key" in data
        assert data["blob_path"] == "/data/blobs"
        assert data["log_path"] == "/data/logs/server.jsonl"
        assert data["cursor_path"] == "/data/cursors"
        assert data["server_host"] == "0.0.0.0"
        assert data["server_port"] == 8080
        assert data["neo4j_browser_url"] == "http://localhost:9474"


class TestLogPathDirectoryNormalisation:
    def test_directory_log_path_appends_server_jsonl(self, tmp_path: Path) -> None:
        """When log_path has no file extension (a directory), config stores <dir>/server.jsonl."""
        config_path = tmp_path / "server-config.yaml"
        log_dir = "/some/dir"  # no extension → treated as directory

        run_init(
            config_path=config_path,
            neo4j_url="bolt://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="pw",
            log_path=log_dir,
        )

        data = yaml.safe_load(config_path.read_text())
        assert data["log_path"] == "/some/dir/server.jsonl", (
            f"Expected /some/dir/server.jsonl, got: {data['log_path']}"
        )

    def test_full_log_path_with_extension_unchanged(self, tmp_path: Path) -> None:
        """When log_path already has a .jsonl extension, it is stored as-is."""
        config_path = tmp_path / "server-config.yaml"

        run_init(
            config_path=config_path,
            neo4j_url="bolt://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="pw",
            log_path="/some/dir/server.jsonl",
        )

        data = yaml.safe_load(config_path.read_text())
        assert data["log_path"] == "/some/dir/server.jsonl"


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

    def test_default_neo4j_url_is_bolt(self, tmp_path: Path) -> None:
        """Default --neo4j-url uses bolt:// not neo4j:// (Community Edition compat)."""
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
                "--neo4j-password",
                "pw",
            ],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent.parent),
        )

        assert result.returncode == 0
        data = yaml.safe_load(config_path.read_text())
        assert data["neo4j_url"] == "bolt://localhost:7687"
        assert not data["neo4j_url"].startswith("neo4j://")

    def test_cli_accepts_neo4j_browser_url_flag(self, tmp_path: Path) -> None:
        """CLI --neo4j-browser-url flag is written to config verbatim."""
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
                "--neo4j-password",
                "pw",
                "--neo4j-browser-url",
                "http://neo4j-host:37474",
            ],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent.parent),
        )
        assert result.returncode == 0
        data = yaml.safe_load(config_path.read_text())
        assert data["neo4j_browser_url"] == "http://neo4j-host:37474"

    def test_cli_accepts_all_new_flags(self, tmp_path: Path) -> None:
        """CLI accepts --blob-path, --log-path, --cursor-path, --server-host, --server-port."""
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
                "--neo4j-password",
                "pw",
                "--blob-path",
                "/data/blobs",
                "--log-path",
                "/data/logs/server.jsonl",
                "--cursor-path",
                "/data/cursors",
                "--server-host",
                "127.0.0.1",
                "--server-port",
                "9000",
            ],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent.parent),
        )

        assert result.returncode == 0
        data = yaml.safe_load(config_path.read_text())
        assert data["blob_path"] == "/data/blobs"
        assert data["log_path"] == "/data/logs/server.jsonl"
        assert data["cursor_path"] == "/data/cursors"
        assert data["server_host"] == "127.0.0.1"
        assert data["server_port"] == 9000
        assert isinstance(data["server_port"], int)

    def test_cli_omitting_neo4j_browser_url_does_not_write_it(
        self, tmp_path: Path
    ) -> None:
        """Omitting --neo4j-browser-url from CLI means the key is absent from config."""
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
                "--neo4j-password",
                "pw",
            ],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent.parent),
        )
        assert result.returncode == 0
        data = yaml.safe_load(config_path.read_text())
        assert "neo4j_browser_url" not in data
