"""Tests for logging_config module with stdout + rotating file handlers."""

import json
import logging
import logging.handlers
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch


class TestSetupLogging:
    """Tests for setup_logging() function."""

    def teardown_method(self) -> None:
        """Remove all handlers added to root logger by setup_logging() calls."""
        root_logger = logging.getLogger()
        for handler in list(root_logger.handlers):
            handler.close()
            root_logger.removeHandler(handler)

    def _make_mock_settings(self, log_path: str, log_level: str = "INFO") -> MagicMock:
        """Create a mock settings object."""
        settings = MagicMock()
        settings.log_path = log_path
        settings.log_level = log_level
        return settings

    def test_setup_logging_adds_stream_handler(self) -> None:
        """setup_logging() should attach a StreamHandler(sys.stdout) to root logger."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = str(Path(tmpdir) / "server.jsonl")
            mock_settings = self._make_mock_settings(log_path)

            with patch(
                "context_intelligence_server.logging_config.get_settings",
                return_value=mock_settings,
            ):
                from context_intelligence_server.logging_config import setup_logging

                setup_logging()

            root_logger = logging.getLogger()
            stream_handlers = [
                h
                for h in root_logger.handlers
                if isinstance(h, logging.StreamHandler)
                and not isinstance(h, logging.handlers.RotatingFileHandler)
            ]
            assert len(stream_handlers) >= 1
            # Verify it streams to stdout
            stdout_handlers = [h for h in stream_handlers if h.stream is sys.stdout]
            assert len(stdout_handlers) >= 1

    def test_setup_logging_adds_rotating_file_handler(self) -> None:
        """setup_logging() should attach a RotatingFileHandler to root logger."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = str(Path(tmpdir) / "server.jsonl")
            mock_settings = self._make_mock_settings(log_path)

            with patch(
                "context_intelligence_server.logging_config.get_settings",
                return_value=mock_settings,
            ):
                from context_intelligence_server.logging_config import setup_logging

                setup_logging()

            root_logger = logging.getLogger()
            rotating_handlers = [
                h
                for h in root_logger.handlers
                if isinstance(h, logging.handlers.RotatingFileHandler)
            ]
            assert len(rotating_handlers) >= 1

    def test_setup_logging_creates_parent_directory(self) -> None:
        """setup_logging() should create the parent directory of log_path if missing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Use a nested directory path that doesn't exist yet
            log_path = str(Path(tmpdir) / "nested" / "deep" / "server.jsonl")
            parent_dir = Path(log_path).parent
            assert not parent_dir.exists(), "Parent dir should not exist yet"

            mock_settings = self._make_mock_settings(log_path)

            with patch(
                "context_intelligence_server.logging_config.get_settings",
                return_value=mock_settings,
            ):
                from context_intelligence_server.logging_config import setup_logging

                setup_logging()

            assert parent_dir.exists(), "Parent directory should be created"

    def test_rotating_file_handler_maxbytes_and_backups(self) -> None:
        """RotatingFileHandler should have maxBytes=10MB and backupCount=5."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = str(Path(tmpdir) / "server.jsonl")
            mock_settings = self._make_mock_settings(log_path)

            with patch(
                "context_intelligence_server.logging_config.get_settings",
                return_value=mock_settings,
            ):
                from context_intelligence_server.logging_config import setup_logging

                setup_logging()

            root_logger = logging.getLogger()
            rotating_handlers = [
                h
                for h in root_logger.handlers
                if isinstance(h, logging.handlers.RotatingFileHandler)
            ]
            assert len(rotating_handlers) >= 1
            handler = rotating_handlers[0]
            assert handler.maxBytes == 10 * 1024 * 1024, (
                f"Expected maxBytes=10MB, got {handler.maxBytes}"
            )
            assert handler.backupCount == 5, (
                f"Expected backupCount=5, got {handler.backupCount}"
            )

    def test_setup_logging_writes_json_to_file(self) -> None:
        """Logs written after setup_logging() should be parseable JSON with time, level, message."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = str(Path(tmpdir) / "server.jsonl")
            mock_settings = self._make_mock_settings(log_path)

            with patch(
                "context_intelligence_server.logging_config.get_settings",
                return_value=mock_settings,
            ):
                from context_intelligence_server.logging_config import setup_logging

                setup_logging()

            # Log a test message using the root logger
            test_message = "test JSON log entry"
            logging.getLogger().warning(test_message)

            # Flush all handlers
            root_logger = logging.getLogger()
            for handler in root_logger.handlers:
                handler.flush()

            # Read and parse the log file
            log_file = Path(log_path)
            assert log_file.exists(), "Log file should be created after logging"
            content = log_file.read_text().strip()
            assert content, "Log file should not be empty"

            # Parse each line as JSON (last non-empty line)
            lines = [line for line in content.splitlines() if line.strip()]
            assert lines, "Log file should have at least one line"
            last_line = lines[-1]
            parsed = json.loads(last_line)

            assert "time" in parsed, f"JSON log should have 'time' key, got: {parsed}"
            assert "level" in parsed, f"JSON log should have 'level' key, got: {parsed}"
            assert "message" in parsed, (
                f"JSON log should have 'message' key, got: {parsed}"
            )
