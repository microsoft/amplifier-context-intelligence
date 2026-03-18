"""Tests for docs/service-setup.md documentation.

Validates that the service setup guide:
- Exists and is non-empty
- Has the correct heading on line 1
- Does NOT mention neo4j_database (key does not exist in example config)
- Contains all 5 major sections as specified
- Contains key content: prerequisites, configuration, systemd unit file, launchd, verification
"""

from pathlib import Path

# tests/ -> amplifier-context-intelligence/ -> repo root
SUBMODULE_ROOT = Path(__file__).parent.parent
DOCS_PATH = SUBMODULE_ROOT / "docs" / "service-setup.md"


def _content() -> str:
    """Return file content (cached per test run via call-through)."""
    return DOCS_PATH.read_text()


# ---------------------------------------------------------------------------
# Existence checks
# ---------------------------------------------------------------------------


def test_service_setup_file_exists():
    """docs/service-setup.md must exist at the expected path."""
    assert DOCS_PATH.exists(), f"File not found: {DOCS_PATH}"


def test_service_setup_file_is_nonempty():
    """docs/service-setup.md must not be empty."""
    assert DOCS_PATH.stat().st_size > 0, "File is empty"


# ---------------------------------------------------------------------------
# Heading and first-line checks
# ---------------------------------------------------------------------------


def test_first_line_is_title():
    """First line must be '# Running as a System Service'."""
    first_line = _content().splitlines()[0]
    assert first_line == "# Running as a System Service", (
        f"Expected '# Running as a System Service', got {first_line!r}"
    )


def test_first_five_lines_contain_title_and_description():
    """First 5 lines must include heading and description."""
    lines = _content().splitlines()[:5]
    text = "\n".join(lines)
    assert "# Running as a System Service" in text
    assert "context-intelligence-server" in text


# ---------------------------------------------------------------------------
# Forbidden content
# ---------------------------------------------------------------------------


def test_no_neo4j_database_key():
    """neo4j_database must NOT appear (it does not exist in example config)."""
    count = _content().count("neo4j_database")
    assert count == 0, f"neo4j_database appears {count} time(s) — must be absent"


# ---------------------------------------------------------------------------
# Section presence checks
# ---------------------------------------------------------------------------


def test_section_prerequisites():
    """Section '## 1. Prerequisites' must be present."""
    assert "## 1. Prerequisites" in _content()


def test_section_configuration():
    """Section '## 2. Configuration' must be present."""
    assert "## 2. Configuration" in _content()


def test_section_linux_systemd():
    """Section '## 3. Linux' with systemd content must be present."""
    content = _content()
    assert "## 3. Linux" in content
    assert "systemd" in content


def test_section_macos_launchd():
    """Section '## 4. macOS' with launchd content must be present."""
    content = _content()
    assert "## 4. macOS" in content
    assert "launchd" in content


def test_section_verification_troubleshooting():
    """Section '## 5. Verification' must be present."""
    assert "## 5. Verification" in _content()


# ---------------------------------------------------------------------------
# Prerequisites content
# ---------------------------------------------------------------------------


def test_prerequisites_uv_install_command():
    """Prerequisites section must include the uv install curl command."""
    assert "https://astral.sh/uv/install.sh" in _content()


def test_prerequisites_uv_tool_install_command():
    """Prerequisites section must include the uv tool install command."""
    assert (
        "uv tool install git+https://github.com/colombod/amplifier-context-intelligence"
        in _content()
    )


def test_prerequisites_binary_path():
    """Must note the binary at ~/.local/bin/context-intelligence-server."""
    assert "~/.local/bin/context-intelligence-server" in _content()


def test_prerequisites_upgrade_command():
    """Must include uv tool upgrade command."""
    assert "uv tool upgrade context-intelligence-server" in _content()


# ---------------------------------------------------------------------------
# Configuration content
# ---------------------------------------------------------------------------


def test_configuration_config_dir_creation():
    """Must include mkdir for config directory."""
    assert "mkdir -p ~/.config/context-intelligence" in _content()


def test_configuration_curl_download():
    """Must include curl command to download example config from GitHub raw URL."""
    content = _content()
    assert "curl" in content
    assert "raw.githubusercontent.com" in content
    assert "server-config.example.yaml" in content


def test_configuration_server_settings_table():
    """Must contain server settings: server_host, server_port, log_level."""
    content = _content()
    assert "server_host" in content
    assert "server_port" in content
    assert "log_level" in content


def test_configuration_neo4j_settings_table():
    """Must contain Neo4j settings: neo4j_url, neo4j_user, neo4j_password."""
    content = _content()
    assert "neo4j_url" in content
    assert "neo4j_user" in content
    assert "neo4j_password" in content


def test_configuration_storage_settings_table():
    """Must contain storage settings: blob_path, log_path, cursor_path."""
    content = _content()
    assert "blob_path" in content
    assert "log_path" in content
    assert "cursor_path" in content


def test_configuration_cursor_path_persistence_note():
    """Must note cursor_path persistence improvement over Docker Compose."""
    content = _content()
    assert "cursor" in content.lower()
    assert "docker" in content.lower() or "Docker" in content


def test_configuration_storage_dirs_creation():
    """Must include mkdir for storage directories with brace expansion."""
    assert (
        "mkdir -p ~/.local/share/context-intelligence/{blobs,logs,cursors}"
        in _content()
    )


# ---------------------------------------------------------------------------
# Linux systemd content
# ---------------------------------------------------------------------------


def test_systemd_unit_file_path():
    """Must include the unit file path."""
    assert "~/.config/systemd/user/context-intelligence-server.service" in _content()


def test_systemd_unit_description():
    """Unit file must have Description=Context Intelligence Server."""
    assert "Description=Context Intelligence Server" in _content()


def test_systemd_unit_after_network():
    """Unit file must have After=network.target."""
    assert "After=network.target" in _content()


def test_systemd_service_type_simple():
    """Service section must have Type=simple."""
    assert "Type=simple" in _content()


def test_systemd_exec_start():
    """ExecStart must use %h specifier for the binary path."""
    assert "ExecStart=%h/.local/bin/context-intelligence-server" in _content()


def test_systemd_environment_config():
    """Environment must include config file path with %h specifier."""
    content = _content()
    assert (
        "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE=%h/.config/context-intelligence/server-config.yaml"
        in content
    )


def test_systemd_restart_on_failure():
    """Service must have Restart=on-failure."""
    assert "Restart=on-failure" in _content()


def test_systemd_restart_sec():
    """Service must have RestartSec=5s."""
    assert "RestartSec=5s" in _content()


def test_systemd_standard_output_journal():
    """Service must have StandardOutput=journal."""
    assert "StandardOutput=journal" in _content()


def test_systemd_standard_error_journal():
    """Service must have StandardError=journal."""
    assert "StandardError=journal" in _content()


def test_systemd_wanted_by_default_target():
    """Install section must have WantedBy=default.target."""
    assert "WantedBy=default.target" in _content()


def test_systemd_h_specifier_note():
    """Must explain that %h is the systemd home directory specifier."""
    content = _content()
    assert "%h" in content
    assert "specifier" in content or "home directory" in content.lower()


def test_systemd_enable_start_commands():
    """Must include systemctl --user daemon-reload, enable, and start commands."""
    content = _content()
    assert "systemctl --user daemon-reload" in content
    assert "systemctl --user enable context-intelligence-server" in content
    assert "systemctl --user start context-intelligence-server" in content


def test_systemd_status_and_logs():
    """Must include status check and journalctl log commands."""
    content = _content()
    assert "systemctl --user status context-intelligence-server" in content
    assert "journalctl --user -u context-intelligence-server" in content


def test_systemd_linger_command():
    """Must include loginctl enable-linger $USER for boot autostart."""
    assert "loginctl enable-linger $USER" in _content()


# ---------------------------------------------------------------------------
# macOS launchd content
# ---------------------------------------------------------------------------


def test_macos_sed_substitution():
    """Must include sed command to expand HOME_DIR placeholder."""
    content = _content()
    assert "sed" in content
    assert "HOME_DIR" in content
    assert "$HOME" in content


def test_macos_launch_agents_path():
    """Must include ~/Library/LaunchAgents/ path."""
    assert "~/Library/LaunchAgents" in _content()


def test_macos_plist_filename():
    """Must reference com.context-intelligence.server.plist."""
    assert "com.context-intelligence.server.plist" in _content()


def test_macos_why_not_tilde_note():
    """Must explain why ~ cannot be used in launchd plists."""
    content = _content()
    # Should mention that launchd doesn't expand ~ or shell variables
    assert "launchd" in content
    # The note about why not ~
    assert "~" in content or "tilde" in content.lower()


def test_macos_launchctl_load():
    """Must include launchctl load command."""
    assert "launchctl load" in _content()


def test_macos_launchctl_list_grep():
    """Must include launchctl list | grep command for status check."""
    content = _content()
    assert "launchctl list" in content
    assert "grep context-intelligence" in content


def test_macos_launchctl_unload():
    """Must include launchctl unload command."""
    assert "launchctl unload" in _content()


def test_macos_view_logs_tail():
    """Must include tail -f commands for viewing macOS logs."""
    content = _content()
    assert "tail -f" in content
    assert "server.stdout.log" in content
    assert "server.stderr.log" in content


# ---------------------------------------------------------------------------
# Verification & troubleshooting content
# ---------------------------------------------------------------------------


def test_verification_health_check_curl():
    """Must include curl health check command."""
    assert "curl http://localhost:8000/status" in _content()


def test_troubleshooting_log_locations_table():
    """Must include log locations table with Linux and macOS entries."""
    content = _content()
    assert "journalctl" in content
    assert "tail -f" in content


def test_troubleshooting_command_not_found():
    """Must address 'command not found' issue with PATH fix."""
    content = _content()
    assert "command not found" in content or "PATH" in content
    assert ".local/bin" in content


def test_troubleshooting_neo4j_connection():
    """Must address Neo4j connection issue."""
    content = _content()
    assert "Neo4j" in content or "neo4j" in content
    # Service starts then stops
    assert (
        "immediately stops" in content
        or "immediately stop" in content
        or "neo4j_url" in content
    )


def test_troubleshooting_permission_denied():
    """Must address permission denied issue."""
    assert (
        "Permission denied" in _content() or "permission denied" in _content().lower()
    )


def test_troubleshooting_port_in_use():
    """Must address port conflict issue."""
    content = _content()
    assert "port" in content.lower()
    assert "8000" in content


def test_troubleshooting_linux_boot():
    """Must address Linux boot start (linger) issue."""
    content = _content()
    assert "linger" in content.lower() or "loginctl" in content


def test_troubleshooting_macos_silent_failure():
    """Must address macOS silent failure (check stderr log)."""
    content = _content()
    assert "stderr" in content
    assert "macOS" in content or "macos" in content.lower()
