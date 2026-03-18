"""Tests for the macOS launchd plist template.

Validates that the template file:
- Exists and is non-empty
- Contains exactly 6 HOME_DIR placeholder tokens
- Produces valid XML after sed-style substitution
- Contains no remaining HOME_DIR tokens after substitution
- Contains the required launchd keys per specification
"""

import xml.dom.minidom
from pathlib import Path

# tests/ -> repo root (submodule)
REPO_ROOT = Path(__file__).parent.parent
TEMPLATE_PATH = (
    REPO_ROOT / "service" / "macos" / "com.context-intelligence.server.plist.template"
)
FAKE_HOME = "/home/testuser"

# Enough chars to reach <true/> after the key tag and surrounding whitespace
_PLIST_KEY_LOOKAHEAD = 100


def _substituted() -> str:
    """Return template content with HOME_DIR replaced by FAKE_HOME."""
    return TEMPLATE_PATH.read_text().replace("HOME_DIR", FAKE_HOME)


# ---------------------------------------------------------------------------
# Existence checks
# ---------------------------------------------------------------------------


def test_template_file_exists():
    """Template file must exist at the expected path."""
    assert TEMPLATE_PATH.exists(), f"Template file not found: {TEMPLATE_PATH}"


def test_template_file_is_nonempty():
    """Template file must not be empty."""
    assert TEMPLATE_PATH.stat().st_size > 0, "Template file is empty"


# ---------------------------------------------------------------------------
# HOME_DIR placeholder count
# ---------------------------------------------------------------------------


def test_template_has_exactly_six_home_dir_occurrences():
    """Exactly 6 HOME_DIR tokens must appear (spec requirement)."""
    content = TEMPLATE_PATH.read_text()
    count = content.count("HOME_DIR")
    assert count == 6, f"Expected 6 HOME_DIR occurrences, got {count}"


def test_template_does_not_contain_tilde_or_dollar_home():
    """launchd does not expand ~ or $HOME; only HOME_DIR placeholder is allowed."""
    content = TEMPLATE_PATH.read_text()
    assert "~/" not in content, "Template must not use ~ (launchd won't expand it)"
    assert "$HOME" not in content, (
        "Template must not use $HOME (launchd won't expand it)"
    )


# ---------------------------------------------------------------------------
# Substitution correctness
# ---------------------------------------------------------------------------


def test_sed_substitution_removes_all_home_dir_tokens():
    """After substitution, no HOME_DIR tokens should remain."""
    result = _substituted()
    assert "HOME_DIR" not in result, "HOME_DIR tokens remain after substitution"


def test_substituted_content_is_valid_xml():
    """After substitution, the content must be well-formed XML."""
    result = _substituted()
    # xml.dom.minidom.parseString raises if XML is not well-formed
    xml.dom.minidom.parseString(result.encode("utf-8"))


# ---------------------------------------------------------------------------
# Required plist content checks (post-substitution)
# ---------------------------------------------------------------------------


def test_plist_label():
    """Label must be com.context-intelligence.server."""
    result = _substituted()
    assert "com.context-intelligence.server" in result


def test_plist_program_arguments():
    """ProgramArguments must reference the server binary under FAKE_HOME."""
    result = _substituted()
    assert f"{FAKE_HOME}/.local/bin/context-intelligence-server" in result


def test_plist_config_env_variable():
    """EnvironmentVariables must include the config file path."""
    result = _substituted()
    assert "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE" in result
    assert f"{FAKE_HOME}/.config/context-intelligence/server-config.yaml" in result


def test_plist_run_at_load():
    """RunAtLoad must be set to true."""
    result = _substituted()
    assert "<key>RunAtLoad</key>" in result
    # In plist XML <true/> follows RunAtLoad key
    idx = result.index("<key>RunAtLoad</key>")
    assert "<true/>" in result[idx : idx + _PLIST_KEY_LOOKAHEAD]


def test_plist_keep_alive():
    """KeepAlive must be set to true."""
    result = _substituted()
    assert "<key>KeepAlive</key>" in result
    idx = result.index("<key>KeepAlive</key>")
    assert "<true/>" in result[idx : idx + _PLIST_KEY_LOOKAHEAD]


def test_plist_standard_out_path():
    """StandardOutPath must point to logs/server.stdout.log."""
    result = _substituted()
    assert (
        f"{FAKE_HOME}/.local/share/context-intelligence/logs/server.stdout.log"
        in result
    )


def test_plist_standard_error_path():
    """StandardErrorPath must point to logs/server.stderr.log."""
    result = _substituted()
    assert (
        f"{FAKE_HOME}/.local/share/context-intelligence/logs/server.stderr.log"
        in result
    )


def test_plist_has_xml_declaration():
    """File must start with XML declaration (version 1.0)."""
    content = TEMPLATE_PATH.read_text()
    assert content.startswith('<?xml version="1.0"'), "Missing XML declaration"


def test_plist_has_doctype():
    """File must include the Apple plist DOCTYPE declaration."""
    content = TEMPLATE_PATH.read_text()
    assert "DOCTYPE plist" in content
