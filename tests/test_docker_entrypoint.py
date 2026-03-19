"""Tests for the Docker entrypoint script.

TDD phase: These tests fail BEFORE docker-entrypoint.sh is created.
"""

import os
import pathlib
import subprocess

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
ENTRYPOINT = PROJECT_ROOT / "docker-entrypoint.sh"


def test_entrypoint_syntax_is_valid_bash() -> None:
    """bash -n must return 0 (no syntax errors)."""
    result = subprocess.run(
        ["bash", "-n", str(ENTRYPOINT)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"bash -n reported syntax errors:\n{result.stderr}"


def test_entrypoint_is_executable() -> None:
    """docker-entrypoint.sh must have the executable bit set."""
    assert ENTRYPOINT.exists(), "docker-entrypoint.sh must exist"
    assert os.access(ENTRYPOINT, os.X_OK), (
        "docker-entrypoint.sh must be executable (chmod +x)"
    )


def test_entrypoint_starts_with_bash_shebang() -> None:
    """Script must start with #!/bin/bash."""
    content = ENTRYPOINT.read_text()
    first_line = content.splitlines()[0]
    assert first_line == "#!/bin/bash", (
        f"First line must be '#!/bin/bash', got: {first_line!r}"
    )


def test_entrypoint_has_strict_mode() -> None:
    """Script must contain 'set -euo pipefail'."""
    content = ENTRYPOINT.read_text()
    assert "set -euo pipefail" in content, "Script must contain 'set -euo pipefail'"
