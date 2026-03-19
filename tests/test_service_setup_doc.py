"""
Tests for service-setup.md documentation changes.
Verifies all 4 required changes are present in the file.
"""

from pathlib import Path

DOC_PATH = Path(__file__).parent.parent / "docs" / "service-setup.md"


def get_doc_content() -> str:
    return DOC_PATH.read_text(encoding="utf-8")


def test_option_a_server_init_is_first_config_subsection():
    """Change 1: Option A with context-intelligence-server-init appears before Option B
    and before the manual config content in the Configuration section."""
    content = get_doc_content()

    # Option A heading must be present
    assert "### Option A" in content, "Option A heading not found"
    assert "context-intelligence-server-init" in content, (
        "server-init command not found"
    )

    # Option B heading must be present
    assert "### Option B" in content, "Option B heading not found"

    # Option A must appear before Option B
    pos_a = content.index("### Option A")
    pos_b = content.index("### Option B")
    assert pos_a < pos_b, "Option A must appear before Option B"

    # Option A must appear before the manual mkdir -p command
    pos_mkdir = content.index("mkdir -p ~/.config/context-intelligence")
    assert pos_a < pos_mkdir, "Option A must appear before manual mkdir instructions"

    # Option A must contain the server-init command with flags
    assert "--neo4j-url neo4j://localhost:7687" in content, (
        "server-init --neo4j-url flag not found"
    )
    assert "--neo4j-user neo4j" in content, "server-init --neo4j-user flag not found"

    # Option A must mention api_key generation
    assert "context_intelligence_api_key" in content, (
        "context_intelligence_api_key not mentioned in Option A"
    )


def test_option_a_heading_text():
    """Option A heading matches exact spec text."""
    content = get_doc_content()
    assert (
        "### Option A \u2014 Generate config with `context-intelligence-server-init` (recommended)"
        in content
    ), "Option A heading does not match exact spec text"


def test_option_b_heading_text():
    """Option B heading matches spec text (manual config)."""
    content = get_doc_content()
    assert "### Option B \u2014 Manual config (advanced)" in content, (
        "Option B heading does not match spec text"
    )


def test_server_settings_table_has_api_key_row():
    """Change 2: Server settings table must contain an api_key row."""
    content = get_doc_content()

    # Find the Server settings section
    assert "### Server settings" in content, "Server settings section not found"

    server_section_start = content.index("### Server settings")
    # Find end of server settings table (next ### heading)
    next_section = content.index("###", server_section_start + 1)
    server_section = content[server_section_start:next_section]

    assert "`api_key`" in server_section, (
        "api_key row not found in Server settings table"
    )
    assert "Bearer token" in server_section, (
        "Bearer token description not found in api_key row"
    )
    assert "Authorization: Bearer" in server_section, (
        "Authorization: Bearer not found in api_key row"
    )
    assert "context-intelligence-server-init" in server_section, (
        "context-intelligence-server-init not referenced in api_key row"
    )


def test_neo4j_password_description_updated():
    """Change 3: neo4j_password row must say 'Always required for Docker deployments'."""
    content = get_doc_content()

    # Find the line with neo4j_password
    lines = content.splitlines()
    neo4j_pwd_lines = [line for line in lines if "`neo4j_password`" in line]
    assert len(neo4j_pwd_lines) >= 1, "neo4j_password row not found"

    neo4j_pwd_line = neo4j_pwd_lines[0]
    assert "Always required for Docker deployments" in neo4j_pwd_line, (
        f"'Always required for Docker deployments' not found in neo4j_password row. Got: {neo4j_pwd_line}"
    )


def test_auth_troubleshooting_rows_present():
    """Change 4: Two new auth troubleshooting rows added to Common issues table."""
    content = get_doc_content()

    # Check first new row about circuit breaker
    assert "Events stop dispatching" in content, (
        "Circuit breaker troubleshooting row not found"
    )
    assert "circuit breaker tripped" in content, (
        "Circuit breaker text not found in troubleshooting row"
    )
    assert "context_intelligence_api_key" in content, (
        "context_intelligence_api_key not found in troubleshooting row"
    )

    # Check second new row about dashboard API key prompt
    assert 'Dashboard shows "Enter your API key"' in content, (
        "Dashboard API key prompt troubleshooting row not found"
    )
    assert "won't load" in content or "won\u2019t load" in content, (
        "won't load text not found in dashboard troubleshooting row"
    )
    assert "server-config.yaml" in content and "api_key:" in content, (
        "server-config.yaml / api_key: reference not found in dashboard troubleshooting row"
    )


def test_auth_troubleshooting_rows_at_end_of_table():
    """The two new auth rows appear at the end of the Common issues table."""
    content = get_doc_content()

    pos_circuit = content.find("Events stop dispatching")
    pos_dashboard = content.find('Dashboard shows "Enter your API key"')

    assert pos_circuit != -1, "Circuit breaker row not found"
    assert pos_dashboard != -1, "Dashboard API key row not found"

    # Both rows must appear after "macOS: plist loaded" (the last original row)
    pos_last_original = content.find("macOS: plist loaded but service not running")
    assert pos_last_original != -1, "Original last troubleshooting row not found"

    assert pos_circuit > pos_last_original, (
        "Circuit breaker row must appear after the last original troubleshooting row"
    )
    assert pos_dashboard > pos_last_original, (
        "Dashboard row must appear after the last original troubleshooting row"
    )


# ===== Tests for README documentation updates =====


def get_server_readme_content() -> str:
    """Load the server README.md file."""
    readme_path = Path(__file__).parent.parent / "README.md"
    return readme_path.read_text(encoding="utf-8")


def get_bundle_readme_content() -> str:
    """Load the bundle README.md file."""
    readme_path = (
        Path(__file__).parent.parent.parent
        / "amplifier-bundle-context-intelligence"
        / "README.md"
    )
    return readme_path.read_text(encoding="utf-8")


def test_server_readme_mentions_start_sh():
    """Server README.md must mention start.sh for first run."""
    content = get_server_readme_content()
    assert "./start.sh" in content or "start.sh" in content, (
        "start.sh is not mentioned in server README.md"
    )


def test_server_readme_docker_section_uses_start_sh_first():
    """In server README, Docker Quick Start must reference start.sh before docker compose up -d."""
    content = get_server_readme_content()

    # Find the Docker Compose section
    docker_section_start = content.find("## Running with Docker Compose")
    assert docker_section_start != -1, (
        "Docker Compose section not found in server README"
    )

    # Find the next major section (##) after Docker Compose
    next_section = content.find("\n## ", docker_section_start + 1)
    if next_section == -1:
        docker_section = content[docker_section_start:]
    else:
        docker_section = content[docker_section_start:next_section]

    # Within the Docker section, start.sh should appear before docker compose up -d
    pos_start_sh = docker_section.find("./start.sh")
    pos_docker_up = docker_section.find("docker compose up -d")

    assert pos_start_sh != -1, (
        "./start.sh not found in Docker Quick Start section of server README"
    )
    assert pos_docker_up != -1, (
        "docker compose up -d not found in Docker Quick Start section of server README"
    )
    assert pos_start_sh < pos_docker_up, (
        "./start.sh must appear before docker compose up -d in the Docker Quick Start section"
    )


def test_server_readme_no_init_service_reference():
    """Server README.md must not reference 'init' service or '3 services' anywhere in the file."""
    content = get_server_readme_content()

    # Check the service table - should show 2 services, not 3
    assert "| **Ingestion server**" in content, (
        "Ingestion server not found in services table"
    )
    assert "| **Neo4j**" in content, "Neo4j not found in services table"

    # Get the services section
    docker_section_start = content.find("## Running with Docker Compose")
    assert docker_section_start != -1, "Docker Compose section not found"

    next_section = content.find("\n## ", docker_section_start + 1)
    if next_section == -1:
        docker_section = content[docker_section_start:]
    else:
        docker_section = content[docker_section_start:next_section]

    # The init service should not be mentioned
    assert "| **init**" not in docker_section, (
        "Init service row should not be in services table"
    )
    assert "3 services" not in docker_section, (
        "Docker section should not mention '3 services'"
    )

    # Check full file content - no stale references to "3-service stack" or "init + server + neo4j"
    assert "3-service stack" not in content, (
        "Full README should not contain '3-service stack' reference"
    )
    assert "init + server + neo4j" not in content, (
        "Full README should not contain 'init + server + neo4j' reference"
    )


def test_bundle_readme_mentions_start_sh():
    """Bundle README.md must mention start.sh when starting the server."""
    content = get_bundle_readme_content()
    assert "./start.sh" in content or "start.sh" in content, (
        "start.sh is not mentioned in bundle README.md"
    )


def test_bundle_readme_quick_start_uses_start_sh():
    """Bundle README Quick Start must use ./start.sh instead of docker compose up -d for first run."""
    content = get_bundle_readme_content()

    # Find the Quick Start section
    quick_start_pos = content.find("## Quick Start")
    assert quick_start_pos != -1, "Quick Start section not found in bundle README"

    # Find the server startup subsection
    server_startup_pos = content.find("**Start the server:**", quick_start_pos)
    assert server_startup_pos != -1, "Server startup instructions not found"

    # Find the next major section after Quick Start
    next_major_section = content.find("\n## ", quick_start_pos + 1)
    if next_major_section == -1:
        quick_start_section = content[quick_start_pos:]
    else:
        quick_start_section = content[quick_start_pos:next_major_section]

    # Within Quick Start, ./start.sh should appear before docker compose up -d (if both mentioned)
    pos_start_sh = quick_start_section.find("./start.sh")
    pos_docker_up = quick_start_section.find("docker compose up -d")

    # ./start.sh must be present unconditionally
    assert pos_start_sh != -1, (
        "./start.sh not found in bundle README Quick Start section"
    )
    # If docker compose up -d is mentioned, start.sh should come first
    if pos_docker_up != -1:
        assert pos_start_sh < pos_docker_up, (
            "./start.sh must appear before docker compose up -d in bundle README Quick Start"
        )


def test_bundle_readme_no_init_service_reference():
    """Bundle README.md must not reference the 'init' service."""
    content = get_bundle_readme_content()

    # Find the Quick Start or Docker-related sections
    quick_start_pos = content.find("## Quick Start")
    assert quick_start_pos != -1, "Quick Start section not found"

    next_major_section = content.find("\n## ", quick_start_pos + 1)
    if next_major_section == -1:
        relevant_section = content[quick_start_pos:]
    else:
        relevant_section = content[quick_start_pos:next_major_section]

    # Should not mention 'init service' or '| **init**' explicitly
    assert (
        "| **init**" not in relevant_section
        and "init service" not in relevant_section.lower()
    ), "Bundle README should not mention 'init' service in Quick Start"
