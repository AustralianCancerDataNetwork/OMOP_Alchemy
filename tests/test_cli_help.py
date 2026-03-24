from typer.testing import CliRunner

from omop_alchemy.maintenance.cli import app


runner = CliRunner()


def _help_command_order(stdout: str) -> list[str]:
    commands: list[str] = []
    in_commands_section = False

    for line in stdout.splitlines():
        if "Commands" in line:
            in_commands_section = True
            continue
        if not in_commands_section:
            continue
        if line.startswith("╰"):
            break
        if not line.startswith("│ "):
            continue

        command_name = line[2:24].strip()
        if command_name:
            commands.append(command_name)

    return commands


def test_top_level_help_annotates_postgresql_only_commands():
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "PostgreSQL-only commands" in result.stdout
    assert "reset-sequences" in result.stdout
    assert "foreign-keys" in result.stdout


def test_top_level_help_lists_portable_commands_before_postgresql_only_commands():
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    commands = _help_command_order(result.stdout)

    assert commands.index("data-summary") < commands.index("backup-database")
    assert commands.index("create-missing-tables") < commands.index("reset-sequences")


def test_command_help_marks_postgresql_only_support():
    result = runner.invoke(app, ["reset-sequences", "--help"])

    assert result.exit_code == 0
    assert "PostgreSQL only" in result.stdout
