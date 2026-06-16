"""Tests for omop_alchemy CLI — entry point smoke tests."""

from typer.testing import CliRunner

from omop_alchemy.maintenance.cli import app

runner = CliRunner()


def test_help_exits_cleanly():
    """--help exits 0 without raising."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0


def test_no_config_subcommand():
    """The old 'config' subcommand no longer exists; config is managed via omop-config."""
    result = runner.invoke(app, ["config", "--help"])
    assert result.exit_code != 0
