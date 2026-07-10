"""Tests for handle_error's FileNotFoundError branch (missing oa-configurator config)."""

import pytest
import typer

from omop_alchemy.maintenance._cli_utils import handle_error
from omop_alchemy.maintenance.ui import console


@pytest.fixture(autouse=True)
def _wide_console():
    """Rich wraps/truncates Panel text at the console's default 80-column
    width, which is narrower than this message under pytest's captured,
    non-tty output. Widen it so assertions can match the full text."""
    original_width = console.width
    console.width = 200
    yield
    console.width = original_width


def test_file_not_found_exits_with_code_1():
    with pytest.raises(typer.Exit) as exc_info:
        handle_error(FileNotFoundError("Config file not found: /home/cava/.config/omop/config.toml"))
    assert exc_info.value.exit_code == 1


def test_file_not_found_prints_configure_hint(capsys):
    with pytest.raises(typer.Exit):
        handle_error(FileNotFoundError("Config file not found: /home/cava/.config/omop/config.toml"))
    captured = capsys.readouterr()
    assert "omop-config configure omop_alchemy" in captured.out


def test_file_not_found_does_not_leak_raw_traceback_path(capsys):
    """The friendly message should not surface the raw exception text."""
    with pytest.raises(typer.Exit):
        handle_error(FileNotFoundError("Config file not found: /home/cava/.config/omop/config.toml"))
    captured = capsys.readouterr()
    assert "/home/cava/.config/omop/config.toml" not in captured.out
