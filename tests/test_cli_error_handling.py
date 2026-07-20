"""Tests for missing-config error handling.

get_cdm_context() converts a missing oa-configurator stack file into a clear
RuntimeError at the source (config.py), rather than handle_error() pattern-
matching on the raw FileNotFoundError type. This keeps handle_error's
FileNotFoundError-shaped catch from also swallowing unrelated missing-file
errors raised elsewhere inside a command body (e.g. a missing pg_dump binary).
"""

import pytest
import typer

from omop_alchemy.config import get_cdm_context
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


def _raise_file_not_found():
    raise FileNotFoundError("Config file not found: /home/cava/.config/omop/config.toml")


def test_get_cdm_context_raises_runtime_error_when_config_missing(monkeypatch):
    monkeypatch.setattr("omop_alchemy.config.load_stack_config", _raise_file_not_found)
    with pytest.raises(RuntimeError, match="omop-config configure omop_alchemy"):
        get_cdm_context()


def test_get_cdm_context_runtime_error_does_not_leak_raw_path(monkeypatch):
    """The friendly message should not surface the raw exception text."""
    monkeypatch.setattr("omop_alchemy.config.load_stack_config", _raise_file_not_found)
    with pytest.raises(RuntimeError) as exc_info:
        get_cdm_context()
    assert "/home/cava/.config/omop/config.toml" not in str(exc_info.value)


def test_get_cdm_context_runtime_error_chains_original_exception(monkeypatch):
    monkeypatch.setattr("omop_alchemy.config.load_stack_config", _raise_file_not_found)
    with pytest.raises(RuntimeError) as exc_info:
        get_cdm_context()
    assert isinstance(exc_info.value.__cause__, FileNotFoundError)


def test_handle_error_runtime_error_exits_with_code_1_and_prints_hint(capsys):
    """The RuntimeError from get_cdm_context is handled cleanly, same as any other."""
    with pytest.raises(typer.Exit) as exc_info:
        handle_error(
            RuntimeError(
                "No omop-alchemy configuration found. "
                "Run `omop-config configure omop_alchemy` to set it up."
            )
        )
    assert exc_info.value.exit_code == 1
    captured = capsys.readouterr()
    assert "omop-config configure omop_alchemy" in captured.out


def test_handle_error_does_not_special_case_file_not_found():
    """A stray FileNotFoundError from elsewhere in a command body must not be
    mislabeled as a missing-configuration error — it should propagate as-is."""
    exc = FileNotFoundError("pg_dump: No such file or directory")
    with pytest.raises(FileNotFoundError) as exc_info:
        handle_error(exc)
    assert exc_info.value is exc
