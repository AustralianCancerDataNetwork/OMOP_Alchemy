"""
Tests for driver-selection logic in omop_alchemy.config.

These tests do not require a database; they exercise the driver-mapping
constants, _missing_driver_message(), and create_cdm_engine()
using mock exceptions to simulate missing packages.
"""
import pytest

from omop_alchemy.config import (
    _POSTGRES_DRIVER_MODULES as POSTGRES_DRIVER_MODULES,
    _missing_driver_message,
    create_cdm_engine,
)


def _make_module_not_found(module_name: str) -> ModuleNotFoundError:
    exc = ModuleNotFoundError(f"No module named '{module_name}'")
    exc.name = module_name
    return exc


# ---------------------------------------------------------------------------
# Driver-mapping constants
# ---------------------------------------------------------------------------

def test_bare_postgresql_url_aliases_to_psycopg():
    """Bare postgresql:// now resolves to psycopg, not psycopg2."""
    assert POSTGRES_DRIVER_MODULES["postgresql"] == "psycopg"


def test_psycopg_driver_mapping():
    assert POSTGRES_DRIVER_MODULES["postgresql+psycopg"] == "psycopg"


def test_psycopg2_driver_mapping_retained_for_error_quality():
    """psycopg2 entry is kept so users get a clear error message."""
    assert POSTGRES_DRIVER_MODULES["postgresql+psycopg2"] == "psycopg2"


# ---------------------------------------------------------------------------
# _missing_driver_message()
# ---------------------------------------------------------------------------

def test_missing_driver_message_for_psycopg():
    exc = _make_module_not_found("psycopg")
    msg = _missing_driver_message("postgresql+psycopg://host/db", exc)

    assert msg is not None
    assert "psycopg" in msg
    assert "postgres" in msg.lower()


def test_missing_driver_message_for_bare_postgresql_url():
    """Bare postgresql:// is now aliased to psycopg; missing psycopg gives a helpful error."""
    exc = _make_module_not_found("psycopg")
    msg = _missing_driver_message("postgresql://host/db", exc)

    assert msg is not None
    assert "psycopg" in msg


def test_missing_driver_message_for_psycopg2():
    exc = _make_module_not_found("psycopg2")
    msg = _missing_driver_message("postgresql+psycopg2://host/db", exc)

    assert msg is not None
    assert "psycopg2" in msg


def test_missing_driver_message_returns_none_for_unrelated_module():
    """A ModuleNotFoundError for an unrelated package should not be intercepted."""
    exc = _make_module_not_found("pandas")
    msg = _missing_driver_message("postgresql+psycopg://host/db", exc)

    assert msg is None


def test_missing_driver_message_returns_none_for_sqlite_url():
    exc = _make_module_not_found("psycopg")
    msg = _missing_driver_message("sqlite:///test.db", exc)

    assert msg is None


# ---------------------------------------------------------------------------
# create_engine_with_dependencies()
# ---------------------------------------------------------------------------

def test_sqlite_url_not_intercepted():
    """create_cdm_engine should work for sqlite without wrapping errors."""
    from oa_configurator.resolver import ResolvedDatabaseTarget
    target = ResolvedDatabaseTarget(name="test", url="sqlite:///:memory:", safe_url="sqlite:///:memory:")
    from unittest.mock import MagicMock
    resolved = MagicMock()
    resolved.create_engine.return_value = target.create_engine()
    resolved.database.url = "sqlite:///:memory:"
    engine = create_cdm_engine(resolved)
    engine.dispose()


def test_create_engine_raises_runtime_for_missing_postgres_driver(monkeypatch):
    """When psycopg is missing, create_cdm_engine raises RuntimeError with install hint."""
    from unittest.mock import MagicMock
    exc = _make_module_not_found("psycopg")

    resolved = MagicMock()
    resolved.create_engine.side_effect = exc
    resolved.database.url = "postgresql+psycopg://host/db"

    with pytest.raises(RuntimeError, match="psycopg"):
        create_cdm_engine(resolved)
