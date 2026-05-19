"""
Tests for omop_alchemy.config driver-selection logic.

These tests do not require a database; they exercise the driver-mapping
constants, _missing_driver_message(), and create_engine_with_dependencies()
using mock exceptions to simulate missing packages.
"""
import pytest

from omop_alchemy.config import (
    POSTGRES_DRIVER_MODULES,
    _missing_driver_message,
    create_engine_with_dependencies,
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
    """create_engine_with_dependencies should work for sqlite without wrapping errors."""
    engine = create_engine_with_dependencies("sqlite:///:memory:", future=True)
    engine.dispose()


def test_create_engine_raises_runtime_for_missing_postgres_driver(monkeypatch):
    """When psycopg is missing, create_engine_with_dependencies raises RuntimeError with install hint."""
    import sqlalchemy as sa

    def fake_create_engine(url, **kwargs):
        raise ModuleNotFoundError.__new__(
            ModuleNotFoundError,
        )

    exc = _make_module_not_found("psycopg")

    def raising_create_engine(url, **kwargs):
        raise exc

    monkeypatch.setattr(sa, "create_engine", raising_create_engine)

    with pytest.raises(RuntimeError, match="psycopg"):
        create_engine_with_dependencies("postgresql+psycopg://host/db")
