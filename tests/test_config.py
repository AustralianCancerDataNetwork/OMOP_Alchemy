import pytest

from omop_alchemy.config import create_engine_with_dependencies


def test_create_engine_with_dependencies_wraps_missing_postgres_driver(monkeypatch):
    def fake_create_engine(*args, **kwargs):
        raise ModuleNotFoundError("No module named 'psycopg2'")

    monkeypatch.setattr("omop_alchemy.config.sa.create_engine", fake_create_engine)

    with pytest.raises(RuntimeError) as exc_info:
        create_engine_with_dependencies("postgresql+psycopg2://example")

    assert "uv sync --extra postgres" in str(exc_info.value)


def test_create_engine_with_dependencies_reraises_other_missing_modules(monkeypatch):
    def fake_create_engine(*args, **kwargs):
        raise ModuleNotFoundError("No module named 'something_else'")

    monkeypatch.setattr("omop_alchemy.config.sa.create_engine", fake_create_engine)

    with pytest.raises(ModuleNotFoundError):
        create_engine_with_dependencies("sqlite:///example.db")
