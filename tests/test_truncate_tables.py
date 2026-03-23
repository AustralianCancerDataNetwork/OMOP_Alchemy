import importlib
import sqlalchemy as sa
import pytest
from typer.testing import CliRunner

from omop_alchemy.maintenance.cli import app
from omop_alchemy.maintenance.create_tables import create_missing_tables
from omop_alchemy.maintenance.tables import TableCategory, TableScope
from omop_alchemy.maintenance.truncate_tables import TruncateTableResult, truncate_tables

runner = CliRunner()
truncate_tables_module = importlib.import_module("omop_alchemy.maintenance.truncate_tables")


def test_truncate_tables_requires_postgresql(tmp_path):
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'truncate.db'}", future=True)

    with pytest.raises(RuntimeError) as exc_info:
        truncate_tables(engine, scope=TableScope.CLINICAL, dry_run=True)

    assert "only supported for PostgreSQL engines" in str(exc_info.value)


def test_truncate_tables_reports_blocking_foreign_key_references(monkeypatch, tmp_path):
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'truncate_fk.db'}", future=True)
    create_missing_tables(engine, vocabulary_included=True)

    monkeypatch.setattr(truncate_tables_module, "require_backend", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError) as exc_info:
        truncate_tables(engine, scope=TableScope.CLINICAL, dry_run=False)

    message = str(exc_info.value)
    assert "foreign key references from tables outside the current selection" in message
    assert "Blocking references:" in message
    assert "--cascade" in message


def test_truncate_tables_cli_requires_confirmation():
    result = runner.invoke(app, ["truncate-tables", "--scope", "clinical"])

    assert result.exit_code == 1
    assert "--yes" in result.stdout


def test_truncate_tables_cli_invokes_management(monkeypatch):
    calls: dict[str, object] = {}

    def fake_load_environment(dotenv: str) -> None:
        calls["dotenv"] = dotenv

    def fake_get_engine_name(schema: str | None = None) -> str:
        calls["engine_schema"] = schema
        return "postgresql+psycopg://example"

    def fake_create_engine(url: str, *, future: bool) -> str:
        calls["engine_url"] = url
        calls["future"] = future
        return "ENGINE"

    def fake_truncate_tables(
        engine: object,
        *,
        db_schema: str | None = None,
        scope: TableScope | None = None,
        table_names: tuple[str, ...] | None = None,
        restart_identities: bool = False,
        cascade: bool = False,
        dry_run: bool = False,
    ) -> list[TruncateTableResult]:
        calls["engine"] = engine
        calls["db_schema"] = db_schema
        calls["scope"] = scope
        calls["table_names"] = table_names
        calls["restart_identities"] = restart_identities
        calls["cascade"] = cascade
        calls["dry_run"] = dry_run
        return [
            TruncateTableResult(
                table_name="person",
                category=TableCategory.CLINICAL,
                model_name="Person",
                model_module="omop_alchemy.cdm.model.clinical.person",
                row_count=10,
                status="planned",
                detail="table would be truncated",
            )
        ]

    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli.load_environment",
        fake_load_environment,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli.get_engine_name",
        fake_get_engine_name,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli.create_engine_with_dependencies",
        fake_create_engine,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli.truncate_tables",
        fake_truncate_tables,
    )

    result = runner.invoke(
        app,
        [
            "truncate-tables",
            "--scope",
            "clinical",
            "--restart-identities",
            "--cascade",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert calls["scope"] == TableScope.CLINICAL
    assert calls["table_names"] is None
    assert calls["restart_identities"] is True
    assert calls["cascade"] is True
    assert calls["dry_run"] is True
    assert "truncate-tables" in result.stdout
