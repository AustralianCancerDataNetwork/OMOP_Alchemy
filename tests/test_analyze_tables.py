import sqlalchemy as sa
import pytest
from typer.testing import CliRunner

from omop_alchemy.maintenance.analyze_tables import AnalyzeTableResult, analyze_tables
from omop_alchemy.maintenance.cli import app
from omop_alchemy.maintenance.create_tables import create_missing_tables
from omop_alchemy.maintenance.tables import TableCategory, TableScope

runner = CliRunner()


def _engine(tmp_path):
    """Create an isolated SQLite engine for analyze-table tests."""
    return sa.create_engine(f"sqlite:///{tmp_path / 'analyze.db'}", future=True)


def test_analyze_tables_runs_on_sqlite(tmp_path):
    """Analyze applies successfully on SQLite for selected OMOP tables."""
    engine = _engine(tmp_path)
    create_missing_tables(engine, vocabulary_included=True)

    results = analyze_tables(
        engine,
        scope=TableScope.CLINICAL,
        dry_run=False,
    )

    assert any(
        result.table_name == "person" and result.status == "applied"
        for result in results
    )


def test_analyze_tables_rejects_vacuum_on_sqlite(tmp_path):
    """VACUUM ANALYZE is rejected on SQLite with a clear runtime error."""
    engine = _engine(tmp_path)
    create_missing_tables(engine, vocabulary_included=True)

    with pytest.raises(RuntimeError) as exc_info:
        analyze_tables(engine, scope=TableScope.CLINICAL, vacuum=True)

    assert "VACUUM ANALYZE is only supported for PostgreSQL" in str(exc_info.value)


def test_analyze_tables_cli_invokes_management(monkeypatch):
    """CLI forwards selection and dry-run flags to analyze_tables."""
    calls: dict[str, object] = {}

    def fake_load_environment(dotenv: str) -> None:
        calls["dotenv"] = dotenv

    def fake_get_engine_name(schema: str | None = None) -> str:
        calls["engine_schema"] = schema
        return "sqlite:///tmp/test.db"

    def fake_create_engine(url: str, *, future: bool) -> str:
        calls["engine_url"] = url
        calls["future"] = future
        return "ENGINE"

    def fake_analyze_tables(
        engine: object,
        *,
        db_schema: str | None = None,
        scope: TableScope | None = None,
        table_names: tuple[str, ...] | None = None,
        vacuum: bool = False,
        dry_run: bool = False,
    ) -> list[AnalyzeTableResult]:
        calls["engine"] = engine
        calls["db_schema"] = db_schema
        calls["scope"] = scope
        calls["table_names"] = table_names
        calls["vacuum"] = vacuum
        calls["dry_run"] = dry_run
        return [
            AnalyzeTableResult(
                table_name="person",
                category=TableCategory.CLINICAL,
                model_name="Person",
                model_module="omop_alchemy.cdm.model.clinical.person",
                operation="ANALYZE",
                status="planned",
                detail="analyze would run",
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
        "omop_alchemy.maintenance.cli.analyze_tables",
        fake_analyze_tables,
    )

    result = runner.invoke(
        app,
        ["analyze-tables", "--scope", "clinical", "--dry-run"],
    )

    assert result.exit_code == 0
    assert calls["scope"] == TableScope.CLINICAL
    assert calls["table_names"] is None
    assert calls["dry_run"] is True
    assert "analyze-tables" in result.stdout
