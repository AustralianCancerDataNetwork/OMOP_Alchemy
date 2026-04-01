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

