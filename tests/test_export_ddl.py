from pathlib import Path

from typer.testing import CliRunner

from omop_alchemy.maintenance.cli import app
from omop_alchemy.maintenance.export_ddl import DDLDialect, export_ddl


runner = CliRunner()


def test_export_ddl_writes_postgresql_script(tmp_path):
    """DDL export writes a portable PostgreSQL schema artifact from ORM metadata."""
    output_path = tmp_path / "omop_cdm_postgresql.sql"

    result = export_ddl(output_path=output_path)

    ddl = output_path.read_text(encoding="utf-8")
    assert result.status == "created"
    assert result.dialect is DDLDialect.POSTGRESQL
    assert result.table_count > 0
    assert result.index_count > 0
    assert "CREATE TABLE IF NOT EXISTS person" in ddl
    assert "CREATE TABLE IF NOT EXISTS concept" in ddl
    assert "CREATE INDEX IF NOT EXISTS" in ddl


def test_export_ddl_rejects_sqlite_schema_qualification(tmp_path):
    """SQLite export fails fast when asked to render schema-qualified SQL."""
    output_path = tmp_path / "omop_cdm_sqlite.sql"

    try:
        export_ddl(
            output_path=output_path,
            dialect=DDLDialect.SQLITE,
            db_schema="cdm",
        )
    except RuntimeError as exc:
        assert "not supported for SQLite" in str(exc)
    else:
        raise AssertionError("Expected schema-qualified SQLite export to fail.")


def test_export_ddl_cli_writes_requested_file(tmp_path):
    """The CLI command emits a distributable SQL file without connecting to a database."""
    output_path = tmp_path / "shared_schema.sql"

    result = runner.invoke(
        app,
        [
            "export-ddl",
            "--output-path",
            str(output_path),
            "--dialect",
            "sqlite",
            "--no-indexes",
        ],
    )

    assert result.exit_code == 0
    assert output_path.exists()
    ddl = output_path.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS person" in ddl
    assert "CREATE INDEX IF NOT EXISTS" not in ddl
