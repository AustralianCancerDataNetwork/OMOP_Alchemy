import sqlalchemy as sa
from typer.testing import CliRunner

from omop_alchemy.maintenance.cli import app
from omop_alchemy.maintenance.create_tables import create_missing_tables
from omop_alchemy.maintenance.info import (
    CommandSupport,
    DependencyStatus,
    MaintenanceInfo,
    collect_maintenance_info,
)

runner = CliRunner()


def test_collect_maintenance_info_without_engine_configuration(monkeypatch):
    def fake_get_engine_name(schema: str | None = None) -> str:
        raise RuntimeError("No database engine configured for schema 'cdm'")

    monkeypatch.setattr(
        "omop_alchemy.maintenance.info.get_engine_name",
        fake_get_engine_name,
    )

    info = collect_maintenance_info(engine_schema="cdm")

    assert info.engine_created is False
    assert info.connection_ready is False
    assert "No database engine configured" in (info.engine_error or "")

    commands = {item.command_name: item for item in info.command_support}
    assert commands["doctor"].status == "blocked"
    assert commands["data-summary"].status == "blocked"
    assert commands["analyze-tables"].status == "blocked"
    assert commands["reset-sequences"].status == "blocked"
    assert commands["fulltext install"].status == "blocked"
    assert commands["truncate-tables"].status == "blocked"
    assert commands["foreign-keys validate"].status == "blocked"
    assert commands["load-vocab-source"].status == "blocked"


def test_collect_maintenance_info_reports_sqlite_capabilities(monkeypatch, tmp_path):
    database_path = tmp_path / "info.db"
    engine = sa.create_engine(f"sqlite:///{database_path}", future=True)
    create_missing_tables(engine)

    def fake_get_engine_name(schema: str | None = None) -> str:
        return f"sqlite:///{database_path}"

    def fake_create_engine(url: str, *, future: bool) -> sa.Engine:
        return sa.create_engine(url, future=future)

    monkeypatch.setattr(
        "omop_alchemy.maintenance.info.get_engine_name",
        fake_get_engine_name,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.info.create_engine_with_dependencies",
        fake_create_engine,
    )

    info = collect_maintenance_info(vocabulary_included=False)

    assert info.backend == "sqlite"
    assert info.engine_created is True
    assert info.connection_ready is True
    assert info.existing_table_count == info.managed_table_count
    assert info.missing_table_count == 0

    commands = {item.command_name: item for item in info.command_support}
    assert commands["doctor"].status == "ready"
    assert commands["data-summary"].status == "ready"
    assert commands["analyze-tables"].status == "limited"
    assert commands["indexes enable"].status == "limited"
    assert commands["fulltext install"].status == "unsupported"
    assert commands["reset-sequences"].status == "unsupported"
    assert commands["truncate-tables"].status == "unsupported"
    assert commands["foreign-keys validate"].status == "unsupported"
    assert commands["load-vocab-source"].status == "ready"


def test_collect_maintenance_info_loads_dotenv(monkeypatch):
    calls: dict[str, object] = {}

    def fake_load_environment(dotenv: str) -> None:
        calls["dotenv"] = dotenv

    def fake_get_engine_name(schema: str | None = None) -> str:
        raise RuntimeError("No database engine configured")

    monkeypatch.setattr(
        "omop_alchemy.maintenance.info.load_environment",
        fake_load_environment,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.info.get_engine_name",
        fake_get_engine_name,
    )

    collect_maintenance_info(dotenv=".env", engine_schema="cdm")

    assert calls["dotenv"] == ".env"


def test_info_cli_renders_summary(monkeypatch):
    def fake_load_environment(dotenv: str) -> None:
        return None

    def fake_collect_info(
        *,
        engine_schema: str | None = None,
        db_schema: str | None = None,
        dotenv: str | None = None,
        vocabulary_included: bool = True,
    ) -> MaintenanceInfo:
        return MaintenanceInfo(
            package_version="0.5.12",
            cli_path="/tmp/omop-maint",
            pg_dump_path="/usr/bin/pg_dump",
            pg_restore_path="/usr/bin/pg_restore",
            psql_path="/usr/bin/psql",
            defaults_file="/tmp/.omop-maint.toml",
            defaults_exists=True,
            dotenv_path=".env",
            dotenv_exists=True,
            engine_schema=engine_schema,
            db_schema=db_schema,
            engine_url="sqlite:///tmp/test.db",
            backend="sqlite",
            engine_created=True,
            engine_error=None,
            connection_ready=True,
            connection_error=None,
            managed_table_count=10,
            existing_table_count=8,
            missing_table_count=2,
            vocabulary_included=vocabulary_included,
            dependencies=(
                DependencyStatus("sqlalchemy", True, "2.0"),
                DependencyStatus("psycopg2-binary", False, None),
            ),
            command_support=(
                CommandSupport(
                    "data-summary",
                    "Any SQLAlchemy backend",
                    "ready",
                    "Ready on SQLite.",
                ),
                CommandSupport(
                    "reset-sequences",
                    "PostgreSQL",
                    "unsupported",
                    "Requires PostgreSQL. Current backend: SQLite.",
                ),
            ),
        )

    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli.load_environment",
        fake_load_environment,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli.collect_maintenance_info",
        fake_collect_info,
    )

    result = runner.invoke(app, ["info", "--no-vocabulary-included"])

    assert result.exit_code == 0
    assert "info" in result.stdout
    assert "Environment" in result.stdout
    assert "Database" in result.stdout
    assert "data-summary" in result.stdout
    assert "reset-sequences" in result.stdout
    assert "UNSUPPORTED" in result.stdout
