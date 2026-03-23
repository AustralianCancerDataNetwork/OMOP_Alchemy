from typer.testing import CliRunner
import pytest

from omop_alchemy.maintenance.cli import app
from omop_alchemy.maintenance.doctor import (
    DoctorCheck,
    DoctorRecommendation,
    DoctorReport,
    collect_doctor_report,
)
from omop_alchemy.maintenance.info import (
    CommandSupport,
    DependencyStatus,
    MaintenanceInfo,
)

runner = CliRunner()


def test_doctor_cli_renders_summary(monkeypatch):
    def fake_load_environment(dotenv: str) -> None:
        return None

    def fake_collect_doctor_report(
        *,
        engine_schema: str | None = None,
        db_schema: str | None = None,
        dotenv: str | None = None,
        vocabulary_included: bool = True,
        deep: bool = False,
    ) -> DoctorReport:
        return DoctorReport(
            info=MaintenanceInfo(
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
                ),
                command_support=(
                    CommandSupport(
                        "doctor",
                        "Any SQLAlchemy backend",
                        "ready",
                        "Ready on SQLite.",
                    ),
                ),
            ),
            checks=(
                DoctorCheck("connection", "passed", "Target database connection succeeded."),
                DoctorCheck("schema drift", "warning", "2 difference(s) detected."),
            ),
            recommendations=(
                DoctorRecommendation(
                    "warning",
                    "2 ORM-managed table(s) are missing from the target database.",
                    "Run `omop-maint create-missing-tables` before attempting bulk operations.",
                ),
            ),
            reconciliation=None,
            foreign_key_status=None,
            foreign_key_validation=None,
        )

    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli.load_environment",
        fake_load_environment,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli.collect_doctor_report",
        fake_collect_doctor_report,
    )

    result = runner.invoke(app, ["doctor", "--deep", "--no-vocabulary-included"])

    assert result.exit_code == 0
    assert "doctor" in result.stdout
    assert "Doctor Checks" in result.stdout
    assert "Recommended Next Steps" in result.stdout
    assert "Warnings" in result.stdout


def test_collect_doctor_report_loads_dotenv_and_skips_reconcile_without_deep(monkeypatch):
    calls: dict[str, object] = {}

    class _FakeEngine:
        def dispose(self) -> None:
            calls["disposed"] = True

    def fake_load_environment(dotenv: str) -> None:
        calls["dotenv"] = dotenv

    def fake_collect_maintenance_info(**kwargs) -> MaintenanceInfo:
        return MaintenanceInfo(
            package_version="0.5.12",
            cli_path="/tmp/omop-maint",
            pg_dump_path=None,
            pg_restore_path=None,
            psql_path=None,
            defaults_file="/tmp/.omop-maint.toml",
            defaults_exists=True,
            dotenv_path=kwargs.get("dotenv"),
            dotenv_exists=True,
            engine_schema=kwargs.get("engine_schema"),
            db_schema=kwargs.get("db_schema"),
            engine_url="sqlite:///tmp/test.db",
            backend="sqlite",
            engine_created=True,
            engine_error=None,
            connection_ready=True,
            connection_error=None,
            managed_table_count=10,
            existing_table_count=10,
            missing_table_count=0,
            vocabulary_included=bool(kwargs.get("vocabulary_included", True)),
            dependencies=(),
            command_support=(),
        )

    def fake_get_engine_name(schema: str | None = None) -> str:
        calls["engine_schema"] = schema
        return "sqlite:///tmp/test.db"

    def fake_create_engine(url: str, *, future: bool):
        calls["engine_url"] = url
        return _FakeEngine()

    monkeypatch.setattr(
        "omop_alchemy.maintenance.doctor.load_environment",
        fake_load_environment,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.doctor.collect_maintenance_info",
        fake_collect_maintenance_info,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.doctor.get_engine_name",
        fake_get_engine_name,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.doctor.create_engine_with_dependencies",
        fake_create_engine,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.doctor.reconcile_schema",
        lambda *args, **kwargs: pytest.fail("reconcile_schema should not run without --deep"),
    )

    report = collect_doctor_report(dotenv=".env", engine_schema="cdm", deep=False)

    assert calls["dotenv"] == ".env"
    assert calls["disposed"] is True
    assert any(
        check.name == "schema drift" and check.status == "skipped"
        for check in report.checks
    )
