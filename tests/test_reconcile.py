import sqlalchemy as sa
from typer.testing import CliRunner

from omop_alchemy.maintenance.cli import app
from omop_alchemy.maintenance.create_tables import create_missing_tables
from omop_alchemy.maintenance.reconcile import (
    ReconciliationIssue,
    SchemaReconciliationReport,
    TableReconciliationResult,
    reconcile_schema,
)
from omop_alchemy.maintenance.tables import TableCategory

runner = CliRunner()


def _engine(tmp_path):
    return sa.create_engine(f"sqlite:///{tmp_path / 'reconcile.db'}", future=True)


def test_reconcile_schema_matches_clean_sqlite_schema(tmp_path):
    engine = _engine(tmp_path)
    create_missing_tables(engine)

    report = reconcile_schema(engine)

    assert report.backend == "sqlite"
    assert len(report.issues) == 0
    assert report.table_results
    assert all(result.status == "matched" for result in report.table_results)


def test_reconcile_schema_detects_drifted_table_components(tmp_path):
    engine = _engine(tmp_path)
    create_missing_tables(engine)

    with engine.begin() as connection:
        connection.exec_driver_sql("DROP INDEX idx_gender")
        connection.exec_driver_sql("ALTER TABLE person ADD COLUMN debug_only INTEGER")

    report = reconcile_schema(engine)

    person_result = next(result for result in report.table_results if result.table_name == "person")
    assert person_result.status == "drifted"

    issues = [issue for issue in report.issues if issue.table_name == "person"]
    assert any(issue.component == "index" and issue.object_name == "idx_gender" and issue.status == "missing" for issue in issues)
    assert any(issue.component == "column" and issue.object_name == "debug_only" and issue.status == "unexpected" for issue in issues)


def test_reconcile_schema_cli_renders_report(monkeypatch):
    def fake_load_environment(dotenv: str) -> None:
        return None

    def fake_get_engine_name(schema: str | None = None) -> str:
        return "sqlite:///:memory:"

    def fake_create_engine(url: str, *, future: bool) -> str:
        return "ENGINE"

    def fake_reconcile_schema(
        engine: object,
        *,
        db_schema: str | None = None,
        vocabulary_included: bool = False,
    ) -> SchemaReconciliationReport:
        return SchemaReconciliationReport(
            backend="sqlite",
            table_results=(
                TableReconciliationResult(
                    table_name="person",
                    category=TableCategory.CLINICAL,
                    model_name="Person",
                    model_module="omop_alchemy.cdm.model.clinical.person",
                    status="drifted",
                    issue_count=1,
                    detail="1 difference(s) detected.",
                ),
            ),
            issues=(
                ReconciliationIssue(
                    table_name="person",
                    category=TableCategory.CLINICAL,
                    component="index",
                    object_name="idx_gender",
                    status="missing",
                    expected="gender_concept_id",
                    actual=None,
                    detail="Index is defined in ORM metadata but missing from the database.",
                ),
            ),
        )

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
        "omop_alchemy.maintenance.cli.reconcile_schema",
        fake_reconcile_schema,
    )

    result = runner.invoke(app, ["reconcile-schema"])

    assert result.exit_code == 0
    assert "reconcile-schema" in result.stdout
    assert "person" in result.stdout
    assert "DRIFTED" in result.stdout
    assert "idx_gender" in result.stdout
