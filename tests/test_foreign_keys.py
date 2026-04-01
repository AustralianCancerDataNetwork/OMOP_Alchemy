import sqlalchemy as sa
import pytest
from typer.testing import CliRunner

from omop_alchemy.maintenance.cli import app
from omop_alchemy.maintenance.create_tables import create_missing_tables
from omop_alchemy.maintenance.foreign_keys import (
    ForeignKeyAction,
    ForeignKeyConstraintViolation,
    validate_foreign_key_constraints,
    collect_foreign_key_targets,
    collect_foreign_key_trigger_status,
    manage_foreign_key_triggers,
)

runner = CliRunner()


def _engine(tmp_path):
    return sa.create_engine(f"sqlite:///{tmp_path / 'foreign_keys.db'}", future=True)


def test_collect_foreign_key_targets_finds_participating_tables(tmp_path):
    """Test collect foreign key targets finds participating tables."""
    engine = _engine(tmp_path)
    create_missing_tables(engine)

    targets = {
        target.table_name: target
        for target in collect_foreign_key_targets(engine)
    }

    assert "person" in targets
    assert targets["person"].incoming_constraint_count > 0


def test_manage_foreign_key_triggers_supports_dry_run(tmp_path):
    """Test manage foreign key triggers supports dry run."""
    engine = _engine(tmp_path)
    create_missing_tables(engine)

    with pytest.raises(RuntimeError) as exc_info:
        manage_foreign_key_triggers(
            engine,
            action=ForeignKeyAction.DISABLE,
            dry_run=True,
        )

    assert "only supported for PostgreSQL" in str(exc_info.value)


def test_collect_foreign_key_trigger_status_is_safe_on_sqlite(tmp_path):
    """Test collect foreign key trigger status is safe on sqlite."""
    engine = _engine(tmp_path)
    create_missing_tables(engine)

    with pytest.raises(RuntimeError) as exc_info:
        collect_foreign_key_trigger_status(engine)

    assert "only supported for PostgreSQL" in str(exc_info.value)


def test_validate_foreign_key_constraints_is_safe_on_sqlite(tmp_path):
    """Test validate foreign key constraints is safe on sqlite."""
    engine = _engine(tmp_path)
    create_missing_tables(engine)

    with pytest.raises(RuntimeError) as exc_info:
        validate_foreign_key_constraints(engine)

    assert "only supported for PostgreSQL" in str(exc_info.value)


def test_disable_foreign_keys_cli_fails_gracefully_for_sqlite(monkeypatch):
    """Test disable foreign keys cli fails gracefully for sqlite."""
    def fake_load_environment(dotenv: str) -> None:
        return None

    def fake_get_engine_name(schema: str | None = None) -> str:
        return "sqlite:///:memory:"

    def fake_create_engine(url: str, *, future: bool) -> sa.Engine:
        return sa.create_engine(url, future=future)

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

    result = runner.invoke(
        app,
        ["foreign-keys", "disable", "--dry-run"],
    )

    assert result.exit_code == 1
    assert "only supported for PostgreSQL engines" in result.stdout


def test_manage_foreign_key_triggers_strict_does_not_enable_on_validation_failure(monkeypatch):
    """Test manage foreign key triggers strict does not enable on validation failure."""
    statements: list[str] = []

    class _FakeConnection:
        def exec_driver_sql(self, statement: str):
            statements.append(statement)
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeEngine:
        def begin(self):
            return _FakeConnection()

    monkeypatch.setattr(
        "omop_alchemy.maintenance.foreign_keys._ensure_postgresql_supported",
        lambda engine, *, feature: None,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.foreign_keys.collect_foreign_key_targets",
        lambda engine, *, db_schema=None, vocabulary_included=False: [
            type("Target", (), {
                "table_name": "person",
                "category": "clinical",
                "model_name": "Person",
                "model_module": "omop_alchemy.cdm.model.clinical.person",
                "outgoing_constraint_count": 1,
                "incoming_constraint_count": 2,
            })(),
            type("Target", (), {
                "table_name": "visit_occurrence",
                "category": "health_system",
                "model_name": "VisitOccurrence",
                "model_module": "omop_alchemy.cdm.model.health_system.visit_occurrence",
                "outgoing_constraint_count": 2,
                "incoming_constraint_count": 0,
            })(),
        ],
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.foreign_keys._collect_strict_validation_failures",
        lambda connection, *, db_schema=None, vocabulary_included=False: {
            "visit_occurrence": [
                ForeignKeyConstraintViolation(
                    source_table_name="visit_occurrence",
                    referred_table_name="person",
                    constraint_name="fk_visit_occurrence_person_id_person",
                    violation_count=3,
                )
            ]
        },
    )

    results = manage_foreign_key_triggers(
        _FakeEngine(),
        action=ForeignKeyAction.ENABLE,
        strict=True,
    )

    assert statements == []
    assert [result.status for result in results] == ["skipped", "failed"]
    assert "no FK triggers were enabled" in results[0].detail
    assert "fk_visit_occurrence_person_id_person" in results[1].detail


def test_manage_foreign_key_triggers_strict_enables_when_validation_passes(monkeypatch):
    """Test manage foreign key triggers strict enables when validation passes."""
    statements: list[str] = []

    class _FakeConnection:
        def exec_driver_sql(self, statement: str):
            statements.append(statement)
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeEngine:
        def begin(self):
            return _FakeConnection()

    monkeypatch.setattr(
        "omop_alchemy.maintenance.foreign_keys._ensure_postgresql_supported",
        lambda engine, *, feature: None,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.foreign_keys.collect_foreign_key_targets",
        lambda engine, *, db_schema=None, vocabulary_included=False: [
            type("Target", (), {
                "table_name": "person",
                "category": "clinical",
                "model_name": "Person",
                "model_module": "omop_alchemy.cdm.model.clinical.person",
                "outgoing_constraint_count": 1,
                "incoming_constraint_count": 2,
            })(),
        ],
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.foreign_keys._collect_strict_validation_failures",
        lambda connection, *, db_schema=None, vocabulary_included=False: {},
    )

    results = manage_foreign_key_triggers(
        _FakeEngine(),
        action=ForeignKeyAction.ENABLE,
        strict=True,
    )

    assert statements == ["ALTER TABLE person ENABLE TRIGGER ALL"]
    assert results[0].status == "applied"
    assert "Strict FK validation passed" in results[0].detail


def test_enable_foreign_keys_strict_cli_invokes_strict_management(monkeypatch):
    """Test enable foreign keys strict cli invokes strict management."""
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

    def fake_manage_foreign_key_triggers(
        engine: object,
        *,
        action: ForeignKeyAction,
        db_schema: str | None = None,
        vocabulary_included: bool = False,
        dry_run: bool = False,
        strict: bool = False,
    ):
        calls["engine"] = engine
        calls["action"] = action
        calls["db_schema"] = db_schema
        calls["vocabulary_included"] = vocabulary_included
        calls["dry_run"] = dry_run
        calls["strict"] = strict
        return []

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
        "omop_alchemy.maintenance.cli.manage_foreign_key_triggers",
        fake_manage_foreign_key_triggers,
    )

    result = runner.invoke(
        app,
        ["foreign-keys", "enable", "--strict", "--dry-run"],
    )

    assert result.exit_code == 0
    assert calls["strict"] is True
    assert calls["action"] is ForeignKeyAction.ENABLE
    assert "enable --strict" in result.stdout


def test_validate_foreign_key_constraints_reports_failures(monkeypatch):
    """Test validate foreign key constraints reports failures."""
    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeEngine:
        def connect(self):
            return _FakeConnection()

    monkeypatch.setattr(
        "omop_alchemy.maintenance.foreign_keys._ensure_postgresql_supported",
        lambda engine, *, feature: None,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.foreign_keys.collect_foreign_key_targets",
        lambda engine, *, db_schema=None, vocabulary_included=False: [
            type("Target", (), {
                "table_name": "person",
                "category": "clinical",
                "model_name": "Person",
                "model_module": "omop_alchemy.cdm.model.clinical.person",
                "outgoing_constraint_count": 1,
                "incoming_constraint_count": 2,
            })(),
            type("Target", (), {
                "table_name": "visit_occurrence",
                "category": "health_system",
                "model_name": "VisitOccurrence",
                "model_module": "omop_alchemy.cdm.model.health_system.visit_occurrence",
                "outgoing_constraint_count": 2,
                "incoming_constraint_count": 0,
            })(),
        ],
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.foreign_keys._collect_strict_validation_failures",
        lambda connection, *, db_schema=None, vocabulary_included=False: {
            "visit_occurrence": [
                ForeignKeyConstraintViolation(
                    source_table_name="visit_occurrence",
                    referred_table_name="person",
                    constraint_name="fk_visit_occurrence_person_id_person",
                    violation_count=3,
                )
            ]
        },
    )

    report = validate_foreign_key_constraints(_FakeEngine())

    assert [result.status for result in report.results] == ["passed", "failed"]
    assert report.results[0].violating_row_count == 0
    assert report.results[1].violating_constraint_count == 1
    assert "fk_visit_occurrence_person_id_person" in report.results[1].detail
    assert len(report.violations) == 1


def test_foreign_keys_validate_cli_invokes_validation(monkeypatch):
    """Test foreign keys validate cli invokes validation."""
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

    def fake_validate_foreign_key_constraints(
        engine: object,
        *,
        db_schema: str | None = None,
        vocabulary_included: bool = False,
    ):
        from omop_alchemy.maintenance.foreign_keys import (
            ForeignKeyConstraintViolation,
            ForeignKeyValidationReport,
            ForeignKeyValidationResult,
        )
        from omop_alchemy.maintenance.tables import TableCategory

        calls["engine"] = engine
        calls["db_schema"] = db_schema
        calls["vocabulary_included"] = vocabulary_included
        return ForeignKeyValidationReport(
            results=(
                ForeignKeyValidationResult(
                    table_name="visit_occurrence",
                    category=TableCategory.HEALTH_SYSTEM,
                    model_name="VisitOccurrence",
                    model_module="omop_alchemy.cdm.model.health_system.visit_occurrence",
                    outgoing_constraint_count=2,
                    incoming_constraint_count=0,
                    violating_constraint_count=1,
                    violating_row_count=3,
                    status="failed",
                    detail="3 violating row(s) across 1 constraint(s): fk_visit_occurrence_person_id_person (3)",
                ),
            ),
            violations=(
                ForeignKeyConstraintViolation(
                    source_table_name="visit_occurrence",
                    referred_table_name="person",
                    constraint_name="fk_visit_occurrence_person_id_person",
                    violation_count=3,
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
        "omop_alchemy.maintenance.cli.validate_foreign_key_constraints",
        fake_validate_foreign_key_constraints,
    )

    result = runner.invoke(
        app,
        ["foreign-keys", "validate"],
    )

    assert result.exit_code == 0
    assert calls["engine"] == "ENGINE"
    assert "foreign-keys validate" in result.stdout
    assert "Violations" in result.stdout
    assert "visit_occurrence" in result.stdout
    assert "Violating rows" in result.stdout
