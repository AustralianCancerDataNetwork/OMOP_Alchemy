import sqlalchemy as sa
import pytest
from typer.testing import CliRunner

from omop_alchemy.maintenance.cli import app
from omop_alchemy.maintenance.defaults import defaults_path, load_connection_defaults
from omop_alchemy.maintenance.indexes import IndexAction, IndexManagementResult
from omop_alchemy.maintenance.reset_sequences import collect_sequence_targets, reset_model_sequences
from omop_alchemy.maintenance.tables import TableCategory

runner = CliRunner()


def test_collect_sequence_targets_excludes_vocabulary_by_default():
    targets = {
        (target.table_name, target.pk_column_name)
        for target in collect_sequence_targets()
    }

    assert ("person", "person_id") in targets
    assert ("concept", "concept_id") not in targets


def test_collect_sequence_targets_can_include_vocabulary():
    targets = {
        (target.table_name, target.pk_column_name)
        for target in collect_sequence_targets(vocabulary_included=True)
    }

    assert ("concept", "concept_id") in targets


def test_top_level_help_annotates_postgresql_only_commands():
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "PostgreSQL-only commands" in result.stdout
    assert "reset-sequences" in result.stdout
    assert "foreign-keys" in result.stdout


def test_top_level_help_lists_portable_commands_before_postgresql_only_commands():
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    commands_section = result.stdout.split("╭─ Commands", maxsplit=1)[1]
    assert commands_section.index("data-summary") < commands_section.index("backup-database")
    assert commands_section.index("create-missing-tables") < commands_section.index("reset-sequences")


def test_command_help_marks_postgresql_only_support():
    result = runner.invoke(app, ["reset-sequences", "--help"])

    assert result.exit_code == 0
    assert "PostgreSQL only" in result.stdout


def test_reset_model_sequences_requires_postgresql(tmp_path):
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'seq.db'}", future=True)

    with pytest.raises(RuntimeError) as exc_info:
        reset_model_sequences(engine)

    assert "only supported for PostgreSQL engines" in str(exc_info.value)


def test_disable_indexes_cli_invokes_management(monkeypatch):
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

    def fake_manage_indexes(
        engine: object,
        *,
        action: IndexAction,
        db_schema: str | None = None,
        vocabulary_included: bool = False,
        dry_run: bool = False,
    ) -> list[IndexManagementResult]:
        calls["engine"] = engine
        calls["action"] = action
        calls["db_schema"] = db_schema
        calls["vocabulary_included"] = vocabulary_included
        calls["dry_run"] = dry_run
        return [
            IndexManagementResult(
                operation="index",
                table_name="person",
                category=TableCategory.CLINICAL,
                model_name="Person",
                model_module="omop_alchemy.cdm.model.clinical.person",
                index_name="idx_gender",
                column_names=("gender_concept_id",),
                unique=False,
                clustered=False,
                action=IndexAction.DISABLE,
                status="planned",
                detail="metadata-defined index would be dropped",
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
        "omop_alchemy.maintenance.cli.manage_indexes",
        fake_manage_indexes,
    )

    result = runner.invoke(
        app,
        [
            "indexes",
            "disable",
            "--dotenv",
            ".env.test",
            "--engine-schema",
            "cdm",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert "indexes disable" in result.stdout
    assert "person" in result.stdout
    assert "disable" in result.stdout
    assert "PLANNED" in result.stdout
    assert "Planned disable on 1 metadata operation(s)." in result.stdout


def test_config_set_overrides_and_show():
    with runner.isolated_filesystem():
        result = runner.invoke(
            app,
            [
                "config",
                "set-overrides",
                "--dotenv",
                ".env.test",
                "--engine-schema",
                "cdm",
                "--db-schema",
                "public",
                "--athena-source",
                "athena_source",
            ],
        )

        assert result.exit_code == 0
        assert defaults_path().exists()
        loaded_defaults = load_connection_defaults()
        assert loaded_defaults.dotenv == str((defaults_path().parent / ".env.test").resolve())
        assert loaded_defaults.engine_schema == "cdm"
        assert loaded_defaults.db_schema == "public"
        assert loaded_defaults.athena_source == str((defaults_path().parent / "athena_source").resolve())

        show_result = runner.invoke(app, ["config", "show"])
        assert show_result.exit_code == 0
        assert "cdm" in show_result.stdout
        assert "public" in show_result.stdout
        assert "athena_source" in show_result.stdout


def test_cli_uses_saved_connection_defaults(monkeypatch):
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

    def fake_manage_indexes(
        engine: object,
        *,
        action: IndexAction,
        db_schema: str | None = None,
        vocabulary_included: bool = False,
        dry_run: bool = False,
    ) -> list[IndexManagementResult]:
        calls["engine"] = engine
        calls["action"] = action
        calls["db_schema"] = db_schema
        calls["vocabulary_included"] = vocabulary_included
        calls["dry_run"] = dry_run
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
        "omop_alchemy.maintenance.cli.manage_indexes",
        fake_manage_indexes,
    )

    with runner.isolated_filesystem():
        set_result = runner.invoke(
            app,
            [
                "config",
                "set-overrides",
                "--dotenv",
                ".env.saved",
                "--engine-schema",
                "cdm",
                "--db-schema",
                "public",
            ],
        )
        assert set_result.exit_code == 0
        expected_dotenv = str((defaults_path().parent / ".env.saved").resolve())

        result = runner.invoke(app, ["indexes", "disable", "--dry-run"])

    assert result.exit_code == 0
    assert calls["dotenv"] == expected_dotenv
    assert calls["engine_schema"] == "cdm"
    assert calls["db_schema"] == "public"
