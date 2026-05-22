from typer.testing import CliRunner

from omop_alchemy.maintenance.cli import app
from omop_alchemy.maintenance.cli_config import defaults_path, ConnectionDefaults
from omop_alchemy.maintenance.cli_indexes import IndexAction, IndexManagementResult
from omop_alchemy.maintenance.tables import TableCategory


runner = CliRunner()


def test_config_set_overrides_and_show():
    """Config override persists values and config show surfaces them."""
    with runner.isolated_filesystem():
        result = runner.invoke(
            app,
            [
                "config",
                "override",
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
        loaded_defaults = ConnectionDefaults.load()
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
    """CLI commands consume persisted connection defaults when flags are omitted."""
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
        "omop_alchemy.db.load_environment",
        fake_load_environment,
    )
    monkeypatch.setattr(
        "omop_alchemy.db.get_engine_name",
        fake_get_engine_name,
    )
    monkeypatch.setattr(
        "omop_alchemy.db.create_engine_with_dependencies",
        fake_create_engine,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli_indexes.manage_indexes",
        fake_manage_indexes,
    )

    with runner.isolated_filesystem():
        set_result = runner.invoke(
            app,
            [
                "config",
                "override",
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


def test_config_show_surfaces_manual_logging_setting() -> None:
    """Config show surfaces manually configured logging mode from defaults file."""
    with runner.isolated_filesystem():
        defaults_path().write_text(
            "[defaults]\nlogging = \"off\"\n",
            encoding="utf-8",
        )

        loaded_defaults = ConnectionDefaults.load()
        assert loaded_defaults.logging == "off"

        show_result = runner.invoke(app, ["config", "show"])
        assert show_result.exit_code == 0
        assert "Logging" in show_result.stdout
        assert "off" in show_result.stdout
