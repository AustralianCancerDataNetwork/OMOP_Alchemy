from pathlib import Path

from omop_alchemy.maintenance.defaults import (
    ConnectionDefaults,
    defaults_path,
    load_connection_defaults,
    save_connection_defaults,
)


def test_load_connection_defaults_supports_defaults_section_and_relative_dotenv(monkeypatch, tmp_path):
    config_path = tmp_path / ".omop-maint.toml"
    config_path.write_text(
        '[defaults]\n'
        'dotenv = ".env"\n'
        'engine_schema = "cdm"\n',
        encoding="utf-8",
    )

    monkeypatch.setenv("OMOP_MAINT_DEFAULTS_FILE", str(config_path))

    defaults = load_connection_defaults()

    assert defaults.engine_schema == "cdm"
    assert defaults.dotenv == str((tmp_path / ".env").resolve())


def test_load_connection_defaults_supports_relative_athena_source(monkeypatch, tmp_path):
    config_path = tmp_path / ".omop-maint.toml"
    config_path.write_text(
        '[defaults]\n'
        'athena_source = "athena_source"\n',
        encoding="utf-8",
    )

    monkeypatch.setenv("OMOP_MAINT_DEFAULTS_FILE", str(config_path))

    defaults = load_connection_defaults()

    assert defaults.athena_source == str((tmp_path / "athena_source").resolve())


def test_save_connection_defaults_writes_defaults_section_with_relative_dotenv(monkeypatch, tmp_path):
    config_path = tmp_path / ".omop-maint.toml"
    dotenv_path = tmp_path / ".env"
    athena_source_path = tmp_path / "athena_source"

    monkeypatch.setenv("OMOP_MAINT_DEFAULTS_FILE", str(config_path))

    save_connection_defaults(
        ConnectionDefaults(
            dotenv=str(dotenv_path),
            engine_schema="cdm",
            athena_source=str(athena_source_path),
        )
    )

    saved_text = config_path.read_text(encoding="utf-8")

    assert "[defaults]" in saved_text
    assert 'dotenv = ".env"' in saved_text
    assert 'engine_schema = "cdm"' in saved_text
    assert 'athena_source = "athena_source"' in saved_text


def test_save_connection_defaults_rebases_relative_paths_to_defaults_file(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    run_dir = tmp_path / "run"
    config_dir.mkdir()
    run_dir.mkdir()
    config_path = config_dir / ".omop-maint.toml"

    monkeypatch.setenv("OMOP_MAINT_DEFAULTS_FILE", str(config_path))
    monkeypatch.chdir(run_dir)

    save_connection_defaults(
        ConnectionDefaults(
            dotenv=".env",
            athena_source="athena_source",
        )
    )

    saved_text = config_path.read_text(encoding="utf-8")
    assert 'dotenv = "../run/.env"' in saved_text
    assert 'athena_source = "../run/athena_source"' in saved_text

    defaults = load_connection_defaults()
    assert defaults.dotenv == str((run_dir / ".env").resolve())
    assert defaults.athena_source == str((run_dir / "athena_source").resolve())


def test_load_connection_defaults_keeps_legacy_connection_section_compatibility(monkeypatch, tmp_path):
    config_path = tmp_path / ".omop-maint.toml"
    config_path.write_text(
        '[connection]\n'
        'dotenv = ".env"\n'
        'engine_schema = "legacy"\n',
        encoding="utf-8",
    )

    monkeypatch.setenv("OMOP_MAINT_DEFAULTS_FILE", str(config_path))

    defaults = load_connection_defaults()

    assert defaults.engine_schema == "legacy"
    assert defaults.dotenv == str((tmp_path / ".env").resolve())


def test_defaults_path_uses_project_root_defaults_file(monkeypatch, tmp_path):
    repo_root = tmp_path / "repo"
    nested = repo_root / "src" / "module"
    nested.mkdir(parents=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname = \"omop-alchemy\"\n", encoding="utf-8")
    config_path = repo_root / ".omop-maint.toml"
    config_path.write_text("[defaults]\nengine_schema = \"cdm\"\n", encoding="utf-8")

    monkeypatch.delenv("OMOP_MAINT_DEFAULTS_FILE", raising=False)
    monkeypatch.chdir(nested)

    assert defaults_path() == config_path.resolve()


def test_defaults_path_falls_back_to_cwd_when_no_defaults_file_exists(monkeypatch, tmp_path):
    monkeypatch.delenv("OMOP_MAINT_DEFAULTS_FILE", raising=False)
    monkeypatch.chdir(tmp_path)

    assert defaults_path() == (tmp_path / ".omop-maint.toml").resolve()


def test_defaults_path_ignores_nested_defaults_in_favor_of_project_root(monkeypatch, tmp_path):
    repo_root = tmp_path / "repo"
    nested = repo_root / "src" / "module"
    nested.mkdir(parents=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname = \"omop-alchemy\"\n", encoding="utf-8")
    (repo_root / ".omop-maint.toml").write_text("[defaults]\nengine_schema = \"root\"\n", encoding="utf-8")
    (repo_root / "src" / ".omop-maint.toml").write_text("[defaults]\nengine_schema = \"nested\"\n", encoding="utf-8")

    monkeypatch.delenv("OMOP_MAINT_DEFAULTS_FILE", raising=False)
    monkeypatch.chdir(nested)

    assert defaults_path() == (repo_root / ".omop-maint.toml").resolve()
