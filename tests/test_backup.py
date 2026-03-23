import subprocess

import sqlalchemy as sa
import pytest
from typer.testing import CliRunner

from omop_alchemy.maintenance.backup import (
    BackupFormat,
    DatabaseBackupResult,
    DatabaseRestoreResult,
    RestoreFormat,
    create_database_backup,
    restore_database_backup,
)
from omop_alchemy.maintenance.cli import app

runner = CliRunner()


class _FakeDialect:
    name = "postgresql"


class _FakeEngine:
    dialect = _FakeDialect()
    url = sa.engine.make_url("postgresql+psycopg://airflow:secret@postgres:5432/cdm")


class _FakeRemoteEngine:
    dialect = _FakeDialect()
    url = sa.engine.make_url(
        "postgresql+psycopg://airflow:secret@db.example.com:5432/cdm"
        "?sslmode=require&application_name=omop-maint"
    )


def test_create_database_backup_requires_postgresql(tmp_path):
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'backup.db'}", future=True)

    with pytest.raises(RuntimeError) as exc_info:
        create_database_backup(engine, output_path=tmp_path / "backup.dump")

    assert "only supported for PostgreSQL engines" in str(exc_info.value)
    assert "Database backup" in str(exc_info.value)


def test_create_database_backup_requires_pg_dump(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "omop_alchemy.maintenance.backup.shutil.which",
        lambda executable_name: None,
    )

    with pytest.raises(RuntimeError) as exc_info:
        create_database_backup(_FakeEngine(), output_path=tmp_path / "backup.dump")

    assert "pg_dump" in str(exc_info.value)


def test_create_database_backup_builds_restore_ready_command(monkeypatch, tmp_path):
    def fake_run(*args, **kwargs):
        raise AssertionError("subprocess.run should not be called during dry-run")

    monkeypatch.setattr(
        "omop_alchemy.maintenance.backup.shutil.which",
        lambda executable_name: "/usr/bin/pg_dump",
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.backup.subprocess.run",
        fake_run,
    )

    result = create_database_backup(
        _FakeEngine(),
        output_path=tmp_path / "backup.dump",
        format=BackupFormat.CUSTOM,
        db_schema="public",
        dry_run=True,
    )

    assert result.status == "planned"
    assert result.database_name == "cdm"
    assert result.command[:3] == ("/usr/bin/pg_dump", "--format", "custom")
    assert "--schema" in result.command
    assert "public" in result.command
    assert "--no-owner" in result.command
    assert "--no-privileges" in result.command
    assert "--host" not in result.command
    assert "--username" not in result.command


def test_create_database_backup_executes_pg_dump(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    def fake_run(command, *, env, check, capture_output, text):
        captured["command"] = command
        captured["env"] = env
        captured["check"] = check
        captured["capture_output"] = capture_output
        captured["text"] = text
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(
        "omop_alchemy.maintenance.backup.shutil.which",
        lambda executable_name: "/usr/bin/pg_dump",
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.backup.subprocess.run",
        fake_run,
    )

    result = create_database_backup(
        _FakeEngine(),
        output_path=tmp_path / "backup.dump",
        format=BackupFormat.PLAIN,
    )

    assert result.status == "created"
    assert captured["check"] is True
    assert captured["capture_output"] is True
    assert captured["text"] is True
    assert captured["env"]["PGPASSWORD"] == "secret"
    assert "--format" in captured["command"]
    assert "plain" in captured["command"]


def test_create_database_backup_preserves_remote_connection_parameters(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "omop_alchemy.maintenance.backup.shutil.which",
        lambda executable_name: "/usr/bin/pg_dump",
    )

    result = create_database_backup(
        _FakeRemoteEngine(),
        output_path=tmp_path / "backup.dump",
        dry_run=True,
    )

    dbname_index = result.command.index("--dbname") + 1
    connection_uri = result.command[dbname_index]

    assert connection_uri.startswith("postgresql://airflow@db.example.com:5432/cdm")
    assert "sslmode=require" in connection_uri
    assert "application_name=omop-maint" in connection_uri
    assert "secret" not in connection_uri


def test_restore_database_backup_requires_existing_artifact(tmp_path):
    with pytest.raises(RuntimeError) as exc_info:
        restore_database_backup(_FakeEngine(), input_path=tmp_path / "missing.dump")

    assert "Backup artifact not found" in str(exc_info.value)


def test_restore_database_backup_requires_postgresql(tmp_path):
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'restore.db'}", future=True)
    artifact = tmp_path / "backup.dump"
    artifact.write_text("stub", encoding="utf-8")

    with pytest.raises(RuntimeError) as exc_info:
        restore_database_backup(engine, input_path=artifact)

    assert "Database restore" in str(exc_info.value)
    assert "Database backup" not in str(exc_info.value)


def test_restore_database_backup_uses_pg_restore_for_custom_dump(monkeypatch, tmp_path):
    artifact = tmp_path / "backup.dump"
    artifact.write_text("stub", encoding="utf-8")

    def fake_run(*args, **kwargs):
        raise AssertionError("subprocess.run should not be called during dry-run")

    monkeypatch.setattr(
        "omop_alchemy.maintenance.backup.shutil.which",
        lambda executable_name: f"/usr/bin/{executable_name}",
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.backup.subprocess.run",
        fake_run,
    )

    result = restore_database_backup(
        _FakeEngine(),
        input_path=artifact,
        db_schema="public",
        dry_run=True,
    )

    assert result.status == "planned"
    assert result.format is BackupFormat.CUSTOM
    assert result.command[0] == "/usr/bin/pg_restore"
    assert "--schema" in result.command
    assert "public" in result.command
    assert "--host" not in result.command


def test_restore_database_backup_uses_psql_for_plain_sql(monkeypatch, tmp_path):
    artifact = tmp_path / "backup.sql"
    artifact.write_text("select 1;", encoding="utf-8")
    captured: dict[str, object] = {}

    def fake_run(command, *, env, check, capture_output, text):
        captured["command"] = command
        captured["env"] = env
        captured["check"] = check
        captured["capture_output"] = capture_output
        captured["text"] = text
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(
        "omop_alchemy.maintenance.backup.shutil.which",
        lambda executable_name: f"/usr/bin/{executable_name}",
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.backup.subprocess.run",
        fake_run,
    )

    result = restore_database_backup(
        _FakeEngine(),
        input_path=artifact,
    )

    assert result.status == "applied"
    assert result.format is BackupFormat.PLAIN
    assert captured["command"][0] == "/usr/bin/psql"
    assert "--single-transaction" in captured["command"]
    assert captured["env"]["PGPASSWORD"] == "secret"


def test_restore_database_backup_preserves_remote_connection_parameters(monkeypatch, tmp_path):
    artifact = tmp_path / "backup.sql"
    artifact.write_text("select 1;", encoding="utf-8")

    monkeypatch.setattr(
        "omop_alchemy.maintenance.backup.shutil.which",
        lambda executable_name: f"/usr/bin/{executable_name}",
    )

    result = restore_database_backup(
        _FakeRemoteEngine(),
        input_path=artifact,
        dry_run=True,
    )

    dbname_index = result.command.index("--dbname") + 1
    connection_uri = result.command[dbname_index]

    assert connection_uri.startswith("postgresql://airflow@db.example.com:5432/cdm")
    assert "sslmode=require" in connection_uri
    assert "application_name=omop-maint" in connection_uri
    assert "secret" not in connection_uri


def test_restore_database_cli_invokes_restore(monkeypatch, tmp_path):
    calls: dict[str, object] = {}
    artifact = tmp_path / "backup.dump"
    artifact.write_text("stub", encoding="utf-8")

    def fake_load_environment(dotenv: str) -> None:
        calls["dotenv"] = dotenv

    def fake_get_engine_name(schema: str | None = None) -> str:
        calls["engine_schema"] = schema
        return "postgresql+psycopg://example"

    def fake_create_engine(url: str, *, future: bool) -> str:
        calls["engine_url"] = url
        calls["future"] = future
        return "ENGINE"

    def fake_restore_database_backup(
        engine: object,
        *,
        input_path: str,
        format: RestoreFormat = RestoreFormat.AUTO,
        db_schema: str | None = None,
        dry_run: bool = False,
    ) -> DatabaseRestoreResult:
        calls["engine"] = engine
        calls["input_path"] = input_path
        calls["format"] = format
        calls["db_schema"] = db_schema
        calls["dry_run"] = dry_run
        return DatabaseRestoreResult(
            input_path=input_path,
            format=BackupFormat.CUSTOM,
            status="planned",
            detail="Database restore would be executed using PostgreSQL client tools.",
            database_name="cdm",
            backend="postgresql",
            schema_name=db_schema,
            command=("/usr/bin/pg_restore",),
            tool_path="/usr/bin/pg_restore",
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
        "omop_alchemy.maintenance.cli.restore_database_backup",
        fake_restore_database_backup,
    )

    result = runner.invoke(
        app,
        ["restore-database", str(artifact), "--dry-run"],
    )

    assert result.exit_code == 0
    assert calls["format"] is RestoreFormat.AUTO
    assert calls["dry_run"] is True
    assert calls["input_path"] == str(artifact)
    assert "restore-database" in result.stdout


def test_backup_database_cli_invokes_backup_creation(monkeypatch):
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

    def fake_create_database_backup(
        engine: object,
        *,
        output_path: str | None = None,
        format: BackupFormat = BackupFormat.CUSTOM,
        db_schema: str | None = None,
        dry_run: bool = False,
    ) -> DatabaseBackupResult:
        calls["engine"] = engine
        calls["output_path"] = output_path
        calls["format"] = format
        calls["db_schema"] = db_schema
        calls["dry_run"] = dry_run
        return DatabaseBackupResult(
            output_path="/tmp/backup.sql",
            format=format,
            status="planned",
            detail="Database backup would be created with pg_dump.",
            database_name="cdm",
            backend="postgresql",
            schema_name=db_schema,
            command=("/usr/bin/pg_dump",),
            tool_path="/usr/bin/pg_dump",
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
        "omop_alchemy.maintenance.cli.create_database_backup",
        fake_create_database_backup,
    )

    result = runner.invoke(
        app,
        ["backup-database", "--dry-run", "--format", "plain", "--output-path", "/tmp/backup.sql"],
    )

    assert result.exit_code == 0
    assert calls["format"] is BackupFormat.PLAIN
    assert calls["dry_run"] is True
    assert "backup-database" in result.stdout
    assert "/tmp/backup.sql" in result.stdout
