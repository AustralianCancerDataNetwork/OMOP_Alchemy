from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
import os
from pathlib import Path
import shutil
import subprocess

import sqlalchemy as sa
import typer

from ..backend_support import Dialect, POSTGRESQL_ONLY_HELP, require_backend
from ._cli_utils import build_engine, handle_error, resolve_connection
from .ui import (
    console,
    render_backup_result,
    render_backup_summary,
    render_command_header,
    render_restore_result,
    render_restore_summary,
)


class BackupFormat(StrEnum):
    CUSTOM = "custom"
    PLAIN = "plain"


FORMAT_SUFFIXES = {
    BackupFormat.CUSTOM: ".dump",
    BackupFormat.PLAIN: ".sql",
}


@dataclass(frozen=True)
class DatabaseBackupResult:
    output_path: str
    format: BackupFormat
    status: str
    detail: str
    database_name: str
    backend: str
    schema_name: str | None
    command: tuple[str, ...]
    tool_path: str


@dataclass(frozen=True)
class DatabaseRestoreResult:
    input_path: str
    format: BackupFormat
    status: str
    detail: str
    database_name: str
    backend: str
    schema_name: str | None
    command: tuple[str, ...]
    tool_path: str


def _pg_dump_path() -> str:
    tool_path = shutil.which("pg_dump")
    if tool_path is None:
        raise RuntimeError(
            "The `pg_dump` executable is required for database backups but was not found on PATH. "
            "Install PostgreSQL client tools and ensure `pg_dump` is available."
        )
    return tool_path


def _pg_restore_path() -> str:
    tool_path = shutil.which("pg_restore")
    if tool_path is None:
        raise RuntimeError(
            "The `pg_restore` executable is required to restore custom PostgreSQL dumps but was not found on PATH. "
            "Install PostgreSQL client tools and ensure `pg_restore` is available."
        )
    return tool_path


def _psql_path() -> str:
    tool_path = shutil.which("psql")
    if tool_path is None:
        raise RuntimeError(
            "The `psql` executable is required to restore plain SQL PostgreSQL dumps but was not found on PATH. "
            "Install PostgreSQL client tools and ensure `psql` is available."
        )
    return tool_path


def _default_output_path(format: BackupFormat) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return Path.cwd() / f"omop-alchemy-backup-{timestamp}{FORMAT_SUFFIXES[format]}"


def _libpq_connection_uri(url: sa.engine.URL) -> str:
    if not url.database:
        raise RuntimeError(
            "Database backup requires a database name in the configured engine URL."
        )

    libpq_url = sa.engine.URL.create(
        drivername="postgresql",
        username=url.username,
        password=None,
        host=url.host,
        port=url.port,
        database=url.database,
        query=url.query,
    )
    return libpq_url.render_as_string(hide_password=False)


def _build_pg_dump_command(
    *,
    engine: sa.Engine,
    output_path: Path,
    format: BackupFormat,
    db_schema: str | None,
    tool_path: str,
) -> tuple[list[str], dict[str, str], str]:
    url = engine.url
    database_name = url.database
    if not database_name:
        raise RuntimeError("Database backup requires a database name in the configured engine URL.")
    connection_uri = _libpq_connection_uri(url)

    command = [
        tool_path,
        "--format",
        format.value,
        "--file",
        str(output_path),
        "--dbname",
        connection_uri,
        "--no-password",
        "--no-owner",
        "--no-privileges",
    ]

    if db_schema:
        command.extend(["--schema", db_schema])

    env = os.environ.copy()
    if url.password:
        env["PGPASSWORD"] = url.password

    return command, env, database_name


def _restore_tool_path(format: BackupFormat) -> str:
    if format == BackupFormat.CUSTOM:
        return _pg_restore_path()
    return _psql_path()


def _build_restore_command(
    *,
    engine: sa.Engine,
    input_path: Path,
    format: BackupFormat,
    db_schema: str | None,
    tool_path: str,
) -> tuple[list[str], dict[str, str], str]:
    url = engine.url
    database_name = url.database
    if not database_name:
        raise RuntimeError("Database restore requires a database name in the configured engine URL.")
    connection_uri = _libpq_connection_uri(url)

    if format == BackupFormat.CUSTOM:
        command = [
            tool_path,
            "--dbname",
            connection_uri,
            "--no-password",
            "--no-owner",
            "--no-privileges",
            "--exit-on-error",
        ]
        if db_schema:
            command.extend(["--schema", db_schema])
        command.append(str(input_path))
    else:
        command = [
            tool_path,
            "--dbname",
            connection_uri,
            "--no-password",
            "--set",
            "ON_ERROR_STOP=1",
            "--single-transaction",
        ]
        command.extend(["--file", str(input_path)])

    env = os.environ.copy()
    if url.password:
        env["PGPASSWORD"] = url.password

    return command, env, database_name


def create_database_backup(
    engine: sa.Engine,
    *,
    output_path: str | Path | None = None,
    format: BackupFormat = BackupFormat.CUSTOM,
    db_schema: str | None = None,
    dry_run: bool = False,
) -> DatabaseBackupResult:
    require_backend(engine, feature="Database backup", supported_dialects=(Dialect.POSTGRESQL,))
    tool_path = _pg_dump_path()
    resolved_output_path = Path(output_path) if output_path is not None else _default_output_path(format)
    resolved_output_path = resolved_output_path.expanduser().resolve()

    command, env, database_name = _build_pg_dump_command(
        engine=engine,
        output_path=resolved_output_path,
        format=format,
        db_schema=db_schema,
        tool_path=tool_path,
    )

    if not dry_run:
        resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            subprocess.run(command, env=env, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            raise RuntimeError(
                "Database backup failed via `pg_dump`." + (f" {stderr}" if stderr else "")
            ) from exc

    return DatabaseBackupResult(
        output_path=str(resolved_output_path),
        format=format,
        status="planned" if dry_run else "created",
        detail=(
            "Database backup would be created with pg_dump."
            if dry_run
            else "Database backup created with pg_dump."
        ),
        database_name=database_name,
        backend=engine.dialect.name,
        schema_name=db_schema,
        command=tuple(command),
        tool_path=tool_path,
    )


def restore_database_backup(
    engine: sa.Engine,
    *,
    input_path: str | Path,
    format: BackupFormat,
    db_schema: str | None = None,
    dry_run: bool = False,
) -> DatabaseRestoreResult:
    require_backend(engine, feature="Database restore", supported_dialects=(Dialect.POSTGRESQL,))
    resolved_input_path = Path(input_path).expanduser().resolve()
    if not resolved_input_path.exists():
        raise RuntimeError(f"Backup artifact not found: {resolved_input_path}")

    tool_path = _restore_tool_path(format)
    command, env, database_name = _build_restore_command(
        engine=engine,
        input_path=resolved_input_path,
        format=format,
        db_schema=db_schema,
        tool_path=tool_path,
    )

    if not dry_run:
        try:
            subprocess.run(command, env=env, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            raise RuntimeError(
                "Database restore failed." + (f" {stderr}" if stderr else "")
            ) from exc

    return DatabaseRestoreResult(
        input_path=str(resolved_input_path),
        format=format,
        status="planned" if dry_run else "applied",
        detail=(
            "Database restore would be executed using PostgreSQL client tools."
            if dry_run
            else "Database restore completed using PostgreSQL client tools."
        ),
        database_name=database_name,
        backend=engine.dialect.name,
        schema_name=db_schema,
        command=tuple(command),
        tool_path=tool_path,
    )


app = typer.Typer(rich_markup_mode="rich")


@app.command(
    "backup-database",
    help=f"Create a PostgreSQL dump artifact that can be restored into another environment. {POSTGRESQL_ONLY_HELP}",
)
def backup_database_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Optional schema-limited backup."),
    output_path: str | None = typer.Option(
        None,
        help="Backup artifact path. Defaults to a timestamped file in the current directory.",
    ),
    format: BackupFormat = typer.Option(BackupFormat.CUSTOM, help="Backup format."),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    conn = resolve_connection(dotenv=dotenv, engine_schema=engine_schema, db_schema=db_schema)
    console.print(
        render_command_header(
            command_name="backup-database",
            engine_schema=conn.engine_schema,
            db_schema=conn.db_schema,
            vocabulary_included=None,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = build_engine(dotenv=conn.dotenv, engine_schema=conn.engine_schema)
        with console.status("Creating restore-ready PostgreSQL backup..."):
            result = create_database_backup(
                engine,
                output_path=output_path,
                format=format,
                db_schema=conn.db_schema,
                dry_run=dry_run,
            )
        console.print(render_backup_result(result))
        console.print(render_backup_summary(result, dry_run=dry_run))
    except Exception as exc:
        handle_error(exc)


@app.command(
    "restore-database",
    help=f"Restore a PostgreSQL backup artifact into the configured target database. {POSTGRESQL_ONLY_HELP}",
)
def restore_database_command(
    input_path: str = typer.Argument(..., help="Backup artifact path to restore."),
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(
        None, help="Optional schema-limited restore for custom-format dumps."
    ),
    format: BackupFormat = typer.Option(
        ..., help="Restore format. Required: choose `custom` or `plain`."
    ),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    conn = resolve_connection(dotenv=dotenv, engine_schema=engine_schema, db_schema=db_schema)
    console.print(
        render_command_header(
            command_name="restore-database",
            engine_schema=conn.engine_schema,
            db_schema=conn.db_schema,
            vocabulary_included=None,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = build_engine(dotenv=conn.dotenv, engine_schema=conn.engine_schema)
        with console.status("Restoring PostgreSQL backup artifact..."):
            result = restore_database_backup(
                engine,
                input_path=input_path,
                format=format,
                db_schema=conn.db_schema,
                dry_run=dry_run,
            )
        console.print(render_restore_result(result))
        console.print(render_restore_summary(result, dry_run=dry_run))
    except Exception as exc:
        handle_error(exc)
