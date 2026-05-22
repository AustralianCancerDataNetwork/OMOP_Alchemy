from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
import subprocess

import sqlalchemy as sa
import typer

from ..backends import resolve_backend, require_backend_support, backend_support_note
from ._cli_utils import handle_error, setup_cli_cmd
from .ui import (
    console,
    render_backup_result,
    render_backup_summary,
    render_restore_result,
    render_restore_summary,
)


class BackupFormat(StrEnum):
    """Supported pg_dump/psql output formats."""

    CUSTOM = "custom"
    PLAIN = "plain"


FORMAT_SUFFIXES = {
    BackupFormat.CUSTOM: ".dump",
    BackupFormat.PLAIN: ".sql",
}


@dataclass(frozen=True)
class BackupResult:
    """Metadata and outcome for a single backup or restore operation."""

    file_path: str
    backup_format: BackupFormat
    status: str
    detail: str
    database_name: str
    backend: str
    schema_name: str | None
    command: tuple[str, ...]
    tool_path: str


def _default_output_path(backup_format: BackupFormat) -> Path:
    """Return a timestamped default output path in the current directory matching the chosen backup format."""
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return Path.cwd() / f"omop-alchemy-backup-{timestamp}{FORMAT_SUFFIXES[backup_format]}"


def create_database_backup(
    engine: sa.Engine,
    *,
    output_path: str | Path | None = None,
    backup_format: BackupFormat = BackupFormat.CUSTOM,
    db_schema: str | None = None,
    dry_run: bool = False,
) -> BackupResult:
    """Create a database backup artifact at output_path; runs the subprocess unless dry_run is True."""
    backend = resolve_backend(engine)
    require_backend_support(backend, "prepare_backup", "Database backup")
    resolved_output_path = Path(output_path) if output_path is not None else _default_output_path(backup_format)
    resolved_output_path = resolved_output_path.expanduser().resolve()

    tool_path, command, env, database_name = backend.prepare_backup(
        engine, str(resolved_output_path), backup_format.value, db_schema
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

    return BackupResult(
        file_path=str(resolved_output_path),
        backup_format=backup_format,
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
    backup_format: BackupFormat,
    db_schema: str | None = None,
    dry_run: bool = False,
) -> BackupResult:
    """Restore a database backup; runs the subprocess unless dry_run is True."""
    backend = resolve_backend(engine)
    require_backend_support(backend, "prepare_restore", "Database restore")
    resolved_input_path = Path(input_path).expanduser().resolve()
    if not resolved_input_path.exists():
        raise RuntimeError(f"Backup artifact not found: {resolved_input_path}")
    tool_path, command, env, database_name = backend.prepare_restore(
        engine, str(resolved_input_path), backup_format.value, db_schema
    )

    if not dry_run:
        try:
            subprocess.run(command, env=env, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            raise RuntimeError(
                "Database restore failed." + (f" {stderr}" if stderr else "")
            ) from exc

    return BackupResult(
        file_path=str(resolved_input_path),
        backup_format=backup_format,
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

# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------
app = typer.Typer(
    rich_markup_mode="rich",
    help=f"Manage database backup and restore operations. {backend_support_note('prepare_backup')}",
)

@app.command("backup-database")
def backup_database_command(
    dotenv: str | None = typer.Option(
        None,
        help="Path to a .env file to load before resolving the connection. Overrides the saved DOTENV default.",
    ),
    engine_schema: str | None = typer.Option(
        None,
        help="Named engine configuration to use (e.g. 'cdm', 'results'). Resolves to the ENGINE_<SCHEMA> environment variable group.",
    ),
    db_schema: str | None = typer.Option(
        None,
        help="Restrict the backup to a single schema (pg_dump --schema). Only supported on PostgreSQL.",
    ),
    output_path: str | None = typer.Option(
        None,
        help="Output path for the backup artifact. Defaults to a timestamped file in the current directory.",
    ),
    backup_format: BackupFormat = typer.Option(
        BackupFormat.CUSTOM,
        help="pg_dump output format. 'custom' produces a binary .dump file; 'plain' produces a plain SQL .sql file.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview planned actions without applying any changes to the database.",
    ),
) -> None:
    """Create a database backup that can be restored with `restore-database`."""
    try:
        conn, engine = setup_cli_cmd(
            console=console,
            dotenv=dotenv,
            engine_schema=engine_schema,
            db_schema=db_schema,
            command_name="backup-database",
            vocabulary_included=None,
            mode_label="dry-run" if dry_run else "apply",
        )
        with console.status("Creating restore-ready database backup..."):
            result = create_database_backup(
                engine,
                output_path=output_path,
                backup_format=backup_format,
                db_schema=conn.db_schema,
                dry_run=dry_run,
            )
        console.print(render_backup_result(result))
        console.print(render_backup_summary(result, dry_run=dry_run))
    except Exception as exc:
        handle_error(exc)


@app.command("restore-database")
def restore_database_command(
    input_path: str = typer.Argument(help="Path to the backup artifact (.dump or .sql) to restore."),
    dotenv: str | None = typer.Option(
        None,
        help="Path to a .env file to load before resolving the connection. Overrides the saved DOTENV default.",
    ),
    engine_schema: str | None = typer.Option(
        None,
        help="Named engine configuration to use (e.g. 'cdm', 'results'). Resolves to the ENGINE_<SCHEMA> environment variable group.",
    ),
    db_schema: str | None = typer.Option(
        None,
        help="Restrict the restore to a single schema (pg_restore --schema). Only valid for custom-format dumps.",
    ),
    backup_format: BackupFormat = typer.Option(
        ...,
        help="Format of the artifact to restore. Must match the format used when the backup was created.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview planned actions without applying any changes to the database.",
    ),
) -> None:
    """Restore a database backup that was created with `backup-database`."""
    try:
        conn, engine = setup_cli_cmd(
            console=console,
            dotenv=dotenv,
            engine_schema=engine_schema,
            db_schema=db_schema,
            command_name="restore-database",
            vocabulary_included=None,
            mode_label="dry-run" if dry_run else "apply",
        )
        with console.status("Restoring database backup..."):
            result = restore_database_backup(
                engine,
                input_path=input_path,
                backup_format=backup_format,
                db_schema=conn.db_schema,
                dry_run=dry_run,
            )
        console.print(render_restore_result(result))
        console.print(render_restore_summary(result, dry_run=dry_run))
    except Exception as exc:
        handle_error(exc)
