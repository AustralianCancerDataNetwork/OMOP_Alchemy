from __future__ import annotations
import typer
from typing import Optional

from ..db import (
    ConnectionDefaults,
    defaults_path,
)
from .ui import console, render_connection_defaults


DEFAULTS_FILENAME = ".omop-maint.toml"
DEFAULTS_ENV_VAR = "OMOP_MAINT_DEFAULTS_FILE"
DEFAULTS_SECTION = "defaults"
LEGACY_DEFAULTS_SECTION = "connection"
PROJECT_MARKER = "pyproject.toml"


app = typer.Typer(
    help="Manage persisted maintenance CLI connection overrides stored in .omop-maint.toml.",
    rich_markup_mode="rich",
)


@app.command("show")
def config_show_command() -> None:
    """Display current saved connection defaults from the nearest .omop-maint.toml file."""
    defaults = ConnectionDefaults.load()
    console.print(render_connection_defaults(defaults, path=str(defaults_path())))


@app.command("override")
def config_override_command(
    dotenv: Optional[str] = typer.Option(
        None,
        help=(
            "Path to a .env file to load before resolving the connection. "
            "Saved as a path relative to .omop-maint.toml and resolved back to absolute on load."
        ),
    ),
    engine_schema: Optional[str] = typer.Option(
        None,
        help="Named engine configuration to use (e.g. 'cdm', 'results'). Resolves to the ENGINE_<SCHEMA> environment variable group.",
    ),
    db_schema: Optional[str] = typer.Option(
        None,
        help="Database schema to target (e.g. 'cdm5', 'vocab'). Sets search_path on PostgreSQL; not supported on SQLite.",
    ),
    athena_source: Optional[str] = typer.Option(
        None,
        help=(
            "Path to the unzipped Athena vocabulary CSV directory. "
            "Saved relative to .omop-maint.toml; used by load-vocab-source when --athena-source is omitted."
        ),
    ),
) -> None:
    """Persist one or more connection overrides to .omop-maint.toml for future CLI invocations."""
    updated, path = ConnectionDefaults.update_and_save_defaults(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
        athena_source=athena_source,
    )
    console.print(render_connection_defaults(updated, path=str(path), title="Saved Overrides"))
