from __future__ import annotations

import typer

from ..backend_support import POSTGRESQL_ONLY_HELP
from ._cli_utils import build_engine, handle_error, resolve_connection
from ..cdm.handlers.fulltext import (
    drop_fulltext_columns,
    install_fulltext_columns,
    populate_fulltext_columns,
)
from .ui import (
    console,
    render_command_header,
    render_fulltext_results,
    render_fulltext_summary,
)

app = typer.Typer(
    help=f"Manage PostgreSQL full-text sidecar tsvector columns for OMOP vocabulary tables. {POSTGRESQL_ONLY_HELP}",
    rich_markup_mode="rich",
)


@app.command("install")
def install_fulltext_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    create_indexes: bool = typer.Option(
        True,
        "--create-indexes/--no-create-indexes",
        help="Create GIN indexes alongside the tsvector columns.",
    ),
    fastupdate: bool = typer.Option(
        False,
        "--fastupdate/--no-fastupdate",
        help="Set PostgreSQL GIN fastupdate on created indexes.",
    ),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    conn = resolve_connection(dotenv=dotenv, engine_schema=engine_schema, db_schema=db_schema)
    console.print(
        render_command_header(
            command_name="fulltext install",
            engine_schema=conn.engine_schema,
            db_schema=conn.db_schema,
            vocabulary_included=True,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = build_engine(dotenv=conn.dotenv, engine_schema=conn.engine_schema)
        with console.status("Managing PostgreSQL full-text sidecar columns..."):
            results = install_fulltext_columns(
                engine,
                db_schema=conn.db_schema,
                create_indexes=create_indexes,
                fastupdate=fastupdate,
                dry_run=dry_run,
            )
        console.print(render_fulltext_results(results))
        console.print(render_fulltext_summary(results, action="install", dry_run=dry_run))
    except Exception as exc:
        handle_error(exc)


@app.command("populate")
def populate_fulltext_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    regconfig: str = typer.Option(
        "english",
        help="PostgreSQL text search configuration to use for vector population.",
    ),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    conn = resolve_connection(dotenv=dotenv, engine_schema=engine_schema, db_schema=db_schema)
    console.print(
        render_command_header(
            command_name="fulltext populate",
            engine_schema=conn.engine_schema,
            db_schema=conn.db_schema,
            vocabulary_included=True,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = build_engine(dotenv=conn.dotenv, engine_schema=conn.engine_schema)
        with console.status("Managing PostgreSQL full-text sidecar columns..."):
            results = populate_fulltext_columns(
                engine,
                db_schema=conn.db_schema,
                regconfig=regconfig,
                dry_run=dry_run,
            )
        console.print(render_fulltext_results(results))
        console.print(render_fulltext_summary(results, action="populate", dry_run=dry_run))
    except Exception as exc:
        handle_error(exc)


@app.command("drop")
def drop_fulltext_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    drop_indexes: bool = typer.Option(
        True,
        "--drop-indexes/--no-drop-indexes",
        help="Drop managed GIN indexes before dropping the tsvector columns.",
    ),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    conn = resolve_connection(dotenv=dotenv, engine_schema=engine_schema, db_schema=db_schema)
    console.print(
        render_command_header(
            command_name="fulltext drop",
            engine_schema=conn.engine_schema,
            db_schema=conn.db_schema,
            vocabulary_included=True,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = build_engine(dotenv=conn.dotenv, engine_schema=conn.engine_schema)
        with console.status("Managing PostgreSQL full-text sidecar columns..."):
            results = drop_fulltext_columns(
                engine,
                db_schema=conn.db_schema,
                drop_indexes=drop_indexes,
                dry_run=dry_run,
            )
        console.print(render_fulltext_results(results))
        console.print(render_fulltext_summary(results, action="drop", dry_run=dry_run))
    except Exception as exc:
        handle_error(exc)
