from __future__ import annotations

import typer
from sqlalchemy.engine import Engine

from ..backends import backend_support_note as _backend_support_note
from ..backends import resolve_backend, require_backend_support
from ..backends.base import FullTextAction, FullTextError, FullTextResult
from ._cli_utils import handle_error, setup_cli_cmd
from .ui import (
    console,
    render_fulltext_results,
    render_fulltext_summary,
)

app = typer.Typer(
    help=f"Manage full-text search for OMOP vocabulary tables. {_backend_support_note('install_fulltext_on_table')}",
    rich_markup_mode="rich",
)


# ── Orchestrators ─────────────────────────────────────────────────────────────

def install_fulltext_columns(
    engine: Engine,
    *,
    db_schema: str | None = None,
    create_indexes: bool = True,
    fastupdate: bool = False,
    dry_run: bool = False,
) -> tuple[FullTextResult, ...]:
    """Install tsvector sidecar columns (and optionally GIN indexes) on OMOP vocabulary tables."""
    backend = resolve_backend(engine)
    require_backend_support(backend, "install_fulltext_on_table", "Full-text search")
    targets = backend.fulltext_targets

    try:
        if not dry_run:
            with engine.begin() as connection:
                for cfg in targets:
                    backend.install_fulltext_on_table(
                        connection,
                        table_name=cfg.table_name,
                        vector_column_name=cfg.vector_column_name,
                        index_name=cfg.index_name,
                        db_schema=db_schema,
                        create_indexes=create_indexes,
                        fastupdate=fastupdate,
                    )
            backend.register_fulltext_metadata()
    except FullTextError:
        raise
    except Exception as exc:
        raise FullTextError(
            f"Full-text install failed. Underlying error: {exc.__class__.__name__}: {exc}"
        ) from exc

    return tuple(
        FullTextResult(
            target_name=cfg.table_name,
            table_name=cfg.table_name,
            source_column_name=cfg.source_column_name,
            vector_column_name=cfg.vector_column_name,
            index_name=cfg.index_name,
            action=FullTextAction.INSTALL,
            status="planned" if dry_run else "applied",
            detail=(
                "tsvector column would be installed"
                if dry_run and not create_indexes
                else "tsvector column and GIN index would be installed"
                if dry_run
                else "tsvector column installed"
                if not create_indexes
                else "tsvector column and GIN index installed"
            ),
        )
        for cfg in targets
    )


def populate_fulltext_columns(
    engine: Engine,
    *,
    db_schema: str | None = None,
    regconfig: str = "english",
    dry_run: bool = False,
) -> tuple[FullTextResult, ...]:
    """Populate tsvector sidecar columns with pre-computed search vectors."""
    backend = resolve_backend(engine)
    require_backend_support(backend, "populate_fulltext_on_table", "Full-text search")
    targets = backend.fulltext_targets

    row_counts: dict[str, int | None] = {}
    try:
        if not dry_run:
            with engine.begin() as connection:
                for cfg in targets:
                    row_counts[cfg.table_name] = backend.populate_fulltext_on_table(
                        connection,
                        table_name=cfg.table_name,
                        vector_column_name=cfg.vector_column_name,
                        source_column_name=cfg.source_column_name,
                        db_schema=db_schema,
                        regconfig=regconfig,
                    )
            backend.register_fulltext_metadata()
    except FullTextError:
        raise
    except Exception as exc:
        raise FullTextError(
            f"Full-text populate failed. Underlying error: {exc.__class__.__name__}: {exc}"
        ) from exc

    return tuple(
        FullTextResult(
            target_name=cfg.table_name,
            table_name=cfg.table_name,
            source_column_name=cfg.source_column_name,
            vector_column_name=cfg.vector_column_name,
            index_name=cfg.index_name,
            action=FullTextAction.POPULATE,
            status="planned" if dry_run else "applied",
            detail=(
                "tsvector column would be populated from source text"
                if dry_run
                else "tsvector column populated from source text"
            ),
            row_count=None if dry_run else row_counts.get(cfg.table_name),
        )
        for cfg in targets
    )


def drop_fulltext_columns(
    engine: Engine,
    *,
    db_schema: str | None = None,
    drop_indexes: bool = True,
    dry_run: bool = False,
) -> tuple[FullTextResult, ...]:
    """Remove tsvector sidecar columns and their associated GIN indexes."""
    backend = resolve_backend(engine)
    require_backend_support(backend, "drop_fulltext_on_table", "Full-text search")
    targets = backend.fulltext_targets

    try:
        if not dry_run:
            with engine.begin() as connection:
                for cfg in targets:
                    backend.drop_fulltext_on_table(
                        connection,
                        table_name=cfg.table_name,
                        vector_column_name=cfg.vector_column_name,
                        index_name=cfg.index_name,
                        db_schema=db_schema,
                        drop_indexes=drop_indexes,
                    )
            backend.unregister_fulltext_metadata()
    except FullTextError:
        raise
    except Exception as exc:
        raise FullTextError(
            f"Full-text drop failed. Underlying error: {exc.__class__.__name__}: {exc}"
        ) from exc

    return tuple(
        FullTextResult(
            target_name=cfg.table_name,
            table_name=cfg.table_name,
            source_column_name=cfg.source_column_name,
            vector_column_name=cfg.vector_column_name,
            index_name=cfg.index_name,
            action=FullTextAction.DROP,
            status="planned" if dry_run else "applied",
            detail=(
                "tsvector column would be dropped"
                if dry_run and not drop_indexes
                else "tsvector column and GIN index would be dropped"
                if dry_run
                else "tsvector column dropped"
                if not drop_indexes
                else "tsvector column and GIN index dropped"
            ),
        )
        for cfg in targets
    )


# ── CLI commands ──────────────────────────────────────────────────────────────

@app.command("install")
def install_fulltext_command(
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
        help="Database schema to target (e.g. 'cdm5', 'vocab'). Sets search_path on PostgreSQL; not supported on SQLite.",
    ),
    create_indexes: bool = typer.Option(
        True,
        "--create-indexes/--no-create-indexes",
        help="Create GIN indexes alongside the tsvector columns for fast full-text search.",
    ),
    fastupdate: bool = typer.Option(
        False,
        "--fastupdate/--no-fastupdate",
        help="Enable PostgreSQL GIN fastupdate on newly created indexes (trades write speed for query latency).",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview planned actions without applying any changes to the database.",
    ),
) -> None:
    """Add tsvector sidecar columns to vocabulary tables and optionally create GIN indexes for fast full-text search."""
    try:
        conn, engine = setup_cli_cmd(
            console=console,
            dotenv=dotenv,
            engine_schema=engine_schema,
            db_schema=db_schema,
            command_name="fulltext install",
            vocabulary_included=True,
            mode_label="dry-run" if dry_run else "apply",
        )
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
        help="Database schema to target (e.g. 'cdm5', 'vocab'). Sets search_path on PostgreSQL; not supported on SQLite.",
    ),
    regconfig: str = typer.Option(
        "english",
        help="PostgreSQL text search configuration to use when building tsvector values (e.g. 'english', 'simple').",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview planned actions without applying any changes to the database.",
    ),
) -> None:
    """Fill tsvector sidecar columns with pre-computed search vectors using the specified PostgreSQL text search configuration."""
    try:
        conn, engine = setup_cli_cmd(
            console=console,
            dotenv=dotenv,
            engine_schema=engine_schema,
            db_schema=db_schema,
            command_name="fulltext populate",
            vocabulary_included=True,
            mode_label="dry-run" if dry_run else "apply",
        )
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
        help="Database schema to target (e.g. 'cdm5', 'vocab'). Sets search_path on PostgreSQL; not supported on SQLite.",
    ),
    drop_indexes: bool = typer.Option(
        True,
        "--drop-indexes/--no-drop-indexes",
        help="Drop managed GIN indexes before dropping the tsvector columns.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview planned actions without applying any changes to the database.",
    ),
) -> None:
    """Remove tsvector sidecar columns and their associated GIN indexes from vocabulary tables."""
    try:
        conn, engine = setup_cli_cmd(
            console=console,
            dotenv=dotenv,
            engine_schema=engine_schema,
            db_schema=db_schema,
            command_name="fulltext drop",
            vocabulary_included=True,
            mode_label="dry-run" if dry_run else "apply",
        )
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
