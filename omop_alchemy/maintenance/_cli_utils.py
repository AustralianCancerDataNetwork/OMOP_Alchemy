from __future__ import annotations
import typer
from typing import Optional
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError
from rich.console import Console

from .tables import TableScope
from .ui import console, render_error, render_command_header
from ..db import build_engine, resolve_connection, ConnectionDefaults
from ..backends import BackendNotSupportedError


def handle_error(exc: Exception) -> None:
    if isinstance(exc, BackendNotSupportedError):
        console.print(render_error(f"Not supported: {exc}"))
        raise typer.Exit(code=1) from exc
    if isinstance(exc, RuntimeError):
        console.print(render_error(str(exc)))
        raise typer.Exit(code=1) from exc
    if isinstance(exc, SQLAlchemyError):
        detail = str(exc).strip()
        message = f"Database operation failed: {exc.__class__.__name__}."
        if detail:
            message = f"{message} Detail: {detail}"
        console.print(render_error(message))
        raise typer.Exit(code=1) from exc
    raise exc


def resolve_selection(
    *,
    scope: TableScope | None,
    tables: list[str] | None,
    default_scope: TableScope | None = None,
) -> tuple[TableScope | None, tuple[str, ...] | None]:
    if scope is not None and tables:
        raise RuntimeError("Use either `--scope` or `--table`, not both.")
    selected = tuple(tables) if tables else None
    if selected is not None:
        return None, selected
    return scope or default_scope, None


def setup_cli_cmd(
    *,
    console: Console,
    dotenv: Optional[str],
    engine_schema: Optional[str],
    db_schema: Optional[str],
    command_name: str,
    vocabulary_included: Optional[bool],
    mode_label: str,
    athena_source: Optional[str] = None,
) -> tuple[ConnectionDefaults, Engine]:
    """Convenience function to resolve connection, print command header, and build engine for CLI commands."""

    conn = resolve_connection(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
        athena_source=athena_source,
    )
    console.print(
        render_command_header(
            command_name=command_name,
            engine_schema=conn.engine_schema,
            db_schema=conn.db_schema,
            vocabulary_included=vocabulary_included,
            mode_label=mode_label,
        )
    )
    engine = build_engine(dotenv=conn.dotenv, engine_schema=conn.engine_schema)
    return conn, engine

    
