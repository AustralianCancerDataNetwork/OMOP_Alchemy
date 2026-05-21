from __future__ import annotations

import functools
import logging

import typer
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from omop_alchemy import create_engine_with_dependencies, get_engine_name, load_environment

from .cli_config import ConnectionDefaults, defaults_path, load_connection_defaults
from .tables import TableScope
from .ui import console, render_error


def resolve_connection(
    *,
    dotenv: str | None,
    engine_schema: str | None,
    db_schema: str | None,
    athena_source: str | None = None,
) -> ConnectionDefaults:
    saved = load_connection_defaults()
    return ConnectionDefaults(
        dotenv=dotenv if dotenv is not None else saved.dotenv,
        engine_schema=engine_schema if engine_schema is not None else saved.engine_schema,
        db_schema=db_schema if db_schema is not None else saved.db_schema,
        athena_source=athena_source if athena_source is not None else saved.athena_source,
    )


def build_engine(*, dotenv: str | None, engine_schema: str | None) -> Engine:
    load_environment(dotenv or "")
    return create_engine_with_dependencies(get_engine_name(engine_schema), future=True)


def handle_error(exc: Exception) -> None:
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


@functools.lru_cache(maxsize=None)
def configure_logging() -> None:
    mode = (load_connection_defaults().logging or "file").strip().lower()
    if mode not in {"file", "console", "off"}:
        mode = "file"
    if mode == "off":
        return

    formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    if mode == "file":
        log_path = defaults_path().parent / "logging" / "omop-alchemy.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handler: logging.Handler = logging.FileHandler(log_path, encoding="utf-8")
    else:
        handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    if root_logger.level in {logging.NOTSET, logging.WARNING, logging.ERROR, logging.CRITICAL}:
        root_logger.setLevel(logging.INFO)
