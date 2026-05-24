from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, Optional, TypeVar

import typer
from sqlalchemy.exc import SQLAlchemyError

from .tables import TableScope
from .ui import console, render_error, render_command_header
from ..db import build_engine, resolve_connection
from ..backends import BackendNotSupportedError


_F = TypeVar("_F", bound=Callable[..., Any])

# ── Shared injected CLI params ────────────────────────────────────────────────
# Built once and reused so every decorated command gets identical help text.

_DOTENV_PARAM = inspect.Parameter(
    "dotenv",
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
    default=typer.Option(
        None,
        help="Path to a .env file to load before resolving the connection. Overrides the saved DOTENV default.",
    ),
    annotation=Optional[str],
)
_ENGINE_SCHEMA_PARAM = inspect.Parameter(
    "engine_schema",
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
    default=typer.Option(
        None,
        help="Named engine configuration to use (e.g. 'cdm', 'results'). Resolves to the ENGINE_<SCHEMA> environment variable group.",
    ),
    annotation=Optional[str],
)
_DB_SCHEMA_PARAM = inspect.Parameter(
    "db_schema",
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
    default=typer.Option(
        None,
        help="Database schema to target (e.g. 'cdm5', 'vocab'). Sets search_path on PostgreSQL; not supported on SQLite.",
    ),
    annotation=Optional[str],
)
_DRY_RUN_PARAM = inspect.Parameter(
    "dry_run",
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
    default=typer.Option(
        False,
        "--dry-run",
        help="Preview planned actions without applying any changes to the database.",
    ),
    annotation=bool,
)

_INJECTED_NAMES = {"dotenv", "engine_schema", "db_schema"}


# ── Decorator ─────────────────────────────────────────────────────────────────

def omop_command(
    command_name: str,
    *,
    vocabulary_included: bool | None = None,
    dry_run: bool = False,
    mode_label: str | None = None,
) -> Callable[[_F], _F]:
    """Decorator that eliminates CLI boilerplate for every omop-alchemy command.

    Injects ``dotenv``, ``engine_schema``, ``db_schema`` (and optionally
    ``dry_run``) into the Typer CLI signature, calls :func:`setup_cli_cmd`,
    and wraps the body in ``try/except handle_error``.

    The decorated function must accept ``(conn, engine, ...)`` as its first
    two positional parameters; the decorator supplies them.  Any
    ``vocabulary_included`` or ``athena_source`` parameter declared in the
    function is automatically forwarded to :func:`setup_cli_cmd`.
    """
    def decorator(func: _F) -> _F:
        @functools.wraps(func)
        def wrapper(**kwargs: Any) -> Any:
            dotenv = kwargs.pop("dotenv", None)
            engine_schema = kwargs.pop("engine_schema", None)
            db_schema = kwargs.pop("db_schema", None)
            athena_source = kwargs.pop("athena_source", None)
            _dry_run = kwargs.pop("dry_run", False) if dry_run else False
            _vocab = kwargs.get("vocabulary_included", vocabulary_included)
            _mode = mode_label if mode_label is not None else ("dry-run" if _dry_run else "apply")
            try:
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
                        vocabulary_included=_vocab,
                        mode_label=_mode,
                    )
                )
                engine = build_engine(dotenv=conn.dotenv, engine_schema=conn.engine_schema)
                if dry_run:
                    return func(conn, engine, dry_run=_dry_run, **kwargs)  # type: ignore[arg-type]
                return func(conn, engine, **kwargs)  # type: ignore[arg-type]
            except Exception as exc:
                handle_error(exc)

        # Rebuild the Typer-visible signature:
        # • skip conn/engine (decorator supplies them)
        # • skip dotenv/engine_schema/db_schema (decorator injects them)
        # • skip dry_run if the decorator owns it (to avoid duplication)
        orig_params = list(inspect.signature(func).parameters.values())
        func_params = [
            p for p in orig_params[2:]
            if p.name not in _INJECTED_NAMES
            and not (dry_run and p.name == "dry_run")
        ]
        new_params = [_DOTENV_PARAM, _ENGINE_SCHEMA_PARAM, _DB_SCHEMA_PARAM] + func_params
        if dry_run:
            new_params.append(_DRY_RUN_PARAM)
        wrapper.__signature__ = inspect.signature(func).replace(parameters=new_params)  # type: ignore[attr-defined]

        return wrapper  # type: ignore[return-value]
    return decorator  # type: ignore[return-value]


# ── Helpers ───────────────────────────────────────────────────────────────────

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

