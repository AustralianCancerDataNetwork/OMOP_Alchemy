"""Shared utilities: the @omop_command decorator, error handling, and injected CLI parameter definitions."""

from __future__ import annotations

import functools
import inspect
from dataclasses import dataclass
from typing import Any, Callable, TypeVar

import typer
from sqlalchemy.exc import SQLAlchemyError

from .tables import TableScope
from .ui import console, render_error, render_command_header
from ..backends import BackendNotSupportedError


_F = TypeVar("_F", bound=Callable[..., Any])


@dataclass(frozen=True)
class _ConnContext:
    """Connection context derived from the oa_configurator resolved resource."""
    db_schema: str | None
    engine_url: str = ""
    athena_source: str | None = None  # from OmopAlchemyConfig.athena_source_path


# ── Decorator ─────────────────────────────────────────────────────────────────

def omop_command(
    command_name: str,
    *,
    vocabulary_included: bool | None = None,
    dry_run: bool = False,
    mode_label: str | None = None,
) -> Callable[[_F], _F]:
    """Decorator that eliminates CLI boilerplate for every omop-alchemy command.

    Resolves the database connection from oa_configurator, calls
    :func:`render_command_header`, and wraps the body in ``try/except handle_error``.

    The decorated function must accept ``(conn, engine, ...)`` as its first two
    positional parameters. The decorator supplies them.
    """
    def decorator(func: _F) -> _F:
        @functools.wraps(func)
        def wrapper(**kwargs: Any) -> Any:
            _dry_run = kwargs.pop("dry_run", False) if dry_run else False
            _vocab = kwargs.get("vocabulary_included", vocabulary_included)
            _mode = mode_label if mode_label is not None else ("dry-run" if _dry_run else "apply")
            try:
                from ..config import create_cdm_engine, get_cdm_context
                pkg_config, resolved = get_cdm_context()
                engine = create_cdm_engine(resolved)
                conn = _ConnContext(
                    db_schema=resolved.cdm_schema,
                    engine_url=engine.url.render_as_string(hide_password=True),
                    athena_source=pkg_config.athena_source_path,
                )
                console.print(
                    render_command_header(
                        command_name=command_name,
                        engine_url=conn.engine_url,
                        db_schema=conn.db_schema,
                        vocabulary_included=_vocab,
                        mode_label=_mode,
                    )
                )
                if dry_run:
                    return func(conn, engine, dry_run=_dry_run, **kwargs)  # type: ignore[arg-type]
                return func(conn, engine, **kwargs)  # type: ignore[arg-type]
            except Exception as exc:
                handle_error(exc)

        # Rebuild the Typer-visible signature:
        # • skip conn/engine (decorator supplies them)
        # • skip dry_run if the decorator owns it
        orig_params = list(inspect.signature(func).parameters.values())
        func_params = [
            p for p in orig_params[2:]
            if not (dry_run and p.name == "dry_run")
        ]
        new_params = func_params[:]
        if dry_run:
            new_params.append(
                inspect.Parameter(
                    "dry_run",
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    default=typer.Option(
                        False,
                        "--dry-run",
                        help="Preview planned actions without applying any changes to the database.",
                    ),
                    annotation=bool,
                )
            )
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
