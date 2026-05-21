from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa
import typer

from ..backend_support import Dialect, POSTGRESQL_ONLY_HELP, require_backend
from ._cli_utils import build_engine, handle_error, resolve_connection, resolve_selection
from .tables import (
    TableCategory,
    TableScope,
    qualified_table_name,
    resolve_maintenance_tables,
    select_omop_tables,
)
from .ui import (
    console,
    render_analyze_note,
    render_analyze_results,
    render_analyze_summary,
    render_command_header,
    render_error,
    render_sequence_reset_results,
    render_sequence_reset_summary,
    render_truncate_note,
    render_truncate_results,
    render_truncate_summary,
)


# ---------------------------------------------------------------------------
# analyze_tables
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AnalyzeTableResult:
    table_name: str
    category: TableCategory
    model_name: str
    model_module: str
    operation: str
    status: str
    detail: str


def analyze_tables(
    engine: sa.Engine,
    *,
    db_schema: str | None = None,
    scope: TableScope | None = None,
    table_names: tuple[str, ...] | None = None,
    vacuum: bool = False,
    dry_run: bool = False,
) -> list[AnalyzeTableResult]:
    if scope is not None and table_names is not None:
        raise RuntimeError("Use either `scope` or `table_names`, not both.")

    require_backend(
        engine,
        feature="Table analysis",
        supported_dialects=(Dialect.POSTGRESQL, Dialect.SQLITE),
    )

    if vacuum and engine.dialect.name != Dialect.POSTGRESQL:
        raise RuntimeError(
            "VACUUM ANALYZE is only supported for PostgreSQL engines. "
            f"Current dialect: '{engine.dialect.name}'."
        )

    selected_tables = resolve_maintenance_tables(scope=scope, table_names=table_names)
    inspector = sa.inspect(engine)
    operation = "VACUUM ANALYZE" if vacuum else "ANALYZE"
    results: list[AnalyzeTableResult] = []

    connection_factory = (
        engine.connect().execution_options(isolation_level="AUTOCOMMIT")
        if vacuum
        else engine.connect()
    )

    with connection_factory as connection:
        for maintenance_table in selected_tables:
            if not inspector.has_table(maintenance_table.table_name, schema=db_schema):
                results.append(
                    AnalyzeTableResult(
                        table_name=maintenance_table.table_name,
                        category=maintenance_table.category,
                        model_name=maintenance_table.model_name,
                        model_module=maintenance_table.model_module,
                        operation=operation,
                        status="skipped",
                        detail="table not present in target database",
                    )
                )
                continue

            qualified_name = qualified_table_name(maintenance_table.table_name, db_schema)
            if not dry_run:
                connection.exec_driver_sql(f"{operation} {qualified_name}")

            results.append(
                AnalyzeTableResult(
                    table_name=maintenance_table.table_name,
                    category=maintenance_table.category,
                    model_name=maintenance_table.model_name,
                    model_module=maintenance_table.model_module,
                    operation=operation,
                    status="planned" if dry_run else "applied",
                    detail=(
                        f"{operation.lower()} would run"
                        if dry_run
                        else f"{operation.lower()} completed"
                    ),
                )
            )

    return results


# ---------------------------------------------------------------------------
# truncate_tables
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TruncateTableResult:
    table_name: str
    category: TableCategory
    model_name: str
    model_module: str
    row_count: int | None
    status: str
    detail: str


def _blocking_foreign_key_references(
    inspector: sa.Inspector,
    *,
    db_schema: str | None,
    selected_table_names: set[str],
) -> dict[str, set[str]]:
    blockers: dict[str, set[str]] = {}

    for table_name in inspector.get_table_names(schema=db_schema):
        if table_name in selected_table_names:
            continue

        for foreign_key in inspector.get_foreign_keys(table_name, schema=db_schema):
            referred_table = foreign_key.get("referred_table")
            if referred_table not in selected_table_names:
                continue
            blockers.setdefault(str(referred_table), set()).add(table_name)

    return blockers


def _format_blocking_reference_error(blockers: dict[str, set[str]]) -> str:
    blocker_parts = [
        f"{table_name} <- {', '.join(sorted(referencing_tables))}"
        for table_name, referencing_tables in sorted(blockers.items())
    ]
    preview = "; ".join(blocker_parts[:5])
    if len(blocker_parts) > 5:
        preview = f"{preview}; +{len(blocker_parts) - 5} more"

    return (
        "Truncation would be blocked by foreign key references from tables outside the current selection. "
        f"Blocking references: {preview}. "
        "Use `--cascade`, expand the table selection, or disable foreign key trigger enforcement first."
    )


def truncate_tables(
    engine: sa.Engine,
    *,
    db_schema: str | None = None,
    scope: TableScope | None = None,
    table_names: tuple[str, ...] | None = None,
    restart_identities: bool = False,
    cascade: bool = False,
    dry_run: bool = False,
) -> list[TruncateTableResult]:
    if scope is not None and table_names is not None:
        raise RuntimeError("Use either `scope` or `table_names`, not both.")
    if scope is None and table_names is None:
        raise RuntimeError("Select tables to truncate with `scope` or `table_names`.")

    require_backend(engine, feature="Table truncation", supported_dialects=(Dialect.POSTGRESQL,))

    selected_tables = resolve_maintenance_tables(scope=scope, table_names=table_names)
    inspector = sa.inspect(engine)
    results: list[TruncateTableResult] = []
    existing_tables: list[str] = []

    with engine.begin() as connection:
        for maintenance_table in selected_tables:
            if not inspector.has_table(maintenance_table.table_name, schema=db_schema):
                results.append(
                    TruncateTableResult(
                        table_name=maintenance_table.table_name,
                        category=maintenance_table.category,
                        model_name=maintenance_table.model_name,
                        model_module=maintenance_table.model_module,
                        row_count=None,
                        status="skipped",
                        detail="table not present in target database",
                    )
                )
                continue

            row_count = int(
                connection.exec_driver_sql(
                    f"SELECT COUNT(*) FROM {qualified_table_name(maintenance_table.table_name, db_schema)}"
                ).scalar_one()
            )
            existing_tables.append(maintenance_table.table_name)
            results.append(
                TruncateTableResult(
                    table_name=maintenance_table.table_name,
                    category=maintenance_table.category,
                    model_name=maintenance_table.model_name,
                    model_module=maintenance_table.model_module,
                    row_count=row_count,
                    status="planned" if dry_run else "applied",
                    detail="table would be truncated" if dry_run else "table truncated",
                )
            )

        if existing_tables and not dry_run and not cascade:
            blockers = _blocking_foreign_key_references(
                inspector,
                db_schema=db_schema,
                selected_table_names=set(existing_tables),
            )
            if blockers:
                raise RuntimeError(_format_blocking_reference_error(blockers))

        if existing_tables and not dry_run:
            truncate_sql = (
                "TRUNCATE TABLE "
                + ", ".join(
                    qualified_table_name(table_name, db_schema)
                    for table_name in existing_tables
                )
            )
            if restart_identities:
                truncate_sql += " RESTART IDENTITY"
            if cascade:
                truncate_sql += " CASCADE"
            connection.exec_driver_sql(truncate_sql)

    return results


# ---------------------------------------------------------------------------
# reset_sequences
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SequenceTarget:
    table_name: str
    category: TableCategory
    model_name: str
    model_module: str
    pk_column_name: str


@dataclass(frozen=True)
class SequenceResetResult:
    table_name: str
    category: TableCategory
    model_name: str
    model_module: str
    pk_column_name: str
    sequence_name: str | None
    next_value: int | None
    status: str
    detail: str


def collect_sequence_targets(
    *,
    vocabulary_included: bool = False,
) -> list[SequenceTarget]:
    targets: list[SequenceTarget] = []
    for table in select_omop_tables(
        vocabulary_included=vocabulary_included,
        require_single_integer_primary_key=True,
    ):
        pk_column_name = table.single_primary_key_name
        if pk_column_name is None:
            continue
        targets.append(
            SequenceTarget(
                table_name=table.table_name,
                category=table.category,
                model_name=table.model_name,
                model_module=table.model_module,
                pk_column_name=pk_column_name,
            )
        )
    return targets


def reset_model_sequences(
    engine: sa.Engine,
    *,
    db_schema: str | None = None,
    vocabulary_included: bool = False,
    dry_run: bool = False,
) -> list[SequenceResetResult]:
    require_backend(engine, feature="Sequence reset", supported_dialects=(Dialect.POSTGRESQL,))

    inspector = sa.inspect(engine)
    targets = collect_sequence_targets(vocabulary_included=vocabulary_included)
    results: list[SequenceResetResult] = []

    with engine.begin() as connection:
        for target in targets:
            if not inspector.has_table(target.table_name, schema=db_schema):
                continue

            fully_qualified_table_name = qualified_table_name(target.table_name, db_schema)
            sequence_name = connection.execute(
                sa.text("SELECT pg_get_serial_sequence(:table_name, :column_name)"),
                {
                    "table_name": fully_qualified_table_name,
                    "column_name": target.pk_column_name,
                },
            ).scalar_one_or_none()

            if sequence_name is None:
                results.append(
                    SequenceResetResult(
                        table_name=target.table_name,
                        category=target.category,
                        model_name=target.model_name,
                        model_module=target.model_module,
                        pk_column_name=target.pk_column_name,
                        sequence_name=None,
                        next_value=None,
                        status="skipped",
                        detail="no owned PostgreSQL sequence found",
                    )
                )
                continue

            current_max = connection.execute(
                sa.text(
                    f"SELECT COALESCE(MAX({target.pk_column_name}), 0) "
                    f"FROM {fully_qualified_table_name}"
                )
            ).scalar_one()
            next_value = int(current_max) + 1

            if not dry_run:
                connection.execute(
                    sa.text("SELECT setval(:sequence_name, :next_value, false)"),
                    {"sequence_name": sequence_name, "next_value": next_value},
                )

            results.append(
                SequenceResetResult(
                    table_name=target.table_name,
                    category=target.category,
                    model_name=target.model_name,
                    model_module=target.model_module,
                    pk_column_name=target.pk_column_name,
                    sequence_name=sequence_name,
                    next_value=next_value,
                    status="planned" if dry_run else "reset",
                    detail=(
                        "sequence would be reset from table max + 1"
                        if dry_run
                        else "sequence reset from table max + 1"
                    ),
                )
            )

    return results


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------

app = typer.Typer(rich_markup_mode="rich")


@app.command(
    "analyze-tables",
    help="Refresh planner statistics for selected ORM-managed tables.",
)
def analyze_tables_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    scope: TableScope | None = typer.Option(
        None,
        "--scope",
        help="Category scope to analyze. Defaults to all ORM-managed tables when omitted.",
        case_sensitive=False,
    ),
    table: list[str] | None = typer.Option(
        None,
        "--table",
        help="Specific ORM-managed table name to analyze. Repeat for multiple tables.",
    ),
    vacuum: bool = typer.Option(
        False,
        "--vacuum",
        help="Use VACUUM ANALYZE instead of ANALYZE. PostgreSQL only.",
    ),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    resolved_scope, resolved_tables = resolve_selection(
        scope=scope, tables=table, default_scope=TableScope.ALL
    )
    conn = resolve_connection(dotenv=dotenv, engine_schema=engine_schema, db_schema=db_schema)
    console.print(
        render_command_header(
            command_name="analyze-tables",
            engine_schema=conn.engine_schema,
            db_schema=conn.db_schema,
            vocabulary_included=None,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = build_engine(dotenv=conn.dotenv, engine_schema=conn.engine_schema)
        with console.status("Refreshing planner statistics for selected tables..."):
            results = analyze_tables(
                engine,
                db_schema=conn.db_schema,
                scope=resolved_scope,
                table_names=resolved_tables,
                vacuum=vacuum,
                dry_run=dry_run,
            )
        console.print(render_analyze_results(results))
        console.print(render_analyze_summary(results, dry_run=dry_run))
        console.print(render_analyze_note())
    except Exception as exc:
        handle_error(exc)


@app.command(
    "reset-sequences",
    help=f"Reset owned sequences from table max + 1. {POSTGRESQL_ONLY_HELP}",
)
def reset_sequences_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(False, "--vocab/--no-vocab"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    conn = resolve_connection(dotenv=dotenv, engine_schema=engine_schema, db_schema=db_schema)
    console.print(
        render_command_header(
            command_name="reset-sequences",
            engine_schema=conn.engine_schema,
            db_schema=conn.db_schema,
            vocabulary_included=vocabulary_included,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = build_engine(dotenv=conn.dotenv, engine_schema=conn.engine_schema)
        with console.status("Resetting PostgreSQL sequences..."):
            results = reset_model_sequences(
                engine,
                db_schema=conn.db_schema,
                vocabulary_included=vocabulary_included,
                dry_run=dry_run,
            )
        console.print(render_sequence_reset_results(results))
        console.print(render_sequence_reset_summary(results, dry_run=dry_run))
    except Exception as exc:
        handle_error(exc)


@app.command(
    "truncate-tables",
    help=f"Truncate selected ORM-managed tables. {POSTGRESQL_ONLY_HELP}",
)
def truncate_tables_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    scope: TableScope | None = typer.Option(
        None,
        "--scope",
        help="Category scope to truncate.",
        case_sensitive=False,
    ),
    table: list[str] | None = typer.Option(
        None,
        "--table",
        help="Specific ORM-managed table name to truncate. Repeat for multiple tables.",
    ),
    restart_identities: bool = typer.Option(
        False,
        "--restart-identities",
        help="Restart owned identities during truncation.",
    ),
    cascade: bool = typer.Option(
        False,
        "--cascade",
        help="Include dependent tables via PostgreSQL CASCADE.",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        help="Confirm that you want to apply this destructive operation.",
    ),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    resolved_scope, resolved_tables = resolve_selection(scope=scope, tables=table)
    if resolved_scope is None and resolved_tables is None:
        console.print(
            render_error("Select tables to truncate with `--scope` or one or more `--table` values.")
        )
        raise typer.Exit(code=1)
    if not dry_run and not yes:
        console.print(
            render_error("Truncation is destructive. Re-run with `--yes`, or use `--dry-run` first.")
        )
        raise typer.Exit(code=1)

    conn = resolve_connection(dotenv=dotenv, engine_schema=engine_schema, db_schema=db_schema)
    console.print(
        render_command_header(
            command_name="truncate-tables",
            engine_schema=conn.engine_schema,
            db_schema=conn.db_schema,
            vocabulary_included=None,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = build_engine(dotenv=conn.dotenv, engine_schema=conn.engine_schema)
        with console.status("Truncating selected tables..."):
            results = truncate_tables(
                engine,
                db_schema=conn.db_schema,
                scope=resolved_scope,
                table_names=resolved_tables,
                restart_identities=restart_identities,
                cascade=cascade,
                dry_run=dry_run,
            )
        console.print(render_truncate_results(results))
        console.print(
            render_truncate_summary(
                results,
                dry_run=dry_run,
                restart_identities=restart_identities,
                cascade=cascade,
            )
        )
        console.print(render_truncate_note())
    except Exception as exc:
        handle_error(exc)
