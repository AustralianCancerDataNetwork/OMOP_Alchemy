from __future__ import annotations

import typer
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from omop_alchemy import create_engine_with_dependencies, get_engine_name, load_environment
from omop_alchemy.cdm.handlers.fulltext import (
    drop_fulltext_columns,
    install_fulltext_columns,
    populate_fulltext_columns,
)

from .analyze_tables import analyze_tables
from .backup import BackupFormat, RestoreFormat, create_database_backup, restore_database_backup
from .backend_support import POSTGRESQL_ONLY_HELP
from .create_tables import create_missing_tables
from .data_summary import collect_data_summary
from .defaults import (
    ConnectionDefaults,
    clear_connection_defaults,
    defaults_path,
    load_connection_defaults,
    save_connection_defaults,
)
from .foreign_keys import (
    ForeignKeyAction,
    collect_foreign_key_trigger_status,
    manage_foreign_key_triggers,
    validate_foreign_key_constraints,
)
from .doctor import collect_doctor_report
from .help import install_help_customizations
from .info import collect_maintenance_info
from .indexes import IndexAction, manage_indexes
from .load_vocab import load_vocab_source
from .reconcile import reconcile_schema
from .reset_sequences import reset_model_sequences
from .tables import TableScope
from .truncate_tables import truncate_tables
from .ui import (
    render_analyze_note,
    render_analyze_results,
    render_analyze_summary,
    render_backup_result,
    render_backup_summary,
    console,
    render_command_header,
    render_connection_defaults,
    render_data_summary_results,
    render_data_summary_summary,
    render_doctor_checks,
    render_doctor_recommendations,
    render_doctor_summary,
    render_error,
    render_foreign_key_note,
    render_foreign_key_results,
    render_foreign_key_status_results,
    render_foreign_key_status_summary,
    render_foreign_key_summary,
    render_foreign_key_validation_issues,
    render_foreign_key_validation_results,
    render_foreign_key_validation_summary,
    render_fulltext_results,
    render_fulltext_summary,
    render_info_command_support,
    render_info_database,
    render_info_dependencies,
    render_info_environment,
    render_info_summary,
    render_index_note,
    render_index_results,
    render_index_summary,
    render_vocab_load_results,
    render_vocab_load_summary,
    render_reconciliation_issues,
    render_reconciliation_results,
    render_reconciliation_summary,
    render_restore_result,
    render_restore_summary,
    render_sequence_reset_results,
    render_sequence_reset_summary,
    render_table_creation_results,
    render_table_creation_summary,
    render_truncate_note,
    render_truncate_results,
    render_truncate_summary,
)
install_help_customizations()

app = typer.Typer(
    help=(
        "OMOP Alchemy maintenance utilities.\n\n"
        "PostgreSQL-only commands: reset-sequences, truncate-tables, "
        "foreign-keys, backup-database, restore-database, fulltext."
    ),
    rich_markup_mode="rich",
)
config_app = typer.Typer(
    help="Manage persisted maintenance CLI connection overrides.",
    rich_markup_mode="rich",
)
foreign_keys_app = typer.Typer(
    help=f"Manage PostgreSQL RI trigger enforcement for OMOP tables. {POSTGRESQL_ONLY_HELP}",
    rich_markup_mode="rich",
)
indexes_app = typer.Typer(
    help="Manage ORM-defined secondary indexes.",
    rich_markup_mode="rich",
)
fulltext_app = typer.Typer(
    help=f"Manage PostgreSQL full-text sidecar tsvector columns for OMOP vocabulary tables. {POSTGRESQL_ONLY_HELP}",
    rich_markup_mode="rich",
)
app.add_typer(config_app, name="config")
app.add_typer(foreign_keys_app, name="foreign-keys")
app.add_typer(indexes_app, name="indexes")
app.add_typer(fulltext_app, name="fulltext")


def main() -> None:
    app()


def _resolve_connection_context(
    *,
    dotenv: str | None,
    engine_schema: str | None,
    db_schema: str | None,
    athena_source: str | None = None,
) -> ConnectionDefaults:
    saved_defaults = load_connection_defaults()
    return ConnectionDefaults(
        dotenv=dotenv if dotenv is not None else saved_defaults.dotenv,
        engine_schema=(
            engine_schema if engine_schema is not None else saved_defaults.engine_schema
        ),
        db_schema=db_schema if db_schema is not None else saved_defaults.db_schema,
        athena_source=(
            athena_source if athena_source is not None else saved_defaults.athena_source
        ),
    )


def _build_engine(*, dotenv: str | None, engine_schema: str | None) -> Engine:
    load_environment(dotenv or "")
    return create_engine_with_dependencies(
        get_engine_name(engine_schema),
        future=True,
    )


def _handle_cli_error(exc: Exception) -> None:
    if isinstance(exc, RuntimeError):
        console.print(render_error(str(exc)))
        raise typer.Exit(code=1) from exc

    if isinstance(exc, SQLAlchemyError):
        detail = str(exc).strip()
        message = f"Database operation failed: {exc.__class__.__name__}."
        if detail:
            message = f"{message} Detail: {detail}"
        console.print(
            render_error(message)
        )
        raise typer.Exit(code=1) from exc

    raise exc


def _resolve_selection(
    *,
    scope: TableScope | None,
    tables: list[str] | None,
    default_scope: TableScope | None = None,
) -> tuple[TableScope | None, tuple[str, ...] | None]:
    if scope is not None and tables:
        raise RuntimeError("Use either `--scope` or `--table`, not both.")

    selected_tables = tuple(tables) if tables else None
    if selected_tables is not None:
        return None, selected_tables

    return scope or default_scope, None


@config_app.command("show")
def config_show_command() -> None:
    defaults = load_connection_defaults()
    console.print(
        render_connection_defaults(
            defaults,
            path=str(defaults_path()),
        )
    )


@config_app.command("set-overrides")
def config_set_overrides_command(
    dotenv: str | None = typer.Option(None, help="Override dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Override engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Override database schema."),
    athena_source: str | None = typer.Option(None, help="Override path to unzipped Athena vocabulary files."),
) -> None:
    current = load_connection_defaults()
    updated = current.with_updates(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
        athena_source=athena_source,
    )
    path = save_connection_defaults(updated)
    console.print(
        render_connection_defaults(
            updated,
            path=str(path),
            title="Saved Overrides",
        )
    )


@config_app.command("clear-overrides")
def config_clear_overrides_command(
    dotenv: bool = typer.Option(False, "--dotenv", help="Clear overridden dotenv."),
    engine_schema: bool = typer.Option(False, "--engine-schema", help="Clear overridden engine schema."),
    db_schema: bool = typer.Option(False, "--db-schema", help="Clear overridden database schema."),
    athena_source: bool = typer.Option(False, "--athena-source", help="Clear overridden Athena source path."),
) -> None:
    path = clear_connection_defaults(
        clear_dotenv=dotenv,
        clear_engine_schema=engine_schema,
        clear_db_schema=db_schema,
        clear_athena_source=athena_source,
    )

    if path is None:
        console.print(
            render_connection_defaults(
                ConnectionDefaults(),
                path=str(defaults_path()),
                title="Overrides Already Clear",
            )
        )
        return

    console.print(
        render_connection_defaults(
            load_connection_defaults(),
            path=str(path),
            title="Overrides Cleared",
        )
    )


@app.command(
    "info",
    help="Inspect maintenance CLI readiness, backend compatibility, and current installation state.",
)
def info_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(True, "--vocabulary-included/--no-vocabulary-included"),
) -> None:
    connection_defaults = _resolve_connection_context(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
    )
    console.print(
        render_command_header(
            command_name="info",
            engine_schema=connection_defaults.engine_schema,
            db_schema=connection_defaults.db_schema,
            vocabulary_included=vocabulary_included,
            mode_label="inspect",
        )
    )
    try:
        load_environment(connection_defaults.dotenv or "")
        with console.status("Inspecting maintenance environment..."):
            info = collect_maintenance_info(
                dotenv=connection_defaults.dotenv,
                engine_schema=connection_defaults.engine_schema,
                db_schema=connection_defaults.db_schema,
                vocabulary_included=vocabulary_included,
            )
        console.print(render_info_environment(info))
        console.print(render_info_database(info))
        console.print(render_info_dependencies(info))
        console.print(render_info_command_support(info.command_support))
        console.print(render_info_summary(info))
    except Exception as exc:
        _handle_cli_error(exc)


@app.command(
    "doctor",
    help="Run a read-only maintenance health check across connection readiness, schema drift, and FK state.",
)
def doctor_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(False, "--vocabulary-included/--no-vocabulary-included"),
    deep: bool = typer.Option(False, "--deep", help="Include heavier checks such as PostgreSQL foreign key validation."),
) -> None:
    connection_defaults = _resolve_connection_context(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
    )
    console.print(
        render_command_header(
            command_name="doctor",
            engine_schema=connection_defaults.engine_schema,
            db_schema=connection_defaults.db_schema,
            vocabulary_included=vocabulary_included,
            mode_label="inspect",
        )
    )
    try:
        load_environment(connection_defaults.dotenv or "")
        with console.status("Running maintenance doctor checks..."):
            report = collect_doctor_report(
                dotenv=connection_defaults.dotenv,
                engine_schema=connection_defaults.engine_schema,
                db_schema=connection_defaults.db_schema,
                vocabulary_included=vocabulary_included,
                deep=deep,
            )
        console.print(render_info_environment(report.info))
        console.print(render_info_database(report.info))
        console.print(render_doctor_checks(report.checks))
        if deep and report.foreign_key_validation is not None:
            console.print(
                render_foreign_key_validation_issues(
                    report.foreign_key_validation.violations
                )
            )
        console.print(render_doctor_recommendations(report.recommendations))
        console.print(render_doctor_summary(report, deep=deep))
    except Exception as exc:
        _handle_cli_error(exc)


@app.command(
    "backup-database",
    help=f"Create a PostgreSQL dump artifact that can be restored into another environment. {POSTGRESQL_ONLY_HELP}",
)
def backup_database_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Optional schema-limited backup."),
    output_path: str | None = typer.Option(None, help="Backup artifact path. Defaults to a timestamped file in the current directory."),
    format: BackupFormat = typer.Option(BackupFormat.CUSTOM, help="Backup format."),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    connection_defaults = _resolve_connection_context(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
    )
    console.print(
        render_command_header(
            command_name="backup-database",
            engine_schema=connection_defaults.engine_schema,
            db_schema=connection_defaults.db_schema,
            vocabulary_included=None,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = _build_engine(
            dotenv=connection_defaults.dotenv,
            engine_schema=connection_defaults.engine_schema,
        )
        with console.status("Creating restore-ready PostgreSQL backup..."):
            result = create_database_backup(
                engine,
                output_path=output_path,
                format=format,
                db_schema=connection_defaults.db_schema,
                dry_run=dry_run,
            )
        console.print(render_backup_result(result))
        console.print(render_backup_summary(result, dry_run=dry_run))
    except Exception as exc:
        _handle_cli_error(exc)


@app.command(
    "restore-database",
    help=f"Restore a PostgreSQL backup artifact into the configured target database. {POSTGRESQL_ONLY_HELP}",
)
def restore_database_command(
    input_path: str = typer.Argument(..., help="Backup artifact path to restore."),
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Optional schema-limited restore for custom-format dumps."),
    format: RestoreFormat = typer.Option(RestoreFormat.AUTO, help="Restore format. Defaults to auto-detect from the input file."),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    connection_defaults = _resolve_connection_context(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
    )
    console.print(
        render_command_header(
            command_name="restore-database",
            engine_schema=connection_defaults.engine_schema,
            db_schema=connection_defaults.db_schema,
            vocabulary_included=None,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = _build_engine(
            dotenv=connection_defaults.dotenv,
            engine_schema=connection_defaults.engine_schema,
        )
        with console.status("Restoring PostgreSQL backup artifact..."):
            result = restore_database_backup(
                engine,
                input_path=input_path,
                format=format,
                db_schema=connection_defaults.db_schema,
                dry_run=dry_run,
            )
        console.print(render_restore_result(result))
        console.print(render_restore_summary(result, dry_run=dry_run))
    except Exception as exc:
        _handle_cli_error(exc)


@app.command(
    "reconcile-schema",
    help="Compare ORM-managed SQLAlchemy metadata against the current target database schema.",
)
def reconcile_schema_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(False, "--vocabulary-included/--no-vocabulary-included"),
) -> None:
    connection_defaults = _resolve_connection_context(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
    )
    console.print(
        render_command_header(
            command_name="reconcile-schema",
            engine_schema=connection_defaults.engine_schema,
            db_schema=connection_defaults.db_schema,
            vocabulary_included=vocabulary_included,
            mode_label="inspect",
        )
    )
    try:
        engine = _build_engine(
            dotenv=connection_defaults.dotenv,
            engine_schema=connection_defaults.engine_schema,
        )
        with console.status("Reconciling ORM metadata against target database schema..."):
            report = reconcile_schema(
                engine,
                db_schema=connection_defaults.db_schema,
                vocabulary_included=vocabulary_included,
            )
        console.print(render_reconciliation_results(report.table_results))
        console.print(render_reconciliation_issues(report.issues))
        console.print(render_reconciliation_summary(report))
    except Exception as exc:
        _handle_cli_error(exc)


@app.command(
    "reset-sequences",
    help=f"Reset owned sequences from table max + 1. {POSTGRESQL_ONLY_HELP}",
)
def reset_sequences_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(False, "--vocabulary-included/--no-vocabulary-included"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    connection_defaults = _resolve_connection_context(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
    )
    console.print(
        render_command_header(
            command_name="reset-sequences",
            engine_schema=connection_defaults.engine_schema,
            db_schema=connection_defaults.db_schema,
            vocabulary_included=vocabulary_included,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = _build_engine(
            dotenv=connection_defaults.dotenv,
            engine_schema=connection_defaults.engine_schema,
        )
        with console.status("Resetting PostgreSQL sequences..."):
            results = reset_model_sequences(
                engine,
                db_schema=connection_defaults.db_schema,
                vocabulary_included=vocabulary_included,
                dry_run=dry_run,
            )
        console.print(render_sequence_reset_results(results))
        console.print(render_sequence_reset_summary(results, dry_run=dry_run))
    except Exception as exc:
        _handle_cli_error(exc)


@app.command(
    "data-summary",
    help="Summarise ORM-managed OMOP tables present in the target database.",
)
def data_summary_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(False, "--vocabulary-included/--no-vocabulary-included"),
    include_missing: bool = typer.Option(False, "--include-missing"),
) -> None:
    connection_defaults = _resolve_connection_context(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
    )
    console.print(
        render_command_header(
            command_name="data-summary",
            engine_schema=connection_defaults.engine_schema,
            db_schema=connection_defaults.db_schema,
            vocabulary_included=vocabulary_included,
            mode_label="inspect",
        )
    )
    try:
        engine = _build_engine(
            dotenv=connection_defaults.dotenv,
            engine_schema=connection_defaults.engine_schema,
        )
        with console.status("Collecting table summary..."):
            results = collect_data_summary(
                engine,
                db_schema=connection_defaults.db_schema,
                vocabulary_included=vocabulary_included,
                existing_only=not include_missing,
            )
        console.print(render_data_summary_results(results))
        console.print(render_data_summary_summary(results))
    except Exception as exc:
        _handle_cli_error(exc)


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
    resolved_scope, resolved_tables = _resolve_selection(
        scope=scope,
        tables=table,
        default_scope=TableScope.ALL,
    )
    connection_defaults = _resolve_connection_context(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
    )
    console.print(
        render_command_header(
            command_name="analyze-tables",
            engine_schema=connection_defaults.engine_schema,
            db_schema=connection_defaults.db_schema,
            vocabulary_included=None,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = _build_engine(
            dotenv=connection_defaults.dotenv,
            engine_schema=connection_defaults.engine_schema,
        )
        with console.status("Refreshing planner statistics for selected tables..."):
            results = analyze_tables(
                engine,
                db_schema=connection_defaults.db_schema,
                scope=resolved_scope,
                table_names=resolved_tables,
                vacuum=vacuum,
                dry_run=dry_run,
            )
        console.print(render_analyze_results(results))
        console.print(render_analyze_summary(results, dry_run=dry_run))
        console.print(render_analyze_note())
    except Exception as exc:
        _handle_cli_error(exc)


@app.command(
    "create-missing-tables",
    help="Create missing ORM-managed OMOP tables from metadata.",
)
def create_missing_tables_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(True, "--vocabulary-included/--no-vocabulary-included"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    connection_defaults = _resolve_connection_context(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
    )
    console.print(
        render_command_header(
            command_name="create-missing-tables",
            engine_schema=connection_defaults.engine_schema,
            db_schema=connection_defaults.db_schema,
            vocabulary_included=vocabulary_included,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = _build_engine(
            dotenv=connection_defaults.dotenv,
            engine_schema=connection_defaults.engine_schema,
        )
        with console.status("Creating missing tables..."):
            results = create_missing_tables(
                engine,
                db_schema=connection_defaults.db_schema,
                vocabulary_included=vocabulary_included,
                dry_run=dry_run,
            )
        console.print(render_table_creation_results(results))
        console.print(render_table_creation_summary(results, dry_run=dry_run))
    except Exception as exc:
        _handle_cli_error(exc)


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
    resolved_scope, resolved_tables = _resolve_selection(
        scope=scope,
        tables=table,
    )
    if resolved_scope is None and resolved_tables is None:
        console.print(
            render_error(
                "Select tables to truncate with `--scope` or one or more `--table` values."
            )
        )
        raise typer.Exit(code=1)
    if not dry_run and not yes:
        console.print(
            render_error(
                "Truncation is destructive. Re-run with `--yes`, or use `--dry-run` first."
            )
        )
        raise typer.Exit(code=1)

    connection_defaults = _resolve_connection_context(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
    )
    console.print(
        render_command_header(
            command_name="truncate-tables",
            engine_schema=connection_defaults.engine_schema,
            db_schema=connection_defaults.db_schema,
            vocabulary_included=None,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = _build_engine(
            dotenv=connection_defaults.dotenv,
            engine_schema=connection_defaults.engine_schema,
        )
        with console.status("Truncating selected tables..."):
            results = truncate_tables(
                engine,
                db_schema=connection_defaults.db_schema,
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
        _handle_cli_error(exc)


@app.command(
    "load-vocab-source",
    help="Load Athena vocabulary CSV files from a configured source path using the ORM staged CSV loader.",
)
def load_vocab_source_command(
    athena_source: str | None = typer.Option(None, help="Path to unzipped Athena vocabulary CSV files."),
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override. PostgreSQL only; uses search_path for ORM CSV loading."),
    merge_strategy: str = typer.Option(
        "upsert",
        help="CSV merge strategy passed to the ORM loader. Defaults to non-destructive `upsert`; use `replace` to overwrite matching primary keys.",
    ),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    connection_defaults = _resolve_connection_context(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
        athena_source=athena_source,
    )
    console.print(
        render_command_header(
            command_name="load-vocab-source",
            engine_schema=connection_defaults.engine_schema,
            db_schema=connection_defaults.db_schema,
            vocabulary_included=True,
            mode_label="dry-run" if dry_run else "apply",
        )
    )

    if connection_defaults.athena_source is None:
        console.print(
            render_error(
                "No Athena vocabulary source path is configured. "
                "Set it with `omop-maint config set-overrides --athena-source <path>` "
                "or pass `--athena-source`."
            )
        )
        raise typer.Exit(code=1)

    try:
        engine = _build_engine(
            dotenv=connection_defaults.dotenv,
            engine_schema=connection_defaults.engine_schema,
        )
        with console.status("Loading Athena vocabulary source via ORM staged CSV loader..."):
            report = load_vocab_source(
                engine,
                source_path=connection_defaults.athena_source,
                db_schema=connection_defaults.db_schema,
                dry_run=dry_run,
                merge_strategy=merge_strategy,
            )
        console.print(render_vocab_load_results(report.results))
        console.print(render_vocab_load_summary(report, dry_run=dry_run))
    except Exception as exc:
        _handle_cli_error(exc)


def _foreign_key_command(
    *,
    action: ForeignKeyAction,
    strict: bool,
    dotenv: str | None,
    engine_schema: str | None,
    db_schema: str | None,
    vocabulary_included: bool,
    dry_run: bool,
) -> None:
    connection_defaults = _resolve_connection_context(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
    )
    console.print(
        render_command_header(
            command_name=(
                f"foreign-keys {action.value} --strict"
                if action is ForeignKeyAction.ENABLE and strict
                else f"foreign-keys {action.value}"
            ),
            engine_schema=connection_defaults.engine_schema,
            db_schema=connection_defaults.db_schema,
            vocabulary_included=vocabulary_included,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = _build_engine(
            dotenv=connection_defaults.dotenv,
            engine_schema=connection_defaults.engine_schema,
        )
        with console.status(
            "Validating and enabling PostgreSQL foreign key trigger enforcement..."
            if action is ForeignKeyAction.ENABLE and strict
            else "Managing PostgreSQL foreign key trigger enforcement..."
        ):
            results = manage_foreign_key_triggers(
                engine,
                action=action,
                db_schema=connection_defaults.db_schema,
                vocabulary_included=vocabulary_included,
                dry_run=dry_run,
                strict=strict,
            )
        console.print(render_foreign_key_results(results))
        console.print(render_foreign_key_summary(results, dry_run=dry_run))
        console.print(render_foreign_key_note(action, strict=strict))
    except Exception as exc:
        _handle_cli_error(exc)


@foreign_keys_app.command("disable")
def disable_foreign_keys_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(False, "--vocabulary-included/--no-vocabulary-included"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    _foreign_key_command(
        action=ForeignKeyAction.DISABLE,
        strict=False,
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
        vocabulary_included=vocabulary_included,
        dry_run=dry_run,
    )


@foreign_keys_app.command("enable")
def enable_foreign_keys_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(False, "--vocabulary-included/--no-vocabulary-included"),
    strict: bool = typer.Option(False, "--strict", help="Validate all selected foreign key relationships before enabling trigger enforcement."),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    _foreign_key_command(
        action=ForeignKeyAction.ENABLE,
        strict=strict,
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
        vocabulary_included=vocabulary_included,
        dry_run=dry_run,
    )


@foreign_keys_app.command("status")
def foreign_key_status_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(False, "--vocabulary-included/--no-vocabulary-included"),
) -> None:
    connection_defaults = _resolve_connection_context(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
    )
    console.print(
        render_command_header(
            command_name="foreign-keys status",
            engine_schema=connection_defaults.engine_schema,
            db_schema=connection_defaults.db_schema,
            vocabulary_included=vocabulary_included,
            mode_label="inspect",
        )
    )
    try:
        engine = _build_engine(
            dotenv=connection_defaults.dotenv,
            engine_schema=connection_defaults.engine_schema,
        )
        with console.status("Inspecting foreign key trigger status..."):
            results = collect_foreign_key_trigger_status(
                engine,
                db_schema=connection_defaults.db_schema,
                vocabulary_included=vocabulary_included,
            )
        console.print(render_foreign_key_status_results(results))
        console.print(render_foreign_key_status_summary(results))
    except Exception as exc:
        _handle_cli_error(exc)


@foreign_keys_app.command(
    "validate",
    help="Validate selected foreign key relationships and report violating constraints.",
)
def foreign_key_validate_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(False, "--vocabulary-included/--no-vocabulary-included"),
) -> None:
    connection_defaults = _resolve_connection_context(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
    )
    console.print(
        render_command_header(
            command_name="foreign-keys validate",
            engine_schema=connection_defaults.engine_schema,
            db_schema=connection_defaults.db_schema,
            vocabulary_included=vocabulary_included,
            mode_label="inspect",
        )
    )
    try:
        engine = _build_engine(
            dotenv=connection_defaults.dotenv,
            engine_schema=connection_defaults.engine_schema,
        )
        with console.status("Validating selected foreign key relationships..."):
            report = validate_foreign_key_constraints(
                engine,
                db_schema=connection_defaults.db_schema,
                vocabulary_included=vocabulary_included,
            )
        console.print(render_foreign_key_validation_results(report.results))
        console.print(render_foreign_key_validation_issues(report.violations))
        console.print(render_foreign_key_validation_summary(report))
    except Exception as exc:
        _handle_cli_error(exc)


def _index_command(
    *,
    action: IndexAction,
    dotenv: str | None,
    engine_schema: str | None,
    db_schema: str | None,
    vocabulary_included: bool,
    dry_run: bool,
) -> None:
    connection_defaults = _resolve_connection_context(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
    )
    console.print(
        render_command_header(
            command_name=f"indexes {action.value}",
            engine_schema=connection_defaults.engine_schema,
            db_schema=connection_defaults.db_schema,
            vocabulary_included=vocabulary_included,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = _build_engine(
            dotenv=connection_defaults.dotenv,
            engine_schema=connection_defaults.engine_schema,
        )
        with console.status("Managing metadata-defined indexes..."):
            results = manage_indexes(
                engine,
                action=action,
                db_schema=connection_defaults.db_schema,
                vocabulary_included=vocabulary_included,
                dry_run=dry_run,
            )
        console.print(render_index_results(results))
        console.print(render_index_summary(results, dry_run=dry_run))
        console.print(render_index_note(action))
    except Exception as exc:
        _handle_cli_error(exc)


@indexes_app.command("disable")
def disable_indexes_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(False, "--vocabulary-included/--no-vocabulary-included"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    _index_command(
        action=IndexAction.DISABLE,
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
        vocabulary_included=vocabulary_included,
        dry_run=dry_run,
    )


@indexes_app.command("enable")
def enable_indexes_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(False, "--vocabulary-included/--no-vocabulary-included"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    _index_command(
        action=IndexAction.ENABLE,
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
        vocabulary_included=vocabulary_included,
        dry_run=dry_run,
    )


def _fulltext_command(
    *,
    action: str,
    dotenv: str | None,
    engine_schema: str | None,
    db_schema: str | None,
    dry_run: bool,
    regconfig: str | None = None,
    create_indexes: bool | None = None,
    fastupdate: bool | None = None,
    drop_indexes: bool | None = None,
) -> None:
    connection_defaults = _resolve_connection_context(
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
    )
    console.print(
        render_command_header(
            command_name=f"fulltext {action}",
            engine_schema=connection_defaults.engine_schema,
            db_schema=connection_defaults.db_schema,
            vocabulary_included=True,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = _build_engine(
            dotenv=connection_defaults.dotenv,
            engine_schema=connection_defaults.engine_schema,
        )
        with console.status("Managing PostgreSQL full-text sidecar columns..."):
            if action == "install":
                results = install_fulltext_columns(
                    engine,
                    db_schema=connection_defaults.db_schema,
                    create_indexes=True if create_indexes is None else create_indexes,
                    fastupdate=False if fastupdate is None else fastupdate,
                    dry_run=dry_run,
                )
            elif action == "populate":
                results = populate_fulltext_columns(
                    engine,
                    db_schema=connection_defaults.db_schema,
                    regconfig="english" if regconfig is None else regconfig,
                    dry_run=dry_run,
                )
            else:
                results = drop_fulltext_columns(
                    engine,
                    db_schema=connection_defaults.db_schema,
                    drop_indexes=True if drop_indexes is None else drop_indexes,
                    dry_run=dry_run,
                )
        console.print(render_fulltext_results(results))
        console.print(
            render_fulltext_summary(
                results,
                action=action,
                dry_run=dry_run,
            )
        )
    except Exception as exc:
        _handle_cli_error(exc)


@fulltext_app.command("install")
def install_fulltext_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    create_indexes: bool = typer.Option(True, "--create-indexes/--no-create-indexes", help="Create GIN indexes alongside the tsvector columns."),
    fastupdate: bool = typer.Option(False, "--fastupdate/--no-fastupdate", help="Set PostgreSQL GIN fastupdate on created indexes."),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    _fulltext_command(
        action="install",
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
        dry_run=dry_run,
        create_indexes=create_indexes,
        fastupdate=fastupdate,
    )


@fulltext_app.command("populate")
def populate_fulltext_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    regconfig: str = typer.Option("english", help="PostgreSQL text search configuration to use for vector population."),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    _fulltext_command(
        action="populate",
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
        dry_run=dry_run,
        regconfig=regconfig,
    )


@fulltext_app.command("drop")
def drop_fulltext_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    drop_indexes: bool = typer.Option(True, "--drop-indexes/--no-drop-indexes", help="Drop managed GIN indexes before dropping the tsvector columns."),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    _fulltext_command(
        action="drop",
        dotenv=dotenv,
        engine_schema=engine_schema,
        db_schema=db_schema,
        dry_run=dry_run,
        drop_indexes=drop_indexes,
    )


if __name__ == "__main__":
    main()
