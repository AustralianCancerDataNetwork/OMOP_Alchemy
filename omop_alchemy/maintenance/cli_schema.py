from __future__ import annotations

from dataclasses import dataclass
import importlib.metadata
import importlib.util
import os
import shutil

import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError
import typer

from omop_alchemy import create_engine_with_dependencies, get_engine_name, load_environment

from ..backend_support import Dialect, backend_label
from ._cli_utils import build_engine, handle_error, resolve_connection
from .cli_config import defaults_path
from .cli_foreign_keys import (
    ForeignKeyStatusResult,
    ForeignKeyValidationReport,
    collect_foreign_key_trigger_status,
    validate_foreign_key_constraints,
)
from .cli_indexes import _cluster_target_name
from .tables import (
    MaintenanceTable,
    TableCategory,
    TableScope,
    collect_maintenance_tables,
    missing_maintenance_tables,
    qualified_table_name,
    schema_adjusted_metadata,
    select_maintenance_tables,
    select_omop_tables,
)
from .ui import (
    console,
    render_command_header,
    render_data_summary_results,
    render_data_summary_summary,
    render_doctor_checks,
    render_doctor_recommendations,
    render_doctor_summary,
    render_foreign_key_validation_issues,
    render_info_command_support,
    render_info_database,
    render_info_dependencies,
    render_info_environment,
    render_info_summary,
    render_reconciliation_issues,
    render_reconciliation_results,
    render_reconciliation_summary,
    render_table_creation_results,
    render_table_creation_summary,
)


# ---------------------------------------------------------------------------
# info
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DependencyStatus:
    name: str
    installed: bool
    version: str | None


@dataclass(frozen=True)
class CommandSupport:
    command_name: str
    requirement: str
    status: str
    detail: str


@dataclass(frozen=True)
class MaintenanceInfo:
    package_version: str
    cli_path: str | None
    pg_dump_path: str | None
    pg_restore_path: str | None
    psql_path: str | None
    defaults_file: str
    defaults_exists: bool
    dotenv_path: str | None
    dotenv_exists: bool | None
    engine_schema: str | None
    db_schema: str | None
    engine_url: str | None
    backend: str | None
    engine_created: bool
    engine_error: str | None
    connection_ready: bool
    connection_error: str | None
    managed_table_count: int
    existing_table_count: int | None
    missing_table_count: int | None
    vocabulary_included: bool
    dependencies: tuple[DependencyStatus, ...]
    command_support: tuple[CommandSupport, ...]


def _package_version() -> str:
    return importlib.metadata.version("omop-alchemy")


def _dependency_status(distribution_name: str, module_name: str) -> DependencyStatus:
    installed = importlib.util.find_spec(module_name) is not None
    version: str | None = None
    if installed:
        try:
            version = importlib.metadata.version(distribution_name)
        except importlib.metadata.PackageNotFoundError:
            version = None
    return DependencyStatus(name=distribution_name, installed=installed, version=version)


def _external_dependency_status(name: str, executable_name: str) -> DependencyStatus:
    return DependencyStatus(
        name=name,
        installed=shutil.which(executable_name) is not None,
        version=None,
    )


def _command_support_for_unavailable_engine(detail: str) -> tuple[CommandSupport, ...]:
    blocked = "blocked"
    return (
        CommandSupport("doctor", "Any SQLAlchemy backend", blocked, detail),
        CommandSupport("data-summary", "Any SQLAlchemy backend", blocked, detail),
        CommandSupport("analyze-tables", "PostgreSQL/SQLite", blocked, detail),
        CommandSupport("create-missing-tables", "Any SQLAlchemy backend", blocked, detail),
        CommandSupport("indexes disable", "Any SQLAlchemy backend", blocked, detail),
        CommandSupport("indexes enable", "Any SQLAlchemy backend", blocked, detail),
        CommandSupport("reconcile-schema", "Any SQLAlchemy backend", blocked, detail),
        CommandSupport("load-vocab-source", "SQLite/PostgreSQL + Athena CSV source", blocked, detail),
        CommandSupport("backup-database", "PostgreSQL + pg_dump", blocked, detail),
        CommandSupport("restore-database", "PostgreSQL + pg_restore/psql", blocked, detail),
        CommandSupport("fulltext install", "PostgreSQL", blocked, detail),
        CommandSupport("fulltext populate", "PostgreSQL", blocked, detail),
        CommandSupport("fulltext drop", "PostgreSQL", blocked, detail),
        CommandSupport("reset-sequences", "PostgreSQL", blocked, detail),
        CommandSupport("truncate-tables", "PostgreSQL", blocked, detail),
        CommandSupport("foreign-keys disable", "PostgreSQL", blocked, detail),
        CommandSupport("foreign-keys enable", "PostgreSQL", blocked, detail),
        CommandSupport("foreign-keys enable --strict", "PostgreSQL", blocked, detail),
        CommandSupport("foreign-keys status", "PostgreSQL", blocked, detail),
        CommandSupport("foreign-keys validate", "PostgreSQL", blocked, detail),
    )


def _command_support_for_backend(
    *,
    backend: str,
    engine_created: bool,
    engine_error: str | None,
    connection_ready: bool,
    connection_error: str | None,
    pg_dump_path: str | None,
    pg_restore_path: str | None,
    psql_path: str | None,
) -> tuple[CommandSupport, ...]:
    current_backend = backend_label(backend)
    if not engine_created:
        blocked_detail = (
            f"Backend resolved to {current_backend}, but the engine could not be created: {engine_error}"
            if engine_error
            else f"Backend resolved to {current_backend}, but the engine could not be created."
        )
    else:
        blocked_detail = (
            f"Backend resolved to {current_backend}, but the connection test failed: {connection_error}"
            if connection_error
            else f"Backend resolved to {current_backend}, but the connection test failed."
        )
    portable_status = "ready" if connection_ready else "blocked"
    portable_detail = (
        f"Ready on {current_backend}." if connection_ready else blocked_detail
    )

    if backend == Dialect.POSTGRESQL:
        analyze_status = portable_status
        analyze_detail = (
            "Ready on PostgreSQL; ANALYZE and VACUUM ANALYZE are both supported."
            if connection_ready
            else blocked_detail
        )
        enable_indexes_status = portable_status
        enable_indexes_detail = (
            "Ready on PostgreSQL; index DDL and clustering metadata are both supported."
            if connection_ready
            else blocked_detail
        )
        postgresql_status = portable_status
        postgresql_detail = "Ready on PostgreSQL." if connection_ready else blocked_detail
        vocab_load_status = portable_status
        vocab_load_detail = (
            "Ready on PostgreSQL when an Athena source path is configured."
            if connection_ready
            else blocked_detail
        )
    elif backend == "sqlite":
        analyze_status = "limited" if connection_ready else "blocked"
        analyze_detail = (
            "Ready on SQLite; ANALYZE is supported, but `--vacuum` is unavailable."
            if connection_ready
            else blocked_detail
        )
        enable_indexes_status = "limited" if connection_ready else "blocked"
        enable_indexes_detail = (
            "Ready on SQLite; index DDL is supported, but clustering metadata will be skipped."
            if connection_ready
            else blocked_detail
        )
        postgresql_status = "unsupported" if connection_ready else "blocked"
        postgresql_detail = (
            f"Requires PostgreSQL. Current backend: {current_backend}."
            if connection_ready
            else blocked_detail
        )
        vocab_load_status = portable_status
        vocab_load_detail = (
            "Ready on SQLite when an Athena source path is configured."
            if connection_ready
            else blocked_detail
        )
    else:
        analyze_status = "unsupported" if connection_ready else "blocked"
        analyze_detail = (
            f"Requires PostgreSQL or SQLite. Current backend: {current_backend}."
            if connection_ready
            else blocked_detail
        )
        enable_indexes_status = "limited" if connection_ready else "blocked"
        enable_indexes_detail = (
            f"Ready on {current_backend}; index DDL is supported, but clustering metadata will be skipped."
            if connection_ready
            else blocked_detail
        )
        postgresql_status = "unsupported" if connection_ready else "blocked"
        postgresql_detail = (
            f"Requires PostgreSQL. Current backend: {current_backend}."
            if connection_ready
            else blocked_detail
        )
        vocab_load_status = "unsupported" if connection_ready else "blocked"
        vocab_load_detail = (
            f"Requires SQLite or PostgreSQL plus a configured Athena source path. Current backend: {current_backend}."
            if connection_ready
            else blocked_detail
        )

    return (
        CommandSupport("doctor", "Any SQLAlchemy backend", portable_status, portable_detail),
        CommandSupport("data-summary", "Any SQLAlchemy backend", portable_status, portable_detail),
        CommandSupport("analyze-tables", "PostgreSQL/SQLite", analyze_status, analyze_detail),
        CommandSupport("create-missing-tables", "Any SQLAlchemy backend", portable_status, portable_detail),
        CommandSupport("indexes disable", "Any SQLAlchemy backend", portable_status, portable_detail),
        CommandSupport("indexes enable", "Any SQLAlchemy backend", enable_indexes_status, enable_indexes_detail),
        CommandSupport("reconcile-schema", "Any SQLAlchemy backend", portable_status, portable_detail),
        CommandSupport("load-vocab-source", "SQLite/PostgreSQL + Athena CSV source", vocab_load_status, vocab_load_detail),
        CommandSupport(
            "backup-database",
            "PostgreSQL + pg_dump",
            (
                "ready"
                if connection_ready and backend == Dialect.POSTGRESQL and pg_dump_path is not None
                else "blocked"
                if backend == Dialect.POSTGRESQL
                else "unsupported"
                if connection_ready
                else "blocked"
            ),
            (
                "Ready on PostgreSQL; `pg_dump` is available."
                if connection_ready and backend == Dialect.POSTGRESQL and pg_dump_path is not None
                else "PostgreSQL is configured, but `pg_dump` is not on PATH."
                if connection_ready and backend == Dialect.POSTGRESQL
                else f"Requires PostgreSQL. Current backend: {current_backend}."
                if connection_ready
                else blocked_detail
            ),
        ),
        CommandSupport(
            "restore-database",
            "PostgreSQL + pg_restore/psql",
            (
                "ready"
                if connection_ready and backend == Dialect.POSTGRESQL and (pg_restore_path is not None or psql_path is not None)
                else "blocked"
                if backend == Dialect.POSTGRESQL
                else "unsupported"
                if connection_ready
                else "blocked"
            ),
            (
                "Ready on PostgreSQL; restore client tooling is available."
                if connection_ready and backend == Dialect.POSTGRESQL and (pg_restore_path is not None or psql_path is not None)
                else "PostgreSQL is configured, but neither `pg_restore` nor `psql` is on PATH."
                if connection_ready and backend == Dialect.POSTGRESQL
                else f"Requires PostgreSQL. Current backend: {current_backend}."
                if connection_ready
                else blocked_detail
            ),
        ),
        CommandSupport("fulltext install", "PostgreSQL", postgresql_status, postgresql_detail),
        CommandSupport("fulltext populate", "PostgreSQL", postgresql_status, postgresql_detail),
        CommandSupport("fulltext drop", "PostgreSQL", postgresql_status, postgresql_detail),
        CommandSupport("reset-sequences", "PostgreSQL", postgresql_status, postgresql_detail),
        CommandSupport("truncate-tables", "PostgreSQL", postgresql_status, postgresql_detail),
        CommandSupport("foreign-keys disable", "PostgreSQL", postgresql_status, postgresql_detail),
        CommandSupport("foreign-keys enable", "PostgreSQL", postgresql_status, postgresql_detail),
        CommandSupport("foreign-keys enable --strict", "PostgreSQL", postgresql_status, postgresql_detail),
        CommandSupport("foreign-keys status", "PostgreSQL", postgresql_status, postgresql_detail),
        CommandSupport("foreign-keys validate", "PostgreSQL", postgresql_status, postgresql_detail),
    )


def collect_maintenance_info(
    *,
    engine_schema: str | None = None,
    db_schema: str | None = None,
    dotenv: str | None = None,
    vocabulary_included: bool = True,
) -> MaintenanceInfo:
    load_environment(dotenv or "")
    pg_dump_path = shutil.which("pg_dump")
    pg_restore_path = shutil.which("pg_restore")
    psql_path = shutil.which("psql")
    defaults_file = defaults_path()
    dependencies = (
        _dependency_status("sqlalchemy", "sqlalchemy"),
        _dependency_status("typer", "typer"),
        _dependency_status("rich", "rich"),
        _dependency_status("psycopg", "psycopg"),
        _dependency_status("psycopg2-binary", "psycopg2"),
        _external_dependency_status("pg_dump", "pg_dump"),
        _external_dependency_status("pg_restore", "pg_restore"),
        _external_dependency_status("psql", "psql"),
    )
    managed_tables = select_maintenance_tables(
        exclude_categories=(() if vocabulary_included else (TableCategory.VOCABULARY,))
    )
    cli_path = shutil.which("omop-alchemy")
    dotenv_exists = None if dotenv is None else os.path.exists(dotenv)

    engine_name: str | None = None
    engine_url: str | None = None
    backend: str | None = None
    engine_created = False
    engine_error: str | None = None
    connection_ready = False
    connection_error: str | None = None
    existing_table_count: int | None = None
    missing_table_count: int | None = None

    try:
        engine_name = get_engine_name(engine_schema)
        url = sa.engine.make_url(engine_name)
        engine_url = url.render_as_string(hide_password=True)
        backend = url.get_backend_name()
    except RuntimeError as exc:
        engine_error = str(exc)
    except Exception as exc:
        engine_error = f"Could not resolve engine configuration: {exc}"

    if engine_name is not None:
        try:
            engine = create_engine_with_dependencies(engine_name, future=True)
            engine_created = True
        except RuntimeError as exc:
            engine_error = str(exc)
        except Exception as exc:
            engine_error = f"Could not create engine: {exc}"
        else:
            try:
                with engine.connect() as connection:
                    connection.exec_driver_sql("SELECT 1")
                connection_ready = True
                missing_tables = collect_missing_tables(
                    engine,
                    db_schema=db_schema,
                    vocabulary_included=vocabulary_included,
                )
                missing_table_count = len(missing_tables)
                existing_table_count = len(managed_tables) - missing_table_count
            except SQLAlchemyError as exc:
                connection_error = f"{exc.__class__.__name__}: {exc}"
            except Exception as exc:
                connection_error = str(exc)
            finally:
                engine.dispose()

    if backend is None:
        command_support = _command_support_for_unavailable_engine(
            engine_error or "No engine configuration could be resolved."
        )
    else:
        command_support = _command_support_for_backend(
            backend=backend,
            engine_created=engine_created,
            engine_error=engine_error,
            connection_ready=connection_ready,
            connection_error=connection_error,
            pg_dump_path=pg_dump_path,
            pg_restore_path=pg_restore_path,
            psql_path=psql_path,
        )

    return MaintenanceInfo(
        package_version=_package_version(),
        cli_path=cli_path,
        pg_dump_path=pg_dump_path,
        pg_restore_path=pg_restore_path,
        psql_path=psql_path,
        defaults_file=str(defaults_file),
        defaults_exists=defaults_file.exists(),
        dotenv_path=dotenv,
        dotenv_exists=dotenv_exists,
        engine_schema=engine_schema,
        db_schema=db_schema,
        engine_url=engine_url,
        backend=backend,
        engine_created=engine_created,
        engine_error=engine_error,
        connection_ready=connection_ready,
        connection_error=connection_error,
        managed_table_count=len(managed_tables),
        existing_table_count=existing_table_count,
        missing_table_count=missing_table_count,
        vocabulary_included=vocabulary_included,
        dependencies=dependencies,
        command_support=command_support,
    )


# ---------------------------------------------------------------------------
# doctor
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DoctorCheck:
    name: str
    status: str
    detail: str


@dataclass(frozen=True)
class DoctorRecommendation:
    status: str
    summary: str
    action: str | None


@dataclass(frozen=True)
class DoctorReport:
    info: MaintenanceInfo
    checks: tuple[DoctorCheck, ...]
    recommendations: tuple[DoctorRecommendation, ...]
    reconciliation: SchemaReconciliationReport | None
    foreign_key_status: tuple[ForeignKeyStatusResult, ...] | None
    foreign_key_validation: ForeignKeyValidationReport | None


def _build_recommendations(
    *,
    info: MaintenanceInfo,
    reconciliation: SchemaReconciliationReport | None,
    foreign_key_status: tuple[ForeignKeyStatusResult, ...] | None,
    foreign_key_validation: ForeignKeyValidationReport | None,
) -> tuple[DoctorRecommendation, ...]:
    recommendations: list[DoctorRecommendation] = []

    if not info.connection_ready:
        recommendations.append(
            DoctorRecommendation(
                status="failed",
                summary="Database connection is not ready for maintenance operations.",
                action="Check the engine configuration, backend driver, and target database reachability.",
            )
        )
        return tuple(recommendations)

    if info.missing_table_count:
        recommendations.append(
            DoctorRecommendation(
                status="warning",
                summary=f"{info.missing_table_count} ORM-managed table(s) are missing from the target database.",
                action="Run `omop-alchemy create-missing-tables` before attempting bulk operations.",
            )
        )

    if reconciliation is not None and reconciliation.issues:
        recommendations.append(
            DoctorRecommendation(
                status="warning",
                summary=f"Schema reconciliation found {len(reconciliation.issues)} difference(s) against ORM metadata.",
                action="Review `omop-alchemy reconcile-schema` output before continuing with ETL or maintenance work.",
            )
        )

    if foreign_key_status is not None and any(
        item.disabled_trigger_count > 0 for item in foreign_key_status
    ):
        recommendations.append(
            DoctorRecommendation(
                status="warning",
                summary="Some PostgreSQL RI triggers are currently disabled.",
                action="If loading is complete, run `omop-alchemy foreign-keys validate` and then `omop-alchemy foreign-keys enable --strict`.",
            )
        )

    if (
        foreign_key_validation is not None
        and any(result.status == "failed" for result in foreign_key_validation.results)
    ):
        recommendations.append(
            DoctorRecommendation(
                status="failed",
                summary="Foreign key validation found violating rows.",
                action="Fix the reported rows, then rerun `omop-alchemy foreign-keys enable --strict`.",
            )
        )

    if info.backend == Dialect.POSTGRESQL and info.pg_dump_path is None:
        recommendations.append(
            DoctorRecommendation(
                status="warning",
                summary="`pg_dump` is not on PATH, so backup-database is unavailable from this machine.",
                action="Install PostgreSQL client tools on the machine running `omop-alchemy`.",
            )
        )

    if (
        info.backend == Dialect.POSTGRESQL
        and info.pg_restore_path is None
        and info.psql_path is None
    ):
        recommendations.append(
            DoctorRecommendation(
                status="warning",
                summary="Neither `pg_restore` nor `psql` is on PATH, so restore-database is unavailable from this machine.",
                action="Install PostgreSQL client tools on the machine running `omop-alchemy`.",
            )
        )

    if not recommendations:
        recommendations.append(
            DoctorRecommendation(
                status="passed",
                summary="No obvious maintenance blockers were detected.",
                action=None,
            )
        )

    return tuple(recommendations)


def collect_doctor_report(
    *,
    engine_schema: str | None = None,
    db_schema: str | None = None,
    dotenv: str | None = None,
    vocabulary_included: bool = True,
    deep: bool = False,
) -> DoctorReport:
    load_environment(dotenv or "")
    info = collect_maintenance_info(
        engine_schema=engine_schema,
        db_schema=db_schema,
        dotenv=dotenv,
        vocabulary_included=vocabulary_included,
    )

    checks = [
        DoctorCheck(
            name="connection",
            status="passed" if info.connection_ready else "failed",
            detail=(
                "Target database connection succeeded."
                if info.connection_ready
                else info.connection_error or info.engine_error or "Connection could not be established."
            ),
        )
    ]

    reconciliation: SchemaReconciliationReport | None = None
    foreign_key_status: tuple[ForeignKeyStatusResult, ...] | None = None
    foreign_key_validation: ForeignKeyValidationReport | None = None

    if info.connection_ready:
        engine = create_engine_with_dependencies(get_engine_name(engine_schema), future=True)
        try:
            missing_table_count = info.missing_table_count or 0
            checks.append(
                DoctorCheck(
                    name="managed tables",
                    status="passed" if missing_table_count == 0 else "warning",
                    detail=(
                        "All selected ORM-managed tables exist."
                        if missing_table_count == 0
                        else f"{missing_table_count} selected table(s) are missing."
                    ),
                )
            )

            if deep:
                reconciliation = reconcile_schema(
                    engine,
                    db_schema=db_schema,
                    vocabulary_included=vocabulary_included,
                )
                checks.append(
                    DoctorCheck(
                        name="schema drift",
                        status="passed" if not reconciliation.issues else "warning",
                        detail=(
                            "ORM metadata matches the target database."
                            if not reconciliation.issues
                            else f"{len(reconciliation.issues)} difference(s) detected."
                        ),
                    )
                )
            else:
                checks.append(
                    DoctorCheck(
                        name="schema drift",
                        status="skipped",
                        detail="Run `omop-alchemy doctor --deep` to reconcile ORM metadata against the target database.",
                    )
                )

            if info.backend == Dialect.POSTGRESQL:
                foreign_key_status = tuple(
                    collect_foreign_key_trigger_status(
                        engine,
                        db_schema=db_schema,
                        vocabulary_included=vocabulary_included,
                    )
                )
                disabled_tables = sum(
                    item.disabled_trigger_count > 0 for item in foreign_key_status
                )
                checks.append(
                    DoctorCheck(
                        name="foreign keys",
                        status="passed" if disabled_tables == 0 else "warning",
                        detail=(
                            "All inspected RI triggers are enabled."
                            if disabled_tables == 0
                            else f"{disabled_tables} table(s) still have disabled RI triggers."
                        ),
                    )
                )

                if deep:
                    foreign_key_validation = validate_foreign_key_constraints(
                        engine,
                        db_schema=db_schema,
                        vocabulary_included=vocabulary_included,
                    )
                    violating_tables = sum(
                        result.status == "failed" for result in foreign_key_validation.results
                    )
                    checks.append(
                        DoctorCheck(
                            name="foreign key validation",
                            status="passed" if violating_tables == 0 else "failed",
                            detail=(
                                "All selected foreign key relationships passed validation."
                                if violating_tables == 0
                                else f"{violating_tables} table(s) have violating foreign key rows."
                            ),
                        )
                    )
                else:
                    checks.append(
                        DoctorCheck(
                            name="foreign key validation",
                            status="skipped",
                            detail="Run `omop-alchemy doctor --deep` to validate selected foreign key relationships.",
                        )
                    )
            else:
                checks.append(
                    DoctorCheck(
                        name="foreign keys",
                        status="skipped",
                        detail="Foreign key trigger inspection is only available on PostgreSQL.",
                    )
                )
                checks.append(
                    DoctorCheck(
                        name="foreign key validation",
                        status="skipped",
                        detail="Foreign key validation is only available on PostgreSQL.",
                    )
                )
        finally:
            engine.dispose()
    else:
        checks.extend(
            (
                DoctorCheck(
                    name="managed tables",
                    status="skipped",
                    detail="Skipped because the database connection is not ready.",
                ),
                DoctorCheck(
                    name="foreign keys",
                    status="skipped",
                    detail="Skipped because the database connection is not ready.",
                ),
                DoctorCheck(
                    name="schema drift",
                    status="skipped",
                    detail="Skipped because the database connection is not ready.",
                ),
                DoctorCheck(
                    name="foreign key validation",
                    status="skipped",
                    detail="Skipped because the database connection is not ready.",
                ),
            )
        )

    if info.backend == Dialect.POSTGRESQL:
        backup_tools_ready = info.pg_dump_path is not None and (
            info.pg_restore_path is not None or info.psql_path is not None
        )
        checks.append(
            DoctorCheck(
                name="backup tooling",
                status="passed" if backup_tools_ready else "warning",
                detail=(
                    "PostgreSQL backup and restore client tools are available."
                    if backup_tools_ready
                    else "PostgreSQL client tools are incomplete on this machine."
                ),
            )
        )
    else:
        checks.append(
            DoctorCheck(
                name="backup tooling",
                status="skipped",
                detail="Backup and restore tooling checks are only relevant for PostgreSQL targets.",
            )
        )

    return DoctorReport(
        info=info,
        checks=tuple(checks),
        recommendations=_build_recommendations(
            info=info,
            reconciliation=reconciliation,
            foreign_key_status=foreign_key_status,
            foreign_key_validation=foreign_key_validation,
        ),
        reconciliation=reconciliation,
        foreign_key_status=foreign_key_status,
        foreign_key_validation=foreign_key_validation,
    )


# ---------------------------------------------------------------------------
# reconcile_schema
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ReconciliationIssue:
    table_name: str
    category: TableCategory
    component: str
    object_name: str
    status: str
    expected: str | None
    actual: str | None
    detail: str


@dataclass(frozen=True)
class TableReconciliationResult:
    table_name: str
    category: TableCategory
    model_name: str
    model_module: str
    status: str
    issue_count: int
    detail: str


@dataclass(frozen=True)
class SchemaReconciliationReport:
    backend: str
    table_results: tuple[TableReconciliationResult, ...]
    issues: tuple[ReconciliationIssue, ...]


def _schema_table(table: sa.Table, db_schema: str | None) -> sa.Table:
    if db_schema is None:
        return table

    metadata = sa.MetaData()
    return table.to_metadata(
        metadata,
        schema=db_schema,
        referred_schema_fn=(
            lambda _table, to_schema, _constraint, _referred_schema: to_schema
        ),
    )


def _normalized_type(type_: sa.types.TypeEngine[object], dialect: sa.engine.Dialect) -> str:
    return type_.compile(dialect=dialect).lower().replace(" ", "")


def _expected_foreign_keys(
    table: sa.Table,
) -> dict[tuple[tuple[str, ...], str, tuple[str, ...]], sa.ForeignKeyConstraint]:
    expected: dict[tuple[tuple[str, ...], str, tuple[str, ...]], sa.ForeignKeyConstraint] = {}
    for constraint in table.foreign_key_constraints:
        constrained_columns = tuple(element.parent.name for element in constraint.elements)
        referred_columns = tuple(element.column.name for element in constraint.elements)
        referred_table = constraint.referred_table.name
        expected[(constrained_columns, referred_table, referred_columns)] = constraint
    return expected


def _actual_foreign_keys(
    inspector: sa.Inspector,
    table_name: str,
    db_schema: str | None,
) -> dict[tuple[tuple[str, ...], str, tuple[str, ...]], dict[str, object]]:
    actual: dict[tuple[tuple[str, ...], str, tuple[str, ...]], dict[str, object]] = {}
    for foreign_key in inspector.get_foreign_keys(table_name, schema=db_schema):
        constrained_columns = tuple(foreign_key.get("constrained_columns") or [])
        referred_columns = tuple(foreign_key.get("referred_columns") or [])
        referred_table = str(foreign_key.get("referred_table"))
        actual[(constrained_columns, referred_table, referred_columns)] = foreign_key
    return actual


def _expected_indexes(table: sa.Table) -> dict[str, sa.Index]:
    return {
        str(index.name): index
        for index in table.indexes
        if index.name is not None
    }


def _actual_indexes(
    inspector: sa.Inspector,
    table_name: str,
    db_schema: str | None,
) -> dict[str, dict[str, object]]:
    return {
        str(index["name"]): index
        for index in inspector.get_indexes(table_name, schema=db_schema)
        if index.get("name") is not None
    }


def _actual_cluster_index_name(
    connection: sa.Connection,
    *,
    table_name: str,
    db_schema: str | None,
) -> str | None:
    result = connection.execute(
        sa.text(
            """
            SELECT i.relname
            FROM pg_index ix
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE ix.indisclustered
              AND t.relname = :table_name
              AND (:db_schema IS NULL OR n.nspname = :db_schema)
            """
        ),
        {"table_name": table_name, "db_schema": db_schema},
    ).scalar_one_or_none()
    return str(result) if result is not None else None


def reconcile_schema(
    engine: sa.Engine,
    *,
    db_schema: str | None = None,
    vocabulary_included: bool = False,
) -> SchemaReconciliationReport:
    excluded_categories: tuple[TableCategory, ...] = (
        () if vocabulary_included else (TableCategory.VOCABULARY,)
    )
    selected_tables = select_maintenance_tables(exclude_categories=excluded_categories)
    inspector = sa.inspect(engine)
    all_issues: list[ReconciliationIssue] = []
    table_results: list[TableReconciliationResult] = []

    with engine.connect() as connection:
        for maintenance_table in selected_tables:
            table_issues: list[ReconciliationIssue] = []
            exists = inspector.has_table(maintenance_table.table_name, schema=db_schema)
            if not exists:
                table_issues.append(
                    ReconciliationIssue(
                        table_name=maintenance_table.table_name,
                        category=maintenance_table.category,
                        component="table",
                        object_name=maintenance_table.table_name,
                        status="missing",
                        expected="present",
                        actual="absent",
                        detail="ORM-managed table is missing from the target database.",
                    )
                )
                table_results.append(
                    TableReconciliationResult(
                        table_name=maintenance_table.table_name,
                        category=maintenance_table.category,
                        model_name=maintenance_table.model_name,
                        model_module=maintenance_table.model_module,
                        status="missing",
                        issue_count=1,
                        detail="Table is missing from the target database.",
                    )
                )
                all_issues.extend(table_issues)
                continue

            expected_table = _schema_table(maintenance_table.table, db_schema)
            expected_columns = {column.name: column for column in expected_table.columns}
            actual_columns = {
                str(column["name"]): column
                for column in inspector.get_columns(maintenance_table.table_name, schema=db_schema)
            }
            actual_pk_names = tuple(
                inspector.get_pk_constraint(maintenance_table.table_name, schema=db_schema).get("constrained_columns") or []
            )
            expected_pk_names = tuple(column.name for column in expected_table.primary_key.columns)

            for column_name, column in expected_columns.items():
                if column_name not in actual_columns:
                    table_issues.append(
                        ReconciliationIssue(
                            table_name=maintenance_table.table_name,
                            category=maintenance_table.category,
                            component="column",
                            object_name=column_name,
                            status="missing",
                            expected=_normalized_type(column.type, engine.dialect),
                            actual=None,
                            detail="Column is defined in ORM metadata but missing from the database.",
                        )
                    )

            for column_name, column in actual_columns.items():
                if column_name not in expected_columns:
                    table_issues.append(
                        ReconciliationIssue(
                            table_name=maintenance_table.table_name,
                            category=maintenance_table.category,
                            component="column",
                            object_name=column_name,
                            status="unexpected",
                            expected=None,
                            actual=_normalized_type(column["type"], engine.dialect),
                            detail="Column exists in the database but is not defined in ORM metadata.",
                        )
                    )

            for column_name in sorted(set(expected_columns).intersection(actual_columns)):
                expected_column = expected_columns[column_name]
                actual_column = actual_columns[column_name]
                expected_type = _normalized_type(expected_column.type, engine.dialect)
                actual_type = _normalized_type(actual_column["type"], engine.dialect)
                if expected_type != actual_type:
                    table_issues.append(
                        ReconciliationIssue(
                            table_name=maintenance_table.table_name,
                            category=maintenance_table.category,
                            component="column",
                            object_name=column_name,
                            status="mismatch",
                            expected=expected_type,
                            actual=actual_type,
                            detail="Column type differs from ORM metadata.",
                        )
                    )

                expected_nullable = False if column_name in expected_pk_names else bool(expected_column.nullable)
                actual_nullable = False if column_name in actual_pk_names else bool(actual_column["nullable"])
                if expected_nullable != actual_nullable:
                    table_issues.append(
                        ReconciliationIssue(
                            table_name=maintenance_table.table_name,
                            category=maintenance_table.category,
                            component="column",
                            object_name=column_name,
                            status="mismatch",
                            expected="nullable" if expected_nullable else "not nullable",
                            actual="nullable" if actual_nullable else "not nullable",
                            detail="Column nullability differs from ORM metadata.",
                        )
                    )

            if expected_pk_names != actual_pk_names:
                table_issues.append(
                    ReconciliationIssue(
                        table_name=maintenance_table.table_name,
                        category=maintenance_table.category,
                        component="primary_key",
                        object_name=maintenance_table.table_name,
                        status="mismatch",
                        expected=", ".join(expected_pk_names),
                        actual=", ".join(actual_pk_names) if actual_pk_names else None,
                        detail="Primary key columns differ from ORM metadata.",
                    )
                )

            expected_fks = _expected_foreign_keys(expected_table)
            actual_fks = _actual_foreign_keys(inspector, maintenance_table.table_name, db_schema)

            for signature, constraint in expected_fks.items():
                if signature not in actual_fks:
                    constrained_columns, referred_table, referred_columns = signature
                    table_issues.append(
                        ReconciliationIssue(
                            table_name=maintenance_table.table_name,
                            category=maintenance_table.category,
                            component="foreign_key",
                            object_name=constraint.name or ",".join(constrained_columns),
                            status="missing",
                            expected=f"{','.join(constrained_columns)} -> {referred_table}({','.join(referred_columns)})",
                            actual=None,
                            detail="Foreign key is defined in ORM metadata but missing from the database.",
                        )
                    )

            for signature, foreign_key in actual_fks.items():
                if signature not in expected_fks:
                    constrained_columns, referred_table, referred_columns = signature
                    table_issues.append(
                        ReconciliationIssue(
                            table_name=maintenance_table.table_name,
                            category=maintenance_table.category,
                            component="foreign_key",
                            object_name=str(foreign_key.get("name") or ",".join(constrained_columns)),
                            status="unexpected",
                            expected=None,
                            actual=f"{','.join(constrained_columns)} -> {referred_table}({','.join(referred_columns)})",
                            detail="Foreign key exists in the database but is not defined in ORM metadata.",
                        )
                    )

            expected_idxs = _expected_indexes(expected_table)
            actual_idxs = _actual_indexes(inspector, maintenance_table.table_name, db_schema)

            for index_name, index in expected_idxs.items():
                if index_name not in actual_idxs:
                    table_issues.append(
                        ReconciliationIssue(
                            table_name=maintenance_table.table_name,
                            category=maintenance_table.category,
                            component="index",
                            object_name=index_name,
                            status="missing",
                            expected=", ".join(column.name for column in index.columns),
                            actual=None,
                            detail="Index is defined in ORM metadata but missing from the database.",
                        )
                    )
                    continue

                actual_index = actual_idxs[index_name]
                expected_columns_for_index = tuple(column.name for column in index.columns)
                actual_columns_for_index = tuple(actual_index.get("column_names") or [])
                if expected_columns_for_index != actual_columns_for_index:
                    table_issues.append(
                        ReconciliationIssue(
                            table_name=maintenance_table.table_name,
                            category=maintenance_table.category,
                            component="index",
                            object_name=index_name,
                            status="mismatch",
                            expected=", ".join(expected_columns_for_index),
                            actual=", ".join(actual_columns_for_index) if actual_columns_for_index else None,
                            detail="Index columns differ from ORM metadata.",
                        )
                    )
                if bool(index.unique) != bool(actual_index.get("unique")):
                    table_issues.append(
                        ReconciliationIssue(
                            table_name=maintenance_table.table_name,
                            category=maintenance_table.category,
                            component="index",
                            object_name=index_name,
                            status="mismatch",
                            expected="unique" if index.unique else "non-unique",
                            actual="unique" if actual_index.get("unique") else "non-unique",
                            detail="Index uniqueness differs from ORM metadata.",
                        )
                    )

            for index_name, index in actual_idxs.items():
                if index_name not in expected_idxs:
                    table_issues.append(
                        ReconciliationIssue(
                            table_name=maintenance_table.table_name,
                            category=maintenance_table.category,
                            component="index",
                            object_name=index_name,
                            status="unexpected",
                            expected=None,
                            actual=", ".join(index.get("column_names") or []),
                            detail="Index exists in the database but is not defined in ORM metadata.",
                        )
                    )

            if engine.dialect.name == Dialect.POSTGRESQL:
                expected_cluster = _cluster_target_name(maintenance_table)
                actual_cluster = _actual_cluster_index_name(
                    connection,
                    table_name=maintenance_table.table_name,
                    db_schema=db_schema,
                )
                if expected_cluster != actual_cluster:
                    table_issues.append(
                        ReconciliationIssue(
                            table_name=maintenance_table.table_name,
                            category=maintenance_table.category,
                            component="cluster",
                            object_name=maintenance_table.table_name,
                            status=(
                                "missing"
                                if expected_cluster and not actual_cluster
                                else "unexpected"
                                if actual_cluster and not expected_cluster
                                else "mismatch"
                            ),
                            expected=expected_cluster,
                            actual=actual_cluster,
                            detail="Table clustering differs from ORM metadata.",
                        )
                    )

            table_status = "matched" if not table_issues else "drifted"
            table_results.append(
                TableReconciliationResult(
                    table_name=maintenance_table.table_name,
                    category=maintenance_table.category,
                    model_name=maintenance_table.model_name,
                    model_module=maintenance_table.model_module,
                    status=table_status,
                    issue_count=len(table_issues),
                    detail=(
                        "No differences detected."
                        if not table_issues
                        else f"{len(table_issues)} difference(s) detected."
                    ),
                )
            )
            all_issues.extend(table_issues)

    return SchemaReconciliationReport(
        backend=engine.dialect.name,
        table_results=tuple(table_results),
        issues=tuple(all_issues),
    )


# ---------------------------------------------------------------------------
# create_missing_tables
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TableCreationResult:
    table_name: str
    category: TableCategory
    model_name: str
    model_module: str
    status: str
    detail: str


def _table_dependencies(table: MaintenanceTable) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                constraint.referred_table.name
                for constraint in table.table.foreign_key_constraints
            }
        )
    )


def collect_missing_tables(
    engine: sa.Engine,
    *,
    db_schema: str | None = None,
    vocabulary_included: bool = True,
) -> list[MaintenanceTable]:
    inspector = sa.inspect(engine)
    return missing_maintenance_tables(
        inspector,
        db_schema=db_schema,
        vocabulary_included=vocabulary_included,
    )


def create_missing_tables(
    engine: sa.Engine,
    *,
    db_schema: str | None = None,
    vocabulary_included: bool = True,
    dry_run: bool = False,
) -> list[TableCreationResult]:
    inspector = sa.inspect(engine)
    missing_tables = collect_missing_tables(
        engine,
        db_schema=db_schema,
        vocabulary_included=vocabulary_included,
    )
    existing_table_names = set(inspector.get_table_names(schema=db_schema))
    missing_table_names = {table.table_name for table in missing_tables}

    blocked_dependencies: dict[str, tuple[str, ...]] = {}
    for maintenance_table in missing_tables:
        unresolved_dependencies = tuple(
            dependency_name
            for dependency_name in _table_dependencies(maintenance_table)
            if dependency_name not in existing_table_names
            and dependency_name not in missing_table_names
        )
        if unresolved_dependencies:
            blocked_dependencies[maintenance_table.table_name] = unresolved_dependencies

    creatable_tables = [
        table
        for table in missing_tables
        if table.table_name not in blocked_dependencies
    ]

    results: list[TableCreationResult] = []
    with engine.begin() as connection:
        if creatable_tables and not dry_run:
            metadata, adjusted_tables = schema_adjusted_metadata(
                collect_maintenance_tables(),
                db_schema=db_schema,
            )
            metadata.create_all(
                bind=connection,
                tables=[adjusted_tables[table.table_name] for table in creatable_tables],
                checkfirst=True,
            )

        for maintenance_table in missing_tables:
            blocked = blocked_dependencies.get(maintenance_table.table_name)
            results.append(
                TableCreationResult(
                    table_name=maintenance_table.table_name,
                    category=maintenance_table.category,
                    model_name=maintenance_table.model_name,
                    model_module=maintenance_table.model_module,
                    status=(
                        "blocked"
                        if blocked is not None
                        else "planned"
                        if dry_run
                        else "created"
                    ),
                    detail=(
                        "table blocked by unresolved dependencies: " + ", ".join(blocked)
                        if blocked is not None
                        else "table would be created from ORM metadata"
                        if dry_run
                        else "table created from ORM metadata"
                    ),
                )
            )

    return results


# ---------------------------------------------------------------------------
# data_summary
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TableSummaryResult:
    table_name: str
    category: TableCategory
    model_name: str
    model_module: str
    primary_key_columns: tuple[str, ...]
    exists: bool
    row_count: int | None


def collect_data_summary(
    engine: sa.Engine,
    *,
    db_schema: str | None = None,
    vocabulary_included: bool = False,
    existing_only: bool = True,
) -> list[TableSummaryResult]:
    inspector = sa.inspect(engine)
    tables = select_omop_tables(vocabulary_included=vocabulary_included)

    results: list[TableSummaryResult] = []
    with engine.connect() as connection:
        for table in tables:
            exists = inspector.has_table(table.table_name, schema=db_schema)
            if not exists and existing_only:
                continue

            row_count: int | None = None
            if exists:
                row_count = int(
                    connection.execute(
                        sa.text(
                            f"SELECT COUNT(*) FROM {qualified_table_name(table.table_name, db_schema)}"
                        )
                    ).scalar_one()
                )

            results.append(
                TableSummaryResult(
                    table_name=table.table_name,
                    category=table.category,
                    model_name=table.model_name,
                    model_module=table.model_module,
                    primary_key_columns=table.primary_key_names,
                    exists=exists,
                    row_count=row_count,
                )
            )

    return results


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------

app = typer.Typer(rich_markup_mode="rich")


@app.command(
    "info",
    help="Inspect maintenance CLI readiness, backend compatibility, and current installation state.",
)
def info_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(False, "--vocab/--no-vocab"),
) -> None:
    conn = resolve_connection(dotenv=dotenv, engine_schema=engine_schema, db_schema=db_schema)
    console.print(
        render_command_header(
            command_name="info",
            engine_schema=conn.engine_schema,
            db_schema=conn.db_schema,
            vocabulary_included=vocabulary_included,
            mode_label="inspect",
        )
    )
    try:
        load_environment(conn.dotenv or "")
        with console.status("Inspecting maintenance environment..."):
            info = collect_maintenance_info(
                dotenv=conn.dotenv,
                engine_schema=conn.engine_schema,
                db_schema=conn.db_schema,
                vocabulary_included=vocabulary_included,
            )
        console.print(render_info_environment(info))
        console.print(render_info_database(info))
        console.print(render_info_dependencies(info))
        console.print(render_info_command_support(info.command_support))
        console.print(render_info_summary(info))
    except Exception as exc:
        handle_error(exc)


@app.command(
    "doctor",
    help="Run a read-only maintenance health check across connection readiness, schema drift, and FK state.",
)
def doctor_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(False, "--vocab/--no-vocab"),
    deep: bool = typer.Option(
        False,
        "--deep",
        help="Include heavier checks such as PostgreSQL foreign key validation.",
    ),
) -> None:
    conn = resolve_connection(dotenv=dotenv, engine_schema=engine_schema, db_schema=db_schema)
    console.print(
        render_command_header(
            command_name="doctor",
            engine_schema=conn.engine_schema,
            db_schema=conn.db_schema,
            vocabulary_included=vocabulary_included,
            mode_label="inspect",
        )
    )
    try:
        load_environment(conn.dotenv or "")
        with console.status("Running maintenance doctor checks..."):
            report = collect_doctor_report(
                dotenv=conn.dotenv,
                engine_schema=conn.engine_schema,
                db_schema=conn.db_schema,
                vocabulary_included=vocabulary_included,
                deep=deep,
            )
        console.print(render_info_environment(report.info))
        console.print(render_info_database(report.info))
        console.print(render_doctor_checks(report.checks))
        if deep and report.foreign_key_validation is not None:
            console.print(render_foreign_key_validation_issues(report.foreign_key_validation.violations))
        console.print(render_doctor_recommendations(report.recommendations))
        console.print(render_doctor_summary(report, deep=deep))
    except Exception as exc:
        handle_error(exc)


@app.command(
    "reconcile-schema",
    help="Compare ORM-managed SQLAlchemy metadata against the current target database schema.",
)
def reconcile_schema_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(False, "--vocab/--no-vocab"),
) -> None:
    conn = resolve_connection(dotenv=dotenv, engine_schema=engine_schema, db_schema=db_schema)
    console.print(
        render_command_header(
            command_name="reconcile-schema",
            engine_schema=conn.engine_schema,
            db_schema=conn.db_schema,
            vocabulary_included=vocabulary_included,
            mode_label="inspect",
        )
    )
    try:
        engine = build_engine(dotenv=conn.dotenv, engine_schema=conn.engine_schema)
        with console.status("Reconciling ORM metadata against target database schema..."):
            report = reconcile_schema(engine, db_schema=conn.db_schema, vocabulary_included=vocabulary_included)
        console.print(render_reconciliation_results(report.table_results))
        console.print(render_reconciliation_issues(report.issues))
        console.print(render_reconciliation_summary(report))
    except Exception as exc:
        handle_error(exc)


@app.command(
    "create-missing-tables",
    help="Create missing ORM-managed OMOP tables from metadata.",
)
def create_missing_tables_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(True, "--vocab/--no-vocab"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    conn = resolve_connection(dotenv=dotenv, engine_schema=engine_schema, db_schema=db_schema)
    console.print(
        render_command_header(
            command_name="create-missing-tables",
            engine_schema=conn.engine_schema,
            db_schema=conn.db_schema,
            vocabulary_included=vocabulary_included,
            mode_label="dry-run" if dry_run else "apply",
        )
    )
    try:
        engine = build_engine(dotenv=conn.dotenv, engine_schema=conn.engine_schema)
        with console.status("Creating missing tables..."):
            results = create_missing_tables(
                engine,
                db_schema=conn.db_schema,
                vocabulary_included=vocabulary_included,
                dry_run=dry_run,
            )
        console.print(render_table_creation_results(results))
        console.print(render_table_creation_summary(results, dry_run=dry_run))
    except Exception as exc:
        handle_error(exc)


@app.command(
    "data-summary",
    help="Summarise ORM-managed OMOP tables present in the target database.",
)
def data_summary_command(
    dotenv: str | None = typer.Option(None, help="Optional dotenv file to load."),
    engine_schema: str | None = typer.Option(None, help="Engine schema selector."),
    db_schema: str | None = typer.Option(None, help="Database schema override."),
    vocabulary_included: bool = typer.Option(False, "--vocab/--no-vocab"),
    include_missing: bool = typer.Option(False, "--include-missing"),
) -> None:
    conn = resolve_connection(dotenv=dotenv, engine_schema=engine_schema, db_schema=db_schema)
    console.print(
        render_command_header(
            command_name="data-summary",
            engine_schema=conn.engine_schema,
            db_schema=conn.db_schema,
            vocabulary_included=vocabulary_included,
            mode_label="inspect",
        )
    )
    try:
        engine = build_engine(dotenv=conn.dotenv, engine_schema=conn.engine_schema)
        with console.status("Collecting table summary..."):
            results = collect_data_summary(
                engine,
                db_schema=conn.db_schema,
                vocabulary_included=vocabulary_included,
                existing_only=not include_missing,
            )
        console.print(render_data_summary_results(results))
        console.print(render_data_summary_summary(results))
    except Exception as exc:
        handle_error(exc)
