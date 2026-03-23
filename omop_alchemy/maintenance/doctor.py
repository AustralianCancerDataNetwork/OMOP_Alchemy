from __future__ import annotations

from dataclasses import dataclass

from omop_alchemy import create_engine_with_dependencies, get_engine_name, load_environment

from .backend_support import POSTGRESQL_DIALECT
from .foreign_keys import (
    ForeignKeyStatusResult,
    ForeignKeyValidationReport,
    collect_foreign_key_trigger_status,
    validate_foreign_key_constraints,
)
from .info import MaintenanceInfo, collect_maintenance_info
from .reconcile import SchemaReconciliationReport, reconcile_schema


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
                action="Run `omop-maint create-missing-tables` before attempting bulk operations.",
            )
        )

    if reconciliation is not None and reconciliation.issues:
        recommendations.append(
            DoctorRecommendation(
                status="warning",
                summary=f"Schema reconciliation found {len(reconciliation.issues)} difference(s) against ORM metadata.",
                action="Review `omop-maint reconcile-schema` output before continuing with ETL or maintenance work.",
            )
        )

    if foreign_key_status is not None and any(
        item.disabled_trigger_count > 0
        for item in foreign_key_status
    ):
        recommendations.append(
            DoctorRecommendation(
                status="warning",
                summary="Some PostgreSQL RI triggers are currently disabled.",
                action="If loading is complete, run `omop-maint foreign-keys validate` and then `omop-maint foreign-keys enable --strict`.",
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
                action="Fix the reported rows, then rerun `omop-maint foreign-keys enable --strict`.",
            )
        )

    if info.backend == POSTGRESQL_DIALECT and info.pg_dump_path is None:
        recommendations.append(
            DoctorRecommendation(
                status="warning",
                summary="`pg_dump` is not on PATH, so backup-database is unavailable from this machine.",
                action="Install PostgreSQL client tools on the machine running `omop-maint`.",
            )
        )

    if (
        info.backend == POSTGRESQL_DIALECT
        and info.pg_restore_path is None
        and info.psql_path is None
    ):
        recommendations.append(
            DoctorRecommendation(
                status="warning",
                summary="Neither `pg_restore` nor `psql` is on PATH, so restore-database is unavailable from this machine.",
                action="Install PostgreSQL client tools on the machine running `omop-maint`.",
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
        engine = create_engine_with_dependencies(
            get_engine_name(engine_schema),
            future=True,
        )
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
                        detail="Run `omop-maint doctor --deep` to reconcile ORM metadata against the target database.",
                    )
                )

            if info.backend == POSTGRESQL_DIALECT:
                foreign_key_status = tuple(
                    collect_foreign_key_trigger_status(
                        engine,
                        db_schema=db_schema,
                        vocabulary_included=vocabulary_included,
                    )
                )
                disabled_tables = sum(
                    item.disabled_trigger_count > 0
                    for item in foreign_key_status
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
                        result.status == "failed"
                        for result in foreign_key_validation.results
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
                            detail="Run `omop-maint doctor --deep` to validate selected foreign key relationships.",
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

    if info.backend == POSTGRESQL_DIALECT:
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
