from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

import sqlalchemy as sa

from ..backend_support import Dialect, require_backend
from .tables import (
    MaintenanceTable,
    TableCategory,
    existing_maintenance_tables,
    qualified_table_name,
)


class ForeignKeyAction(StrEnum):
    DISABLE = "disable"
    ENABLE = "enable"


@dataclass(frozen=True)
class ForeignKeyTarget:
    table_name: str
    category: TableCategory
    model_name: str
    model_module: str
    outgoing_constraint_count: int
    incoming_constraint_count: int


@dataclass(frozen=True)
class ForeignKeyManagementResult:
    table_name: str
    category: TableCategory
    model_name: str
    model_module: str
    outgoing_constraint_count: int
    incoming_constraint_count: int
    action: ForeignKeyAction
    status: str
    detail: str


@dataclass(frozen=True)
class ForeignKeyStatusResult:
    table_name: str
    category: TableCategory
    model_name: str
    model_module: str
    disabled_trigger_count: int
    enabled_trigger_count: int
    outgoing_constraint_count: int
    incoming_constraint_count: int


@dataclass(frozen=True)
class ForeignKeyConstraintViolation:
    source_table_name: str
    referred_table_name: str
    constraint_name: str
    violation_count: int


@dataclass(frozen=True)
class ForeignKeyValidationResult:
    table_name: str
    category: TableCategory
    model_name: str
    model_module: str
    outgoing_constraint_count: int
    incoming_constraint_count: int
    violating_constraint_count: int
    violating_row_count: int
    status: str
    detail: str


@dataclass(frozen=True)
class ForeignKeyValidationReport:
    results: tuple[ForeignKeyValidationResult, ...]
    violations: tuple[ForeignKeyConstraintViolation, ...]
def _ensure_postgresql_supported(
    engine: sa.Engine,
    *,
    feature: str,
) -> None:
    require_backend(
        engine,
        feature=feature,
        supported_dialects=(Dialect.POSTGRESQL,),
    )


def _selected_existing_tables(
    inspector: sa.Inspector,
    *,
    db_schema: str | None,
    vocabulary_included: bool,
) -> list[MaintenanceTable]:
    return existing_maintenance_tables(
        inspector,
        db_schema=db_schema,
        vocabulary_included=vocabulary_included,
    )


def collect_foreign_key_targets(
    engine: sa.Engine,
    *,
    db_schema: str | None = None,
    vocabulary_included: bool = False,
) -> list[ForeignKeyTarget]:
    inspector = sa.inspect(engine)
    selected_tables = _selected_existing_tables(
        inspector,
        db_schema=db_schema,
        vocabulary_included=vocabulary_included,
    )
    selected_names = {
        table.table_name
        for table in selected_tables
    }

    incoming_counts = {name: 0 for name in selected_names}
    outgoing_counts = {name: 0 for name in selected_names}

    for table_name in selected_names:
        foreign_keys = inspector.get_foreign_keys(table_name, schema=db_schema)
        relevant_foreign_keys = [
            foreign_key
            for foreign_key in foreign_keys
            if foreign_key.get("referred_table") in selected_names
        ]
        outgoing_counts[table_name] = len(relevant_foreign_keys)
        for foreign_key in relevant_foreign_keys:
            referred_table = foreign_key.get("referred_table")
            if referred_table is not None:
                incoming_counts[referred_table] += 1

    results: list[ForeignKeyTarget] = []
    for table in selected_tables:
        if table.table_name not in selected_names:
            continue
        outgoing_count = outgoing_counts[table.table_name]
        incoming_count = incoming_counts[table.table_name]
        if outgoing_count == 0 and incoming_count == 0:
            continue
        results.append(
            ForeignKeyTarget(
                table_name=table.table_name,
                category=table.category,
                model_name=table.model_name,
                model_module=table.model_module,
                outgoing_constraint_count=outgoing_count,
                incoming_constraint_count=incoming_count,
            )
        )

    return results


def _collect_strict_validation_failures(
    connection: sa.Connection,
    *,
    db_schema: str | None,
    vocabulary_included: bool,
) -> dict[str, list[ForeignKeyConstraintViolation]]:
    inspector = sa.inspect(connection)
    selected_tables = _selected_existing_tables(
        inspector,
        db_schema=db_schema,
        vocabulary_included=vocabulary_included,
    )
    selected_names = {table.table_name for table in selected_tables}
    failures: dict[str, list[ForeignKeyConstraintViolation]] = {
        table_name: []
        for table_name in selected_names
    }

    for table_name in sorted(selected_names):
        for foreign_key in inspector.get_foreign_keys(table_name, schema=db_schema):
            referred_table = foreign_key.get("referred_table")
            constrained_columns = foreign_key.get("constrained_columns") or []
            referred_columns = foreign_key.get("referred_columns") or []

            if (
                referred_table not in selected_names
                or len(constrained_columns) == 0
                or len(constrained_columns) != len(referred_columns)
            ):
                continue

            source_table_name = qualified_table_name(table_name, db_schema)
            referred_table_name = qualified_table_name(str(referred_table), db_schema)
            non_null_predicate = " AND ".join(
                f"src.{column_name} IS NOT NULL"
                for column_name in constrained_columns
            )
            join_predicate = " AND ".join(
                f"ref.{referred_column} = src.{constrained_column}"
                for constrained_column, referred_column in zip(
                    constrained_columns,
                    referred_columns,
                    strict=True,
                )
            )

            violation_count = int(
                connection.exec_driver_sql(
                    f"""
                    SELECT COUNT(*)
                    FROM {source_table_name} AS src
                    WHERE {non_null_predicate}
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {referred_table_name} AS ref
                          WHERE {join_predicate}
                      )
                    """
                ).scalar_one()
            )

            if violation_count == 0:
                continue

            failures[table_name].append(
                ForeignKeyConstraintViolation(
                    source_table_name=table_name,
                    referred_table_name=str(referred_table),
                    constraint_name=foreign_key.get("name") or "(unnamed constraint)",
                    violation_count=violation_count,
                )
            )

    return {
        table_name: violations
        for table_name, violations in failures.items()
        if violations
    }


def _strict_failure_detail(
    violations: list[ForeignKeyConstraintViolation],
) -> str:
    constraint_summary = ", ".join(
        f"{violation.constraint_name} ({violation.violation_count})"
        for violation in violations[:3]
    )
    if len(violations) > 3:
        constraint_summary = f"{constraint_summary}, +{len(violations) - 3} more"

    total_violations = sum(violation.violation_count for violation in violations)
    return (
        "Strict validation failed; no FK triggers were enabled. "
        f"{total_violations} violating row(s) across {len(violations)} constraint(s): "
        f"{constraint_summary}"
    )


def _validation_failure_detail(
    violations: list[ForeignKeyConstraintViolation],
) -> str:
    constraint_summary = ", ".join(
        f"{violation.constraint_name} ({violation.violation_count})"
        for violation in violations[:3]
    )
    if len(violations) > 3:
        constraint_summary = f"{constraint_summary}, +{len(violations) - 3} more"

    total_violations = sum(violation.violation_count for violation in violations)
    return (
        f"{total_violations} violating row(s) across {len(violations)} constraint(s): "
        f"{constraint_summary}"
    )


def validate_foreign_key_constraints(
    engine: sa.Engine,
    *,
    db_schema: str | None = None,
    vocabulary_included: bool = False,
) -> ForeignKeyValidationReport:
    _ensure_postgresql_supported(
        engine,
        feature="Foreign key constraint validation",
    )

    targets = collect_foreign_key_targets(
        engine,
        db_schema=db_schema,
        vocabulary_included=vocabulary_included,
    )

    with engine.connect() as connection:
        validation_failures = _collect_strict_validation_failures(
            connection,
            db_schema=db_schema,
            vocabulary_included=vocabulary_included,
        )

    results: list[ForeignKeyValidationResult] = []
    all_violations: list[ForeignKeyConstraintViolation] = []

    for target in targets:
        violations = validation_failures.get(target.table_name, [])
        violating_constraint_count = len(violations)
        violating_row_count = sum(
            violation.violation_count
            for violation in violations
        )
        results.append(
            ForeignKeyValidationResult(
                table_name=target.table_name,
                category=target.category,
                model_name=target.model_name,
                model_module=target.model_module,
                outgoing_constraint_count=target.outgoing_constraint_count,
                incoming_constraint_count=target.incoming_constraint_count,
                violating_constraint_count=violating_constraint_count,
                violating_row_count=violating_row_count,
                status="failed" if violations else "passed",
                detail=(
                    _validation_failure_detail(violations)
                    if violations
                    else "No FK violations found for this table."
                ),
            )
        )
        all_violations.extend(violations)

    all_violations.sort(
        key=lambda violation: (
            violation.source_table_name,
            violation.constraint_name,
        )
    )
    return ForeignKeyValidationReport(
        results=tuple(results),
        violations=tuple(all_violations),
    )


def manage_foreign_key_triggers(
    engine: sa.Engine,
    *,
    action: ForeignKeyAction,
    db_schema: str | None = None,
    vocabulary_included: bool = False,
    dry_run: bool = False,
    strict: bool = False,
) -> list[ForeignKeyManagementResult]:
    _ensure_postgresql_supported(
        engine,
        feature="Foreign key trigger management",
    )

    targets = collect_foreign_key_targets(
        engine,
        db_schema=db_schema,
        vocabulary_included=vocabulary_included,
    )

    trigger_action = "DISABLE" if action is ForeignKeyAction.DISABLE else "ENABLE"
    results: list[ForeignKeyManagementResult] = []
    with engine.begin() as connection:
        if action is ForeignKeyAction.ENABLE and strict:
            validation_failures = _collect_strict_validation_failures(
                connection,
                db_schema=db_schema,
                vocabulary_included=vocabulary_included,
            )
            if validation_failures:
                for target in targets:
                    violations = validation_failures.get(target.table_name)
                    results.append(
                        ForeignKeyManagementResult(
                            table_name=target.table_name,
                            category=target.category,
                            model_name=target.model_name,
                            model_module=target.model_module,
                            outgoing_constraint_count=target.outgoing_constraint_count,
                            incoming_constraint_count=target.incoming_constraint_count,
                            action=action,
                            status="failed" if violations else "skipped",
                            detail=(
                                _strict_failure_detail(violations)
                                if violations
                                else "Strict validation failed on other tables; no FK triggers were enabled."
                            ),
                        )
                    )
                return results

        for target in targets:
            detail = (
                "FK trigger enforcement would be disabled"
                if action is ForeignKeyAction.DISABLE and dry_run
                else "FK trigger enforcement disabled"
                if action is ForeignKeyAction.DISABLE
                else "Strict FK validation passed; trigger enforcement would be enabled"
                if strict and dry_run
                else "Strict FK validation passed; trigger enforcement enabled"
                if strict
                else "FK trigger enforcement would be enabled"
                if dry_run
                else "FK trigger enforcement enabled"
            )
            if not dry_run:
                connection.exec_driver_sql(
                    f"ALTER TABLE {qualified_table_name(target.table_name, db_schema)} "
                    f"{trigger_action} TRIGGER ALL"
                )

            results.append(
                ForeignKeyManagementResult(
                    table_name=target.table_name,
                    category=target.category,
                    model_name=target.model_name,
                    model_module=target.model_module,
                    outgoing_constraint_count=target.outgoing_constraint_count,
                    incoming_constraint_count=target.incoming_constraint_count,
                    action=action,
                    status="planned" if dry_run else "applied",
                    detail=detail,
                )
            )

    return results


def collect_foreign_key_trigger_status(
    engine: sa.Engine,
    *,
    db_schema: str | None = None,
    vocabulary_included: bool = False,
) -> list[ForeignKeyStatusResult]:
    _ensure_postgresql_supported(
        engine,
        feature="Foreign key trigger status inspection",
    )

    targets = collect_foreign_key_targets(
        engine,
        db_schema=db_schema,
        vocabulary_included=vocabulary_included,
    )

    results: list[ForeignKeyStatusResult] = []
    query = sa.text(
        """
        SELECT
            SUM(CASE WHEN t.tgenabled = 'D' THEN 1 ELSE 0 END) AS disabled_count,
            SUM(CASE WHEN t.tgenabled <> 'D' THEN 1 ELSE 0 END) AS enabled_count
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE t.tgisinternal
          AND t.tgname LIKE 'RI_ConstraintTrigger%'
          AND c.relname = :table_name
          AND (:db_schema IS NULL OR n.nspname = :db_schema)
        """
    )

    with engine.connect() as connection:
        for target in targets:
            disabled_count, enabled_count = connection.execute(
                query,
                {
                    "table_name": target.table_name,
                    "db_schema": db_schema,
                },
            ).one()

            results.append(
                ForeignKeyStatusResult(
                    table_name=target.table_name,
                    category=target.category,
                    model_name=target.model_name,
                    model_module=target.model_module,
                    disabled_trigger_count=int(disabled_count or 0),
                    enabled_trigger_count=int(enabled_count or 0),
                    outgoing_constraint_count=target.outgoing_constraint_count,
                    incoming_constraint_count=target.incoming_constraint_count,
                )
            )

    return results
