from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa

from ..backend_support import POSTGRESQL_DIALECT, require_backend
from .tables import (
    TableCategory,
    TableScope,
    qualified_table_name,
    resolve_maintenance_tables,
)


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

    require_backend(
        engine,
        feature="Table truncation",
        supported_dialects=(POSTGRESQL_DIALECT,),
    )

    selected_tables = resolve_maintenance_tables(
        scope=scope,
        table_names=table_names,
    )
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
                    detail=(
                        "table would be truncated"
                        if dry_run
                        else "table truncated"
                    ),
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
