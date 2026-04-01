from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa

from ..backend_support import Dialect, require_backend
from .tables import (
    TableCategory,
    TableScope,
    qualified_table_name,
    resolve_maintenance_tables,
)


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

    selected_tables = resolve_maintenance_tables(
        scope=scope,
        table_names=table_names,
    )
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
