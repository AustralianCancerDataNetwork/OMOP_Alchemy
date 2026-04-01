from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa

from .tables import (
    MaintenanceTable,
    TableCategory,
    collect_maintenance_tables,
    missing_maintenance_tables,
    schema_adjusted_metadata,
)


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
    missing_table_names = {
        table.table_name
        for table in missing_tables
    }

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
                tables=[
                    adjusted_tables[table.table_name]
                    for table in creatable_tables
                ],
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
                        "table blocked by unresolved dependencies: "
                        + ", ".join(blocked)
                        if blocked is not None
                        else "table would be created from ORM metadata"
                        if dry_run
                        else "table created from ORM metadata"
                    ),
                )
            )

    return results
