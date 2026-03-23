from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa

from .tables import TableCategory, qualified_table_name, select_omop_tables


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
