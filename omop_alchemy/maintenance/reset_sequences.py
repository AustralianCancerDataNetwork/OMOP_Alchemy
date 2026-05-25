from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa

from ..backend_support import Dialect, require_backend
from .tables import (
    TableCategory,
    qualified_table_name,
    select_omop_tables,
)


@dataclass(frozen=True)
class SequenceTarget:
    table_name: str
    category: TableCategory
    model_name: str
    model_module: str
    schema_name: str | None
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
                schema_name=table.table.schema,
                pk_column_name=pk_column_name,
            )
        )
    return targets
def _ensure_postgresql_supported(engine: sa.Engine) -> None:
    require_backend(
        engine,
        feature="Sequence reset",
        supported_dialects=(Dialect.POSTGRESQL,),
    )


def reset_model_sequences(
    engine: sa.Engine,
    *,
    db_schema: str | None = None,
    vocabulary_included: bool = False,
    dry_run: bool = False,
) -> list[SequenceResetResult]:
    _ensure_postgresql_supported(engine)

    inspector = sa.inspect(engine)
    targets = collect_sequence_targets(vocabulary_included=vocabulary_included)
    results: list[SequenceResetResult] = []

    with engine.begin() as connection:
        for target in targets:
            if not inspector.has_table(
                target.table_name,
                schema=db_schema if db_schema is not None else target.schema_name,
            ):
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
                    {
                        "sequence_name": sequence_name,
                        "next_value": next_value,
                    },
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
