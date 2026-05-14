from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.schema import AddConstraint, CreateIndex, CreateSchema, CreateTable

from .tables import schema_adjusted_metadata, select_omop_tables


class DDLDialect(StrEnum):
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"


@dataclass(frozen=True)
class DDLExportResult:
    status: str
    dialect: DDLDialect
    output_path: str
    db_schema: str | None
    vocabulary_included: bool
    indexes_included: bool
    schema_statement_included: bool
    table_count: int
    index_count: int
    statement_count: int
    detail: str


def _build_dialect(dialect: DDLDialect) -> sa.engine.interfaces.Dialect:
    if dialect is DDLDialect.POSTGRESQL:
        return postgresql.dialect()
    if dialect is DDLDialect.SQLITE:
        return sqlite.dialect()
    raise RuntimeError(f"Unsupported DDL dialect: {dialect.value}")


def _compile_statement(
    statement: sa.schema.ExecutableDDLElement,
    *,
    dialect: sa.engine.interfaces.Dialect,
) -> str:
    rendered = str(
        statement.compile(
            dialect=dialect,
            compile_kwargs={"literal_binds": True},
        )
    ).strip()
    if not rendered:
        return ""
    if rendered.endswith(";"):
        return rendered
    return f"{rendered};"


def export_ddl(
    *,
    output_path: str | Path,
    dialect: DDLDialect = DDLDialect.POSTGRESQL,
    db_schema: str | None = None,
    vocabulary_included: bool = True,
    indexes_included: bool = True,
    schema_statement_included: bool = True,
    if_not_exists: bool = True,
) -> DDLExportResult:
    if db_schema and dialect is DDLDialect.SQLITE:
        raise RuntimeError(
            "Schema-qualified DDL export is not supported for SQLite. "
            "Omit `--db-schema` or use `--dialect postgresql`."
        )

    target_path = Path(output_path).expanduser().resolve()
    target_path.parent.mkdir(parents=True, exist_ok=True)

    metadata, _ = schema_adjusted_metadata(
        select_omop_tables(vocabulary_included=vocabulary_included),
        db_schema=db_schema,
    )
    ddl_dialect = _build_dialect(dialect)

    statements: list[str] = [
        "-- OMOP Alchemy generated DDL",
        f"-- dialect: {dialect.value}",
        f"-- schema: {db_schema or 'default'}",
        f"-- vocabulary tables included: {'yes' if vocabulary_included else 'no'}",
        f"-- secondary indexes included: {'yes' if indexes_included else 'no'}",
        "",
    ]

    statement_count = 0
    table_count = 0
    index_count = 0
    foreign_key_constraint_count = 0

    ordered_tables = sorted(
        metadata.tables.values(),
        key=lambda table: ((table.schema or ""), table.name),
    )

    if schema_statement_included and db_schema:
        statements.append(
            _compile_statement(
                CreateSchema(db_schema, if_not_exists=if_not_exists),
                dialect=ddl_dialect,
            )
        )
        statements.append("")
        statement_count += 1

    for table in ordered_tables:
        include_foreign_keys = None
        if dialect is DDLDialect.POSTGRESQL:
            # Emit PostgreSQL foreign keys separately so cyclic OMOP references
            # become valid distributable SQL.
            include_foreign_keys = []
        statements.append(
            _compile_statement(
                CreateTable(
                    table,
                    include_foreign_key_constraints=include_foreign_keys,
                    if_not_exists=if_not_exists,
                ),
                dialect=ddl_dialect,
            )
        )
        statements.append("")
        statement_count += 1
        table_count += 1

    if dialect is DDLDialect.POSTGRESQL:
        for table in ordered_tables:
            for constraint in sorted(
                table.foreign_key_constraints,
                key=lambda item: item.name or "_".join(column.name for column in item.columns),
            ):
                statements.append(
                    _compile_statement(
                        AddConstraint(constraint),
                        dialect=ddl_dialect,
                    )
                )
                statements.append("")
                statement_count += 1
                foreign_key_constraint_count += 1

    if indexes_included:
        for table in ordered_tables:
            for index in sorted(table.indexes, key=lambda item: item.name or ""):
                statements.append(
                    _compile_statement(
                        CreateIndex(index, if_not_exists=if_not_exists),
                        dialect=ddl_dialect,
                    )
                )
                statements.append("")
                statement_count += 1
                index_count += 1

    target_path.write_text("\n".join(statements).rstrip() + "\n", encoding="utf-8")

    schema_summary = f"schema `{db_schema}`" if db_schema else "default schema"
    detail = (
        f"Wrote {table_count} table statement(s)"
        f"{f', {foreign_key_constraint_count} foreign key statement(s)' if foreign_key_constraint_count else ''}"
        f"{f' and {index_count} index statement(s)' if indexes_included else ''} "
        f"for {schema_summary}."
    )

    return DDLExportResult(
        status="created",
        dialect=dialect,
        output_path=str(target_path),
        db_schema=db_schema,
        vocabulary_included=vocabulary_included,
        indexes_included=indexes_included,
        schema_statement_included=bool(schema_statement_included and db_schema),
        table_count=table_count,
        index_count=index_count,
        statement_count=statement_count,
        detail=detail,
    )
