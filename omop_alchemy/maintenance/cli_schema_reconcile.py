from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa
from sqlalchemy.engine.interfaces import ReflectedForeignKeyConstraint, ReflectedIndex

from ..backends import backend_supports, resolve_backend
from .cli_indexes import _cluster_target_name
from .tables import (
    TableCategory,
    select_maintenance_tables,
)


@dataclass(frozen=True)
class ReconciliationIssue:
    """A single schema drift detail: column, index, FK, or cluster mismatch between ORM metadata and the database."""

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
    """Per-table schema reconciliation summary: whether ORM metadata matches the live database."""

    table_name: str
    category: TableCategory
    model_name: str
    model_module: str
    status: str
    issue_count: int
    detail: str


@dataclass(frozen=True)
class SchemaReconciliationReport:
    """Complete reconciliation report across all selected ORM-managed tables."""

    backend: str
    table_results: tuple[TableReconciliationResult, ...]
    issues: tuple[ReconciliationIssue, ...]


def _schema_table(table: sa.Table, db_schema: str | None) -> sa.Table:
    """Return table unchanged when db_schema is None, or a schema-qualified copy when a schema is specified."""
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
    """Compile a SQLAlchemy type to its dialect-specific string and normalise whitespace/case for comparison."""
    return type_.compile(dialect=dialect).lower().replace(" ", "")


def _expected_foreign_keys(
    table: sa.Table,
) -> dict[tuple[tuple[str, ...], str, tuple[str, ...]], sa.ForeignKeyConstraint]:
    """Index ORM-defined FK constraints by (constrained_cols, referred_table, referred_cols) for diffing."""
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
) -> dict[tuple[tuple[str, ...], str, tuple[str, ...]], ReflectedForeignKeyConstraint]:
    """Index live FK constraints from the database inspector by the same key tuple used by _expected_foreign_keys."""
    actual: dict[tuple[tuple[str, ...], str, tuple[str, ...]], ReflectedForeignKeyConstraint] = {}
    for foreign_key in inspector.get_foreign_keys(table_name, schema=db_schema):
        constrained_columns = tuple(foreign_key.get("constrained_columns") or [])
        referred_columns = tuple(foreign_key.get("referred_columns") or [])
        referred_table = str(foreign_key.get("referred_table"))
        actual[(constrained_columns, referred_table, referred_columns)] = foreign_key
    return actual


def _expected_indexes(table: sa.Table) -> dict[str, sa.Index]:
    """Return ORM-defined named indexes for a table, keyed by index name."""
    return {
        str(index.name): index
        for index in table.indexes
        if index.name is not None
    }


def _actual_indexes(
    inspector: sa.Inspector,
    table_name: str,
    db_schema: str | None,
) -> dict[str, ReflectedIndex]:
    """Return live named indexes from the database inspector, keyed by index name."""
    return {
        str(index["name"]): index
        for index in inspector.get_indexes(table_name, schema=db_schema)
        if index.get("name") is not None
    }


def reconcile_schema(
    engine: sa.Engine,
    *,
    db_schema: str | None = None,
    vocabulary_included: bool = False,
) -> SchemaReconciliationReport:
    """Compare ORM metadata against the live database schema; reports missing columns, indexes, FKs, and cluster state."""
    excluded_categories: tuple[TableCategory, ...] = (
        () if vocabulary_included else (TableCategory.VOCABULARY,)
    )
    _backend = resolve_backend(engine)
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
                            object_name=constraint.name if isinstance(constraint.name, str) else ",".join(constrained_columns),
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
                actual_columns_for_index = tuple(c for c in (actual_index.get("column_names") or []) if c is not None)
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
                            actual=", ".join(c for c in (index.get("column_names") or []) if c is not None),
                            detail="Index exists in the database but is not defined in ORM metadata.",
                        )
                    )

            if backend_supports(_backend, "get_clustered_index_name"):
                expected_cluster = _cluster_target_name(maintenance_table)
                actual_cluster = _backend.get_clustered_index_name(
                    connection,
                    maintenance_table.table_name,
                    db_schema,
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
