from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, cast

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.engine import Engine
from sqlalchemy.sql import ColumnElement, ColumnExpressionArgument, func

from ...model.vocabulary.concept import Concept
from ...model.vocabulary.concept_synonym import Concept_Synonym

from ....backend_support import POSTGRESQL_DIALECT


@dataclass(frozen=True)
class FullTextTarget:
    name: str
    table: sa.Table
    source_column_name: str
    vector_column_name: str
    index_name: str

    @property
    def table_name(self) -> str:
        return self.table.name

    @property
    def source_column(self) -> sa.Column[object]:
        return self.table.c[self.source_column_name]


class FullTextAction(StrEnum):
    INSTALL = "install"
    POPULATE = "populate"
    DROP = "drop"


@dataclass(frozen=True)
class FullTextResult:
    target_name: str
    table_name: str
    source_column_name: str
    vector_column_name: str
    index_name: str
    action: FullTextAction
    status: str
    detail: str
    row_count: int | None = None


class FullTextError(RuntimeError):
    """Raised when PostgreSQL full-text maintenance fails."""


CONCEPT_NAME_TSVECTOR_COLUMN = "concept_name_tsvector"
CONCEPT_SYNONYM_NAME_TSVECTOR_COLUMN = "concept_synonym_name_tsvector"

FULLTEXT_TARGETS = (
    FullTextTarget(
        name="concept",
        table=cast(sa.Table, Concept.__table__),
        source_column_name="concept_name",
        vector_column_name=CONCEPT_NAME_TSVECTOR_COLUMN,
        index_name="idx_gin_concept_name_tsvector",
    ),
    FullTextTarget(
        name="concept_synonym",
        table=cast(sa.Table, Concept_Synonym.__table__),
        source_column_name="concept_synonym_name",
        vector_column_name=CONCEPT_SYNONYM_NAME_TSVECTOR_COLUMN,
        index_name="idx_gin_concept_synonym_name_tsvector",
    ),
)


def _fulltext_target_for_table(table_name: str) -> FullTextTarget:
    for target in FULLTEXT_TARGETS:
        if target.table_name == table_name:
            return target
    raise RuntimeError(f"No full-text target configured for table `{table_name}`.")


def _stored_tsvector_column(
    table: sa.FromClause,
    column_name: str,
) -> sa.ColumnElement[Any] | None:
    column = table.c.get(column_name)
    if column is None:
        return None
    return column


def _computed_tsvector(
    text_column: ColumnExpressionArgument[str],
    *,
    regconfig: str,
) -> ColumnElement[object]:
    return func.to_tsvector(regconfig, func.coalesce(text_column, ""))


def _optional_tsvector_column(target: FullTextTarget) -> sa.Column[Any]:
    return sa.Column(target.vector_column_name, TSVECTOR, nullable=True)


def register_optional_fulltext_columns() -> None:
    """
    Register optional sidecar tsvector columns on SQLAlchemy metadata.

    This mutates ORM table metadata for the current process so query builders can
    point at stored full-text columns when they exist in the database.
    """
    for target in FULLTEXT_TARGETS:
        if target.vector_column_name in target.table.c:
            continue
        target.table.append_column(_optional_tsvector_column(target))


def unregister_optional_fulltext_columns() -> None:
    """
    Remove optional sidecar tsvector columns from SQLAlchemy metadata.

    This is useful after explicitly dropping the columns from the database, so
    future query builders in the same process fall back to inline expressions.
    """
    for target in FULLTEXT_TARGETS:
        column = target.table.c.get(target.vector_column_name)
        if column is None:
            continue
        target.table._columns.remove(column)


def concept_name_tsvector_expression(*, regconfig: str = "english") -> ColumnElement[object]:
    stored = _stored_tsvector_column(
        Concept.__table__,
        CONCEPT_NAME_TSVECTOR_COLUMN,
    )
    if stored is not None:
        return stored
    return _computed_tsvector(Concept.concept_name, regconfig=regconfig)


def concept_synonym_name_tsvector_expression(
    *,
    regconfig: str = "english",
) -> ColumnElement[object]:
    stored = _stored_tsvector_column(
        Concept_Synonym.__table__,
        CONCEPT_SYNONYM_NAME_TSVECTOR_COLUMN,
    )
    if stored is not None:
        return stored
    return _computed_tsvector(
        Concept_Synonym.concept_synonym_name,
        regconfig=regconfig,
    )


def _qualified_index_name(index_name: str, db_schema: str | None) -> str:
    if db_schema:
        return f"{db_schema}.{index_name}"
    return index_name


def _qualified_table_name(table_name: str, db_schema: str | None) -> str:
    if db_schema:
        return f"{db_schema}.{table_name}"
    return table_name


def _ensure_supported_backend(engine: Engine) -> None:
    if engine.dialect.name == POSTGRESQL_DIALECT:
        return
    raise RuntimeError(
        "PostgreSQL full-text vector column management is only supported for "
        f"PostgreSQL engines. Current dialect: '{engine.dialect.name}'."
    )


def _install_target(
    connection: sa.Connection,
    *,
    target: FullTextTarget,
    db_schema: str | None,
    create_indexes: bool,
    fastupdate: bool,
) -> None:
    connection.exec_driver_sql(
        f"""
        ALTER TABLE {_qualified_table_name(target.table_name, db_schema)}
        ADD COLUMN IF NOT EXISTS {target.vector_column_name} tsvector
        """
    )

    if not create_indexes:
        return

    connection.exec_driver_sql(
        f"""
        CREATE INDEX IF NOT EXISTS {_qualified_index_name(target.index_name, db_schema)}
        ON {_qualified_table_name(target.table_name, db_schema)}
        USING GIN ({target.vector_column_name})
        WITH (fastupdate = {'on' if fastupdate else 'off'})
        """
    )


def install_fulltext_columns(
    engine: Engine,
    *,
    db_schema: str | None = None,
    create_indexes: bool = True,
    fastupdate: bool = False,
    dry_run: bool = False,
) -> tuple[FullTextResult, ...]:
    _ensure_supported_backend(engine)

    results: list[FullTextResult] = []
    try:
        if not dry_run:
            with engine.begin() as connection:
                for target in FULLTEXT_TARGETS:
                    _install_target(
                        connection,
                        target=target,
                        db_schema=db_schema,
                        create_indexes=create_indexes,
                        fastupdate=fastupdate,
                    )
            register_optional_fulltext_columns()
    except Exception as exc:
        raise FullTextError(
            "Full-text install failed for PostgreSQL sidecar columns. "
            f"Underlying error: {exc.__class__.__name__}: {exc}"
        ) from exc

    for target in FULLTEXT_TARGETS:
        results.append(
            FullTextResult(
                target_name=target.name,
                table_name=target.table_name,
                source_column_name=target.source_column_name,
                vector_column_name=target.vector_column_name,
                index_name=target.index_name,
                action=FullTextAction.INSTALL,
                status="planned" if dry_run else "applied",
                detail=(
                    "tsvector column would be installed"
                    if dry_run and not create_indexes
                    else "tsvector column and GIN index would be installed"
                    if dry_run
                    else "tsvector column installed"
                    if not create_indexes
                    else "tsvector column and GIN index installed"
                ),
            )
        )
    return tuple(results)


def _populate_target(
    connection: sa.Connection,
    *,
    target: FullTextTarget,
    db_schema: str | None,
    regconfig: str,
) -> int | None:
    result = connection.execute(
        sa.text(
            f"""
            UPDATE {_qualified_table_name(target.table_name, db_schema)}
            SET {target.vector_column_name} = to_tsvector(
                CAST(:regconfig AS regconfig),
                coalesce({target.source_column_name}, '')
            )
            """
        ),
        {"regconfig": regconfig},
    )
    if result.rowcount is None or result.rowcount < 0:
        return None
    return int(result.rowcount)


def populate_fulltext_columns(
    engine: Engine,
    *,
    db_schema: str | None = None,
    regconfig: str = "english",
    dry_run: bool = False,
) -> tuple[FullTextResult, ...]:
    _ensure_supported_backend(engine)

    row_counts: dict[str, int | None] = {}
    try:
        if not dry_run:
            with engine.begin() as connection:
                for target in FULLTEXT_TARGETS:
                    row_counts[target.name] = _populate_target(
                        connection,
                        target=target,
                        db_schema=db_schema,
                        regconfig=regconfig,
                    )
            register_optional_fulltext_columns()
    except Exception as exc:
        raise FullTextError(
            "Full-text populate failed for PostgreSQL sidecar columns. "
            f"Underlying error: {exc.__class__.__name__}: {exc}"
        ) from exc

    results: list[FullTextResult] = []
    for target in FULLTEXT_TARGETS:
        row_count = None if dry_run else row_counts.get(target.name)
        results.append(
            FullTextResult(
                target_name=target.name,
                table_name=target.table_name,
                source_column_name=target.source_column_name,
                vector_column_name=target.vector_column_name,
                index_name=target.index_name,
                action=FullTextAction.POPULATE,
                status="planned" if dry_run else "applied",
                detail=(
                    "tsvector column would be populated from source text"
                    if dry_run
                    else "tsvector column populated from source text"
                ),
                row_count=row_count,
            )
        )
    return tuple(results)


def _drop_target(
    connection: sa.Connection,
    *,
    target: FullTextTarget,
    db_schema: str | None,
    drop_indexes: bool,
) -> None:
    if drop_indexes:
        connection.exec_driver_sql(
            f"DROP INDEX IF EXISTS {_qualified_index_name(target.index_name, db_schema)}"
        )
    connection.exec_driver_sql(
        f"""
        ALTER TABLE {_qualified_table_name(target.table_name, db_schema)}
        DROP COLUMN IF EXISTS {target.vector_column_name}
        """
    )


def drop_fulltext_columns(
    engine: Engine,
    *,
    db_schema: str | None = None,
    drop_indexes: bool = True,
    dry_run: bool = False,
) -> tuple[FullTextResult, ...]:
    _ensure_supported_backend(engine)

    try:
        if not dry_run:
            with engine.begin() as connection:
                for target in FULLTEXT_TARGETS:
                    _drop_target(
                        connection,
                        target=target,
                        db_schema=db_schema,
                        drop_indexes=drop_indexes,
                    )
            unregister_optional_fulltext_columns()
    except Exception as exc:
        raise FullTextError(
            "Full-text drop failed for PostgreSQL sidecar columns. "
            f"Underlying error: {exc.__class__.__name__}: {exc}"
        ) from exc

    results: list[FullTextResult] = []
    for target in FULLTEXT_TARGETS:
        results.append(
            FullTextResult(
                target_name=target.name,
                table_name=target.table_name,
                source_column_name=target.source_column_name,
                vector_column_name=target.vector_column_name,
                index_name=target.index_name,
                action=FullTextAction.DROP,
                status="planned" if dry_run else "applied",
                detail=(
                    "tsvector column would be dropped"
                    if dry_run and not drop_indexes
                    else "tsvector column and GIN index would be dropped"
                    if dry_run
                    else "tsvector column dropped"
                    if not drop_indexes
                    else "tsvector column and GIN index dropped"
                ),
            )
        )
    return tuple(results)


__all__ = [
    "CONCEPT_NAME_TSVECTOR_COLUMN",
    "CONCEPT_SYNONYM_NAME_TSVECTOR_COLUMN",
    "FULLTEXT_TARGETS",
    "FullTextAction",
    "FullTextError",
    "FullTextResult",
    "FullTextTarget",
    "concept_name_tsvector_expression",
    "concept_synonym_name_tsvector_expression",
    "drop_fulltext_columns",
    "install_fulltext_columns",
    "populate_fulltext_columns",
    "register_optional_fulltext_columns",
    "unregister_optional_fulltext_columns",
]
