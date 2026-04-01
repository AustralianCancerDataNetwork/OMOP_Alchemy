from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

import sqlalchemy as sa

from omop_alchemy.cdm.base.indexing import OMOP_CLUSTER_INDEX_INFO_KEY

from ..backend_support import Dialect, backend_label, supports_backend
from .tables import (
    MaintenanceTable,
    TableCategory,
    qualified_table_name,
    schema_adjusted_metadata,
    select_omop_tables,
)


class IndexAction(StrEnum):
    DISABLE = "disable"
    ENABLE = "enable"


@dataclass(frozen=True)
class IndexTarget:
    table_name: str
    category: TableCategory
    model_name: str
    model_module: str
    index_name: str
    column_names: tuple[str, ...]
    unique: bool
    clustered: bool


@dataclass(frozen=True)
class IndexManagementResult:
    operation: str
    table_name: str
    category: TableCategory
    model_name: str
    model_module: str
    index_name: str
    column_names: tuple[str, ...]
    unique: bool
    clustered: bool
    action: IndexAction
    status: str
    detail: str
def _schema_metadata_indexes(
    tables: list[MaintenanceTable],
    db_schema: str | None,
) -> dict[tuple[str, str], sa.Index]:
    indexes: dict[tuple[str, str], sa.Index] = {}

    if db_schema is None:
        for table in tables:
            for index in table.table.indexes:
                indexes[(table.table_name, str(index.name))] = index
        return indexes

    _, copied_tables = schema_adjusted_metadata(
        tables,
        db_schema=db_schema,
    )
    for table_name, table in copied_tables.items():
        for index in table.indexes:
            indexes[(table_name, str(index.name))] = index

    return indexes


def _cluster_target_name(table: MaintenanceTable) -> str | None:
    cluster_indexes = [
        str(index.name)
        for index in table.table.indexes
        if index.info.get(OMOP_CLUSTER_INDEX_INFO_KEY) is True
    ]
    if cluster_indexes:
        return cluster_indexes[0]

    cluster_name = table.table.info.get(OMOP_CLUSTER_INDEX_INFO_KEY)
    if isinstance(cluster_name, str):
        return cluster_name

    return None


def _cluster_column_names(
    table: MaintenanceTable,
    cluster_index_name: str,
) -> tuple[str, ...]:
    for index in table.table.indexes:
        if str(index.name) == cluster_index_name:
            return tuple(column.name for column in index.columns)
    return table.primary_key_names
def collect_index_targets(
    engine: sa.Engine,
    *,
    db_schema: str | None = None,
    vocabulary_included: bool = False,
) -> list[IndexTarget]:
    inspector = sa.inspect(engine)
    selected_tables = select_omop_tables(vocabulary_included=vocabulary_included)

    targets: list[IndexTarget] = []
    for table in selected_tables:
        if not inspector.has_table(table.table_name, schema=db_schema):
            continue

        existing_index_names = {
            index["name"]
            for index in inspector.get_indexes(table.table_name, schema=db_schema)
        }

        for metadata_index in sorted(table.table.indexes, key=lambda idx: idx.name or ""):
            if metadata_index.name not in existing_index_names:
                continue

            targets.append(
                IndexTarget(
                    table_name=table.table_name,
                    category=table.category,
                    model_name=table.model_name,
                    model_module=table.model_module,
                    index_name=str(metadata_index.name),
                    column_names=tuple(column.name for column in metadata_index.columns),
                    unique=bool(metadata_index.unique),
                    clustered=metadata_index.info.get(OMOP_CLUSTER_INDEX_INFO_KEY) is True,
                )
            )

    return targets


def manage_indexes(
    engine: sa.Engine,
    *,
    action: IndexAction,
    db_schema: str | None = None,
    vocabulary_included: bool = False,
    dry_run: bool = False,
) -> list[IndexManagementResult]:
    inspector = sa.inspect(engine)
    selected_tables = select_omop_tables(vocabulary_included=vocabulary_included)
    metadata_indexes = _schema_metadata_indexes(selected_tables, db_schema)
    clustering_supported = supports_backend(
        engine,
        supported_dialects=(Dialect.POSTGRESQL,),
    )

    results: list[IndexManagementResult] = []

    with engine.begin() as connection:
        for table in selected_tables:
            if not inspector.has_table(table.table_name, schema=db_schema):
                continue

            existing_index_names = {
                index["name"]
                for index in inspector.get_indexes(table.table_name, schema=db_schema)
            }

            for metadata_index in sorted(table.table.indexes, key=lambda idx: idx.name or ""):
                index_name = str(metadata_index.name)
                exists = index_name in existing_index_names
                should_apply = (
                    action is IndexAction.DISABLE and exists
                ) or (
                    action is IndexAction.ENABLE and not exists
                )

                if not should_apply:
                    continue

                schema_index = metadata_indexes[(table.table_name, index_name)]
                if not dry_run:
                    if action is IndexAction.DISABLE:
                        schema_index.drop(bind=connection, checkfirst=True)
                    else:
                        schema_index.create(bind=connection, checkfirst=True)

                results.append(
                    IndexManagementResult(
                        operation="index",
                        table_name=table.table_name,
                        category=table.category,
                        model_name=table.model_name,
                        model_module=table.model_module,
                        index_name=index_name,
                        column_names=tuple(column.name for column in metadata_index.columns),
                        unique=bool(metadata_index.unique),
                        clustered=metadata_index.info.get(OMOP_CLUSTER_INDEX_INFO_KEY) is True,
                        action=action,
                        status="planned" if dry_run else "applied",
                        detail=(
                            "metadata-defined index would be dropped"
                            if action is IndexAction.DISABLE and dry_run
                            else "metadata-defined index dropped"
                            if action is IndexAction.DISABLE
                            else "metadata-defined index would be created"
                            if dry_run
                            else "metadata-defined index created"
                        ),
                    )
                )

            if action is IndexAction.ENABLE:
                cluster_index_name = _cluster_target_name(table)
                if cluster_index_name is None:
                    continue

                cluster_columns = _cluster_column_names(table, cluster_index_name)
                if not clustering_supported:
                    results.append(
                        IndexManagementResult(
                            operation="cluster",
                            table_name=table.table_name,
                            category=table.category,
                            model_name=table.model_name,
                            model_module=table.model_module,
                            index_name=cluster_index_name,
                            column_names=cluster_columns,
                            unique=False,
                            clustered=True,
                            action=action,
                            status="skipped",
                            detail=(
                                "cluster metadata present but unsupported on "
                                f"{backend_label(engine.dialect.name)}"
                            ),
                        )
                    )
                    continue

                if not dry_run:
                    connection.exec_driver_sql(
                        f"CLUSTER {qualified_table_name(table.table_name, db_schema)} "
                        f"USING {cluster_index_name}"
                    )

                results.append(
                    IndexManagementResult(
                        operation="cluster",
                        table_name=table.table_name,
                        category=table.category,
                        model_name=table.model_name,
                        model_module=table.model_module,
                        index_name=cluster_index_name,
                        column_names=cluster_columns,
                        unique=False,
                        clustered=True,
                        action=action,
                        status="planned" if dry_run else "applied",
                        detail=(
                            "table would be clustered using ORM-defined metadata"
                            if dry_run
                            else "table clustered using ORM-defined metadata"
                        ),
                    )
                )

    return results
