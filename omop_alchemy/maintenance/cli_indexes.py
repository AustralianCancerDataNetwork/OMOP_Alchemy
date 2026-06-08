"""Index management commands for dropping and recreating ORM-defined secondary indexes."""

from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa
import typer

from omop_alchemy.cdm.base.indexing import OMOP_CLUSTER_INDEX_INFO_KEY

from ..backends import resolve_backend, backend_supports
from ._cli_utils import omop_command
from .tables import (
    MaintenanceTable,
    TableCategory,
    schema_adjusted_metadata,
    select_omop_tables,
)
from .ui import (
    console,
    render_index_note,
    render_index_results,
    render_index_summary,
)


@dataclass(frozen=True)
class IndexTarget:
    """An ORM-defined index that currently exists in the target database."""

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
    """Outcome of creating or dropping one ORM-defined index, or clustering a table."""

    operation: str
    table_name: str
    category: TableCategory
    model_name: str
    model_module: str
    index_name: str
    column_names: tuple[str, ...]
    unique: bool
    clustered: bool
    enable: bool
    status: str
    detail: str


def _schema_metadata_indexes(
    tables: list[MaintenanceTable],
    db_schema: str | None,
) -> dict[tuple[str, str], sa.Index]:
    """Return a (table_name, index_name) → Index mapping from ORM metadata, adjusted for db_schema if provided."""
    indexes: dict[tuple[str, str], sa.Index] = {}

    if db_schema is None:
        for table in tables:
            for index in table.table.indexes:
                indexes[(table.table_name, str(index.name))] = index
        return indexes

    _, copied_tables = schema_adjusted_metadata(tables, db_schema=db_schema)
    for table_name, table in copied_tables.items():
        for index in table.indexes:
            indexes[(table_name, str(index.name))] = index

    return indexes


def _cluster_target_name(table: MaintenanceTable) -> str | None:
    """Return the name of the ORM-designated cluster index for a table, or None if no cluster target is defined."""
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
    """Return the column names of the named cluster index. Falls back to the primary key if the index is not found."""
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
    """List ORM-defined indexes that currently exist in the target database."""
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
    enable: bool,
    db_schema: str | None = None,
    vocabulary_included: bool = False,
    dry_run: bool = False,
    cluster: bool = True,
) -> list[IndexManagementResult]:
    """Create or drop all ORM-defined indexes. CLUSTERs tables when enabling and cluster=True."""
    backend = resolve_backend(engine)
    inspector = sa.inspect(engine)
    selected_tables = select_omop_tables(vocabulary_included=vocabulary_included)
    metadata_indexes = _schema_metadata_indexes(selected_tables, db_schema)
    clustering_supported = backend_supports(backend, "cluster_table")

    results: list[IndexManagementResult] = []

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
                not enable and exists
            ) or (
                enable and not exists
            )

            if not should_apply:
                continue

            schema_index = metadata_indexes[(table.table_name, index_name)]
            if not dry_run:
                # Each index gets its own transaction so WAL is committed and
                # checkpointable before the next index build begins. One large
                # transaction for all indexes would accumulate 5+ GB of WAL
                # on vocabulary tables before any checkpoint can reclaim it.
                with engine.begin() as connection:
                    if not enable:
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
                    enable=enable,
                    status="planned" if dry_run else "applied",
                    detail=(
                        "metadata-defined index would be dropped"
                        if not enable and dry_run
                        else "metadata-defined index dropped"
                        if not enable
                        else "metadata-defined index would be created"
                        if dry_run
                        else "metadata-defined index created"
                    ),
                )
            )

        if enable:
            cluster_index_name = _cluster_target_name(table)
            if cluster_index_name is None:
                continue

            cluster_columns = _cluster_column_names(table, cluster_index_name)
            if not clustering_supported or not cluster:
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
                        enable=enable,
                        status="skipped",
                        detail=(
                            f"cluster metadata present but unsupported on {backend.name}"
                            if not clustering_supported
                            else "clustering skipped (run 'indexes cluster' to apply)"
                        ),
                    )
                )
                continue

            if not dry_run:
                with engine.begin() as connection:
                    backend.cluster_table(connection, table.table_name, cluster_index_name, db_schema)

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
                    enable=enable,
                    status="planned" if dry_run else "applied",
                    detail=(
                        "table would be clustered using ORM-defined metadata"
                        if dry_run
                        else "table clustered using ORM-defined metadata"
                    ),
                )
            )

    return results


app = typer.Typer(
    help="Manage ORM-defined secondary indexes.",
    rich_markup_mode="rich",
)


@app.command("disable")
@omop_command("indexes disable", dry_run=True)
def disable_indexes_command(
    conn,
    engine,
    vocabulary_included: bool = typer.Option(
        False,
        "--vocab/--no-vocab",
        help="Include OMOP vocabulary tables in the selection.",
    ),
    dry_run: bool = False,
) -> None:
    """Drop all ORM-defined secondary indexes from the target database. Useful before bulk data loads."""
    with console.status("Managing metadata-defined indexes..."):
        results = manage_indexes(
            engine,
            enable=False,
            db_schema=conn.db_schema,
            vocabulary_included=vocabulary_included,
            dry_run=dry_run,
        )
    console.print(render_index_results(results))
    console.print(render_index_summary(results, dry_run=dry_run))
    console.print(render_index_note(enable=False))


@app.command("enable")
@omop_command("indexes enable", dry_run=True)
def enable_indexes_command(
    conn,
    engine,
    vocabulary_included: bool = typer.Option(
        False,
        "--vocab/--no-vocab",
        help="Include OMOP vocabulary tables in the selection.",
    ),
    dry_run: bool = False,
) -> None:
    """Recreate all ORM-defined secondary indexes. Also CLUSTERs tables on PostgreSQL where metadata specifies it.

    Note: CLUSTER rewrites the full heap and requires ~2× the table size in free disk space.
    On large vocabulary tables, run 'indexes cluster' as a separate step instead.
    """
    with console.status("Managing metadata-defined indexes..."):
        results = manage_indexes(
            engine,
            enable=True,
            db_schema=conn.db_schema,
            vocabulary_included=vocabulary_included,
            dry_run=dry_run,
        )
    console.print(render_index_results(results))
    console.print(render_index_summary(results, dry_run=dry_run))
    console.print(render_index_note(enable=True))


@app.command("cluster")
@omop_command("indexes cluster", dry_run=True)
def cluster_tables_command(
    conn,
    engine,
    vocabulary_included: bool = typer.Option(
        False,
        "--vocab/--no-vocab",
        help="Include OMOP vocabulary tables in the selection.",
    ),
    dry_run: bool = False,
) -> None:
    """CLUSTER tables using their ORM-designated cluster index.

    Physically rewrites table data sorted by the cluster index for improved sequential-scan
    performance. Requires approximately 2× the table size in free disk space per table.

    Run this after 'indexes enable' once you have confirmed sufficient disk headroom.
    On Docker, check Docker Desktop → Resources → Virtual Disk Limit before running on
    vocabulary tables (concept_ancestor alone needs ~5 GB free).
    """
    backend = resolve_backend(engine)
    if not backend_supports(backend, "cluster_table"):
        console.print(f"[yellow]Clustering is not supported on {backend.name}.[/yellow]")
        raise typer.Exit(0)

    inspector = sa.inspect(engine)
    selected_tables = select_omop_tables(vocabulary_included=vocabulary_included)
    results: list[IndexManagementResult] = []

    for table in selected_tables:
        if not inspector.has_table(table.table_name, schema=conn.db_schema):
            continue

        cluster_index_name = _cluster_target_name(table)
        if cluster_index_name is None:
            continue

        cluster_columns = _cluster_column_names(table, cluster_index_name)

        if not dry_run:
            with engine.begin() as connection:
                backend.cluster_table(connection, table.table_name, cluster_index_name, conn.db_schema)
            with engine.connect() as connection:
                backend.analyze_table(connection, table.table_name, conn.db_schema)
                connection.commit()

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
                enable=True,
                status="planned" if dry_run else "applied",
                detail=(
                    "table would be clustered and analyzed"
                    if dry_run
                    else "table clustered and analyzed"
                ),
            )
        )

    console.print(render_index_results(results))
    console.print(render_index_summary(results, dry_run=dry_run))
