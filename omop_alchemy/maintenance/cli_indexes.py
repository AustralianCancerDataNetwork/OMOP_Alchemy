"""Index management commands for dropping and recreating ORM-defined secondary indexes."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError
import typer

from omop_alchemy.cdm.base.indexing import OMOP_CLUSTER_INDEX_INFO_KEY

from ..backends import Backend, resolve_backend, backend_supports
from ._cli_utils import ReservedSchema, dry_label, dry_status, omop_command
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
    index_name: str
    column_names: tuple[str, ...]
    unique: bool
    clustered: bool


@dataclass(frozen=True)
class IndexManagementResult(IndexTarget):
    """Outcome of creating or dropping one ORM-defined index, or clustering a table."""
    operation: str
    enable: bool
    status: str
    detail: str


def _is_plain_index(reflected: Mapping[str, Any]) -> bool:
    """True if a reflected index has no expression components, no partial predicate,
    and uses the default (btree) access method. I.e it's safe to treat as a
    faithful equivalent of an ORM-defined index, and safe to capture and restore."""
    column_names = reflected.get("column_names") or []
    if any(name is None for name in column_names):
        return False
    if reflected.get("expressions"):
        return False
    dialect_options = reflected.get("dialect_options") or {}
    if dialect_options.get("postgresql_where"):
        return False
    if dialect_options.get("postgresql_using"):
        return False
    return True


def _find_equivalent_index(
    existing_indexes: Sequence[Mapping[str, Any]],
    column_names: tuple[str, ...],
    unique: bool,
) -> str | None:
    """Physical name of a plain existing index (any name) whose column_names tuple
    (order-sensitive) and unique flag match an ORM-defined index, or None."""
    for reflected in existing_indexes:
        if not _is_plain_index(reflected):
            continue
        if tuple(reflected.get("column_names") or ()) != column_names:
            continue
        if bool(reflected.get("unique")) != unique:
            continue
        return str(reflected["name"])
    return None


def _find_shape_conflict(
    existing_indexes: Sequence[Mapping[str, Any]],
    column_names: tuple[str, ...],
    unique: bool,
) -> Mapping[str, Any] | None:
    """Like _find_equivalent_index but for a non-plain match: an index covering
    the same columns that can't be safely treated as equivalent (expression,
    partial, or non-btree). Used only to explain why such an index is left alone."""
    for reflected in existing_indexes:
        column_names_reflected = reflected.get("column_names") or []
        if any(name is None for name in column_names_reflected):
            continue
        if tuple(column_names_reflected) != column_names:
            continue
        if bool(reflected.get("unique")) != unique:
            continue
        if _is_plain_index(reflected):
            continue
        return reflected
    return None


def _describe_shape_conflict(reflected: Mapping[str, Any]) -> str:
    dialect_options = reflected.get("dialect_options") or {}
    reasons: list[str] = []
    if dialect_options.get("postgresql_where"):
        reasons.append("has a partial WHERE predicate")
    if dialect_options.get("postgresql_using"):
        reasons.append(f"uses non-btree access method '{dialect_options['postgresql_using']}'")
    if not reasons:
        reasons.append("has an unsupported definition")
    return ", ".join(reasons)


# ── Foreign index capture/restore bookkeeping ───────────────────────────────────
#
# `disable` drops indexes to speed up bulk loads. If the only index covering an
# ORM-defined column set is a foreign (non-OMOP_Alchemy) index (e.g. one created
# by the official OHDSI CDM DDL script) dropping it still gives the bulk-load
# speed benefit, but the definition must be captured somewhere that survives across
# separate CLI invocations (disable and enable are documented as usable as two
# separate commands, not just paired within a single process). This is done by
# creating a bookkeeping table in a reserved schema, and recording the foreign
# index's definition there before dropping it.

_DROPPED_INDEXES_TABLE_NAME = "dropped_indexes"


def get_bookkeeping_schema(backend: Backend) -> str | None:
    """Reserved schema name for the dropped-index bookkeeping table, or None on
    backends that don't support named schemas (e.g. SQLite)."""
    if backend.dialect == "sqlite":
        return None
    elif backend.dialect == "postgresql":
        return ReservedSchema.MAINTENANCE.value
    else:
        raise NotImplementedError(f"Bookkeeping schema not defined for backend {backend.name}")


def _dropped_indexes_table(bookkeeping_schema: str | None) -> sa.Table:
    metadata = sa.MetaData()
    return sa.Table(
        _DROPPED_INDEXES_TABLE_NAME,
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("table_name", sa.String(128), nullable=False),
        sa.Column("index_name", sa.String(128), nullable=False),
        sa.Column("column_names_json", sa.Text, nullable=False),
        sa.Column("is_unique", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.Column("captured_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint(
            "table_name", "column_names_json", "is_unique",
            name="uq_dropped_indexes_table_columns",
        ),
        schema=bookkeeping_schema,
    )


def _record_captured_index(
    connection: sa.Connection,
    backend: Backend,
    *,
    table_name: str,
    index_name: str,
    column_names: tuple[str, ...],
    unique: bool,
) -> bool:
    """Capture a foreign index's definition into the bookkeeping table before dropping it.

    Returns False (and records nothing) if this table/column-set already has a
    pending capture awaiting restore -- e.g. a second `disable` run, without an
    intervening `enable`, that finds a *different* foreign index than the one
    already captured. The bookkeeping table's unique constraint only allows one
    pending capture per table/column-set/uniqueness, since only one original name
    could ever be restored to that slot; the caller must leave the second index
    in place rather than dropping something it can no longer track.
    """
    bookkeeping_schema = get_bookkeeping_schema(backend)
    backend.ensure_schema(connection, bookkeeping_schema)
    bookkeeping_table = _dropped_indexes_table(bookkeeping_schema)
    bookkeeping_table.create(bind=connection, checkfirst=True)

    savepoint = connection.begin_nested()
    try:
        connection.execute(
            bookkeeping_table.insert(),
            {
                "table_name": table_name,
                "index_name": index_name,
                "column_names_json": json.dumps(list(column_names)),
                "is_unique": unique,
            },
        )
    except DBAPIError as exc:
        savepoint.rollback()
        if "unique" not in str(exc.orig).lower():
            raise
        return False
    else:
        savepoint.commit()
        return True


def _restore_captured_index(
    connection: sa.Connection,
    backend: Backend,
    *,
    table_name: str,
    db_schema: str | None,
    column_names: tuple[str, ...],
    unique: bool,
) -> str | None:
    """Recreate a previously captured foreign index under its original name and
    remove its bookkeeping row. Returns the restored physical name, or None if
    nothing was captured for this table/column-set."""
    bookkeeping_schema = get_bookkeeping_schema(backend)
    inspector = sa.inspect(connection)
    if not inspector.has_table(_DROPPED_INDEXES_TABLE_NAME, schema=bookkeeping_schema):
        return None

    bookkeeping_table = _dropped_indexes_table(bookkeeping_schema)
    column_names_json = json.dumps(list(column_names))
    row = connection.execute(
        sa.select(bookkeeping_table.c.id, bookkeeping_table.c.index_name).where(
            bookkeeping_table.c.table_name == table_name,
            bookkeeping_table.c.column_names_json == column_names_json,
            bookkeeping_table.c.is_unique == unique,
        )
    ).one_or_none()
    if row is None:
        return None

    restored_index_name = str(row.index_name)
    target_table = sa.Table(table_name, sa.MetaData(), autoload_with=connection, schema=db_schema)
    sa.Index(restored_index_name, *[target_table.c[name] for name in column_names], unique=unique).create(
        bind=connection, checkfirst=True
    )
    connection.execute(bookkeeping_table.delete().where(bookkeeping_table.c.id == row.id))
    return restored_index_name


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

        existing_indexes = inspector.get_indexes(table.table_name, schema=db_schema)
        existing_index_names = {index["name"] for index in existing_indexes}

        for metadata_index in sorted(table.table.indexes, key=lambda idx: idx.name or ""):
            column_names = tuple(column.name for column in metadata_index.columns)
            unique = bool(metadata_index.unique)
            if metadata_index.name in existing_index_names:
                physical_name = str(metadata_index.name)
            else:
                equivalent_name = _find_equivalent_index(existing_indexes, column_names, unique)
                if equivalent_name is None:
                    continue
                physical_name = equivalent_name

            targets.append(
                IndexTarget(
                    table_name=table.table_name,
                    category=table.category,
                    index_name=physical_name,
                    column_names=column_names,
                    unique=unique,
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

        existing_indexes = inspector.get_indexes(table.table_name, schema=db_schema)
        existing_index_names = {index["name"] for index in existing_indexes}

        created_any = False
        clustered_now = False
        physical_index_names: dict[str, str] = {}

        for metadata_index in sorted(table.table.indexes, key=lambda idx: idx.name or ""):
            index_name = str(metadata_index.name)
            column_names = tuple(column.name for column in metadata_index.columns)
            unique = bool(metadata_index.unique)
            exists = index_name in existing_index_names
            should_apply = (
                not enable
            ) or (
                enable and not exists
            )

            if not should_apply:
                physical_index_names[index_name] = index_name
                continue

            schema_index = metadata_indexes[(table.table_name, index_name)]
            already_present = False
            already_absent = False
            captured_name: str | None = None
            restored_name: str | None = None
            skip_equivalent_name: str | None = None
            conflict: Mapping[str, Any] | None = None

            if not dry_run:
                # Each index gets its own transaction so WAL is committed and
                # checkpointable before the next index build begins.
                with engine.begin() as connection:
                    if not enable:
                        existed_before_drop = backend.index_exists(connection, index_name, db_schema)
                        if existed_before_drop:
                            backend.drop_index_if_exists(connection, index_name, db_schema)
                        else:
                            # Index under a different naming scheme than ours
                            equivalent_name = _find_equivalent_index(existing_indexes, column_names, unique)
                            if equivalent_name is not None:
                                _record_captured_index(
                                    connection, backend,
                                    table_name=table.table_name, index_name=equivalent_name,
                                    column_names=column_names, unique=unique,
                                )
                                backend.drop_index_if_exists(connection, equivalent_name, db_schema)
                                captured_name = equivalent_name
                            else:
                                conflict = _find_shape_conflict(existing_indexes, column_names, unique)
                                if conflict is None:
                                    already_absent = True
                    else:
                        restored_name = _restore_captured_index(
                            connection, backend,
                            table_name=table.table_name, db_schema=db_schema,
                            column_names=column_names, unique=unique,
                        )
                        if restored_name is not None:
                            created_any = True
                        else:
                            equivalent_name = _find_equivalent_index(existing_indexes, column_names, unique)
                            if equivalent_name is not None:
                                skip_equivalent_name = equivalent_name
                            else:
                                savepoint = connection.begin_nested()
                                try:
                                    schema_index.create(bind=connection, checkfirst=True)
                                except DBAPIError as exc:
                                    savepoint.rollback()
                                    if "already exists" not in str(exc.orig).lower():
                                        raise
                                    already_present = True
                                else:
                                    savepoint.commit()
                                    created_any = True
            else:
                # Preview from the live-index snapshot already read above. Never touches
                # the bookkeeping table (capture/restore are not previewed), so a dry run
                # never creates the bookkeeping schema/table.
                if not enable:
                    if not exists:
                        equivalent_name = _find_equivalent_index(existing_indexes, column_names, unique)
                        if equivalent_name is not None:
                            captured_name = equivalent_name
                        else:
                            conflict = _find_shape_conflict(existing_indexes, column_names, unique)
                            if conflict is None:
                                already_absent = True
                else:
                    equivalent_name = _find_equivalent_index(existing_indexes, column_names, unique)
                    if equivalent_name is not None:
                        skip_equivalent_name = equivalent_name

            physical_name = index_name
            if already_present:
                status = "skipped"
                detail = "metadata-defined index already exists (skipped)"
            elif already_absent:
                status = "skipped"
                detail = "metadata-defined index already absent (skipped)"
            elif captured_name is not None:
                physical_name = captured_name
                status = dry_status(dry_run, "captured")
                detail = dry_label(
                    dry_run,
                    planned=f"foreign index '{captured_name}' would be captured and dropped for bulk load",
                    applied=f"foreign index '{captured_name}' captured and dropped for bulk load",
                )
            elif restored_name is not None:
                physical_name = restored_name
                status = "restored"
                detail = f"foreign index '{restored_name}' restored from bulk-load capture"
            elif skip_equivalent_name is not None:
                physical_name = skip_equivalent_name
                status = "skipped"
                detail = dry_label(
                    dry_run,
                    planned=f"equivalent foreign index '{skip_equivalent_name}' already provides this coverage (would skip creation)",
                    applied=f"equivalent foreign index '{skip_equivalent_name}' already provides this coverage (skipped)",
                )
            elif conflict is not None:
                conflict_name = str(conflict["name"])
                physical_name = conflict_name
                status = "warning"
                detail = dry_label(
                    dry_run,
                    planned=f"foreign index '{conflict_name}' {_describe_shape_conflict(conflict)}; would be left in place",
                    applied=f"foreign index '{conflict_name}' {_describe_shape_conflict(conflict)}; left in place",
                )
            else:
                status = dry_status(dry_run)
                detail = dry_label(
                    dry_run,
                    planned="metadata-defined index would be dropped" if not enable else "metadata-defined index would be created",
                    applied="metadata-defined index dropped" if not enable else "metadata-defined index created",
                )

            physical_index_names[index_name] = physical_name
            results.append(
                IndexManagementResult(
                    operation="index",
                    table_name=table.table_name,
                    category=table.category,
                    index_name=physical_name,
                    column_names=column_names,
                    unique=unique,
                    clustered=metadata_index.info.get(OMOP_CLUSTER_INDEX_INFO_KEY) is True,
                    enable=enable,
                    status=status,
                    detail=detail,
                )
            )

        # Clustering for perfomance is a separate operation from index creation
        if enable:
            cluster_index_name = _cluster_target_name(table)
            if cluster_index_name is not None:
                physical_cluster_name = physical_index_names.get(cluster_index_name, cluster_index_name)
                cluster_columns = _cluster_column_names(table, cluster_index_name)
                if not clustering_supported or not cluster:
                    results.append(
                        IndexManagementResult(
                            operation="cluster",
                            table_name=table.table_name,
                            category=table.category,
                            index_name=physical_cluster_name,
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
                else:
                    if not dry_run:
                        with engine.begin() as connection:
                            backend.cluster_table(connection, table.table_name, physical_cluster_name, db_schema)
                        clustered_now = True

                    results.append(
                        IndexManagementResult(
                            operation="cluster",
                            table_name=table.table_name,
                            category=table.category,
                            index_name=physical_cluster_name,
                            column_names=cluster_columns,
                            unique=False,
                            clustered=True,
                            enable=enable,
                            status=dry_status(dry_run),
                            detail=dry_label(dry_run, "table would be clustered using ORM-defined metadata", "table clustered using ORM-defined metadata"),
                        )
                    )

        if not dry_run and (created_any or clustered_now):
            with engine.connect() as connection:
                backend.analyze_table(connection, table.table_name, db_schema)
                connection.commit()

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
    cluster: bool = typer.Option(
        True,
        "--cluster/--no-cluster",
        help="Also CLUSTER tables using their ORM-designated cluster index. Use --no-cluster to skip the full heap rewrite on large vocabulary tables.",
    ),
    dry_run: bool = False,
) -> None:
    """Recreate all ORM-defined secondary indexes. Also CLUSTERs tables on PostgreSQL where metadata specifies it.

    Note: CLUSTER rewrites the full heap and requires ~2× the table size in free disk space.
    Pass --no-cluster to create/recreate indexes only, or run 'indexes cluster' as a
    separate step once you've confirmed sufficient disk headroom for large vocabulary tables.
    """
    with console.status("Managing metadata-defined indexes..."):
        results = manage_indexes(
            engine,
            enable=True,
            db_schema=conn.db_schema,
            vocabulary_included=vocabulary_included,
            dry_run=dry_run,
            cluster=cluster,
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
        # Resolve to whichever index actually covers these columns.
        existing_indexes = inspector.get_indexes(table.table_name, schema=conn.db_schema)
        physical_cluster_name = cluster_index_name
        if cluster_index_name not in {index["name"] for index in existing_indexes}:
            equivalent_name = _find_equivalent_index(existing_indexes, cluster_columns, False)
            if equivalent_name is not None:
                physical_cluster_name = equivalent_name

        if not dry_run:
            with engine.begin() as connection:
                backend.cluster_table(connection, table.table_name, physical_cluster_name, conn.db_schema)
            with engine.connect() as connection:
                backend.analyze_table(connection, table.table_name, conn.db_schema)
                connection.commit()

        results.append(
            IndexManagementResult(
                operation="cluster",
                table_name=table.table_name,
                category=table.category,
                index_name=physical_cluster_name,
                column_names=cluster_columns,
                unique=False,
                clustered=True,
                enable=True,
                status=dry_status(dry_run),
                detail=dry_label(dry_run, "table would be clustered and analyzed", "table clustered and analyzed"),
            )
        )

    console.print(render_index_results(results))
    console.print(render_index_summary(results, dry_run=dry_run))
