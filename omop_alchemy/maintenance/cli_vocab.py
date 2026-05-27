"""Vocabulary loading command for Athena CDM CSV files."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypeAlias, cast

from enum import StrEnum
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.exc import OperationalError
import typer
from sqlalchemy.pool import NullPool
from orm_loader.tables.typing import CSVTableProtocol
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn, TimeElapsedColumn

from ..backends.resolve import SupportedDialect
from omop_alchemy.cdm.model.vocabulary import (
    Concept,
    Concept_Ancestor,
    Concept_Class,
    Concept_Relationship,
    Concept_Synonym,
    Domain,
    Drug_Strength,
    Relationship,
    Source_To_Concept_Map,
    Vocabulary,
)

from ..backends import resolve_backend
from ._cli_utils import omop_command
from .cli_foreign_keys import manage_foreign_key_triggers
from .cli_indexes import manage_indexes
from .cli_tables import reset_model_sequences
from .tables import TableCategory, schema_adjusted_metadata, select_maintenance_tables
from .ui import (
    console,
    render_error,
    render_vocab_load_results,
    render_vocab_load_summary,
)

MergeStrategy: TypeAlias = Literal["replace", "upsert", "insert_if_empty"]

VocabularyModel: TypeAlias = type[CSVTableProtocol]
VocabularyLoadProgressCallback: TypeAlias = Callable[["VocabularyLoadProgress"], None]


@dataclass(frozen=True)
class VocabularyLoadResult:
    """Outcome of loading one Athena vocabulary CSV file via the staged ORM CSV loader."""

    table_name: str
    status: str
    row_count: int | None
    csv_path: str | None
    required: bool
    detail: str


@dataclass(frozen=True)
class VocabularyLoadReport:
    """Complete vocabulary load report: per-table outcomes and load session metadata."""

    source_path: str
    backend: str
    db_schema: str | None
    merge_strategy: MergeStrategy
    created_table_count: int
    sequence_reset_count: int
    results: tuple[VocabularyLoadResult, ...]


class VocabularyLoadPhase(StrEnum):
    START = "start"
    DISABLING_FK = "disabling_fk"
    DISABLING_INDEXES = "disabling_indexes"
    LOADING = "loading"
    DONE = "done"
    REBUILDING_INDEXES = "rebuilding_indexes"
    REBUILDING_FK = "rebuilding_fk"
    COMPLETE = "complete"

@dataclass(frozen=True)
class VocabularyLoadProgress:
    """Progress event emitted after each table load. Drives the CLI progress bar."""

    phase: VocabularyLoadPhase
    table_name: str | None
    table_index: int
    table_count: int
    percent: float
    detail: str
    rows_this_table: int | None = None
    rows_cumulative: int = 0


REQUIRED_VOCAB_MODELS: tuple[VocabularyModel, ...] = cast(
    tuple[VocabularyModel, ...],
    (
        Domain,
        Vocabulary,
        Concept_Class,
        Relationship,
        Concept,
        Concept_Ancestor,
        Concept_Relationship,
        Concept_Synonym,
    ),
)

OPTIONAL_VOCAB_MODELS: tuple[VocabularyModel, ...] = cast(
    tuple[VocabularyModel, ...],
    (
        Drug_Strength,
        Source_To_Concept_Map,
    ),
)


class VocabularyLoadError(RuntimeError):
    """Raised when a single Athena vocabulary table load fails."""


_RETRYABLE_FRAGMENTS: tuple[str, ...] = (
    "recovery mode",
    "connection reset",
    "server closed the connection",
)


def _is_retryable_error(exc: Exception) -> bool:
    """True for transient PostgreSQL connection failures that are worth retrying."""
    msg = str(exc).lower()
    return isinstance(exc, OperationalError) and any(s in msg for s in _RETRYABLE_FRAGMENTS)


def _is_missing_staging_table_error(
    exc: Exception,
    *,
    model: VocabularyModel,
) -> bool:
    """Return True if the exception is a ProgrammingError caused by the staging table not existing yet."""
    staging_table_name = model.staging_tablename()
    message = str(exc).lower()
    return (
        exc.__class__.__name__ == "ProgrammingError"
        and "does not exist" in message
        and staging_table_name.lower() in message
    )


def _load_vocab_model_csv(
    session: so.Session,
    *,
    model: VocabularyModel,
    csv_path: Path,
    merge_strategy: MergeStrategy,
    quote_mode: str = "auto",
    chunksize: int | None = None,
    index_strategy: str = "auto",
    merge_batch_size: int = 1_000_000,
) -> int:
    """Call model.load_csv. If the staging table is absent, create it and retry once."""
    load_kwargs: dict[str, object] = {
        "merge_strategy": merge_strategy,
        "quote_mode": quote_mode,
        "index_strategy": index_strategy,
        "merge_batch_size": merge_batch_size,
    }
    if chunksize is not None:
        load_kwargs["chunksize"] = chunksize

    try:
        return int(model.load_csv(session, csv_path, **load_kwargs))  # type: ignore[arg-type]
    except Exception as exc:
        if not _is_missing_staging_table_error(exc, model=model):
            raise

        session.rollback()
        model.create_staging_table(session)
        return int(model.load_csv(session, csv_path, **load_kwargs))  # type: ignore[arg-type]


def _find_vocab_csv_path(source_path: Path, table_name: str) -> Path | None:
    """Locate the CSV file for table_name under source_path, trying exact name, lower, upper, and case-insensitive glob."""
    direct_candidates = (
        source_path / f"{table_name}.csv",
        source_path / f"{table_name.lower()}.csv",
        source_path / f"{table_name.upper()}.csv",
    )
    for candidate in direct_candidates:
        if candidate.exists():
            return candidate

    for candidate in sorted(source_path.glob("*.csv")):
        if candidate.stem.lower() == table_name:
            return candidate

    return None


def _missing_required_files(source_path: Path) -> list[str]:
    """Return the table names of required vocabulary CSVs that cannot be found under source_path."""
    missing: list[str] = []
    for model in REQUIRED_VOCAB_MODELS:
        if _find_vocab_csv_path(source_path, model.__tablename__) is None:
            missing.append(model.__tablename__)
    return missing


def _create_missing_vocabulary_tables(
    connection: sa.Connection,
    *,
    db_schema: str | None,
) -> int:
    """Create any vocabulary-category ORM tables that are absent from the target database. Returns the count created."""
    vocab_tables = select_maintenance_tables(
        categories=(TableCategory.VOCABULARY,),
    )
    inspector = sa.inspect(connection)
    missing_tables = [
        table
        for table in vocab_tables
        if not inspector.has_table(table.table_name, schema=db_schema)
    ]
    if not missing_tables:
        return 0

    metadata, adjusted_tables = schema_adjusted_metadata(
        vocab_tables,
        db_schema=db_schema,
    )
    metadata.create_all(
        bind=connection,
        tables=[
            adjusted_tables[table.table_name]
            for table in missing_tables
        ],
        checkfirst=True,
    )
    return len(missing_tables)


def _configure_loader_connection(
    connection: sa.Connection,
    *,
    db_schema: str | None,
) -> None:
    """Apply schema context to a connection when db_schema is requested. Delegates to the active backend."""
    if db_schema is None:
        return
    resolve_backend(connection.engine).configure_schema_context(connection, db_schema)


def load_vocab_source(
    engine: sa.Engine,
    *,
    source_path: str | Path,
    db_schema: str | None = None,
    dry_run: bool = False,
    merge_strategy: MergeStrategy = "replace",
    chunksize: int | None = 100_000,
    bulk_mode: bool = True,
    merge_batch_size: int = 1_000_000,
    progress_callback: VocabularyLoadProgressCallback | None = None,
) -> VocabularyLoadReport:
    """Load all Athena vocabulary CSVs from source_path. With bulk_mode, indexes and FK triggers are toggled around the load."""
    resolved_source_path = Path(source_path).expanduser().resolve()
    if not resolved_source_path.exists() or not resolved_source_path.is_dir():
        raise RuntimeError(
            f"Athena source directory not found: {resolved_source_path}"
        )

    missing_required = _missing_required_files(resolved_source_path)
    if missing_required:
        raise RuntimeError(
            "Missing required Athena vocabulary CSV files: "
            + ", ".join(sorted(missing_required))
        )

    # NullPool: each session/connection is opened fresh and closed immediately after
    # use. No stale pooled connections survive between tables, which prevents
    # "connection in recovery mode" failures on subsequent tables after a heavy load.
    load_engine = sa.create_engine(engine.url, poolclass=NullPool)

    all_models = REQUIRED_VOCAB_MODELS + OPTIONAL_VOCAB_MODELS
    table_count = sum(
        1 for m in all_models
        if _find_vocab_csv_path(resolved_source_path, m.__tablename__) is not None
    )

    results: list[VocabularyLoadResult] = []
    created_table_count = 0
    sequence_reset_count = 0
    rows_cumulative = 0
    table_index = 0

    if progress_callback is not None:
        progress_callback(VocabularyLoadProgress(
            phase=VocabularyLoadPhase.START,
            table_name=None,
            table_index=0,
            table_count=table_count,
            percent=0.0,
            detail=f"Preparing Athena vocabulary load for {table_count} CSV file(s)",
        ))

    _use_bulk_mode = (
        bulk_mode
        and not dry_run
        and engine.dialect.name == SupportedDialect.POSTGRESQL
    )
    if _use_bulk_mode:
        if progress_callback is not None:
            progress_callback(VocabularyLoadProgress(
                phase=VocabularyLoadPhase.DISABLING_FK,
                table_name=None,
                table_index=0,
                table_count=table_count,
                percent=0.0,
                detail="Disabling FK trigger checks for bulk load...",
            ))
        manage_foreign_key_triggers(
            engine,
            enable=False,
            vocabulary_included=True,
            db_schema=db_schema,
            dry_run=False,
        )
        if progress_callback is not None:
            progress_callback(VocabularyLoadProgress(
                phase=VocabularyLoadPhase.DISABLING_INDEXES,
                table_name=None,
                table_index=0,
                table_count=table_count,
                percent=0.0,
                detail="Dropping indexes for bulk load...",
            ))
        manage_indexes(
            engine,
            enable=False,
            vocabulary_included=True,
            db_schema=db_schema,
            dry_run=False,
        )

    if not dry_run:
        with load_engine.connect() as pre_conn:
            _configure_loader_connection(pre_conn, db_schema=db_schema)
            created_table_count = _create_missing_vocabulary_tables(pre_conn, db_schema=db_schema)
            pre_conn.commit()

    for model in all_models:
        csv_path = _find_vocab_csv_path(resolved_source_path, model.__tablename__)
        required = model in REQUIRED_VOCAB_MODELS

        if csv_path is None:
            results.append(VocabularyLoadResult(
                table_name=model.__tablename__,
                status="skipped",
                row_count=None,
                csv_path=None,
                required=required,
                detail="optional Athena CSV not found; table skipped",
            ))
            continue

        table_index += 1

        if progress_callback is not None:
            progress_callback(VocabularyLoadProgress(
                phase=VocabularyLoadPhase.LOADING,
                table_name=model.__tablename__,
                table_index=table_index,
                table_count=table_count,
                percent=(table_index - 1) / table_count * 100,
                detail=f"Loading {model.__tablename__} ({table_index}/{table_count})...",
                rows_cumulative=rows_cumulative,
            ))

        if dry_run:
            results.append(VocabularyLoadResult(
                table_name=model.__tablename__,
                status="planned",
                row_count=None,
                csv_path=str(csv_path),
                required=required,
                detail="Athena CSV would be loaded via staged ORM CSV loader using tab-delimited input and auto-detected quote mode",
            ))
            continue

        recovery_hint = (
            " Indexes and FK triggers may still be disabled; run "
            "'omop-alchemy indexes enable --vocab' and 'omop-alchemy foreign-keys enable' to recover."
            if _use_bulk_mode else ""
        )

        row_count = 0
        _prev_attempt_was_crash = False
        for attempt in range(3):
            try:
                with so.Session(load_engine) as session:
                    _configure_loader_connection(session.connection(), db_schema=db_schema)
                    if _prev_attempt_was_crash and merge_strategy == "insert_if_empty":
                        # A DB crash left partial data committed in this table.
                        # Truncate so insert_if_empty can retry cleanly. Safe because
                        # bulk_mode's manage_foreign_key_triggers ran ALTER TABLE ...
                        # DISABLE TRIGGER ALL on all vocabulary tables, and that state
                        # persists across crash+recovery in pg_trigger.tgenabled.
                        session.execute(sa.text(f'TRUNCATE TABLE "{model.__tablename__}"'))
                        session.commit()
                    row_count = _load_vocab_model_csv(
                        session,
                        model=model,
                        csv_path=csv_path,
                        merge_strategy=merge_strategy,
                        quote_mode="auto",
                        index_strategy="keep" if _use_bulk_mode else "auto",
                        chunksize=chunksize,
                        merge_batch_size=merge_batch_size,
                    )
                    session.commit()
                break
            except Exception as exc:
                if attempt < 2 and _is_retryable_error(exc):
                    _prev_attempt_was_crash = True
                    time.sleep(10)
                    continue
                raise VocabularyLoadError(
                    "Athena vocabulary load failed for "
                    f"table `{model.__tablename__}` from `{csv_path}` "
                    f"using merge strategy `{merge_strategy}` on backend `{engine.dialect.name}`. "
                    f"Underlying error: {exc.__class__.__name__}: {exc}"
                    + recovery_hint
                ) from exc

        rows_cumulative += row_count
        results.append(VocabularyLoadResult(
            table_name=model.__tablename__,
            status="loaded",
            row_count=row_count,
            csv_path=str(csv_path),
            required=required,
            detail="Athena CSV loaded via staged ORM CSV loader using tab-delimited input and auto-detected quote mode",
        ))

        if progress_callback is not None:
            progress_callback(VocabularyLoadProgress(
                phase=VocabularyLoadPhase.DONE,
                table_name=model.__tablename__,
                table_index=table_index,
                table_count=table_count,
                percent=table_index / table_count * 100,
                detail=f"Loaded {model.__tablename__} ({table_index}/{table_count})",
                rows_this_table=row_count,
                rows_cumulative=rows_cumulative,
            ))

    if _use_bulk_mode:
        if progress_callback is not None:
            progress_callback(VocabularyLoadProgress(
                phase=VocabularyLoadPhase.REBUILDING_INDEXES,
                table_name=None,
                table_index=table_count,
                table_count=table_count,
                percent=100.0,
                detail="Rebuilding indexes on vocabulary tables (may take 15+ min)...",
                rows_cumulative=rows_cumulative,
            ))
        manage_indexes(
            engine,
            enable=True,
            vocabulary_included=True,
            db_schema=db_schema,
            dry_run=False,
        )
        if progress_callback is not None:
            progress_callback(VocabularyLoadProgress(
                phase=VocabularyLoadPhase.REBUILDING_FK,
                table_name=None,
                table_index=table_count,
                table_count=table_count,
                percent=100.0,
                detail="Re-enabling FK trigger checks...",
                rows_cumulative=rows_cumulative,
            ))
        manage_foreign_key_triggers(
            engine,
            enable=True,
            vocabulary_included=True,
            db_schema=db_schema,
            dry_run=False,
        )

    if progress_callback is not None:
        progress_callback(VocabularyLoadProgress(
            phase=VocabularyLoadPhase.COMPLETE,
            table_name=None,
            table_index=table_count,
            table_count=table_count,
            percent=100.0,
            detail="Athena vocabulary load complete",
            rows_cumulative=rows_cumulative,
        ))

    if not dry_run and engine.dialect.name == SupportedDialect.POSTGRESQL:
        sequence_results = reset_model_sequences(
            engine,
            db_schema=db_schema,
            vocabulary_included=True,
            dry_run=False,
        )
        sequence_reset_count = sum(
            result.status == "reset"
            for result in sequence_results
        )

    return VocabularyLoadReport(
        source_path=str(resolved_source_path),
        backend=engine.dialect.name,
        db_schema=db_schema,
        merge_strategy=merge_strategy,
        created_table_count=created_table_count,
        sequence_reset_count=sequence_reset_count,
        results=tuple(results),
    )


app = typer.Typer(rich_markup_mode="rich")


@app.command(
    "load-vocab-source",
    help="Load Athena vocabulary CSV files from a configured source path using the ORM staged CSV loader.",
)
@omop_command("load-vocab-source", vocabulary_included=True, dry_run=True)
def load_vocab_source_command(
    conn,
    engine,
    athena_source: str | None = typer.Option(
        None,
        help="Path to the unzipped Athena vocabulary CSV directory. Falls back to the saved athena-source default.",
    ),
    merge_strategy: MergeStrategy = typer.Option(
        "replace",
        help=(
            "CSV merge strategy. `replace` (default) keeps the DB in sync with the source. "
            "`upsert` is incremental and non-destructive. "
            "`insert_if_empty` is the fast path for a fresh empty target."
        ),
    ),
    chunksize: int | None = typer.Option(
        100_000,
        help="Chunk size for fallback ORM CSV loading. Defaults to 100 000 rows; pass 0 to disable chunking.",
    ),
    bulk_mode: bool = typer.Option(
        True,
        "--bulk-mode/--no-bulk-mode",
        help=(
            "Disable FK triggers and drop indexes globally before loading, then rebuild after. "
            "Much faster than per-table management for a full vocabulary reload. "
            "Requires FK trigger and index management support; ignored on backends that do not support it. "
            "If the load fails mid-way, run `indexes enable --vocab` and `foreign-keys enable` to recover."
        ),
    ),
    merge_batch_size: int = typer.Option(
        1_000_000,
        help=(
            "Maximum rows per INSERT/DELETE transaction during the staging-to-target merge. "
            "Lower values reduce peak WAL pressure (safer on memory-constrained systems). "
            "Raise to 5 000 000+ on machines with ample RAM for faster throughput."
        ),
    ),
    dry_run: bool = False,
) -> None:
    """Load all Athena vocabulary CSVs from the configured source path, optionally toggling indexes and FK triggers for speed."""
    effective_athena_source = athena_source or conn.athena_source
    if effective_athena_source is None:
        console.print(
            render_error(
                "No Athena vocabulary source path is configured. "
                "Set it with `omop-config configure omop_alchemy` "
                "or pass `--athena-source`."
            )
        )
        raise typer.Exit(code=1)

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:
        task_id = progress.add_task(
            "Preparing Athena vocabulary load...", total=100.0, completed=0
        )
        completed_tables: list[str] = []

        def _update_progress(event: VocabularyLoadProgress) -> None:
            progress.update(task_id, completed=event.percent, description=event.detail)
            if event.phase == VocabularyLoadPhase.DONE and event.table_name is not None:
                completed_tables.append(event.table_name)
                row_info = (
                    f": [dim]{event.rows_this_table:,} rows[/dim]"
                    if event.rows_this_table is not None else ""
                )
                progress.console.print(
                    f"[green]loaded[/green] [bold]{event.table_name}[/bold]{row_info} "
                    f"({len(completed_tables)}/{event.table_count})"
                )

        report = load_vocab_source(
            engine,
            source_path=effective_athena_source,
            db_schema=conn.db_schema,
            dry_run=dry_run,
            merge_strategy=merge_strategy,
            chunksize=None if chunksize == 0 else chunksize,
            bulk_mode=bulk_mode,
            merge_batch_size=merge_batch_size,
            progress_callback=_update_progress,
        )
        progress.update(task_id, completed=100.0, description="Athena vocabulary load complete")

    console.print(render_vocab_load_results(report.results))
    console.print(render_vocab_load_summary(report, dry_run=dry_run))
