from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias, cast

import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.tables.typing import CSVTableProtocol

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

from ..backend_support import Dialect, require_backend
from .reset_sequences import reset_model_sequences
from .tables import TableCategory, schema_adjusted_metadata, select_maintenance_tables
from .tables import maintenance_table_schema

VocabularyModel: TypeAlias = type[CSVTableProtocol]
VocabularyLoadProgressCallback: TypeAlias = Callable[["VocabularyLoadProgress"], None]

LOAD_PROGRESS_FRACTION = 0.30
COMMIT_PROGRESS_FRACTION = 0.70


@dataclass(frozen=True)
class VocabularyLoadResult:
    table_name: str
    status: str
    row_count: int | None
    csv_path: str | None
    required: bool
    detail: str


@dataclass(frozen=True)
class VocabularyLoadReport:
    source_path: str
    backend: str
    db_schema: str | None
    merge_strategy: str
    created_table_count: int
    sequence_reset_count: int
    results: tuple[VocabularyLoadResult, ...]


@dataclass(frozen=True)
class VocabularyLoadProgress:
    phase: str
    table_name: str | None
    table_index: int
    table_count: int
    completed_units: float
    total_units: float
    percent: float
    detail: str


@dataclass(frozen=True)
class _VocabularyLoadItem:
    model: VocabularyModel
    csv_path: Path
    required: bool
    size_bytes: int


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


def _emit_progress(
    progress_callback: VocabularyLoadProgressCallback | None,
    *,
    phase: str,
    table_name: str | None,
    table_index: int,
    table_count: int,
    completed_units: float,
    total_units: float,
    detail: str,
) -> None:
    if progress_callback is None:
        return

    bounded_total = total_units if total_units > 0 else 1.0
    bounded_completed = min(max(completed_units, 0.0), bounded_total)
    progress_callback(
        VocabularyLoadProgress(
            phase=phase,
            table_name=table_name,
            table_index=table_index,
            table_count=table_count,
            completed_units=bounded_completed,
            total_units=bounded_total,
            percent=(bounded_completed / bounded_total) * 100.0,
            detail=detail,
        )
    )


def _is_missing_staging_table_error(
    exc: Exception,
    *,
    model: VocabularyModel,
) -> bool:
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
    merge_strategy: str,
    quote_mode: str = "csv",
    chunksize: int | None = None,
) -> int:
    load_kwargs: dict[str, object] = {
        "merge_strategy": merge_strategy,
        "quote_mode": quote_mode,
    }
    if chunksize is not None:
        load_kwargs["chunksize"] = chunksize

    try:
        return int(
            model.load_csv(
                session,
                csv_path,
                **load_kwargs, # type: ignore[arg-type]
            )
        )
    except Exception as exc:
        if not _is_missing_staging_table_error(exc, model=model):
            raise

        session.rollback()
        model.create_staging_table(session)
        return int(
            model.load_csv(
                session,
                csv_path,
                **load_kwargs, # type: ignore[arg-type]
            )
        )


def _ensure_supported_backend(engine: sa.Engine) -> None:
    require_backend(
        engine,
        feature="Vocabulary source loading",
        supported_dialects=(Dialect.SQLITE, Dialect.POSTGRESQL),
    )


def _find_vocab_csv_path(source_path: Path, table_name: str) -> Path | None:
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
    vocab_tables = select_maintenance_tables(
        categories=(TableCategory.VOCABULARY,),
    )
    inspector = sa.inspect(connection)
    missing_tables = [
        table
        for table in vocab_tables
        if not inspector.has_table(
            table.table_name,
            schema=maintenance_table_schema(table, db_schema),
        )
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
    if db_schema is None:
        return

    if connection.dialect.name != Dialect.POSTGRESQL:
        raise RuntimeError(
            "Vocabulary source loading with `--db-schema` is only supported on PostgreSQL. "
            "SQLite uses the default database namespace."
        )

    connection.exec_driver_sql(f"SET search_path TO {db_schema}")

def load_vocab_source(
    engine: sa.Engine,
    *,
    source_path: str | Path,
    db_schema: str | None = None,
    dry_run: bool = False,
    merge_strategy: str = "replace",
    chunksize: int | None = None,
    progress_callback: VocabularyLoadProgressCallback | None = None,
) -> VocabularyLoadReport:
    _ensure_supported_backend(engine)

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

    results: list[VocabularyLoadResult] = []
    created_table_count = 0
    sequence_reset_count = 0

    load_items: list[_VocabularyLoadItem] = []
    missing_optional_results: list[VocabularyLoadResult] = []
    for model in REQUIRED_VOCAB_MODELS + OPTIONAL_VOCAB_MODELS:
        csv_path = _find_vocab_csv_path(
            resolved_source_path,
            model.__tablename__,
        )
        required = model in REQUIRED_VOCAB_MODELS
        if csv_path is None:
            missing_optional_results.append(
                VocabularyLoadResult(
                    table_name=model.__tablename__,
                    status="skipped",
                    row_count=None,
                    csv_path=None,
                    required=required,
                    detail="optional Athena CSV not found; table skipped",
                )
            )
            continue

        file_size = csv_path.stat().st_size
        load_items.append(
            _VocabularyLoadItem(
                model=model,
                csv_path=csv_path,
                required=required,
                size_bytes=file_size if file_size > 0 else 1,
            )
        )

    load_items.sort(key=lambda item: (item.size_bytes, item.model.__tablename__))

    total_units = float(sum(item.size_bytes for item in load_items) or 1)
    completed_units = 0.0
    table_count = len(load_items)

    _emit_progress(
        progress_callback,
        phase="start",
        table_name=None,
        table_index=0,
        table_count=table_count,
        completed_units=completed_units,
        total_units=total_units,
        detail=f"Preparing Athena vocabulary load for {table_count} CSV file(s)",
    )

    with engine.connect() as connection:
        _configure_loader_connection(
            connection,
            db_schema=db_schema,
        )

        if not dry_run:
            created_table_count = _create_missing_vocabulary_tables(
                connection,
                db_schema=db_schema,
            )

        Session = so.sessionmaker(bind=connection, future=True)
        session = Session()
        current_model_name: str | None = None
        current_csv_path: str | None = None
        try:
            for table_index, item in enumerate(load_items, start=1):
                model = item.model
                csv_path = item.csv_path
                current_model_name = model.__tablename__
                current_csv_path = str(csv_path)
                if dry_run:
                    _emit_progress(
                        progress_callback,
                        phase="plan",
                        table_name=model.__tablename__,
                        table_index=table_index,
                        table_count=table_count,
                        completed_units=completed_units,
                        total_units=total_units,
                        detail=(
                            f"Planning {model.__tablename__} ({table_index}/{table_count})"
                        ),
                    )
                    completed_units += item.size_bytes
                    _emit_progress(
                        progress_callback,
                        phase="planned",
                        table_name=model.__tablename__,
                        table_index=table_index,
                        table_count=table_count,
                        completed_units=completed_units,
                        total_units=total_units,
                        detail=(
                            f"Planned {model.__tablename__} ({table_index}/{table_count})"
                        ),
                    )
                    results.append(
                        VocabularyLoadResult(
                            table_name=model.__tablename__,
                            status="planned",
                            row_count=None,
                            csv_path=str(csv_path),
                            required=required,
                            detail="Athena CSV would be loaded via staged ORM CSV loader using tab-delimited input and literal quote mode",
                        )
                    )
                    continue

                loader_kwargs: dict[str, object] = {
                    "model": model,
                    "csv_path": csv_path,
                    "merge_strategy": merge_strategy,
                    "quote_mode": "literal",
                }
                if chunksize is not None:
                    loader_kwargs["chunksize"] = chunksize

                _emit_progress(
                    progress_callback,
                    phase="load",
                    table_name=model.__tablename__,
                    table_index=table_index,
                    table_count=table_count,
                    completed_units=completed_units,
                    total_units=total_units,
                    detail=(
                        f"Loading {model.__tablename__} ({table_index}/{table_count})"
                    ),
                )

                row_count = _load_vocab_model_csv(
                    session,
                    **loader_kwargs, # type: ignore[arg-type]
                )

                completed_units += item.size_bytes * LOAD_PROGRESS_FRACTION
                _emit_progress(
                    progress_callback,
                    phase="load-complete",
                    table_name=model.__tablename__,
                    table_index=table_index,
                    table_count=table_count,
                    completed_units=completed_units,
                    total_units=total_units,
                    detail=(
                        f"Loaded {model.__tablename__}; committing ({table_index}/{table_count})"
                    ),
                )

                session.commit()

                completed_units += item.size_bytes * COMMIT_PROGRESS_FRACTION
                _emit_progress(
                    progress_callback,
                    phase="commit-complete",
                    table_name=model.__tablename__,
                    table_index=table_index,
                    table_count=table_count,
                    completed_units=completed_units,
                    total_units=total_units,
                    detail=(
                        f"Committed {model.__tablename__} ({table_index}/{table_count})"
                    ),
                )

                results.append(
                    VocabularyLoadResult(
                        table_name=model.__tablename__,
                        status="loaded",
                        row_count=row_count,
                        csv_path=str(csv_path),
                        required=required,
                        detail="Athena CSV loaded via staged ORM CSV loader using tab-delimited input and literal quote mode",
                    )
                )
            if not dry_run:
                connection.commit()
            _emit_progress(
                progress_callback,
                phase="complete",
                table_name=None,
                table_index=table_count,
                table_count=table_count,
                completed_units=total_units,
                total_units=total_units,
                detail="Athena vocabulary load complete",
            )
        except Exception as exc:
            session.rollback()
            if not dry_run:
                connection.rollback()
            raise VocabularyLoadError(
                "Athena vocabulary load failed for "
                f"table `{current_model_name or 'unknown'}` from `{current_csv_path or '-'}` "
                f"using merge strategy `{merge_strategy}` on backend `{engine.dialect.name}`. "
                f"Underlying error: {exc.__class__.__name__}: {exc}"
            ) from exc
        finally:
            session.close()

    results.extend(missing_optional_results)

    if not dry_run and engine.dialect.name == Dialect.POSTGRESQL:
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
