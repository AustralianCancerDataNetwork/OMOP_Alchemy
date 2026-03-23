from __future__ import annotations

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

from .backend_support import POSTGRESQL_DIALECT, require_backend
from .reset_sequences import reset_model_sequences
from .tables import TableCategory, schema_adjusted_metadata, select_maintenance_tables

VocabularyModel: TypeAlias = type[CSVTableProtocol]


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
) -> int:
    try:
        return int(
            model.load_csv(
                session,
                csv_path,
                merge_strategy=merge_strategy,
                quote_mode=quote_mode,
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
                merge_strategy=merge_strategy,
                quote_mode=quote_mode,
            )
        )


def _ensure_supported_backend(engine: sa.Engine) -> None:
    require_backend(
        engine,
        feature="Vocabulary source loading",
        supported_dialects=("sqlite", POSTGRESQL_DIALECT),
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
    if db_schema is None:
        return

    if connection.dialect.name != POSTGRESQL_DIALECT:
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
    merge_strategy: str = "upsert",
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
            for model in REQUIRED_VOCAB_MODELS + OPTIONAL_VOCAB_MODELS:
                current_model_name = model.__tablename__
                csv_path = _find_vocab_csv_path(
                    resolved_source_path,
                    model.__tablename__,
                )
                current_csv_path = None if csv_path is None else str(csv_path)
                required = model in REQUIRED_VOCAB_MODELS
                if csv_path is None:
                    results.append(
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

                if dry_run:
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

                row_count = _load_vocab_model_csv(
                    session,
                    model=model,
                    csv_path=csv_path,
                    merge_strategy=merge_strategy,
                    quote_mode="literal",
                )
                session.commit()
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

    if not dry_run and engine.dialect.name == POSTGRESQL_DIALECT:
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
