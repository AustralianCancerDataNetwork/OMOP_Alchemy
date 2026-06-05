from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any
import sqlalchemy as sa

if TYPE_CHECKING:
    from sqlalchemy.sql import ColumnElement


# ── Fulltext types ────────────────────────────────────────────────────────────

CONCEPT_NAME_TSVECTOR_COLUMN = "concept_name_tsvector"
CONCEPT_SYNONYM_NAME_TSVECTOR_COLUMN = "concept_synonym_name_tsvector"


@dataclass(frozen=True)
class FullTextTargetConfig:
    """Primitive fulltext target owned by the backend — no ORM dependencies."""
    table_name: str
    source_column_name: str
    vector_column_name: str
    index_name: str


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
    """Raised when a full-text search maintenance operation fails."""


# ── Backend errors ────────────────────────────────────────────────────────────

class BackendNotSupportedError(RuntimeError):
    """Raised when the active backend does not implement a required operation."""

    def __init__(self, feature: str, backend: "Backend") -> None:
        super().__init__(f"'{feature}' is not supported by the {backend.name} backend.")
        self.feature = feature
        self.backend_name = backend.name


def backend_supports(backend: "Backend", method_name: str) -> bool:
    """True if this backend class overrides *method_name* from the base Backend class."""
    return getattr(type(backend), method_name) is not getattr(Backend, method_name)


def require_backend_support(backend: "Backend", method_name: str, feature: str) -> None:
    """Raise BackendNotSupportedError if this backend does not override *method_name*."""
    if not backend_supports(backend, method_name):
        raise BackendNotSupportedError(feature, backend)


def backend_support_note(method_name: str) -> str:
    """Return a help-text note listing which known backends support *method_name*.

    Derived purely from the class hierarchy — no manual list to maintain.
    """
    from .resolve import _DIALECT_TO_BACKEND_MAP

    supported = sorted(
        b.name for b in _DIALECT_TO_BACKEND_MAP.values()
        if backend_supports(b, method_name)
    )
    return f"Supported backends: {', '.join(supported)}." if supported else "Not supported by any backend."


class Backend(ABC):

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def dialect(self) -> str: ...

    # ── FK trigger management ────────────────────────────────────────────────

    def toggle_fk_triggers(
        self,
        conn: sa.Connection,
        table_name: str,
        db_schema: str | None,
        *,
        enable: bool,
    ) -> None:
        raise BackendNotSupportedError("FK trigger management", self)

    def get_fk_trigger_counts(
        self,
        conn: sa.Connection,
        table_name: str,
        db_schema: str | None,
    ) -> tuple[int, int]:
        """Return (disabled_count, enabled_count) for RI triggers on the table."""
        raise BackendNotSupportedError("FK trigger status inspection", self)

    def count_fk_violations(
        self,
        conn: sa.Connection,
        source_table: str,
        referred_table: str,
        constrained_cols: list[str],
        referred_cols: list[str],
        db_schema: str | None,
    ) -> int:
        raise BackendNotSupportedError("FK constraint violation counting", self)

    # ── Clustering ───────────────────────────────────────────────────────────

    def cluster_table(
        self,
        conn: sa.Connection,
        table_name: str,
        index_name: str,
        db_schema: str | None,
    ) -> None:
        raise BackendNotSupportedError("Table clustering", self)

    def get_clustered_index_name(
        self,
        conn: sa.Connection,
        table_name: str,
        db_schema: str | None,
    ) -> str | None:
        raise BackendNotSupportedError("Cluster index inspection", self)

    # ── Table operations ─────────────────────────────────────────────────────

    @abstractmethod
    def analyze_table(
        self,
        conn: sa.Connection,
        table_name: str,
        db_schema: str | None,
        *,
        vacuum: bool = False,
    ) -> None: ...

    def truncate_table_batch(
        self,
        conn: sa.Connection,
        table_names: list[str],
        db_schema: str | None,
        *,
        restart_identities: bool,
        cascade: bool,
    ) -> None:
        raise BackendNotSupportedError("TRUNCATE with RESTART IDENTITY / CASCADE", self)

    # ── Sequence management ──────────────────────────────────────────────────

    def find_sequence_name(
        self,
        conn: sa.Connection,
        table_name: str,
        column_name: str,
        db_schema: str | None,
    ) -> str | None:
        raise BackendNotSupportedError("Owned sequence lookup", self)

    def set_sequence_value(
        self,
        conn: sa.Connection,
        sequence_name: str,
        value: int,
    ) -> None:
        raise BackendNotSupportedError("Sequence value reset", self)

    # ── Schema context ───────────────────────────────────────────────────────

    def configure_schema_context(
        self,
        conn: sa.Connection,
        db_schema: str | None,
    ) -> None:
        pass  # no-op by default; PostgreSQL overrides with SET search_path

    def ensure_schema(
        self,
        conn: sa.Connection,
        schema: str | None,
    ) -> None:
        pass  # no-op by default; backends that support named schemas override this

    # ── Full-text search ─────────────────────────────────────────────────────

    @property
    def fulltext_targets(self) -> tuple[FullTextTargetConfig, ...]:
        """Return the fulltext target configs managed by this backend. Empty by default."""
        return ()

    def register_fulltext_metadata(self) -> None:
        """Append tsvector sidecar columns to SQLAlchemy ORM metadata for this backend's targets."""
        raise BackendNotSupportedError("Full-text metadata registration", self)

    def unregister_fulltext_metadata(self) -> None:
        """Remove tsvector sidecar columns from SQLAlchemy ORM metadata."""
        raise BackendNotSupportedError("Full-text metadata unregistration", self)

    def concept_name_tsvector_expression(
        self, *, regconfig: str = "english"
    ) -> "ColumnElement[Any]":
        """Return a SQLAlchemy expression for the concept_name tsvector."""
        raise BackendNotSupportedError("Full-text search expression", self)

    def concept_synonym_name_tsvector_expression(
        self, *, regconfig: str = "english"
    ) -> "ColumnElement[Any]":
        """Return a SQLAlchemy expression for the concept_synonym_name tsvector."""
        raise BackendNotSupportedError("Full-text search expression", self)

    def install_fulltext_on_table(
        self,
        conn: sa.Connection,
        *,
        table_name: str,
        vector_column_name: str,
        index_name: str,
        db_schema: str | None,
        create_indexes: bool,
        fastupdate: bool,
    ) -> None:
        raise BackendNotSupportedError("Full-text search", self)

    def populate_fulltext_on_table(
        self,
        conn: sa.Connection,
        *,
        table_name: str,
        vector_column_name: str,
        source_column_name: str,
        db_schema: str | None,
        regconfig: str,
    ) -> int | None:
        raise BackendNotSupportedError("Full-text search", self)

    def drop_fulltext_on_table(
        self,
        conn: sa.Connection,
        *,
        table_name: str,
        vector_column_name: str,
        index_name: str,
        db_schema: str | None,
        drop_indexes: bool,
    ) -> None:
        raise BackendNotSupportedError("Full-text search", self)

    # ── Backup / restore ─────────────────────────────────────────────────────

    def prepare_backup(
        self,
        engine: sa.Engine,
        output_path: str,
        backup_format: str,
        db_schema: str | None,
    ) -> tuple[str, list[str], dict[str, str], str]:
        """Return (tool_path, command, env, database_name). subprocess.run stays in CLI."""
        raise BackendNotSupportedError("Database backup", self)

    def prepare_restore(
        self,
        engine: sa.Engine,
        input_path: str,
        backup_format: str,
        db_schema: str | None,
    ) -> tuple[str, list[str], dict[str, str], str]:
        """Return (tool_path, command, env, database_name). subprocess.run stays in CLI."""
        raise BackendNotSupportedError("Database restore", self)


