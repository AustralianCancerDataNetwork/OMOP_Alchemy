from .base import (
    Backend,
    BackendNotSupportedError,
    CONCEPT_NAME_TSVECTOR_COLUMN,
    CONCEPT_SYNONYM_NAME_TSVECTOR_COLUMN,
    FullTextAction,
    FullTextError,
    FullTextResult,
    FullTextTargetConfig,
    backend_supports,
    require_backend_support,
    backend_support_note,
)
from .postgres import PostgresBackend
from .sqlite import SQLiteBackend
from .resolve import resolve_backend, SupportedDialect

__all__ = [
    "Backend",
    "BackendNotSupportedError",
    "CONCEPT_NAME_TSVECTOR_COLUMN",
    "CONCEPT_SYNONYM_NAME_TSVECTOR_COLUMN",
    "FullTextAction",
    "FullTextError",
    "FullTextResult",
    "FullTextTargetConfig",
    "backend_supports",
    "require_backend_support",
    "backend_support_note",
    "PostgresBackend",
    "SQLiteBackend",
    "resolve_backend",
    "SupportedDialect",
]
