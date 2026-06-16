from __future__ import annotations
from enum import StrEnum

import sqlalchemy as sa

from .base import Backend, BackendNotSupportedError
from .postgres import PostgresBackend
from .sqlite import SQLiteBackend


class SupportedDialect(StrEnum):
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"

_DIALECT_TO_BACKEND_MAP: dict[SupportedDialect, Backend] = {
    SupportedDialect.POSTGRESQL: PostgresBackend(),
    SupportedDialect.SQLITE: SQLiteBackend(),
}

def resolve_backend(engine: sa.Engine) -> Backend:
    dialect = engine.dialect.name
    try:
        supported_dialect = SupportedDialect(dialect)
    except ValueError:
        raise BackendNotSupportedError(
            f"Unsupported database dialect: '{dialect}'. "
            f"Supported dialects: {', '.join(sorted(SupportedDialect))}."
        )
    return _DIALECT_TO_BACKEND_MAP[supported_dialect]



