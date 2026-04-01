from __future__ import annotations

from collections.abc import Iterable
from enum import StrEnum

import sqlalchemy as sa

class Dialect(StrEnum):
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"

POSTGRESQL_ONLY_HELP = "PostgreSQL only"

_DIALECT_LABELS: dict[Dialect, str] = {
    Dialect.POSTGRESQL: "PostgreSQL",
    Dialect.SQLITE: "SQLite",
}

def backend_label(dialect_name: str) -> str:
    try:
        return _DIALECT_LABELS[Dialect(dialect_name)]
    except ValueError:
        return dialect_name

def supports_backend(
    engine: sa.Engine,
    *,
    supported_dialects: Iterable[Dialect],
) -> bool:
    return engine.dialect.name in tuple(supported_dialects)


def require_backend(
    engine: sa.Engine,
    *,
    feature: str,
    supported_dialects: Iterable[Dialect],
) -> None:
    supported = tuple(supported_dialects)
    if engine.dialect.name in supported:
        return

    supported_label = ", ".join(
        backend_label(dialect)
        for dialect in sorted(supported)
    )
    raise RuntimeError(
        f"{feature} is only supported for {supported_label} engines. "
        f"Current dialect: '{engine.dialect.name}'."
    )
