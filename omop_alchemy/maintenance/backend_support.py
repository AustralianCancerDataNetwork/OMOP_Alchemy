from __future__ import annotations

from collections.abc import Iterable

import sqlalchemy as sa

POSTGRESQL_DIALECT = "postgresql"
POSTGRESQL_ONLY_HELP = "PostgreSQL only"
BACKEND_LABELS = {
    POSTGRESQL_DIALECT: "PostgreSQL",
    "sqlite": "SQLite",
    "mysql": "MySQL",
    "mariadb": "MariaDB",
    "mssql": "SQL Server",
    "oracle": "Oracle",
}


def backend_label(dialect_name: str) -> str:
    return BACKEND_LABELS.get(dialect_name, dialect_name)


def supports_backend(
    engine: sa.Engine,
    *,
    supported_dialects: Iterable[str],
) -> bool:
    return engine.dialect.name in tuple(supported_dialects)


def require_backend(
    engine: sa.Engine,
    *,
    feature: str,
    supported_dialects: Iterable[str],
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
