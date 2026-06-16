from __future__ import annotations

import sqlalchemy as sa

from .base import Backend, FeatureNotSupportedError


class SQLiteBackend(Backend):

    @property
    def name(self) -> str:
        return "SQLite"

    @property
    def dialect(self) -> str:
        return "sqlite"

    def analyze_table(
        self,
        conn: sa.Connection,
        table_name: str,
        db_schema: str | None,
        *,
        vacuum: bool = False,
    ) -> None:
        if vacuum:
            raise FeatureNotSupportedError("VACUUM ANALYZE", self)
        conn.exec_driver_sql(f'ANALYZE "{table_name}"')
