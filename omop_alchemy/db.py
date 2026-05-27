from __future__ import annotations

from collections.abc import Mapping

import sqlalchemy as sa
from sqlalchemy.engine import Engine


def _missing_driver_message(
    engine_name: str,
    exc: ModuleNotFoundError,
) -> str | None:
    drivername = sa.engine.make_url(engine_name).drivername
    expected_module = POSTGRES_DRIVER_MODULES.get(drivername)
    if expected_module is None:
        return None

    missing_module = exc.name
    if missing_module is None and expected_module in str(exc):
        missing_module = expected_module

    if missing_module != expected_module:
        return None

    return (
        f"Database driver '{expected_module}' is required for engine "
        f"'{drivername}' but is not installed. "
        "Install PostgreSQL support with "
        "`uv sync --extra postgres` "
        "or "
        "`pip install -e '.[postgres]'`."
    )


def create_engine_with_dependencies(
    engine_name: str,
    **engine_kwargs,
) -> sa.Engine:
    """Create a SQLAlchemy engine with clearer dependency errors for postgres."""
    try:
        return sa.create_engine(engine_name, **engine_kwargs)
    except ModuleNotFoundError as exc:
        message = _missing_driver_message(engine_name, exc)
        if message is not None:
            raise RuntimeError(message) from exc
        raise


# from orm-loader 0.4.0 onwards, implicit psycopg2 dependency has been removed in favor of explicit driver modules.
# This mapping is used to provide clearer error messages when a required driver is missing.
POSTGRES_DRIVER_MODULES: Mapping[str, str] = {
    "postgresql": "psycopg",           # bare URL aliased to psycopg
    "postgresql+psycopg": "psycopg",
    "postgresql+psycopg2": "psycopg2", # retained so missing-driver message is clear
}
