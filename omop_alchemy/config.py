import os
from collections.abc import Mapping
from dotenv import load_dotenv
from pathlib import Path
import sqlalchemy as sa
from orm_loader.helpers import get_logger

ROOT_PATH = Path(__file__).parent
TEST_PATH = Path(__file__).parent.parent / "tests"

logger = get_logger(__name__)

POSTGRES_DRIVER_MODULES: Mapping[str, str] = {
    "postgresql": "psycopg2",
    "postgresql+psycopg2": "psycopg2",
    "postgresql+psycopg": "psycopg",
}

def load_environment(dotenv: str = '') -> None:
    """
    Explicitly load environment variables for the application.
    Safe: does not log sensitive values.
    """
    # Dotenv values should take precedence over inherited shell env vars.
    if load_dotenv(dotenv, override=True) or load_dotenv(override=True):
        logger.info("Environment variables loaded from .env file")
    else:
        logger.debug("No .env file loaded")


def get_engine_name(schema: str | None = None) -> str:
    """
    Resolve database engine URI.

    Resolution order:
    1. ENGINE_<SCHEMA> (if schema provided)
    2. ENGINE (fallback / legacy)

    Raises if nothing is configured.
    """
    if schema:
        key = f"ENGINE_{schema.upper()}"
        engine = os.getenv(key)
        if engine:
            logger.info("Database engine configured for schema '%s'", schema)
            return engine
        else:
            logger.debug(
                "No schema-specific engine found for '%s' (%s)",
                schema,
                key,
            )

    engine = os.getenv("ENGINE")
    if engine:
        logger.info("Default database engine configured")
        return engine

    raise RuntimeError(
        f"No database engine configured"
        + (f" for schema '{schema}'" if schema else "")
    )


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
    """
    Create a SQLAlchemy engine with clearer dependency errors for postgres.
    """
    try:
        return sa.create_engine(engine_name, **engine_kwargs)
    except ModuleNotFoundError as exc:
        message = _missing_driver_message(engine_name, exc)
        if message is not None:
            raise RuntimeError(message) from exc
        raise
