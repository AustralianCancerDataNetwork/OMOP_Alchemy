import os
from dotenv import load_dotenv
from pathlib import Path
from orm_loader.helpers import get_logger

ROOT_PATH = Path(__file__).parent
TEST_PATH = Path(__file__).parent.parent / "tests"

logger = get_logger(__name__)

def load_environment(dotenv: str = '') -> None:
    """
    Explicitly load environment variables for the application.
    Safe: does not log sensitive values.
    """
    if load_dotenv(dotenv) or load_dotenv():
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