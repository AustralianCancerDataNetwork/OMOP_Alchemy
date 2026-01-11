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

def get_engine_name() -> str:
    engine = os.getenv("ENGINE", "")
    if engine:
        logger.info("Database engine configured")
    else:
        logger.warning("Database engine not configured")
    return engine