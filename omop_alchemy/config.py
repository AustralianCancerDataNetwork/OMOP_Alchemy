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
    # Dotenv values should take precedence over inherited shell env vars.
    if load_dotenv(dotenv, override=True) or load_dotenv(override=True):
        logger.info("Environment variables loaded from .env file")
    else:
        logger.debug("No .env file loaded")

