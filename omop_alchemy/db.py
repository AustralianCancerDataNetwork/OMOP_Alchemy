from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Optional
from collections.abc import Mapping
import json
import os
from pathlib import Path
import tomllib

from sqlalchemy.engine import Engine
import sqlalchemy as sa

from .config import load_environment
import logging
logger = logging.getLogger(__name__)

DEFAULTS_FILENAME = ".omop-maint.toml"
DEFAULTS_ENV_VAR = "OMOP_MAINT_DEFAULTS_FILE"
DEFAULTS_SECTION = "defaults"
LEGACY_DEFAULTS_SECTION = "connection"
PROJECT_MARKER = "pyproject.toml"


def defaults_path() -> Path:
    configured_path = os.getenv(DEFAULTS_ENV_VAR)
    if configured_path:
        return Path(configured_path).expanduser().resolve()

    current = Path.cwd().resolve()
    for directory in (current, *current.parents):
        if (directory / PROJECT_MARKER).exists():
            return (directory / DEFAULTS_FILENAME).resolve()

    return (current / DEFAULTS_FILENAME).resolve()

def _clean(value: object) -> str | None:
    if value is None:
        return None
    value_str = str(value).strip()
    return value_str or None

def _relative_path_for_storage(config_path: Path, value: str | None) -> str | None:
    cleaned = _clean(value)
    if cleaned is None:
        return None

    path_value = Path(cleaned).expanduser()
    if not path_value.is_absolute():
        path_value = (Path.cwd() / path_value).resolve()

    return path_value.relative_to(config_path.parent).as_posix()

def _resolve_relative_path(config_path: Path, value: object) -> str | None:
    cleaned = _clean(value)
    if cleaned is None:
        return None

    path_value = Path(cleaned).expanduser()
    if path_value.is_absolute():
        return str(path_value)

    return str((config_path.parent / path_value).resolve())

@dataclass(frozen=True)
class ConnectionDefaults:
    """

    Returns:
        _type_: _description_
    """
    dotenv: Optional[str] = None
    engine_schema: Optional[str] = None
    db_schema: Optional[str] = None
    athena_source: Optional[str] = None
    logging: Optional[str] = None

    def to_dict(self) -> dict[str, Optional[str]]:
        return asdict(self)

    def save(self) -> Path:
        path = defaults_path()
        path.parent.mkdir(parents=True, exist_ok=True)

        lines = [f"[{DEFAULTS_SECTION}]"]
        dotenv = _relative_path_for_storage(path, self.dotenv)
        if dotenv is not None:
            lines.append(f"dotenv = {json.dumps(dotenv)}")
        if self.engine_schema is not None:
            lines.append(f"engine_schema = {json.dumps(self.engine_schema)}")
        if self.db_schema is not None:
            lines.append(f"db_schema = {json.dumps(self.db_schema)}")
        athena_source = _relative_path_for_storage(path, self.athena_source)
        if athena_source is not None:
            lines.append(f"athena_source = {json.dumps(athena_source)}")
        if self.logging is not None:
            lines.append(f"logging = {json.dumps(self.logging)}")
        lines.append("")
        path.write_text("\n".join(lines), encoding="utf-8")
        return path
    
    @classmethod
    def load(cls) -> ConnectionDefaults:
        path = defaults_path()
        if not path.exists():
            return ConnectionDefaults()

        data = tomllib.loads(path.read_text(encoding="utf-8"))
        defaults = data.get(DEFAULTS_SECTION, {})
        connection = data.get(LEGACY_DEFAULTS_SECTION, {})

        if not isinstance(defaults, dict):
            defaults = {}
        if not isinstance(connection, dict):
            connection = {}

        return ConnectionDefaults(
            dotenv=_resolve_relative_path(
                path,
                defaults.get("dotenv", connection.get("dotenv")),
            ),
            engine_schema=_clean(defaults.get("engine_schema", connection.get("engine_schema"))),
            db_schema=_clean(defaults.get("db_schema", connection.get("db_schema"))),
            athena_source=_resolve_relative_path(
                path,
                defaults.get("athena_source", connection.get("athena_source")),
            ),
            logging=_clean(defaults.get("logging", connection.get("logging"))),
        )
    
    @classmethod
    def update_and_save_defaults(
        cls,
        *,
        dotenv: Optional[str] = None,
        engine_schema: Optional[str] = None,
        db_schema: Optional[str] = None,
        athena_source: Optional[str] = None,
        logging: Optional[str] = None,
    ) -> tuple[ConnectionDefaults, Path]:
        """Loads current defaults, allows update of any subset of values, and returns updated defaults after it has been saved."""
        current = cls.load()
        updated = ConnectionDefaults(
            dotenv=dotenv if dotenv is not None else current.dotenv,
            engine_schema=engine_schema if engine_schema is not None else current.engine_schema,
            db_schema=db_schema if db_schema is not None else current.db_schema,
            athena_source=athena_source if athena_source is not None else current.athena_source,
            logging=logging if logging is not None else current.logging,
        )
        path = updated.save()
        return updated, path


def resolve_connection(
    *,
    dotenv: str | None,
    engine_schema: str | None,
    db_schema: str | None,
    athena_source: str | None = None,
) -> ConnectionDefaults:
    saved = ConnectionDefaults.load()
    return ConnectionDefaults(
        dotenv=dotenv if dotenv is not None else saved.dotenv,
        engine_schema=engine_schema if engine_schema is not None else saved.engine_schema,
        db_schema=db_schema if db_schema is not None else saved.db_schema,
        athena_source=athena_source if athena_source is not None else saved.athena_source,
    )


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

    raise RuntimeError(f"No database engine configured for {'schema ' + schema if schema else 'default'}. ")


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

def build_engine(*, dotenv: str | None, engine_schema: str | None) -> Engine:
    load_environment(dotenv or "")
    return create_engine_with_dependencies(get_engine_name(engine_schema), future=True)


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

# from orm-loader 0.4.0 onwards, implicit psycopg2 dependency has been removed in favor of explicit driver modules. 
# This mapping is used to provide clearer error messages when a required driver is missing.
POSTGRES_DRIVER_MODULES: Mapping[str, str] = {
    "postgresql": "psycopg",           # bare URL aliased to psycopg
    "postgresql+psycopg": "psycopg",
    "postgresql+psycopg2": "psycopg2", # retained so missing-driver message is clear
}


