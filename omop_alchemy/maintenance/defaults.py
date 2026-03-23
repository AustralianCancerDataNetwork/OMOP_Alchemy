from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import tomllib


DEFAULTS_FILENAME = ".omop-maint.toml"
DEFAULTS_ENV_VAR = "OMOP_MAINT_DEFAULTS_FILE"
DEFAULTS_SECTION = "defaults"
LEGACY_DEFAULTS_SECTION = "connection"
PROJECT_MARKER = "pyproject.toml"


@dataclass(frozen=True)
class ConnectionDefaults:
    dotenv: str | None = None
    engine_schema: str | None = None
    db_schema: str | None = None
    athena_source: str | None = None

    def with_updates(
        self,
        *,
        dotenv: str | None = None,
        engine_schema: str | None = None,
        db_schema: str | None = None,
        athena_source: str | None = None,
    ) -> "ConnectionDefaults":
        return ConnectionDefaults(
            dotenv=self.dotenv if dotenv is None else dotenv,
            engine_schema=self.engine_schema if engine_schema is None else engine_schema,
            db_schema=self.db_schema if db_schema is None else db_schema,
            athena_source=(
                self.athena_source if athena_source is None else athena_source
            ),
        )


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


def _resolve_relative_path(config_path: Path, value: object) -> str | None:
    cleaned = _clean(value)
    if cleaned is None:
        return None

    path_value = Path(cleaned).expanduser()
    if path_value.is_absolute():
        return str(path_value)

    return str((config_path.parent / path_value).resolve())


def _relative_path_for_storage(config_path: Path, value: str | None) -> str | None:
    if value is None:
        return None

    path_value = Path(value).expanduser()
    if not path_value.is_absolute():
        path_value = (Path.cwd() / path_value).resolve()

    return os.path.relpath(path_value, start=config_path.parent)


def load_connection_defaults() -> ConnectionDefaults:
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
            defaults.get("dotenv", defaults.get("env_path", connection.get("dotenv"))),
        ),
        engine_schema=_clean(defaults.get("engine_schema", connection.get("engine_schema"))),
        db_schema=_clean(defaults.get("db_schema", connection.get("db_schema"))),
        athena_source=_resolve_relative_path(
            path,
            defaults.get("athena_source", connection.get("athena_source")),
        ),
    )


def save_connection_defaults(defaults: ConnectionDefaults) -> Path:
    path = defaults_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    lines = [f"[{DEFAULTS_SECTION}]"]
    dotenv = _relative_path_for_storage(path, defaults.dotenv)
    if dotenv is not None:
        lines.append(f"dotenv = {json.dumps(dotenv)}")
    if defaults.engine_schema is not None:
        lines.append(f"engine_schema = {json.dumps(defaults.engine_schema)}")
    if defaults.db_schema is not None:
        lines.append(f"db_schema = {json.dumps(defaults.db_schema)}")
    athena_source = _relative_path_for_storage(path, defaults.athena_source)
    if athena_source is not None:
        lines.append(f"athena_source = {json.dumps(athena_source)}")
    lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def clear_connection_defaults(
    *,
    clear_dotenv: bool = False,
    clear_engine_schema: bool = False,
    clear_db_schema: bool = False,
    clear_athena_source: bool = False,
) -> Path | None:
    path = defaults_path()
    if not path.exists():
        return None

    if not any((clear_dotenv, clear_engine_schema, clear_db_schema, clear_athena_source)):
        path.unlink()
        return path

    current = load_connection_defaults()
    updated = ConnectionDefaults(
        dotenv=None if clear_dotenv else current.dotenv,
        engine_schema=None if clear_engine_schema else current.engine_schema,
        db_schema=None if clear_db_schema else current.db_schema,
        athena_source=None if clear_athena_source else current.athena_source,
    )

    if all(
        value is None
        for value in (
            updated.dotenv,
            updated.engine_schema,
            updated.db_schema,
            updated.athena_source,
        )
    ):
        path.unlink()
        return path

    save_connection_defaults(updated)
    return path
