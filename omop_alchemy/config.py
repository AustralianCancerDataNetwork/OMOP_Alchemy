from __future__ import annotations

from typing import ClassVar

import sqlalchemy as sa
from pydantic import Field
from oa_configurator import PackageConfigBase, ResourceSpec, Resolver, ResolvedResource, load_stack_config


# Mapping of PostgreSQL SQLAlchemy drivernames to the Python module they require.
# Kept here (not in oa_configurator) because the driver choice and install instructions
# are OMOP_Alchemy-specific — orm-loader ≥ 0.4.0 dropped the implicit psycopg2 dependency.
_POSTGRES_DRIVER_MODULES: dict[str, str] = {
    "postgresql": "psycopg",
    "postgresql+psycopg": "psycopg",
    "postgresql+psycopg2": "psycopg2",
}


def _missing_driver_message(url: str, exc: ModuleNotFoundError) -> str | None:
    """Return an install hint if exc is a missing PostgreSQL driver, else None."""
    drivername = sa.engine.make_url(url).drivername
    expected = _POSTGRES_DRIVER_MODULES.get(drivername)
    if expected is None:
        return None
    missing = exc.name
    if missing is None and expected in str(exc):
        missing = expected
    if missing != expected:
        return None
    return (
        f"Database driver '{expected}' is required for dialect '{drivername}' "
        "but is not installed. "
        "Install PostgreSQL support with "
        "`uv sync --extra postgres` or `pip install -e '.[postgres]'`."
    )


class OmopAlchemyConfig(PackageConfigBase):
    CDM_DB: ClassVar[ResourceSpec] = ResourceSpec(
        semantic_name="cdm_db",
        display_name="OMOP CDM Database",
        description="Database containing the OMOP CDM tables and vocabulary.",
        connection_name_hint="cdm",
    )
    TEST_DB: ClassVar[ResourceSpec] = ResourceSpec(
        semantic_name="test_cdm_db",
        display_name="Test OMOP CDM Database",
        description=(
            "Dedicated PostgreSQL database for running integration tests. "
            "Tests drop and recreate the entire public schema on every run."
        ),
        connection_name_hint="pg_test",
        defaults={
            "dialect": "postgresql+psycopg",
            "host": "localhost",
            "port": "55432",
            "user": "test",
            "password": "test",
            "database_name": "test_db",
            "cdm_schema": "public",
        },
    )

    tool_name: ClassVar[str] = "omop_alchemy"
    extra_logging_namespaces: ClassVar[tuple[str, ...]] = ("orm_loader",)
    required_resources: ClassVar[tuple[str, ...]] = (CDM_DB.semantic_name,)
    owned_resources: ClassVar[tuple[ResourceSpec, ...]] = (CDM_DB,)
    test_resources: ClassVar[tuple[ResourceSpec, ...]] = (TEST_DB,)

    athena_source_path: str | None = Field(
        default=None,
        description="Path to Athena vocabulary CSV files.",
    )


def get_cdm_context() -> tuple[OmopAlchemyConfig, ResolvedResource]:
    """Return (pkg_config, resolved_cdm_resource), loading config once.

    The resource is taken from tools.omop_alchemy.default_resource when set;
    otherwise falls back to the canonical CDM_DB resource name.
    """
    stack = load_stack_config()
    pkg_config = OmopAlchemyConfig.from_stack(stack)
    tool = stack.tools.get(OmopAlchemyConfig.tool_name)
    resource_name = (tool.default_resource if tool else None) or OmopAlchemyConfig.CDM_DB.semantic_name
    resolved = Resolver(stack).resolve_resource(resource_name)
    return pkg_config, resolved


def create_cdm_engine(resolved: ResolvedResource) -> sa.Engine:
    """Create the CDM SQLAlchemy engine with helpful PostgreSQL driver error messages."""
    try:
        return resolved.create_engine()
    except ModuleNotFoundError as exc:
        msg = _missing_driver_message(resolved.database.url, exc)
        if msg is not None:
            raise RuntimeError(msg) from exc
        raise
