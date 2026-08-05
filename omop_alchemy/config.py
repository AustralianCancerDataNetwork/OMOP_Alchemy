from __future__ import annotations

from typing import Annotated, ClassVar

import sqlalchemy as sa
from pydantic import Field
from oa_configurator import (
    DatabaseConfig,
    PackageConfigBase,
    RefTo,
    Resolver,
    ResolvedDatabase,
    load_stack_config,
)


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
    """oa-configurator config class for omop-alchemy, the CDM database owner.

    Every downstream package's own ``cdm_db``-named field shares this
    database purely by naming convention (see ``RefTo``), not by importing
    this class.

    Attributes
    ----------
    cdm_db : str
        Name of the ``[databases.*]`` entry holding the CDM database.
    test_cdm_db : str, optional
        Name of the ``[databases.*]`` entry holding the test CDM database,
        marked ``RefTo(DatabaseConfig, is_test=True)``.

    Notes
    -----
    By design, this config is for internal use only and must not be
    imported or resolved by any other package.
    """

    tool_name: ClassVar[str] = "omop_alchemy"
    extra_logging_namespaces: ClassVar[tuple[str, ...]] = ("orm_loader",)

    cdm_db: Annotated[str, RefTo(DatabaseConfig)] = "cdm_db"
    test_cdm_db: Annotated[str | None, RefTo(DatabaseConfig, is_test=True)] = None

    athena_source_path: str | None = Field(
        default=None,
        description="Path to Athena vocabulary CSV files.",
    )


def get_cdm_context() -> tuple[OmopAlchemyConfig, ResolvedDatabase]:
    """Return (pkg_config, resolved_cdm_database), loading config once.

    The CDM database is always whatever ``OmopAlchemyConfig.cdm_db`` resolves
    to -- point a deployment at a second CDM instance via that field's own
    ``--cdm-db`` flag at configure time, not a call-site override.

    Raises
    ------
    RuntimeError
        If no oa-configurator stack config file exists yet.
    """
    try:
        stack = load_stack_config()
    except FileNotFoundError as exc:
        raise RuntimeError(
            "No omop-alchemy configuration found. "
            "Run `omop-config configure omop_alchemy` to set it up."
        ) from exc
    resolver = Resolver(stack)
    pkg_config = resolver.resolve_package_config(OmopAlchemyConfig)
    resolved = resolver.resolve_database(pkg_config.cdm_db)
    return pkg_config, resolved


def create_cdm_engine(resolved: ResolvedDatabase) -> sa.Engine:
    """Create the CDM SQLAlchemy engine with helpful PostgreSQL driver error messages."""
    try:
        return resolved.create_engine()
    except ModuleNotFoundError as exc:
        msg = _missing_driver_message(resolved.connection.url, exc)
        if msg is not None:
            raise RuntimeError(msg) from exc
        raise
