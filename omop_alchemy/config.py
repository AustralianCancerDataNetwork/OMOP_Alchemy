from __future__ import annotations

from typing import ClassVar, Final

from pydantic import Field
from oa_configurator import PackageConfigBase, ResourceSpec, Resolver, ResolvedResource, load_stack_config
from oa_configurator import configure_logging as _configure_logging

CDM_DB_RESOURCE: Final[str] = "cdm_db"
TOOL_NAME: Final[str] = "omop_alchemy"


class OmopAlchemyConfig(PackageConfigBase):
    tool_name: ClassVar[str] = TOOL_NAME
    required_resources: ClassVar[tuple[str, ...]] = (CDM_DB_RESOURCE,)
    owned_resources: ClassVar[tuple[ResourceSpec, ...]] = (
        ResourceSpec(
            semantic_name=CDM_DB_RESOURCE,
            display_name="OMOP CDM Database",
            description="Database containing the OMOP CDM tables and vocabulary.",
            connection_name_hint="cdm",
        ),
    )

    athena_source_path: str | None = Field(
        default=None,
        description="Path to Athena vocabulary CSV files.",
    )


def get_resolver() -> Resolver:
    return Resolver(load_stack_config())


def get_config() -> OmopAlchemyConfig:
    return OmopAlchemyConfig.from_stack(load_stack_config())


def get_cdm_context() -> tuple[OmopAlchemyConfig, ResolvedResource]:
    """Load config once and return (pkg_config, resolved_cdm_resource)."""
    stack = load_stack_config()
    pkg_config = OmopAlchemyConfig.from_stack(stack)
    tool = stack.tools.get(OmopAlchemyConfig.tool_name)
    resource_name = (tool.default_resource if tool else None) or CDM_DB_RESOURCE
    resolved = Resolver(stack).resolve_resource(resource_name)
    return pkg_config, resolved


def configure_logging(verbosity: int = 0) -> None:
    _configure_logging(verbosity=verbosity, extra_namespaces=[TOOL_NAME])