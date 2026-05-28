from __future__ import annotations

from pathlib import Path
from typing import ClassVar, Final

from pydantic import Field
from oa_configurator import PackageConfigBase, ResourceSpec, Resolver, load_stack_config

ROOT_PATH = Path(__file__).parent
TEST_PATH = Path(__file__).parent.parent / "tests"

CDM_DB_RESOURCE: Final[str] = "cdm_db"


class OmopAlchemyConfig(PackageConfigBase):
    tool_name: ClassVar[str] = "omop_alchemy"
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
