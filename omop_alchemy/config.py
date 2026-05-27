from __future__ import annotations

from pathlib import Path
from typing import ClassVar

from pydantic import Field
from oa_configurator import PackageConfigBase, Resolver, load_stack_config

ROOT_PATH = Path(__file__).parent
TEST_PATH = Path(__file__).parent.parent / "tests"


class OmopAlchemyConfig(PackageConfigBase):
    tool_name: ClassVar[str] = "omop_alchemy"

    athena_source_path: str | None = Field(
        default=None,
        description="Path to Athena vocabulary CSV files.",
    )


def get_resolver() -> Resolver:
    return Resolver(load_stack_config())


def get_config() -> OmopAlchemyConfig:
    return OmopAlchemyConfig.from_stack(load_stack_config())
