from __future__ import annotations

from typing import Any

import sqlalchemy as sa

OMOP_CLUSTER_INDEX_INFO_KEY = "omop_cluster"


def omop_index(
    name: str,
    *columns: str,
    unique: bool = False,
    cluster: bool = False,
) -> sa.Index:
    kwargs: dict[str, Any] = {"unique": unique}
    if cluster:
        kwargs["info"] = {OMOP_CLUSTER_INDEX_INFO_KEY: True}
    return sa.Index(name, *columns, **kwargs)


def omop_primary_key_index_name(table_name: str) -> str:
    return f"pk_{table_name}"


def omop_table_options(
    *,
    cluster_on: str | None = None,
) -> dict[str, Any]:
    if cluster_on is None:
        return {}
    return {
        "info": {
            OMOP_CLUSTER_INDEX_INFO_KEY: cluster_on,
        }
    }


def merge_table_args(*parts: Any) -> tuple[Any, ...]:
    items: list[Any] = []
    merged_options: dict[str, Any] = {}
    merged_info: dict[str, Any] = {}

    def consume(part: Any) -> None:
        if not part:
            return

        if isinstance(part, dict):
            options = dict(part)
            info = options.pop("info", None)
            if isinstance(info, dict):
                merged_info.update(info)
            merged_options.update(options)
            return

        if isinstance(part, tuple):
            for item in part:
                consume(item)
            return

        items.append(part)

    for part in parts:
        consume(part)

    if merged_info:
        merged_options["info"] = merged_info
    if merged_options:
        items.append(merged_options)

    return tuple(items)
