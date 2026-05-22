from __future__ import annotations

from omop_alchemy.cdm.handlers.meds._guards import *  # noqa: F401,F403


import pyarrow as pa
import meds
import sqlalchemy as sa
import sqlalchemy.orm as so

from omop_alchemy.cdm.model.vocabulary import Concept_Relationship
from omop_alchemy.cdm.handlers.vocabs_and_mappers import (
    build_concept_id_map,
    CUSTOM_CONCEPT_ID_START,
)

__all__ = [
    "build_concept_id_map",
    "CUSTOM_CONCEPT_ID_START",
    "build_code_metadata",
]


def build_code_metadata(
    session: so.Session,
    emitted_codes: set[str] | None = None,
    *,
    mode: str = "emitted",
) -> pa.Table:
    """Return a CodeMetadataSchema-aligned table for codes.parquet.

    Args:
        session: SQLAlchemy session over an OMOP CDM database.
        emitted_codes: Set of code strings ("vocabulary_id/concept_code") that
            appear in the generated MEDS data.  Required when mode="emitted";
            codes absent from the concept table (e.g. MEDS_BIRTH) are included
            with null description and empty parent_codes.
        mode: "emitted" (default) — rows only for codes in emitted_codes;
              "all" — rows for every concept in the vocabulary tables.

    Returns:
        PyArrow table validated against meds.CodeMetadataSchema, sorted by code.

    Example — write codes only for events that were actually exported::

        import pyarrow.parquet as pq
        from omop_alchemy.cdm.handlers.meds.code_metadata import build_code_metadata

        emitted = {"SNOMED/73211009", "LOINC/2160-0", "MEDS_BIRTH"}
        codes_table = build_code_metadata(session, emitted, mode="emitted")
        pq.write_table(codes_table, "/path/to/metadata/codes.parquet")

        # Each row:  code | description | parent_codes
        # "LOINC/2160-0" | "Creatinine [Mass/volume] in Serum" | ["LOINC/..."]

    Example — full vocabulary snapshot (mode="all") for a shareable dataset::

        codes_table = build_code_metadata(session, mode="all")
    """
    code_map, name_map = build_concept_id_map(session)
    code_to_id: dict[str, int] = {v: k for k, v in code_map.items()}

    # Build parent_index: concept_id → list[parent code strings]
    # Uses Concept_Relationship relationship_id="Is a" (child → parent direction)
    parent_index: dict[int, list[str]] = {}
    for cid1, cid2 in session.execute(
        sa.select(
            Concept_Relationship.concept_id_1,
            Concept_Relationship.concept_id_2,
        ).where(Concept_Relationship.relationship_id == "Is a")
    ):
        parent_code = code_map.get(int(cid2))
        if parent_code:
            parent_index.setdefault(int(cid1), []).append(parent_code)

    # Determine target code set
    if mode == "emitted":
        target_codes: set[str] = set(emitted_codes) if emitted_codes else set()
    else:
        target_codes = set(code_map.values())

    if not target_codes:
        return meds.CodeMetadataSchema.align(pa.table({
            "code": pa.array([], type=pa.string()),
            "description": pa.array([], type=pa.string()),
            "parent_codes": pa.array([], type=pa.list_(pa.string())),
        }))

    rows = []
    for code in sorted(target_codes):
        cid = code_to_id.get(code)
        rows.append({
            "code": code,
            "description": name_map.get(cid) if cid is not None else None,
            "parent_codes": parent_index.get(cid, []) if cid is not None else [],
        })

    table = pa.Table.from_pylist(rows)
    return meds.CodeMetadataSchema.align(table)
