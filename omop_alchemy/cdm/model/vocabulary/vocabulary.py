"""
OMOP CDM Vocabulary tables (v5.4).

These tables define the standardized vocabularies used throughout
the OMOP Common Data Model.

They are reference tables, not clinical fact tables.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base
from omop_alchemy.cdm.base import (
    ReferenceTable,
    cdm_table,
    CDMTableBase,
    merge_table_args,
    omop_primary_key_index_name,
    omop_table_options,
)

@cdm_table
class Vocabulary(Base, ReferenceTable, CDMTableBase):
    __tablename__ = "vocabulary"
    __table_args__ = merge_table_args(
        omop_table_options(cluster_on=omop_primary_key_index_name("vocabulary")),
    )
    vocabulary_id: so.Mapped[str] = so.mapped_column(sa.String(20), primary_key=True)
    vocabulary_name: so.Mapped[str] = so.mapped_column(sa.String(255), nullable=False)
    vocabulary_reference: so.Mapped[Optional[str]] = so.mapped_column(sa.String(255), nullable=True)
    vocabulary_version: so.Mapped[Optional[str]] = so.mapped_column(sa.String(255), nullable=True)
    vocabulary_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("concept.concept_id"),nullable=False,)

    def __repr__(self):
        return f"<Vocabulary {self.vocabulary_id}>"
