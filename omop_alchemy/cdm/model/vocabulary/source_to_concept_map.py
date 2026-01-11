import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional
from datetime import date

from orm_loader.helpers import Base
from orm_loader.registry import ValidationIssue
from omop_alchemy.cdm.base import ReferenceTable, cdm_table, CDMTableBase, DatedEvent

@cdm_table
class Source_To_Concept_Map(
    DatedEvent,
    CDMTableBase,
    ReferenceTable,
    Base,
):
    """
    Maps source codes to standard OMOP concepts.

    This table captures ETL provenance and is critical
    for reproducibility and transparency.
    """
    __tablename__ = "source_to_concept_map"
    __cdm_extra_checks__ = ["source_concept_id_range"]

    source_code: so.Mapped[str] = so.mapped_column(sa.String(50),primary_key=True)
    source_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer,primary_key=True,doc="0 or >= 2,000,000,000 for site-specific concepts")
    source_vocabulary_id: so.Mapped[str] = so.mapped_column(sa.String(20),primary_key=True)
    source_code_description: so.Mapped[Optional[str]] = so.mapped_column(sa.String(255))
    target_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("concept.concept_id"),nullable=False,index=True)
    target_vocabulary_id: so.Mapped[str] = so.mapped_column(sa.ForeignKey("vocabulary.vocabulary_id"),nullable=False)
    valid_start_date: so.Mapped[date] = so.mapped_column(nullable=False)
    valid_end_date: so.Mapped[date] = so.mapped_column(nullable=False)
    invalid_reason: so.Mapped[Optional[str]] = so.mapped_column(sa.String(1))

    @classmethod
    def extra_validate(cls) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        # Only structural; value-level checks require DB scan -> handled by registry semantic pass
        return issues