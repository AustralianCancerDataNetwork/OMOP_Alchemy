import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional
from datetime import date
from orm_loader.helpers import Base
from omop_alchemy.cdm.base import ReferenceTable, cdm_table, CDMTableBase

@cdm_table
class Concept_Relationship(ReferenceTable, CDMTableBase, Base):
    __tablename__ = "concept_relationship"
    concept_id_1: so.Mapped[int] = so.mapped_column(sa.ForeignKey("concept.concept_id"),primary_key=True)
    concept_id_2: so.Mapped[int] = so.mapped_column(sa.ForeignKey("concept.concept_id"),primary_key=True)
    relationship_id: so.Mapped[str] = so.mapped_column(sa.ForeignKey("relationship.relationship_id"),primary_key=True)
    valid_start_date: so.Mapped[date] = so.mapped_column(nullable=False)
    valid_end_date: so.Mapped[date] = so.mapped_column(nullable=False)
    invalid_reason: so.Mapped[Optional[str]] = so.mapped_column(sa.String(1))
