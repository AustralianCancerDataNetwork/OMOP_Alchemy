import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional
from datetime import date
from orm_loader.helpers import Base
from omop_alchemy.cdm.base import ReferenceTable, cdm_table, CDMTableBase, DatedEvent

@cdm_table
class Drug_Strength(
    DatedEvent,
    CDMTableBase,
    ReferenceTable,
    Base,
):
    """
    Defines the strength and composition of drug products.

    This table links drug products to their ingredients
    and quantitative properties.
    """
    __tablename__ = "drug_strength"

    drug_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("concept.concept_id"),primary_key=True)
    ingredient_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("concept.concept_id"),primary_key=True)
    amount_value: so.Mapped[Optional[float]] = so.mapped_column(sa.Float)
    amount_unit_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("concept.concept_id"))
    numerator_value: so.Mapped[Optional[float]] = so.mapped_column(sa.Float)
    numerator_unit_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("concept.concept_id"))
    denominator_value: so.Mapped[Optional[float]] = so.mapped_column(sa.Float)
    denominator_unit_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("concept.concept_id"))
    box_size: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer)
    valid_start_date: so.Mapped[date] = so.mapped_column(nullable=False)
    valid_end_date: so.Mapped[date] = so.mapped_column(nullable=False)
    invalid_reason: so.Mapped[Optional[str]] = so.mapped_column(sa.String(1))
