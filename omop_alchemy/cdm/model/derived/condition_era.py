import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional
from datetime import date
from orm_loader.helpers import Base
from omop_alchemy.cdm.base import (
    cdm_table,
    CDMTableBase,
    optional_concept_fk,
    required_concept_fk,
)

@cdm_table
class Condition_Era(CDMTableBase, Base):
    __tablename__ = "condition_era"

    condition_era_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("person.person_id"), nullable=False, index=True)
    condition_concept_id: so.Mapped[int] = required_concept_fk()
    condition_era_start_date: so.Mapped[date] = so.mapped_column(nullable=False)
    condition_era_end_date: so.Mapped[date] = so.mapped_column(nullable=False)
    condition_occurrence_count: so.Mapped[Optional[int]] = so.mapped_column(nullable=True)
