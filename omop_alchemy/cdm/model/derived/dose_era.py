import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional
from datetime import date
from orm_loader.helpers import Base
from omop_alchemy.cdm.base import (
    cdm_table,
    CDMTableBase,
    required_concept_fk,
)

@cdm_table
class Dose_Era(CDMTableBase, Base):
    __tablename__ = "dose_era"

    dose_era_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("person.person_id"), nullable=False, index=True)
    drug_concept_id: so.Mapped[int] = required_concept_fk()
    unit_concept_id: so.Mapped[int] = required_concept_fk()
    dose_value: so.Mapped[float] = so.mapped_column(nullable=False)
    dose_era_start_date: so.Mapped[date] = so.mapped_column(nullable=False)
    dose_era_end_date: so.Mapped[date] = so.mapped_column(nullable=False)
