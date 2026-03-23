import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional
from datetime import date
from orm_loader.helpers import Base
from omop_alchemy.cdm.base import (
    cdm_table,
    CDMTableBase,
    required_concept_fk,
    merge_table_args,
    omop_index,
)

@cdm_table
class Dose_Era(CDMTableBase, Base):
    __tablename__ = "dose_era"
    __table_args__ = merge_table_args(
        omop_index("idx_dose_era_person_id_1", "person_id", cluster=True),
        omop_index("idx_dose_era_concept_id_1", "drug_concept_id"),
        omop_index("ix_dose_era_unit_concept_id", "unit_concept_id"),
    )

    dose_era_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("person.person_id"), nullable=False)
    drug_concept_id: so.Mapped[int] = required_concept_fk()
    unit_concept_id: so.Mapped[int] = required_concept_fk()
    dose_value: so.Mapped[float] = so.mapped_column(nullable=False)
    dose_era_start_date: so.Mapped[date] = so.mapped_column(nullable=False)
    dose_era_end_date: so.Mapped[date] = so.mapped_column(nullable=False)
