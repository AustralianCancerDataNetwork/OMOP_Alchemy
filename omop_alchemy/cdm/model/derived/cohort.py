import sqlalchemy as sa
import sqlalchemy.orm as so
from datetime import date
from orm_loader.helpers import Base
from omop_alchemy.cdm.base import (
    cdm_table,
    CDMTableBase,
)

@cdm_table
class Cohort(CDMTableBase, Base):
    __tablename__ = "cohort"

    cohort_definition_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    subject_id: so.Mapped[int] = so.mapped_column(primary_key=True)

    cohort_start_date: so.Mapped[date] = so.mapped_column(sa.Date, nullable=False)
    cohort_end_date: so.Mapped[date] = so.mapped_column(sa.Date, nullable=False)

    def __repr__(self) -> str:
        return f"<Cohort def={self.cohort_definition_id} subj={self.subject_id}>"
