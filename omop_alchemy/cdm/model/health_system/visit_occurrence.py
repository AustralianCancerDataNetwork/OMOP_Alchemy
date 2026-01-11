import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional
from datetime import date

from orm_loader.helpers import Base
from omop_alchemy.cdm.base import (
    cdm_table,
    CDMTableBase,
    required_int,
    optional_int,
    required_concept_fk,
    optional_concept_fk,
)

@cdm_table
class Visit_Occurrence(CDMTableBase, Base):
    __tablename__ = "visit_occurrence"

    visit_occurrence_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("person.person_id"),nullable=False,index=True)
    
    visit_concept_id: so.Mapped[int] = required_concept_fk(index=True)
    visit_start_date: so.Mapped[date] = so.mapped_column(sa.Date, nullable=False)
    visit_start_datetime: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime, nullable=True)
    visit_end_date: so.Mapped[date] = so.mapped_column(sa.Date, nullable=False)
    visit_end_datetime: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime, nullable=True)
    visit_type_concept_id: so.Mapped[int] = required_concept_fk(index=True)

    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("provider.provider_id"),nullable=True,index=True)
    care_site_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("care_site.care_site_id"),nullable=True,index=True)

    visit_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    visit_source_concept_id: so.Mapped[Optional[int]] = optional_concept_fk(index=True)
    admitted_from_concept_id: so.Mapped[Optional[int]] = optional_concept_fk(index=True)
    admitted_from_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    discharged_to_concept_id: so.Mapped[Optional[int]] = optional_concept_fk(index=True)
    discharged_to_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)

    preceding_visit_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("visit_occurrence.visit_occurrence_id"),nullable=True,index=True)

    def __repr__(self) -> str:
        return f"<VisitOccurrence {self.visit_occurrence_id}>"
