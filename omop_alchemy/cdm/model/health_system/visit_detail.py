import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional
from datetime import date

from orm_loader.helpers import Base 
from omop_alchemy.cdm.base import (
    cdm_table,
    CDMTableBase,
    required_concept_fk,
    optional_concept_fk,
)

@cdm_table
class Visit_Detail(CDMTableBase, Base):
    __tablename__ = "visit_detail"

    visit_detail_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("person.person_id"),nullable=False,index=True)
    visit_detail_concept_id: so.Mapped[int] = required_concept_fk(index=True)
    visit_detail_start_date: so.Mapped[date] = so.mapped_column(sa.Date, nullable=False)
    visit_detail_start_datetime: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime, nullable=True)
    visit_detail_end_date: so.Mapped[date] = so.mapped_column(sa.Date, nullable=False)
    visit_detail_end_datetime: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime, nullable=True)
    visit_detail_type_concept_id: so.Mapped[int] = required_concept_fk(index=True)
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("provider.provider_id"),nullable=True,index=True)
    care_site_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("care_site.care_site_id"),nullable=True,index=True)     
    visit_detail_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    visit_detail_source_concept_id: so.Mapped[Optional[int]] = optional_concept_fk(index=True)
    admitted_from_concept_id: so.Mapped[Optional[int]] = optional_concept_fk(index=True)
    admitted_from_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    discharged_to_concept_id: so.Mapped[Optional[int]] = optional_concept_fk(index=True)
    discharged_to_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    preceding_visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("visit_detail.visit_detail_id"),nullable=True,index=True)
    parent_visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("visit_detail.visit_detail_id"),nullable=True,index=True)
    visit_occurrence_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("visit_occurrence.visit_occurrence_id"),nullable=False,index=True)

    def __repr__(self) -> str:
        return f"<VisitDetail {self.visit_detail_id}>"
