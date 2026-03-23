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
    merge_table_args,
    omop_index,
)

@cdm_table
class Visit_Detail(CDMTableBase, Base):
    __tablename__ = "visit_detail"
    __table_args__ = merge_table_args(
        omop_index("idx_visit_det_person_id_1", "person_id", cluster=True),
        omop_index("idx_visit_det_concept_id_1", "visit_detail_concept_id"),
        omop_index("idx_visit_det_occ_id", "visit_occurrence_id"),
        omop_index("ix_visit_detail_visit_detail_type_concept_id", "visit_detail_type_concept_id"),
        omop_index("ix_visit_detail_provider_id", "provider_id"),
        omop_index("ix_visit_detail_care_site_id", "care_site_id"),
        omop_index("ix_visit_detail_visit_detail_source_concept_id", "visit_detail_source_concept_id"),
        omop_index("ix_visit_detail_admitted_from_concept_id", "admitted_from_concept_id"),
        omop_index("ix_visit_detail_discharged_to_concept_id", "discharged_to_concept_id"),
        omop_index("ix_visit_detail_preceding_visit_detail_id", "preceding_visit_detail_id"),
        omop_index("ix_visit_detail_parent_visit_detail_id", "parent_visit_detail_id"),
    )

    visit_detail_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("person.person_id"), nullable=False)
    visit_detail_concept_id: so.Mapped[int] = required_concept_fk()
    visit_detail_start_date: so.Mapped[date] = so.mapped_column(sa.Date, nullable=False)
    visit_detail_start_datetime: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime, nullable=True)
    visit_detail_end_date: so.Mapped[date] = so.mapped_column(sa.Date, nullable=False)
    visit_detail_end_datetime: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime, nullable=True)
    visit_detail_type_concept_id: so.Mapped[int] = required_concept_fk()
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("provider.provider_id"), nullable=True)
    care_site_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("care_site.care_site_id"), nullable=True)
    visit_detail_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    visit_detail_source_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()
    admitted_from_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()
    admitted_from_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    discharged_to_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()
    discharged_to_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    preceding_visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("visit_detail.visit_detail_id"), nullable=True)
    parent_visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("visit_detail.visit_detail_id"), nullable=True)
    visit_occurrence_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("visit_occurrence.visit_occurrence_id"), nullable=False)

    def __repr__(self) -> str:
        return f"<VisitDetail {self.visit_detail_id}>"
