from datetime import datetime, date
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Visit_Detail(Base):
    __tablename__ = 'visit_detail'
    # identifier
    visit_detail_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True) 
    # temporal
    visit_detail_start_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    visit_detail_start_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    visit_detail_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    visit_detail_end_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    # strings
    admitted_from_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    visit_detail_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    discharge_to_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    # numeric
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id'))
    provider_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('provider.provider_id'))
    visit_occurrence_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id'))
    visit_detail_parent_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('visit_detail.visit_detail_id'))
    preceding_visit_detail_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('visit_detail.visit_detail_id'))
    care_site_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('care_site.care_site_id'))
    # concept fks
    visit_detail_type_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    discharge_to_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    visit_detail_source_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    visit_detail_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    admitted_from_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))

    # relationships
    person: so.Mapped[Optional['Person']] = so.relationship(foreign_keys=[person_id])
    provider: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    visit_occurrence: so.Mapped[Optional['Visit_Occurrence']] = so.relationship(foreign_keys=[visit_occurrence_id])
    visit_detail_parent: so.Mapped[Optional['Visit_Detail']] = so.relationship(foreign_keys=[visit_detail_parent_id])
    preceding_visit_detail: so.Mapped[Optional['Visit_Detail']] = so.relationship(foreign_keys=[preceding_visit_detail_id])
    care_site: so.Mapped[Optional['Care_Site']] = so.relationship(foreign_keys=[care_site_id])

    # concept_relationships
    visit_detail_type_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[visit_detail_type_concept_id])
    discharge_to_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[discharge_to_concept_id])
    visit_detail_source_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[visit_detail_source_concept_id])
    visit_detail_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[visit_detail_concept_id])
    admitted_from_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[admitted_from_concept_id])



    