from datetime import datetime, date
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Visit_Occurrence(Base):
    __tablename__ = 'visit_occurrence'
    # identifier
    visit_occurrence_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True) 
    # temporal
    visit_start_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    visit_start_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    visit_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    visit_end_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    # strings
    visit_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    admitted_from_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    discharge_to_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    # numeric
    admitted_from_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id', name='vo_fk_1'))
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('provider.provider_id', name='vo_fk_2'), nullable=True)
    care_site_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('care_site.care_site_id', name='vo_fk_3'), nullable=True)
    preceding_visit_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id', name='vo_fk_4'), nullable=True)
    # concept fks
    visit_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='vo_fk_5'))
    visit_source_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='vo_fk_6'), nullable=True)
    discharge_to_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='vo_fk_7'), nullable=True)
    visit_type_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='vo_fk_8'))
    # relationships
    person: so.Mapped[Optional['Person']] = so.relationship(foreign_keys=[person_id])
    provider: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    care_site: so.Mapped[Optional['Care_Site']] = so.relationship(foreign_keys=[care_site_id])
    visit_occurrence: so.Mapped[Optional['Visit_Occurrence']] = so.relationship(foreign_keys=[preceding_visit_occurrence_id])
    # concept_relationships
    visit_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[visit_concept_id])
    visit_source: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[visit_source_concept_id])
    discharge_to: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[discharge_to_concept_id])
    visit_type: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[visit_type_concept_id])