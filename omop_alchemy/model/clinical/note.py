from datetime import datetime, date
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Note(Base):
    __tablename__ = 'note'
    # identifier
    note_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True) 
    # temporal
    note_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    note_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    # strings
    note_title: so.Mapped[Optional[str]] = so.mapped_column(sa.String(250), nullable=True)
    note_text: so.Mapped[Optional[str]] = so.mapped_column(sa.Text, nullable=True)
    note_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    # numeric
    note_event_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id', name='n_fk_1'))
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('provider.provider_id', name='n_fk_2'), nullable=True)
    visit_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id', name='n_fk_3'), nullable=True)
    visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_detail.visit_detail_id', name='n_fk_4'), nullable=True)
    # concept fks
    note_type_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='n_fk_5'))
    note_class_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='n_fk_6'))
    encoding_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='n_fk_7'))
    language_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='n_fk_8'))
    note_event_field_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='n_fk_9'), nullable=True)
    # relationships
    person: so.Mapped[Optional['Person']] = so.relationship(foreign_keys=[person_id])
    provider: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    visit_occurrence: so.Mapped[Optional['Visit_Occurrence']] = so.relationship(foreign_keys=[visit_occurrence_id])
    visit_detail: so.Mapped[Optional['Visit_Detail']] = so.relationship(foreign_keys=[visit_detail_id])
    # concept_relationships
    note_type: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[note_type_concept_id])
    note_class: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[note_class_concept_id])
    encoding: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[encoding_concept_id])
    language: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[language_concept_id])
    note_event_field: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[note_event_field_concept_id])