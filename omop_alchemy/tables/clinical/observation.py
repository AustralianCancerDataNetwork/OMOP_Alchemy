from datetime import datetime, date
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so
from decimal import Decimal

from ...db import Base

class Observation(Base):
    __tablename__ = 'observation'
    # identifier
    observation_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True) 
    # temporal
    observation_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    observation_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    value_as_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    # strings
    value_as_string: so.Mapped[Optional[str]] = so.mapped_column(sa.String(60))
    observation_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    unit_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    qualifier_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    # numeric
    value_as_number: so.Mapped[Optional[Decimal]] = so.mapped_column(sa.Numeric)
    observation_event_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer)
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id'))
    provider_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('provider.provider_id'))
    visit_occurrence_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id'))
    visit_detail_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('visit_detail.visit_detail_id'))
    # concept fks
    observation_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    observation_type_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    observation_source_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    value_as_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    qualifier_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    unit_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    obs_event_field_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    # relationships
    person: so.Mapped[Optional['Person']] = so.relationship(foreign_keys=[person_id])
    provider: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    visit_occurrence: so.Mapped[Optional['Visit_Occurrence']] = so.relationship(foreign_keys=[visit_occurrence_id])
    visit_detail: so.Mapped[Optional['Visit_Detail']] = so.relationship(foreign_keys=[visit_detail_id])
    # concept_relationships
    observation_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[observation_concept_id])
    observation_type_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[observation_type_concept_id])
    observation_source_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[observation_source_concept_id])
    value_as_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[value_as_concept_id])
    qualifier_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[qualifier_concept_id])
    unit_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[unit_concept_id])
    obs_event_field_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[obs_event_field_concept_id])