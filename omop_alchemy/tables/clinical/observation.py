from datetime import datetime, date
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so
from decimal import Decimal

from .concept_links import Concept_Links
from ...db import Base

class Observation(Base, Concept_Links):
    __tablename__ = 'observation'
    labels = {'observation': False, 'observation_type': False, 'observation_source': False, 'value_as': False, 'qualifier': False, 'unit': False, 'obs_event_field': False}

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
    # polymorphic fk
    observation_event_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('modifiable_table.modifier_id'))
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id'))
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('provider.provider_id'))
    visit_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id'))
    visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_detail.visit_detail_id'))
    # relationships
    person: so.Mapped['Person'] = so.relationship(foreign_keys=[person_id])
    provider: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    visit_occurrence: so.Mapped[Optional['Visit_Occurrence']] = so.relationship(foreign_keys=[visit_occurrence_id])
    visit_detail: so.Mapped[Optional['Visit_Detail']] = so.relationship(foreign_keys=[visit_detail_id])
    observed_object: so.Mapped[Optional['Modifiable_Table']] = so.relationship(foreign_keys=[observation_event_id])
