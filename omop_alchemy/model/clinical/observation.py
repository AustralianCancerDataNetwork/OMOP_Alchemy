from datetime import datetime, date
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so
from decimal import Decimal
from sqlalchemy.ext.hybrid import hybrid_property

from ..concept_links import Concept_Links
from ...db import Base

class Observation(Base, Concept_Links):
    __tablename__ = 'observation'
    labels = {'observation': False, 'observation_type': False, 'observation_source': False, 'value_as': False, 'qualifier': False, 'unit': False, 'obs_event_field': False}

    # identifier
    observation_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True) 
    # temporal
    observation_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)
    observation_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime, nullable=True)
    value_as_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime, nullable=True)
    # strings
    value_as_string: so.Mapped[Optional[str]] = so.mapped_column(sa.String(60), nullable=True)
    observation_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    unit_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    qualifier_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    # numeric
    value_as_number: so.Mapped[Optional[Decimal]] = so.mapped_column(sa.Numeric, nullable=True)
    # polymorphic fk
    observation_event_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('modifiable_table.modifier_id', name='o_fk_1'), nullable=True)
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id', name='o_fk_2'))
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('provider.provider_id', name='o_fk_3'), nullable=True)
    visit_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id', name='o_fk_4'), nullable=True)
    visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_detail.visit_detail_id', name='o_fk_5'), nullable=True)
    # relationships
    person: so.Mapped['Person'] = so.relationship(foreign_keys=[person_id])
    provider: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    visit_occurrence: so.Mapped[Optional['Visit_Occurrence']] = so.relationship(foreign_keys=[visit_occurrence_id])
    visit_detail: so.Mapped[Optional['Visit_Detail']] = so.relationship(foreign_keys=[visit_detail_id])
    observed_object: so.Mapped[Optional['Modifiable_Table']] = so.relationship(foreign_keys=[observation_event_id])

    @property
    def observation_dt(self):
        # coalesce over observation datetime and observation_date returning date object or None
        return self.observation_datetime.date if self.observation_date else self.observation_date


