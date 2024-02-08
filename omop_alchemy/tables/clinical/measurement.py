from datetime import date, datetime, time
from typing import Optional
from decimal import Decimal
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base
from ..concept_links import Concept_Links

class Measurement(Base, Concept_Links):
    __tablename__ = 'measurement'
    labels = {'value_as': False, 'unit': False, 'measurement': False, 'measurement_source': False, 'measurement_type': False, 'operator': False, 'modifier_of_field': False}

    # identifier
    measurement_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    # temporal
    measurement_date: so.Mapped[date] = so.mapped_column(sa.Date)
    measurement_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    measurement_time: so.Mapped[Optional[time]] = so.mapped_column(sa.Time)
    # strings
    measurement_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    unit_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    value_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    # numeric
    value_as_number: so.Mapped[Decimal] = so.mapped_column(sa.Numeric)
    range_low: so.Mapped[Optional[Decimal]] = so.mapped_column(sa.Numeric)
    range_high: so.Mapped[Optional[Decimal]] = so.mapped_column(sa.Numeric)
    # polymorphic fk
    modifier_of_event_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('modifiable_table.modifier_id'))
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id'))
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('provider.provider_id'))
    visit_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id'))
    visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_detail.visit_detail_id'))
    # relationships
    person_object: so.Mapped['Person'] = so.relationship(foreign_keys=[person_id])
    provider_object: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    visit_occurrence_object: so.Mapped[Optional['Visit_Occurrence']] = so.relationship(foreign_keys=[visit_occurrence_id])
    visit_detail_object: so.Mapped[Optional['Visit_Detail']] = so.relationship(foreign_keys=[visit_detail_id])
    modified_object: so.Mapped[Optional['Modifiable_Table']] = so.relationship(foreign_keys=[modifier_of_event_id])
