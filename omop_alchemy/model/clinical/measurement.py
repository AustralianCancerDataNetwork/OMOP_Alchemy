from datetime import date, datetime, time
from typing import Optional
from decimal import Decimal
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.hybrid import hybrid_property

from ...db import Base
from ..concept_links import Concept_Links

class Measurement(Base, Concept_Links):
    __tablename__ = 'measurement'
    labels = {'value_as': False, 'unit': False, 'measurement': False, 'measurement_source': False, 'measurement_type': False, 'operator': False, 'modifier_of_field': False}

    # identifier
    measurement_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    # temporal
    measurement_date: so.Mapped[date] = so.mapped_column(sa.Date)
    measurement_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime, nullable=True)
    measurement_time: so.Mapped[Optional[time]] = so.mapped_column(sa.Time, nullable=True)
    # strings
    measurement_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    unit_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    value_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    # numeric
    value_as_number: so.Mapped[Optional[Decimal]] = so.mapped_column(sa.Numeric, nullable=True)
    range_low: so.Mapped[Optional[Decimal]] = so.mapped_column(sa.Numeric, nullable=True)
    range_high: so.Mapped[Optional[Decimal]] = so.mapped_column(sa.Numeric, nullable=True)
    # polymorphic fk
    modifier_of_event_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('modifiable_table.modifier_id', name='m_fk_1'), nullable=True)
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id', name='m_fk_2'))
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('provider.provider_id', name='m_fk_3'), nullable=True)
    visit_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id', name='m_fk_4'), nullable=True)
    visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_detail.visit_detail_id', name='m_fk_5'), nullable=True)
    # relationships
    person_object: so.Mapped['Person'] = so.relationship(foreign_keys=[person_id])
    provider_object: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    visit_occurrence_object: so.Mapped[Optional['Visit_Occurrence']] = so.relationship(foreign_keys=[visit_occurrence_id])
    visit_detail_object: so.Mapped[Optional['Visit_Detail']] = so.relationship(foreign_keys=[visit_detail_id])
    modified_object: so.Mapped[Optional['Modifiable_Table']] = so.relationship(foreign_keys=[modifier_of_event_id])


    @hybrid_property
    def measurement_label(self):
        if self.measurement_concept:
            return self.measurement_concept.concept_code