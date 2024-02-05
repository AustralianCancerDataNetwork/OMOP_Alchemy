from datetime import datetime, date
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Device_Exposure(Base):
    __tablename__ = 'device_exposure'
    # identifier
    device_exposure_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    # temporal
    device_exposure_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    device_exposure_start_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    device_exposure_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    device_exposure_end_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    # strings
    unique_device_id: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    device_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(100))
    # numeric
    quantity: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer)
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id'))
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('provider.provider_id'))
    visit_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id'))
    visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_detail.visit_detail_id'))
    # concept fks
    device_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    device_type_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))    
    device_source_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    # relationships
    person: so.Mapped['Person'] = so.relationship(foreign_keys=[person_id])
    provider: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    visit_occurrence: so.Mapped[Optional['Visit_Occurrence']] = so.relationship(foreign_keys=[visit_occurrence_id])
    visit_detail: so.Mapped[Optional['Visit_Detail']] = so.relationship(foreign_keys=[visit_detail_id])
    # concept relationships
    device_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[device_concept_id])
    device_type_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[device_type_concept_id])
    device_source_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[device_source_concept_id])
    