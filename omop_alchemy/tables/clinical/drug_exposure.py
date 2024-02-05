from datetime import datetime, date
from typing import Optional

import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Drug_Exposure(Base):
    __tablename__ = 'drug_exposure'    
    # identifier
    drug_exposure_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)

    # temporal
    drug_exposure_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    drug_exposure_start_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    drug_exposure_end_date: so.Mapped[date] = so.mapped_column(sa.Date)
    drug_exposure_end_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)    
    verbatim_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    # strings
    stop_reason: so.Mapped[Optional[str]] = so.mapped_column(sa.String(20))
    lot_number: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    drug_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    route_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    dose_unit_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    sig: so.Mapped[Optional[str]] = so.mapped_column(sa.Text)
    # numeric
    refills: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer)
    days_supply: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer)
    quantity: so.Mapped[Optional[int]] = so.mapped_column(sa.Numeric)
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id'))
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('provider.provider_id'))
    visit_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id'))
    visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_detail.visit_detail_id'))
    # concept fks
    drug_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    drug_type_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    route_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    drug_source_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    # relationships
    person: so.Mapped['Person'] = so.relationship(foreign_keys=[person_id])
    provider: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    visit_occurrence: so.Mapped[Optional['Visit_Occurrence']] = so.relationship(foreign_keys=[visit_occurrence_id])
    visit_detail: so.Mapped[Optional['Visit_Detail']] = so.relationship(foreign_keys=[visit_detail_id])
    # concept_relationships
    drug_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[drug_concept_id])
    drug_type_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[drug_type_concept_id])
    route_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[route_concept_id])
    drug_source_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[drug_source_concept_id])

