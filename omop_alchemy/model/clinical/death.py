from datetime import datetime, date
from typing import Optional, List
from sqlalchemy.ext.hybrid import hybrid_property
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Death(Base):
    __tablename__ = 'death'
    
    # we need a primary key here, so we enforce a single death record per person on a given day although this is not
    # per CDM definition - to follow up - may want to add death key transparently instead?
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id', name='dth_fk_1'), primary_key=True)

    death_date: so.Mapped[date] = so.mapped_column(sa.Date, primary_key=True)
    death_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime, nullable=True)
    
    cause_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    
    death_type_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='dth_fk_2'), nullable=True)
    cause_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='dth_fk_3'), nullable=True)
    cause_source_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='dth_fk_4'), nullable=True)
    
    death_type: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[death_type_concept_id])
    cause: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[cause_concept_id])
    cause_source: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[cause_source_concept_id])

    person: so.Mapped['Person'] = so.relationship(foreign_keys=[person_id])
