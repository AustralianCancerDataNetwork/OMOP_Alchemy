from datetime import date
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Observation_Period(Base):
    __tablename__ = 'observation_period'
    # identifier
    observation_period_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True) 
    # temporal
    observation_period_start_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    observation_period_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    # strings
    # numeric
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id'))
    # concept fks
    period_type_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    # relationships
    person: so.Mapped[Optional['Person']] = so.relationship(foreign_keys=[person_id])
    # concept_relationships
    period_type: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[period_type_concept_id])


