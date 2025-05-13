import sqlalchemy.orm as so
from typing import List

from ..definitions.episode_event_subqueries import radiation_therapy_subquery
from ..definitions.diagnosis_subqueries import cohort_by_primary
from ....db import Base

class Fraction(Base):
    __table__ = radiation_therapy_subquery
    person_id = radiation_therapy_subquery.c.person_id
    tx_episode = radiation_therapy_subquery.c.rad_episode_id
    dx_episode = radiation_therapy_subquery.c.episode_parent_id
    rt_datetime = radiation_therapy_subquery.c.procedure_datetime

class Course(Base):
    __table__ = cohort_by_primary
    person_id = cohort_by_primary.c.person_id
    dx_episode = cohort_by_primary.c.dx_episode
    
    fractions: so.Mapped[List['Fraction']] = so.relationship(
        "Fraction",
        primaryjoin=(dx_episode==so.foreign(Fraction.dx_episode)),
        #lazy="joined",
        viewonly=True
    )
    
    @property
    def likely_curative(self):
        ... # if len(self.fractions) > 27 ?
       