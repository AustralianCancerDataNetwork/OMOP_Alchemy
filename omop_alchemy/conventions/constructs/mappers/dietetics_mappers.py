import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import List

from .vitals_mappers import Weights
from .consult_visit_mappers import Visits_By_Specialty
from .rt_mappers import Fraction, Course
from ..definitions.diagnosis_subqueries import cohort_by_primary

from ....db import Base


class HN_Trajectory(Base):
    __table__ = cohort_by_primary
    
    person_id = cohort_by_primary.c.person_id
    primary_condition_start = cohort_by_primary.c.episode_start_datetime
    primary_condition = cohort_by_primary.c.condition_name
    dx_episode = cohort_by_primary.c.dx_episode
    parent_concept_id = cohort_by_primary.c.ancestor_concept_id

    @sa.ext.hybrid.hybrid_property 
    def cohort_parent(self, parent):
        return self.parent_concept_id==parent

    @cohort_parent.expression
    def cohort_parent(cls, parent):
        return cls.parent_concept_id==parent

    
    weights: so.Mapped[List['Weights']] = so.relationship(
        "Weights",
        primaryjoin=(person_id==so.foreign(Weights.person_id)),
        #lazy="joined",
        viewonly=True
    )

    visits: so.Mapped[List['Visits_By_Specialty']] = so.relationship(
        "Visits_By_Specialty",
        primaryjoin=(person_id==so.foreign(Visits_By_Specialty.person_id)),
        #lazy="joined",
        viewonly=True
    )

    rt: so.Mapped[List['Course']] = so.relationship(
        "Course",
        primaryjoin=(dx_episode==so.foreign(Course.dx_episode)),
        #lazy="joined",
        viewonly=True
    )
    
    @property
    def weight_trend(self):
        ...
    
    @property
    def critical_weight_loss(self):
        ...

    @property
    def initial_dx_to_first_dietitian(self):
        ...