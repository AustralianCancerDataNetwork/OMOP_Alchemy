from datetime import date, datetime, time
from typing import Optional, List
from decimal import Decimal
from itertools import chain
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.ext.hybrid import hybrid_property

from ...db import Base
from ...conventions.concept_enumerators import CancerProcedureTypes
from .episode import Episode
from .episode_event import Episode_Event
from ..clinical.person import Person
from ..clinical.modifiable_table import Modifiable_Table
from ..clinical.condition_occurrence import Condition_Occurrence
from ..clinical.drug_exposure import Drug_Exposure
from ..clinical.procedure_occurrence import Procedure_Occurrence
from ..clinical.observation import Observation
from ..vocabulary.concept import Concept
from ..vocabulary.concept_ancestor import Concept_Ancestor
from ...conventions.concept_enumerators import ModifierFields, TreatmentEpisode, DiseaseEpisodeConcepts, DemographyConcepts

class Person_Episodes(Person):
    """
    At the moment, this just returns all SACT eps linked to a person.
    TODO: refine this mapper to have required relationships to all episode objects
    """
    #condition_episodes: so.Mapped[List['Condition_Episode']] = so.relationship(back_populates="person_object", lazy="selectin")
    sact_episodes: so.Mapped[List['Systemic_Therapy_Episode']] = so.relationship(back_populates="person_object", order_by='Systemic_Therapy_Episode.sact_start')

    @property
    def all_agents(self):
        return list(set(chain.from_iterable([se.episode_agents for se in self.sact_episodes])))


dx_treatment_window = (
    sa.select(
        cdm.clinical.Person.person_id,
        cdm.clinical.Person.death_datetime,
        Diagnosis.episode_id,
        Diagnosis.episode_start_datetime,
        Dated_Surgical_Procedure.procedure_datetime,
        Dx_RT_Start.rt_start, 
        Dx_SACT_Start.sact_start, 
        Dx_RT_Start.rt_end, 
        Dx_SACT_Start.sact_end 
    )
    .join(Diagnosis, Diagnosis.person_id==cdm.clinical.Person.person_id)
    .join(Dated_Surgical_Procedure, Dated_Surgical_Procedure.person_id==Diagnosis.person_id, isouter=True)
    .join(Dx_RT_Start, Dx_RT_Start.dx_id==Diagnosis.episode_id, isouter=True)
    .join(Dx_SACT_Start, Dx_SACT_Start.dx_id==Diagnosis.episode_id, isouter=True)
    .subquery()
)

class Treatment_Window(Base):
    """
    This mapper returns the bounds of a treatment window, looking for earliest and latest RT/SACT events 
    
    Note that surgical procedures are not currently mapped into episodes, but current mappings
    are only for manually entered, relevant surgical procedures, so this is robust at the person level.

    TODO: map surgical procedures to treatment eps & add surg_start, surg_end calculations

    """

    __table__ = dx_treatment_window
    person_id = dx_treatment_window.c.person_id
    episode_id = dx_treatment_window.c.episode_id
    episode_start_datetime = so.column_property(dx_treatment_window.c.episode_start_datetime)
    death_datetime = so.column_property(dx_treatment_window.c.death_datetime)
    rt_start = so.column_property(dx_treatment_window.c.rt_start)
    sact_start = so.column_property(dx_treatment_window.c.sact_start)
    rt_end = so.column_property(dx_treatment_window.c.rt_end)
    sact_end = so.column_property(dx_treatment_window.c.sact_end)
    procedure_datetime = so.column_property(dx_treatment_window.c.procedure_datetime)


    @sa.ext.hybrid.hybrid_property 
    def treatment_days_before_death(self):
        """ 
        Hybrid properties like this need to have python and SQL version of their definition, in order
        to allow their use within a query filter. This makes them relatively slow in terms of performance
        as no portion can be lazy loaded. Because of this, use only where you are actually needing their 
        result directly.

        example usage: 
            -> within query, the sql version will be deployed
            tw = db.query(Treatment_Window).filter(Treatment_Window.treatment_days_before_death <= 30).limit(100).all()

            -> client-side, the python property will be invoked instead
            len([t.treatment_days_before_death for t in tw if t.treatment_days_before_death])
        """
        treat_ends = [d for d in [self.rt_end, self.sact_end, self.procedure_datetime] if d is not None]
        if not(treat_ends) or not(self.death_datetime):
            return None
        latest_treatment = max(treat_ends)
        delta = self.death_datetime.date() - latest_treatment
        return delta.days

    @treatment_days_before_death.expression
    def treatment_days_before_death(cls):
        latest_treat_expr = sa.func.greatest(
            sa.case((cls.rt_end != None, cls.rt_end), else_=None),
            sa.case((cls.sact_end != None, cls.sact_end), else_=None),
            sa.case((cls.procedure_datetime != None, sa.cast(cls.procedure_datetime, sa.Date)), else_=None)
        )
        return sa.cast(cls.death_datetime, sa.Date) - latest_treat_expr

    @sa.ext.hybrid.hybrid_property 
    def treatment_days_after_dx(self):
        treat_starts = [d for d in [self.rt_start, self.sact_start, self.procedure_datetime] if d is not None]
        if not(treat_starts):
            return None
        first_treatment = min(treat_starts)
        delta = self.episode_start_datetime.date() - first_treatment
        return delta.days
    
    @treatment_days_after_dx.expression
    def treatment_days_after_dx(cls):
        earliest_treatment_expr = sa.func.least(
            sa.case((cls.rt_end != None, cls.rt_start), else_=None),
            sa.case((cls.sact_end != None, cls.sact_start), else_=None),
            sa.case((cls.procedure_datetime != None, sa.cast(cls.procedure_datetime, sa.Date)), else_=None)
        )
        return earliest_treatment_expr - sa.cast(cls.episode_start_datetime, sa.Date)
        
