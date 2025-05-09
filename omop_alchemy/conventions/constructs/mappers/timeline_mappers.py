from typing import List
from itertools import chain
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.hybrid import hybrid_property

from ....db import Base
from ....conventions.concept_enumerators import CancerProcedureTypes, ModifierFields, TreatmentEpisode, DiseaseEpisodeConcepts, DemographyConcepts
from ....model.onco_ext import Episode, Episode_Event
from ....model.clinical import Person, Modifiable_Table, Condition_Occurrence, Drug_Exposure, Procedure_Occurrence, Observation
from ....model.vocabulary import Concept, Concept_Ancestor

from ..mappers.event_type_mappers import Dx_RT_Start, Dx_SACT_Start, Diagnosis
from ..mappers.surgical_mappers import Dated_Surgical_Procedure

dx_treatment_window = (
    sa.select(
        Person.person_id,
        Person.death_datetime,
        Diagnosis.episode_id,
        Diagnosis.episode_start_datetime,
        Dated_Surgical_Procedure.procedure_datetime,
        Dx_RT_Start.rt_start, 
        Dx_SACT_Start.sact_start, 
        Dx_RT_Start.rt_end, 
        Dx_SACT_Start.sact_end 
    )
    .join(Diagnosis, Diagnosis.person_id==Person.person_id)
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
    def latest_treatment(self):
        treat_ends = [d for d in [self.rt_end, self.sact_end, self.procedure_datetime] if d is not None]
        if not(treat_ends):
            return None        
        return max(treat_ends)

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
        latest_treatment = self.latest_treatment
        if not(latest_treatment) or not(self.death_datetime):
            return None
        delta = self.death_datetime.date() - latest_treatment
        return delta.days

    @latest_treatment.expression
    def latest_treatment(cls):
        return sa.func.greatest(
            sa.case((cls.rt_end != None, cls.rt_end), else_=None),
            sa.case((cls.sact_end != None, cls.sact_end), else_=None),
            sa.case((cls.procedure_datetime != None, sa.cast(cls.procedure_datetime, sa.Date)), else_=None)
        )

    @treatment_days_before_death.expression
    def treatment_days_before_death(cls):
        return sa.cast(cls.death_datetime, sa.Date) - cls.latest_treatment

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
        
