import sqlalchemy as sa
import sqlalchemy.orm as so
import datetime

from ....db import Base
from ....model.onco_ext import Episode, Episode_Event
from ....model.clinical import Person, Modifiable_Table, Condition_Occurrence, Drug_Exposure, Procedure_Occurrence, Observation
from ....model.vocabulary import Concept, Concept_Ancestor
from ....conventions.concept_enumerators import ModifierFields, TreatmentEpisode, DiseaseEpisodeConcepts
from ..definitions.alias_definitions import radiation_therapy
from ..definitions.episode_event_subqueries import sact_episode_events, rt_episode_events, Regimen, Diagnosis
from ..definitions.surgical_subqueries import surgical_procedure

class SACT_Event(Episode_Event):
    __table__ = sact_episode_events
    
    regimen_id = so.column_property(__table__.c.episode_id)
    sact_id = so.column_property(__table__.c.event_id)
    
    __mapper_args__ = {
        'polymorphic_identity': 'sact_event',
        'inherit_condition': (Episode_Event.event_id == __table__.c.event_id)
    }

class RT_Event(Episode_Event):
    
    __table__ = rt_episode_events
    
    regimen_id = so.column_property(__table__.c.episode_id)
    rt_id = so.column_property(__table__.c.event_id)
    
    __mapper_args__ = {
        'polymorphic_identity': 'rt_event',
        'inherit_condition': (Episode_Event.event_id == __table__.c.event_id)
    }


dx_with_regimen = (
    sa.select(
        Diagnosis.person_id,
        Diagnosis.episode_id.label('dx_id'), 
        Diagnosis.episode_start_datetime.label('dx_date'), 
        sa.func.min(Regimen.episode_start_datetime).label('treatment_start'),
        sa.func.max(Regimen.episode_end_datetime).label('treatment_end')
    )
    .join(Regimen, Regimen.episode_parent_id==Diagnosis.episode_id)
    .group_by(Diagnosis.person_id, Diagnosis.episode_id, Diagnosis.episode_start_datetime)
    .subquery()
)

dx_with_sact = (
    sa.select(
        Diagnosis.person_id,
        Diagnosis.episode_id.label('dx_id'), 
        Diagnosis.episode_start_datetime.label('dx_date'), 
        sa.func.min(Drug_Exposure.drug_exposure_start_date).label('sact_start'),
        sa.func.max(Drug_Exposure.drug_exposure_start_date).label('sact_end')
    )
    .join(Regimen, Regimen.episode_parent_id==Diagnosis.episode_id)
    .join(SACT_Event, SACT_Event.regimen_id==Regimen.episode_id)
    .join(Drug_Exposure, Drug_Exposure.drug_exposure_id==SACT_Event.sact_id)
    .group_by(Diagnosis.person_id, Diagnosis.episode_id, Diagnosis.episode_start_datetime)
    .subquery()
)

dx_with_rt = (
    sa.select(
        Diagnosis.person_id, 
        Diagnosis.episode_id.label('dx_id'), 
        Diagnosis.episode_start_datetime.label('dx_date'), 
        sa.func.min(Procedure_Occurrence.procedure_date).label('rt_start'),
        sa.func.max(Procedure_Occurrence.procedure_date).label('rt_end')
    )
    .join(Regimen, Regimen.episode_parent_id==Diagnosis.episode_id)
    .join(RT_Event, RT_Event.regimen_id==Regimen.episode_id)
    .join(Procedure_Occurrence, Procedure_Occurrence.procedure_occurrence_id==RT_Event.rt_id)
    .group_by(Diagnosis.person_id, Diagnosis.episode_id, Diagnosis.episode_start_datetime)
    .subquery()
)

dx_with_surg = (
    sa.select(
        Condition_Occurrence.person_id, 
        Condition_Occurrence.condition_occurrence_id.label('dx_id'), 
        Condition_Occurrence.condition_start_datetime.label('dx_date'), 
        surgical_procedure.c.procedure_concept_id.label('surgery_concept_id'),
        surgical_procedure.c.procedure_datetime.label('surg_date')
    )
    .join(surgical_procedure, surgical_procedure.c.person_id==Condition_Occurrence.person_id, isouter=True)
    .subquery()
)


concurrent_chemort = (
    sa.select(
        dx_with_sact.c.person_id,
        dx_with_sact.c.dx_id,
        dx_with_sact.c.dx_date,
        dx_with_sact.c.sact_start,
        dx_with_sact.c.sact_end,
        dx_with_rt.c.rt_start,
        dx_with_rt.c.rt_end,
        sa.func.min([dx_with_sact.c.sact_start,dx_with_rt.c.rt_start]).label('concurrent_start'),
        sa.func.max([dx_with_sact.c.sact_end,dx_with_rt.c.rt_end]).label('concurrent_end')
    )
    .join(dx_with_rt, dx_with_rt.c.dx_id==dx_with_sact.c.dx_id, isouter=True)
    .filter(
        sa.or_(
            sa.and_(
                dx_with_sact.c.sact_start <= dx_with_rt.c.rt_end + datetime.timedelta(days=30),
                dx_with_sact.c.sact_start >= dx_with_rt.c.rt_start - datetime.timedelta(days=30),
            ),
            sa.and_(
                dx_with_sact.c.sact_end <= dx_with_rt.c.rt_end + datetime.timedelta(days=30),
                dx_with_sact.c.sact_end >= dx_with_rt.c.rt_start - datetime.timedelta(days=30),
            )
        )
    )
    .subquery()
)


concurrent_chemort = (
    sa.select(
        dx_with_sact.c.person_id,
        dx_with_sact.c.dx_id,
        dx_with_sact.c.dx_date,
        dx_with_sact.c.sact_start,
        dx_with_sact.c.sact_end,
        dx_with_rt.c.rt_start,
        dx_with_rt.c.rt_end,
        sa.func.least(dx_with_sact.c.sact_start,dx_with_rt.c.rt_start).label('concurrent_start'),
        sa.func.greatest(dx_with_sact.c.sact_end,dx_with_rt.c.rt_end).label('concurrent_end')
    )
    .join(dx_with_rt, dx_with_rt.c.dx_id==dx_with_sact.c.dx_id, isouter=True)
    .filter(
        sa.or_(
            sa.and_(
                dx_with_sact.c.sact_start <= dx_with_rt.c.rt_end + datetime.timedelta(days=30),
                dx_with_sact.c.sact_start >= dx_with_rt.c.rt_start - datetime.timedelta(days=30),
            ),
            sa.and_(
                dx_with_sact.c.sact_end <= dx_with_rt.c.rt_end + datetime.timedelta(days=30),
                dx_with_sact.c.sact_end >= dx_with_rt.c.rt_start - datetime.timedelta(days=30),
            )
        )
    )
    .subquery()
)




class Dx_Treat_Start(Base):
    __table__ = dx_with_regimen
    person_id = dx_with_regimen.c.person_id
    dx_id = dx_with_regimen.c.dx_id
    dx_date = dx_with_regimen.c.dx_date
    treatment_start = so.column_property(dx_with_regimen.c.treatment_start)
    treatment_end = so.column_property(dx_with_regimen.c.treatment_end)

class Dx_SACT_Start(Base):
    __table__ = dx_with_sact
    person_id = dx_with_sact.c.person_id
    dx_id = dx_with_sact.c.dx_id
    dx_date = dx_with_sact.c.dx_date
    sact_start = so.column_property(dx_with_sact.c.sact_start)
    sact_end = so.column_property(dx_with_sact.c.sact_end)

class Dx_RT_Start(Base):
    __table__ = dx_with_rt
    person_id = dx_with_rt.c.person_id
    dx_id = dx_with_rt.c.dx_id
    dx_date = dx_with_rt.c.dx_date
    rt_start = so.column_property(dx_with_rt.c.rt_start)
    rt_end = so.column_property(dx_with_rt.c.rt_end)

class Dx_Surg(Base):
    __table__ = dx_with_surg
    person_id = dx_with_surg.c.person_id
    dx_id = dx_with_surg.c.dx_id
    dx_date = dx_with_surg.c.dx_date
    surg_date = so.column_property(dx_with_surg.c.surg_date)


class Dx_Concurrent_Start(Base):
    __table__ = concurrent_chemort
    person_id = concurrent_chemort.c.person_id
    dx_id = concurrent_chemort.c.dx_id
    dx_date = concurrent_chemort.c.dx_date
    treatment_start = so.column_property(concurrent_chemort.c.concurrent_start)
    treatment_end = so.column_property(concurrent_chemort.c.concurrent_end)


