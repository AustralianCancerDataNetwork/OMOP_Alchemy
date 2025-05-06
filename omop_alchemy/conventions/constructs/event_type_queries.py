import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base
from ...model.onco_ext import Episode, Episode_Event
from ...model.clinical import Person, Modifiable_Table, Condition_Occurrence, Drug_Exposure, Procedure_Occurrence, Observation
from ...model.vocabulary import Concept, Concept_Ancestor
from ...conventions.concept_enumerators import ModifierFields, TreatmentEpisode, DiseaseEpisodeConcepts
from .alias_definitions import radiation_therapy

class SACT_Event(Episode_Event):
    __table__ = (
        sa.select(Episode_Event)
        .where(Episode_Event.episode_event_field_concept_id==ModifierFields.drug_exposure_id.value)
        .subquery()
    )
    
    regimen_id = so.column_property(__table__.c.episode_id)
    sact_id = so.column_property(__table__.c.event_id)
    
    __mapper_args__ = {
        'polymorphic_identity': 'sact_event',
        'inherit_condition': (Episode_Event.event_id == __table__.c.event_id)
    }

class RT_Event(Episode_Event):
    """
    TODO: this needs updating because it currently fails to actually filter down to 
    TODO: RT-specific events, however it is not an issue, due to the fact that RT events are
    TODO: the only ones mapped to episodes - this will break otherwise
    """
    __table__ = (
        sa.select(Episode_Event)
        .where(Episode_Event.event_polymorphic.of_type(radiation_therapy))
        .subquery()
    )
    
    regimen_id = so.column_property(__table__.c.episode_id)
    rt_id = so.column_property(__table__.c.event_id)
    
    __mapper_args__ = {
        'polymorphic_identity': 'rt_event',
        'inherit_condition': (Episode_Event.event_id == __table__.c.event_id)
    }

regimen_subquery = (
    sa.select(Episode)
    .where(Episode.episode_concept_id==TreatmentEpisode.treatment_regimen.value)
    .subquery()
)

diagnosis_subquery = (
    sa.select(Episode)
    .where(Episode.episode_concept_id==DiseaseEpisodeConcepts.episode_of_care.value)
    .subquery()
)

Regimen = so.with_polymorphic(Episode, [], selectable=regimen_subquery)
Diagnosis = so.with_polymorphic(Episode, [], selectable=diagnosis_subquery)

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
