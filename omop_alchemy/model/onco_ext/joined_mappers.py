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


# select all conditions that have been associated with an episode - TBC if we need to add filtering for overarching?

diagnosis = so.aliased(Condition_Occurrence, flat=True)

dx_episode_subquery = (
    sa.select(
        Episode_Event.episode_id.label('dx_episode_id'),
        diagnosis.person_id,
        diagnosis.condition_start_date,
        Concept.concept_code
    )
    .join(
        Episode_Event,
        Episode_Event.event_polymorphic.of_type(diagnosis)
    )
    .join(
        Concept,
        Concept.concept_id==diagnosis.condition_concept_id
    )
    .subquery()
)

condition_ep_join = sa.join(Condition_Occurrence, Episode_Event, 
                             sa.and_(Condition_Occurrence.condition_occurrence_id==Modifiable_Table.modifier_id,
                                     Modifiable_Table.modifier_id==Episode_Event.event_id,
                                     Episode_Event.episode_event_field_concept_id==ModifierFields.condition_occurrence_id.value))

class Condition_Episode(Base):
    __table__ = condition_ep_join
    episode_id = Episode_Event.episode_id
    person_id = Condition_Occurrence.person_id
    condition_occurrence_id = Condition_Occurrence.condition_occurrence_id  
    condition_concept_id = Condition_Occurrence.condition_concept_id
    condition_code = Condition_Occurrence.condition_code
    modifier_concepts = Condition_Occurrence.modifier_concepts
    episode_start_datetime = Episode_Event.episode_start_datetime

    #person_object: so.Mapped['Person_Episodes'] = so.relationship(back_populates="condition_episodes", foreign_keys=[person_id])


# select all tx episodes that have at least one drug administration event 
# and find the start of chemo administration for that episode
# todo: should this filter on anti-cancer therapies?

systemic_therapy = so.aliased(Drug_Exposure, flat=True)
radiation_therapy = so.aliased(Procedure_Occurrence, flat=True)

systemic_therapy_subquery = (
    sa.select(
        Episode.person_id,
        Episode.episode_id.label('sact_episode_id'),
        Episode.episode_parent_id,
        systemic_therapy.drug_exposure_start_date
    )
    .join(
         Episode_Event,
         Episode.episode_id==Episode_Event.episode_id
     )
    .join_from(
        Episode_Event,
        Episode_Event.event_polymorphic.of_type(systemic_therapy)
    )
    .subquery()
)

radiation_therapy_subquery = (
    sa.select(
        Episode.person_id,
        Episode.episode_id.label('rad_episode_id'),
        Episode.episode_parent_id,
        radiation_therapy.procedure_datetime
    )
    .join(
         Episode_Event,
         Episode.episode_id==Episode_Event.episode_id
     )
    .join_from(
        Episode_Event,
        Episode_Event.event_polymorphic.of_type(radiation_therapy)
    )
    .subquery()
)

systemic_therapy_start = (
    sa.select(
        systemic_therapy_subquery.c.person_id,
        systemic_therapy_subquery.c.sact_episode_id,
        systemic_therapy_subquery.c.episode_parent_id,
        sa.func.min(systemic_therapy_subquery.c.drug_exposure_start_date).label('sact_start')
    )
    .group_by(        
        systemic_therapy_subquery.c.person_id,        
        systemic_therapy_subquery.c.episode_parent_id,
        systemic_therapy_subquery.c.sact_episode_id
    )
    .subquery()
)

radiation_therapy_start = (
    sa.select(
        radiation_therapy_subquery.c.person_id,
        radiation_therapy_subquery.c.rad_episode_id,
        radiation_therapy_subquery.c.episode_parent_id,
        sa.func.min(radiation_therapy_subquery.c.procedure_datetime).label('rt_start')
    )
    .group_by(        
        radiation_therapy_subquery.c.person_id,        
        radiation_therapy_subquery.c.episode_parent_id,
        radiation_therapy_subquery.c.rad_episode_id
    )
    .subquery()
)

diagnosis = so.aliased(Condition_Occurrence, flat=True)

dx_subquery = (
    sa.select(
        Episode.person_id,
        Episode.episode_id.label('dx_episode_id'),
        diagnosis.condition_start_date
    )
    .join(
         Episode_Event,
         Episode.episode_id==Episode_Event.episode_id
     )
    .join_from(
        Episode_Event,
        Episode_Event.event_polymorphic.of_type(diagnosis)
    )
    .subquery()
)

systemic_therapy_with_dx = (
    sa.join(
        dx_subquery, 
        systemic_therapy_start, 
        sa.and_(
            systemic_therapy_start.c.episode_parent_id==dx_subquery.c.dx_episode_id, 
            systemic_therapy_start.c.person_id==dx_subquery.c.person_id
        ),
        isouter=True
    )
)

rt_therapy_with_dx = (
    sa.join(
        dx_subquery, 
        radiation_therapy_start, 
        sa.and_(
            radiation_therapy_start.c.episode_parent_id==dx_subquery.c.dx_episode_id, 
            radiation_therapy_start.c.person_id==dx_subquery.c.person_id
        ),
        isouter=True
    )
)

class Systemic_Therapy_Episode(Base):
    __table__ = systemic_therapy_with_dx
    episode_id = systemic_therapy_start.c.sact_episode_id
    person_id = systemic_therapy_start.c.person_id
    sact_start = so.column_property(systemic_therapy_start.c.sact_start)
    
    dx_ep_id = dx_subquery.c.dx_episode_id

    dx_object: so.Mapped[Optional['Episode']] = so.relationship(foreign_keys=[dx_ep_id], viewonly=True)
    #person_object: so.Mapped['Person_Episodes'] = so.relationship(back_populates="sact_episodes", foreign_keys=[person_id])
    episode_object: so.Mapped['Episode'] = so.relationship(foreign_keys=[episode_id])
    sact_events: AssociationProxy[List['Episode_Event']] = association_proxy("episode_object", "events")

    @property
    def episode_agents(self):
        return list(set([s.event_polymorphic.drug_label for s in self.sact_events if s.event_polymorphic.polymorphic_label=='drug_exposure']))

class Radiation_Therapy_Episode(Base):
    __table__ = rt_therapy_with_dx
    episode_id = radiation_therapy_start.c.rad_episode_id
    person_id = radiation_therapy_start.c.person_id
    rt_start = so.column_property(radiation_therapy_start.c.rt_start)
    dx_ep_id = dx_subquery.c.dx_episode_id

    dx_object: so.Mapped[Optional['Episode']] = so.relationship(foreign_keys=[dx_ep_id], viewonly=True)
    #person_object: so.Mapped['Person_Episodes'] = so.relationship(back_populates="sact_episodes", foreign_keys=[person_id])
    episode_object: so.Mapped['Episode'] = so.relationship(foreign_keys=[episode_id])
    rt_events: AssociationProxy[List['Episode_Event']] = association_proxy("episode_object", "events")


class Person_Episodes(Person):
    #condition_episodes: so.Mapped[List['Condition_Episode']] = so.relationship(back_populates="person_object", lazy="selectin")
    sact_episodes: so.Mapped[List['Systemic_Therapy_Episode']] = so.relationship(back_populates="person_object", order_by='Systemic_Therapy_Episode.sact_start')

    @property
    def all_agents(self):
        return list(set(chain.from_iterable([se.episode_agents for se in self.sact_episodes])))


Systemic_Therapy_Episode.person_object = so.relationship(
    Person_Episodes,
    primaryjoin=so.foreign(Systemic_Therapy_Episode.person_id) == Person_Episodes.person_id
)

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


# Hemonc_Condition.condition_concept = so.relationship(
#     Condition_Map,
#     primaryjoin=Hemonc_Condition.condition_concept_id == so.foreign(Condition_Map.condition_concept_id)
# )


# surgical = so.aliased(
#     Concept, 
#     sa.select(Concept).where(
#         sa.and_(
#             Concept.concept_id==Concept_Ancestor.descendant_concept_id,
#             Concept_Ancestor.ancestor_concept_id==CancerProcedureTypes.surgical_procedure.value
#         )
#     ).subquery(), 
#     name='surgical'
# )


rth_ca = so.aliased(Concept_Ancestor, name='rth_ca')
srg_ca = so.aliased(Concept_Ancestor, name='srg_ca')


surgical = (
    sa.select(
        Concept.concept_name,
        Concept.concept_code,
        Concept.concept_id,
        srg_ca.descendant_concept_id
    )
    .join(srg_ca, Concept.concept_id == srg_ca.descendant_concept_id)
    .filter(srg_ca.ancestor_concept_id==CancerProcedureTypes.surgical_procedure.value)
    .subquery()
)

historical_procedure = so.aliased(
    Observation,
    sa.select(Observation).where(
        Observation.observation_concept_id == CancerProcedureTypes.historical_procedure.value
    ).subquery(), 
    'historical_procedure'
)

historical_surgery = (
    sa.select(
        historical_procedure.person_id,
        historical_procedure.observation_id,
        historical_procedure.value_as_concept_id,
        historical_procedure.observation_datetime
    ).join(
        surgical, surgical.c.concept_id == historical_procedure.value_as_concept_id
    ).subquery()
)

radiotherapy = (
    sa.select(
        rth_ca.descendant_concept_id.label('rt_id')
    )
    .select_from(rth_ca)
    .filter(
        sa.or_(
            rth_ca.ancestor_concept_id==CancerProcedureTypes.rt_procedure.value,
            rth_ca.ancestor_concept_id==CancerProcedureTypes.rn_procedure.value
        )
    )
    .subquery()
)

    
surg_only = (
    sa.join(
        surgical,
        radiotherapy,
        radiotherapy.c.rt_id == surgical.c.descendant_concept_id,
        isouter=True
    )
)

surgical_procedure = (
    sa.select(
        Procedure_Occurrence.person_id,
        Procedure_Occurrence.procedure_occurrence_id,
        Procedure_Occurrence.procedure_concept_id,
        Procedure_Occurrence.procedure_datetime,
        surgical.c.concept_name,
        surgical.c.concept_code
    )
    .join(surg_only, surgical.c.concept_id == Procedure_Occurrence.procedure_concept_id)
    .filter(radiotherapy.c.rt_id == None)
    .subquery()
)

class Dated_Surgical_Procedure(Base):
    __table__ = surgical_procedure
    procedure_occurrence_id = surgical_procedure.c.procedure_occurrence_id
    person_id = surgical_procedure.c.person_id
    procedure_concept_id = surgical_procedure.c.procedure_concept_id
    concept_name = surgical_procedure.c.concept_name
    concept_code = surgical_procedure.c.concept_code
    procedure_datetime = so.column_property(surgical_procedure.c.procedure_datetime)


class Historical_Surgical_Procedure(Base):
    __table__ = historical_surgery
    observation_id = historical_surgery.c.observation_id
    person_id = historical_surgery.c.person_id
    procedure_concept_id = historical_surgery.c.value_as_concept_id
    history_datettime = so.column_property(historical_surgery.c.observation_datetime)




# surgical_procedure = (
#     sa.select(
#         Procedure_Occurrence.person_id,
#         Procedure_Occurrence.procedure_occurrence_id,
#         Procedure_Occurrence.procedure_concept_id,
#         Procedure_Occurrence.procedure_datetime,
#         surgical.concept_name,
#         surgical.concept_code
#     ).join(
#         surgical, surgical.concept_id == Procedure_Occurrence.procedure_concept_id
#     ).subquery()
# )

# class Dated_Surgical_Procedure(Base):
#     __table__ = surgical_procedure
#     procedure_occurrence_id = surgical_procedure.c.procedure_occurrence_id
#     person_id = surgical_procedure.c.person_id
#     procedure_concept_id = surgical_procedure.c.procedure_concept_id
#     procedure_datetime = so.column_property(surgical_procedure.c.procedure_datetime)


# chemo_ep_with_dx = (
#     sa.select(
#         dx_episode_subquery.c.dx_episode_id,
#         dx_episode_subquery.c.person_id,
#         sa.func.min(chemo_ep_subquery.c.drug_exposure_start_date).label('chemo_start')
#     )
#     .join_from(
#         dx_episode_subquery, 
#         chemo_ep_subquery,
#         chemo_ep_subquery.c.episode_parent_id==dx_episode_subquery.c.dx_episode_id
#     )
#     .group_by(
#         dx_episode_subquery.c.dx_episode_id, 
#         dx_episode_subquery.c.person_id,
#     )
#     .subquery()
# )

# class Chemo_LoT(Base):
#     __table__ = chemo_ep_with_dx
#     episode_id = chemo_ep_with_dx.c.dx_episode_id
#     person_id = chemo_ep_with_dx.c.person_id

#     @hybrid_property
#     def chemo_start(self):
#         return chemo_ep_with_dx.c.chemo_start



person_postcode = (
    sa.select(
        Observation.person_id,
        Observation.value_as_number.label('post_code')
    )
    .filter(Observation.observation_concept_id==DemographyConcepts.postcode.value)
    .subquery()
)

person_cob = (
    sa.select(
        Observation.person_id,
        Concept.concept_name.label('country_of_birth')
    )
    .join(Concept, Concept.concept_id==Observation.value_as_concept_id)
    .filter(Observation.observation_concept_id==DemographyConcepts.cob.value)
    .subquery()
)

person_lang = (
    sa.select(
        Observation.person_id,
        Concept.concept_name.label('language_spoken')
    )
    .join(Concept, Concept.concept_id==Observation.value_as_concept_id)
    .filter(Observation.observation_concept_id==DemographyConcepts.language_spoken.value)
    .subquery()
)

demographics_join = (
    sa.select(
        Person.person_id, 
        Person.year_of_birth,
        Person.death_datetime,
        Concept.concept_name.label('gender'),
        person_lang.c.language_spoken,
        person_cob.c.country_of_birth,
        person_postcode.c.post_code
    )
    .join(Concept, Concept.concept_id==Person.gender_concept_id)
    .join(person_lang, person_lang.c.person_id==Person.person_id)
    .join(person_cob, person_cob.c.person_id==Person.person_id)
    .join(person_postcode, person_postcode.c.person_id==Person.person_id)
).subquery()

class PersonDemography(Base):
    __table__ = demographics_join
    person_id = demographics_join.c.person_id
    year_of_birth = demographics_join.c.year_of_birth
    death_datetime = demographics_join.c.death_datetime
    language_spoken = demographics_join.c.language_spoken
    country_of_birth = demographics_join.c.country_of_birth
    person_postcode = demographics_join.c.post_code


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
        
