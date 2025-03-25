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
from ...conventions.concept_enumerators import ModifierFields


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

class Systemic_Therapy_Episode(Base):
    __table__ = systemic_therapy_with_dx
    episode_id = systemic_therapy_start.c.sact_episode_id
    person_id = systemic_therapy_start.c.person_id
    sact_start = so.column_property(systemic_therapy_start.c.sact_start)
    
    dx_ep_id = dx_subquery.c.dx_episode_id

    dx_object: so.Mapped[Optional['Episode']] = so.relationship(foreign_keys=[dx_ep_id])
    person_object: so.Mapped['Person_Episodes'] = so.relationship(back_populates="sact_episodes", foreign_keys=[person_id])
    episode_object: so.Mapped['Episode'] = so.relationship(foreign_keys=[episode_id])
    sact_events: AssociationProxy[List['Episode_Event']] = association_proxy("episode_object", "events")

    @property
    def episode_agents(self):
        return list(set([s.event_polymorphic.drug_label for s in self.sact_events if s.event_polymorphic.polymorphic_label=='drug_exposure']))


class Person_Episodes(Person):
    #condition_episodes: so.Mapped[List['Condition_Episode']] = so.relationship(back_populates="person_object", lazy="selectin")
    sact_episodes: so.Mapped[List['Systemic_Therapy_Episode']] = so.relationship(back_populates="person_object", order_by='Systemic_Therapy_Episode.sact_start')

    @property
    def all_agents(self):
        return list(set(chain.from_iterable([se.episode_agents for se in self.sact_episodes])))

surgical = so.aliased(
    Concept, 
    sa.select(Concept).where(
        sa.and_(
            Concept.concept_id==Concept_Ancestor.descendant_concept_id,
            Concept_Ancestor.ancestor_concept_id==CancerProcedureTypes.surgical_procedure.value
        )
    ).subquery(), 
    name='surgical'
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
        surgical, surgical.concept_id == historical_procedure.value_as_concept_id
    ).subquery()
)

class Historical_Surgical_Procedure(Base):
    __table__ = historical_surgery
    observation_id = historical_surgery.c.observation_id
    person_id = historical_surgery.c.person_id
    procedure_concept_id = historical_surgery.c.value_as_concept_id
    history_datettime = so.column_property(historical_surgery.c.observation_datetime)


surgical_procedure = (
    sa.select(
        Procedure_Occurrence.person_id,
        Procedure_Occurrence.procedure_occurrence_id,
        Procedure_Occurrence.procedure_concept_id,
        Procedure_Occurrence.procedure_datetime,
        surgical.concept_name,
        surgical.concept_code
    ).join(
        surgical, surgical.concept_id == Procedure_Occurrence.procedure_concept_id
    ).subquery()
)

class Dated_Surgical_Procedure(Base):
    __table__ = surgical_procedure
    procedure_occurrence_id = surgical_procedure.c.procedure_occurrence_id
    person_id = surgical_procedure.c.person_id
    procedure_concept_id = surgical_procedure.c.procedure_concept_id
    procedure_datetime = so.column_property(surgical_procedure.c.procedure_datetime)


rth_ca = so.aliased(Concept_Ancestor, name='rth_ca')
srg_ca = so.aliased(Concept_Ancestor, name='srg_ca')

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



