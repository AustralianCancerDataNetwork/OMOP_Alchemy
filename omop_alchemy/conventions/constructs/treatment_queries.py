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



# select all tx episodes that have at least one drug administration event 
# and find the start of chemo administration for that episode
# todo: should this filter on anti-cancer therapies?
# no - this does not need to filter on anti-cancer therapies only, because we only
# pull in drugs that have been attached to an episode

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


from .timeline_queries import Person_Episodes


Systemic_Therapy_Episode.person_object = so.relationship(
    Person_Episodes,
    primaryjoin=so.foreign(Systemic_Therapy_Episode.person_id) == Person_Episodes.person_id
)
