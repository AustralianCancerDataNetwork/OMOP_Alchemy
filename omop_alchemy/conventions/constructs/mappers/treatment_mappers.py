from typing import Optional, List
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy

from ....db import Base
from ....conventions.concept_enumerators import CancerProcedureTypes, ModifierFields, TreatmentEpisode, DiseaseEpisodeConcepts, DemographyConcepts
from ....model.onco_ext import Episode, Episode_Event
from ....model.clinical import Person, Modifiable_Table, Condition_Occurrence, Drug_Exposure, Procedure_Occurrence, Observation
from ....model.vocabulary import Concept, Concept_Ancestor
from ..definitions.alias_definitions import radiation_therapy, systemic_therapy, diagnosis
from ..definitions.diagnosis_subqueries import dx_subquery
from ..definitions.episode_event_subqueries import systemic_therapy_start, radiation_therapy_start

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


# from .timeline_queries import Person_Episodes

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


Systemic_Therapy_Episode.person_object = so.relationship(
    Person_Episodes,
    primaryjoin=so.foreign(Systemic_Therapy_Episode.person_id) == Person_Episodes.person_id
)
