from datetime import date, datetime, time
from typing import Optional, List
from decimal import Decimal
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy

from ...db import Base

from .episode_event import Episode_Event
from ..clinical.modifiable_table import Modifiable_Table
from ..clinical.condition_occurrence import Condition_Occurrence
from ...conventions.concept_enumerators import ModifierFields

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
