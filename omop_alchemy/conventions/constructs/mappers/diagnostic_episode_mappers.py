import sqlalchemy as sa
import sqlalchemy.orm as so

from ....db import Base
from ....model.onco_ext import Episode, Episode_Event
from ....model.clinical import Person, Modifiable_Table, Condition_Occurrence, Drug_Exposure, Procedure_Occurrence, Observation
from ....model.vocabulary import Concept, Concept_Ancestor
from ...concept_enumerators import ModifierFields

# select all conditions that have been associated with an episode - TBC if we need to add filtering for overarching?

condition_ep_join = sa.join(Condition_Occurrence, Episode_Event, 
                             sa.and_(Condition_Occurrence.condition_occurrence_id==Modifiable_Table.modifier_id,
                                     Modifiable_Table.modifier_id==Episode_Event.event_id,
                                     Episode_Event.episode_event_field_concept_id==ModifierFields.condition_occurrence_id.value))

class Condition_Episode(Base):
    """
    This is useful for pulling all associated staging, grading etc. modifiers

    TODO: write a more detailed mapper that explicitly handles all modifiers distinctly
    """
    __table__ = condition_ep_join
    episode_id = Episode_Event.episode_id
    person_id = Condition_Occurrence.person_id
    condition_occurrence_id = Condition_Occurrence.condition_occurrence_id  
    condition_concept_id = Condition_Occurrence.condition_concept_id
    condition_code = Condition_Occurrence.condition_code
    modifier_concepts = Condition_Occurrence.modifier_concepts
    episode_start_datetime = Episode_Event.episode_start_datetime