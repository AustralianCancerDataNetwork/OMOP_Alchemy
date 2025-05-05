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