import sqlalchemy as sa
import sqlalchemy.orm as so

from ....model.clinical import Drug_Exposure, Procedure_Occurrence, Observation
from ....model.vocabulary import Concept, Concept_Ancestor
from ....model.onco_ext import Episode, Episode_Event

from .alias_definitions import diagnosis

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

