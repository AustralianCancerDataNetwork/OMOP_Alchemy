import sqlalchemy as sa
import sqlalchemy.orm as so

from omop_alchemy.model.vocabulary import Concept, Concept_Ancestor
from omop_alchemy.model.clinical import Condition_Occurrence, Person, Observation, Procedure_Occurrence, Measurement, Modifiable_Table, Drug_Exposure, Procedure_Occurrence
from ....model.onco_ext import Episode, Episode_Event
from .MaterializedViewMixin import MaterializedViewMixin
from ...concept_enumerators import ModifierFields, DiseaseEpisodeConcepts
from ....db import Base

condition_event = so.aliased(Episode_Event, name='condition_event')
overarching_episode = so.aliased(Episode, name='overarching_episode')
condition_concept = so.aliased(Concept, name='condition_concept')

progression_occurrence = so.aliased(Condition_Occurrence, name='progression_occurrence')
progression_event = so.aliased(Episode_Event, name='progression_event')
progression_concept = so.aliased(Concept, name='progression_concept')
progression_episode = so.aliased(Episode, name='progression_episode')

treatment_episode = so.aliased(Episode, name='treatment_episode')
treatment_event = so.aliased(Episode_Event, name='treatment_event')


overarching_join = (
    sa.select(
        sa.func.row_number().over().label('mv_id'), 
        Condition_Occurrence.person_id,
        Condition_Occurrence.condition_start_date, 
        Condition_Occurrence.condition_occurrence_id,
        Condition_Occurrence.condition_source_value,
        Condition_Occurrence.condition_concept_id,
        condition_concept.concept_name.label('condition_concept'),
        overarching_episode.episode_id.label('overarching_episode_id'),
        progression_occurrence.condition_start_date.label('progression_start_date'), 
        progression_occurrence.condition_occurrence_id.label('progression_occurrence_id'),
        progression_occurrence.condition_source_value.label('progression_source_value'),
        progression_occurrence.condition_concept_id.label('progression_concept_id'),
        progression_concept.concept_name.label('progression_concept'),
        progression_episode.episode_id.label('progression_episode_id')
    )
    .join(
        condition_event, 
        sa.and_(
            condition_event.event_id==Condition_Occurrence.condition_occurrence_id,
            condition_event.episode_event_field_concept_id==ModifierFields.condition_occurrence_id.value
        )
    )
    .join(
        overarching_episode, 
        sa.and_(
            overarching_episode.episode_id==condition_event.episode_id,
            overarching_episode.episode_concept_id==DiseaseEpisodeConcepts.episode_of_care.value
        )
    )
    .join(condition_concept, condition_concept.concept_id==Condition_Occurrence.condition_concept_id)
    .join(
        progression_episode, 
        sa.and_(
            progression_episode.episode_parent_id==overarching_episode.episode_id,
            progression_episode.episode_concept_id==DiseaseEpisodeConcepts.disease_progression.value
        )
        , isouter=True
    )
    .join(
        progression_event, 
        sa.and_(
            progression_event.episode_id==progression_episode.episode_id,
            progression_event.episode_event_field_concept_id==ModifierFields.condition_occurrence_id.value
        ),
        isouter=True
    )
    .join(
        progression_occurrence, 
        sa.and_(
            progression_event.event_id==progression_occurrence.condition_occurrence_id,
            progression_event.episode_event_field_concept_id==ModifierFields.condition_occurrence_id.value
        ),
        isouter=True
    )
    .join(progression_concept, progression_concept.concept_id==progression_occurrence.condition_concept_id, isouter=True)
)


class OverarchingCondition(MaterializedViewMixin, Base):
    __mv_name__ = 'overarching_condition_mv'
    __mv_select__ = overarching_join.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column()
    condition_occurrence_id = sa.Column()
    condition_start_date = sa.Column()
    condition_source_value = sa.Column()
    condition_concept_id = sa.Column()
    condition_concept = sa.Column()
    overarching_episode_id = sa.Column()
    progression_start_date = sa.Column()
    progression_occurrence_id = sa.Column()
    progression_source_value = sa.Column()
    progression_concept_id = sa.Column()
    progression_concept = sa.Column()
    progression_episode_id = sa.Column()