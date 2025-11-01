from ...vocab_lookups import tnm_lookup
from ...concept_enumerators import ModifierConcepts, ModifierFields
from ....model.clinical import Measurement, Condition_Occurrence
from ....model.onco_ext import Episode, Episode_Event
import sqlalchemy as sa
import sqlalchemy.orm as so

t_stage_query = (
    sa.select(
        Measurement
    )
    .where(
        sa.and_(
            Measurement.measurement_concept_id.in_(tnm_lookup.t_stage_concepts),
            Measurement.modifier_of_field_concept_id==ModifierFields.condition_occurrence_id.value
        )
    )
    .subquery('t_stage_subquery')
)

n_stage_query = (
    sa.select(
        Measurement
    )
    .where(
        sa.and_(
            Measurement.measurement_concept_id.in_(tnm_lookup.n_stage_concepts),
            Measurement.modifier_of_field_concept_id==ModifierFields.condition_occurrence_id.value
        )
    )
    .subquery('n_stage_subquery')
)

m_stage_query = (
    sa.select(
        Measurement
    )
    .where(
        sa.and_(
            Measurement.measurement_concept_id.in_(tnm_lookup.m_stage_concepts),
            Measurement.modifier_of_field_concept_id==ModifierFields.condition_occurrence_id.value
        )
    )
    .subquery('m_stage_subquery')
)

group_stage_query = (
    sa.select(
        Measurement
    )
    .where(
        sa.and_(
            Measurement.measurement_concept_id.in_(tnm_lookup.group_stage_concepts),
            Measurement.modifier_of_field_concept_id==ModifierFields.condition_occurrence_id.value
        )
    )
    .subquery('group_stage_subquery')
)

from omop_alchemy.conventions.concept_enumerators import ModifierConcepts

grade_query = (
    sa.select(
        Measurement
    )
    .where(
        sa.and_(
            Measurement.measurement_concept_id==ModifierConcepts.grade.value,
            Measurement.modifier_of_field_concept_id==ModifierFields.condition_occurrence_id.value
        )
    )
    .subquery('grade_subquery')
)


laterality_query = (
    sa.select(
        Measurement
    )
    .where(
        sa.and_(
            Measurement.measurement_concept_id==ModifierConcepts.laterality.value,
            Measurement.modifier_of_field_concept_id==ModifierFields.condition_occurrence_id.value
        )
    )
    .subquery('laterality_subquery')
)

path_stage_query = (
    sa.select(
        Measurement
    )
    .where(
        sa.and_(
            Measurement.measurement_concept_id.in_(tnm_lookup.path_stage_concepts),
            Measurement.modifier_of_field_concept_id==ModifierFields.condition_occurrence_id.value
        )
    )
    .subquery('path_stage_subquery')
)

T_Stage = so.with_polymorphic(Measurement, [], selectable=t_stage_query)
N_Stage = so.with_polymorphic(Measurement, [], selectable=n_stage_query)
M_Stage = so.with_polymorphic(Measurement, [], selectable=m_stage_query)
Group_Stage = so.with_polymorphic(Measurement, [], selectable=group_stage_query)
Grade = so.with_polymorphic(Measurement, [], selectable=grade_query)
Laterality = so.with_polymorphic(Measurement, [], selectable=laterality_query)
Path_Stage = so.with_polymorphic(Measurement, [], selectable=path_stage_query)

cancer_dx_join = (
    sa.select(
        Condition_Occurrence.person_id.label('person_id'), 
        Condition_Occurrence.condition_occurrence_id.label('cancer_diagnosis_id'), 
        Condition_Occurrence.condition_start_date.label('cancer_start_date'),
        T_Stage.measurement_concept_id.label('t_stage_value'),
        T_Stage.measurement_date.label('t_stage_date'),
        N_Stage.measurement_concept_id.label('n_stage_value'),
        N_Stage.measurement_date.label('n_stage_date'),
        M_Stage.measurement_concept_id.label('m_stage_value'),
        M_Stage.measurement_date.label('m_stage_date'),
        Group_Stage.measurement_concept_id.label('group_stage_value'),
        Group_Stage.measurement_date.label('group_stage_date'),
        Grade.value_as_concept_id.label('grade_value'),
        Grade.measurement_date.label('grade_date'),
        Laterality.value_as_concept_id.label('laterality_value'),
        Laterality.measurement_date.label('laterality_date')
    )
    .join(T_Stage, T_Stage.modifier_of_event_id==Condition_Occurrence.condition_occurrence_id, isouter=True)
    .join(N_Stage, N_Stage.modifier_of_event_id==Condition_Occurrence.condition_occurrence_id, isouter=True)
    .join(M_Stage, M_Stage.modifier_of_event_id==Condition_Occurrence.condition_occurrence_id, isouter=True)
    .join(Group_Stage, Group_Stage.modifier_of_event_id==Condition_Occurrence.condition_occurrence_id, isouter=True)
    .join(Grade, Grade.modifier_of_event_id==Condition_Occurrence.condition_occurrence_id, isouter=True)
    .join(Laterality, Laterality.modifier_of_event_id==Condition_Occurrence.condition_occurrence_id, isouter=True)
    .subquery()
)

path_stage_join = (
    sa.select(
        Condition_Occurrence.person_id.label('person_id'), 
        Condition_Occurrence.condition_occurrence_id.label('cancer_diagnosis_id'), 
        Condition_Occurrence.condition_start_date.label('cancer_start_date'),
        sa.func.min(Path_Stage.measurement_date).label('path_stage_date')
    )
    .join(Path_Stage, Path_Stage.modifier_of_event_id==Condition_Occurrence.condition_occurrence_id, isouter=True)
    .group_by(        
        Condition_Occurrence.person_id,        
        Condition_Occurrence.condition_occurrence_id,
        Condition_Occurrence.condition_start_date
    )
    .subquery()
)


all_stage_query = (
    sa.select(
        Measurement
    )
    .where(
        sa.and_(
            Measurement.measurement_concept_id.in_(tnm_lookup.all_concepts),
            Measurement.modifier_of_field_concept_id==ModifierFields.condition_occurrence_id.value
        )
    )
    .subquery('stage_subquery')
)

All_Stage = so.with_polymorphic(Measurement, [], selectable=all_stage_query)

conditions = so.aliased(Condition_Occurrence, name='conditions', flat=True)

episode_stage_join = (
     sa.select(
        Episode.episode_id,
        Episode.episode_start_datetime, 
        conditions.person_id, 
        All_Stage.measurement_concept_id, 
        All_Stage.measurement_date
    )
    .join(Episode_Event, Episode_Event.episode_id==Episode.episode_id)
    .join(
        conditions, 
        sa.and_(
            conditions.condition_occurrence_id==Episode_Event.event_id,
            Episode_Event.episode_event_field_concept_id==ModifierFields.condition_occurrence_id.value
        )
    )
    .join(All_Stage, All_Stage.modifier_of_event_id==conditions.condition_occurrence_id) 
    .subquery('all_stage_subquery')
)