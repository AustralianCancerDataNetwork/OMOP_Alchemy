from omop_alchemy.conventions.vocab_lookups import tnm_lookup
import sqlalchemy as sa
import sqlalchemy.orm as so

from omop_alchemy.model.vocabulary import Concept, Concept_Ancestor
from omop_alchemy.model.clinical import Condition_Occurrence, Person, Observation, Procedure_Occurrence, Measurement, Modifiable_Table, Drug_Exposure, Procedure_Occurrence
from .MaterializedViewMixin import MaterializedViewMixin
from ...concept_enumerators import ModifierFields, ModifierConcepts
from ...vocab_lookups import CustomLookups, grading_lookup, tnm_lookup
from ....db import Base
from ....model.onco_ext import Episode_Event

modifier_concept = so.aliased(Concept, name='modifier_concept')
condition_concept = so.aliased(Concept, name='condition_concept')

stage_select = (
    sa.select(
        Measurement.person_id,
        Measurement.measurement_id.label('stage_id'),
        Measurement.measurement_datetime.label('stage_datetime'),
        Measurement.measurement_date.label('stage_date'),
        Measurement.measurement_concept_id.label('stage_concept_id'),
        Measurement.modifier_of_event_id,
        Measurement.modifier_of_field_concept_id,
        modifier_concept.concept_name.label('stage_label')
    )
    .join(modifier_concept, modifier_concept.concept_id==Measurement.measurement_concept_id, isouter=True)
    .filter(Measurement.measurement_concept_id.in_(tnm_lookup.all_concepts))
)

class StageModifier(MaterializedViewMixin, Base):
    __mv_name__ = 'stage_modifier_mv'
    __mv_select__ = stage_select.select()
    __mv_pk__ = ["stage_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    person_id = sa.Column(sa.Integer)
    stage_id = sa.Column(primary_key=True)
    stage_date = sa.Column(sa.Date)
    stage_datetime = sa.Column(sa.DateTime)
    stage_concept_id = sa.Column(sa.Integer)
    stage_label = sa.Column(sa.String)
    modifier_of_event_id = sa.Column(sa.Integer)
    modifier_of_field_concept_id = sa.Column(sa.Integer)


grade_select = (
    sa.select(
        Measurement.person_id,
        Measurement.measurement_id.label('grade_id'),
        Measurement.measurement_date.label('grade_date'),
        Measurement.measurement_datetime.label('grade_datetime'),
        Measurement.value_as_concept_id.label('grade_concept_id'),
        modifier_concept.concept_name.label('grade_label'),
        Measurement.modifier_of_event_id,
        Measurement.modifier_of_field_concept_id
    )
    .join(modifier_concept, modifier_concept.concept_id==Measurement.value_as_concept_id, isouter=True)
    .filter(Measurement.measurement_concept_id==ModifierConcepts.grade.value)
)

class GradeModifier(MaterializedViewMixin, Base):
    __mv_name__ = 'grade_modifier_mv'
    __mv_select__ = grade_select.select()
    __mv_pk__ = ["grade_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    person_id = sa.Column(sa.Integer)
    grade_id = sa.Column(primary_key=True)
    grade_date = sa.Column(sa.Date)
    grade_datetime = sa.Column(sa.DateTime)
    grade_concept_id = sa.Column(sa.Integer)
    grade_label = sa.Column(sa.String)
    modifier_of_event_id = sa.Column(sa.Integer)
    modifier_of_field_concept_id = sa.Column(sa.Integer)

size_select = (
    sa.select(
        Measurement.person_id,
        Measurement.measurement_id.label('size_id'),
        Measurement.measurement_date.label('size_date'),
        Measurement.measurement_datetime.label('size_datetime'),
        Measurement.value_as_concept_id.label('size_concept_id'),
        Measurement.modifier_of_event_id,
        Measurement.modifier_of_field_concept_id,
        modifier_concept.concept_name.label('size_label')
    )
    .join(modifier_concept, modifier_concept.concept_id==Measurement.value_as_concept_id, isouter=True)
    .filter(Measurement.measurement_concept_id==ModifierConcepts.tumor_size.value)
)

class SizeModifier(MaterializedViewMixin, Base):
    __mv_name__ = 'size_modifier_mv'
    __mv_select__ = size_select.select()
    __mv_pk__ = ["size_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    person_id = sa.Column(sa.Integer)
    size_id = sa.Column(primary_key=True)
    size_date = sa.Column(sa.Date)
    size_datetime = sa.Column(sa.DateTime)
    size_concept_id = sa.Column(sa.Integer)
    size_label = sa.Column(sa.String)
    modifier_of_event_id = sa.Column(sa.Integer)
    modifier_of_field_concept_id = sa.Column(sa.Integer)

laterality_select = (
    sa.select(
        Measurement.person_id,
        Measurement.measurement_id.label('laterality_id'),
        Measurement.measurement_date.label('laterality_date'),
        Measurement.measurement_datetime.label('laterality_datetime'),
        Measurement.value_as_concept_id.label('laterality_concept_id'),
        Measurement.modifier_of_event_id,
        Measurement.modifier_of_field_concept_id,
        modifier_concept.concept_name.label('laterality_label')
    )
    .join(modifier_concept, modifier_concept.concept_id==Measurement.value_as_concept_id, isouter=True)
    .filter(Measurement.measurement_concept_id==ModifierConcepts.laterality.value)
)

class LatModifier(MaterializedViewMixin, Base):
    __mv_name__ = 'lat_modifier_mv'
    __mv_select__ = laterality_select.select()
    __mv_pk__ = ["laterality_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    person_id = sa.Column(sa.Integer)
    laterality_id = sa.Column(primary_key=True)
    laterality_date = sa.Column(sa.Date)
    laterality_datetime = sa.Column(sa.DateTime)
    laterality_concept_id = sa.Column(sa.Integer)
    laterality_label = sa.Column(sa.String)
    modifier_of_event_id = sa.Column(sa.Integer)
    modifier_of_field_concept_id = sa.Column(sa.Integer)



modified_conditions_join = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        Condition_Occurrence.person_id,
        Condition_Occurrence.condition_start_date, 
        Condition_Occurrence.condition_occurrence_id,
        Condition_Occurrence.condition_source_value,
        Condition_Occurrence.condition_concept_id,
        condition_concept.concept_name.label('condition_concept'),
        Episode_Event.episode_id.label('condition_episode'),
    	StageModifier.stage_id,
    	StageModifier.stage_date,
    	StageModifier.stage_concept_id,
    	StageModifier.stage_label,
        GradeModifier.grade_id,
    	GradeModifier.grade_date,
    	GradeModifier.grade_concept_id,
    	GradeModifier.grade_label,
    	SizeModifier.size_id,
    	SizeModifier.size_date,
    	SizeModifier.size_concept_id,
    	SizeModifier.size_label,
    	LatModifier.laterality_id,
    	LatModifier.laterality_date,
    	LatModifier.laterality_concept_id,
    	LatModifier.laterality_label,
    )
    .join(
        Episode_Event, 
        sa.and_(
            Episode_Event.event_id==Condition_Occurrence.condition_occurrence_id,
            Episode_Event.episode_event_field_concept_id==ModifierFields.condition_occurrence_id.value
        ),
        isouter=True
    )
    .join(
        StageModifier, 
        sa.and_(
            StageModifier.modifier_of_field_concept_id==ModifierFields.condition_occurrence_id.value,
            Condition_Occurrence.condition_occurrence_id==StageModifier.modifier_of_event_id
        ),
        isouter=True
    )
    .join(
        GradeModifier, 
        sa.and_(
            GradeModifier.modifier_of_field_concept_id==ModifierFields.condition_occurrence_id.value,
            Condition_Occurrence.condition_occurrence_id==GradeModifier.modifier_of_event_id
        ),
        isouter=True
    )
    .join(
        SizeModifier, 
        sa.and_(
            SizeModifier.modifier_of_field_concept_id==ModifierFields.condition_occurrence_id.value,
            Condition_Occurrence.condition_occurrence_id==SizeModifier.modifier_of_event_id
        ),
        isouter=True
    )
    .join(
        LatModifier, 
        sa.and_(
            LatModifier.modifier_of_field_concept_id==ModifierFields.condition_occurrence_id.value,
            Condition_Occurrence.condition_occurrence_id==LatModifier.modifier_of_event_id
        ),
        isouter=True
    )
    .join(condition_concept, condition_concept.concept_id==Condition_Occurrence.condition_concept_id)
)


class ModifiedCondition(MaterializedViewMixin, Base):
    __mv_name__ = 'modified_conditions_mv'
    __mv_select__ = modified_conditions_join.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    condition_start_date = sa.Column(sa.Date)
    condition_occurrence_id = sa.Column(sa.Integer)
    condition_source_value = sa.Column(sa.String)
    condition_concept_id = sa.Column(sa.Integer)
    condition_concept = sa.Column(sa.String)
    condition_episode = sa.Column(sa.Integer)
    stage_id = sa.Column(sa.Integer)
    stage_date = sa.Column(sa.Date)
    stage_concept_id = sa.Column(sa.Integer)
    stage_label = sa.Column(sa.String)
    grade_id = sa.Column(sa.Integer)
    grade_date = sa.Column(sa.Date)
    grade_concept_id = sa.Column(sa.Integer)
    grade_label = sa.Column(sa.String)
    size_id = sa.Column(sa.Integer)
    size_date = sa.Column(sa.Date)
    size_concept_id = sa.Column(sa.Integer)
    size_label = sa.Column(sa.String)
    laterality_id = sa.Column(sa.Integer)
    laterality_date = sa.Column(sa.Date)
    laterality_concept_id = sa.Column(sa.Integer)
    laterality_label = sa.Column(sa.String)