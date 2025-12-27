from omop_alchemy.conventions.vocab_lookups import tnm_lookup
import sqlalchemy as sa
import sqlalchemy.orm as so

from omop_alchemy.model.vocabulary import Concept, Concept_Ancestor
from omop_alchemy.model.clinical import Condition_Occurrence, Person, Observation, Procedure_Occurrence, Measurement, Modifiable_Table, Drug_Exposure, Procedure_Occurrence
from .MaterializedViewMixin import MaterializedViewMixin
from ...concept_enumerators import ModifierFields, ModifierConcepts
from ...vocab_lookups import CustomLookups, grading_lookup, tnm_lookup, mets_lookup
from ....db import Base
from ....model.onco_ext import Episode_Event

modifier_concept = so.aliased(Concept, name='modifier_concept')
condition_concept = so.aliased(Concept, name='condition_concept')

stage_select = (
    sa.select(
        Measurement.person_id,
        Measurement.measurement_id.label('stage_id'),
        Measurement.measurement_date.label('stage_date'),
        Measurement.measurement_concept_id.label('stage_concept_id'),
        Measurement.modifier_of_event_id,
        Measurement.modifier_of_field_concept_id,
        modifier_concept.concept_name.label('stage_label')
    )
    .join(modifier_concept, modifier_concept.concept_id==Measurement.measurement_concept_id, isouter=True)
)

path_stage_select = stage_select.filter(Measurement.measurement_concept_id.in_(tnm_lookup.path_stage_concepts))
clin_stage_select = stage_select.filter(Measurement.measurement_concept_id.in_(tnm_lookup.clinical_stage_concepts))


def get_stage_subtype(stage_type_select, path_or_clin_select, label):
    q = sa.intersect(stage_type_select, path_or_clin_select).subquery()
    return sa.select(*q.c, sa.sql.expression.literal(label).label('stage_type'))

def get_overall_for_stage_type(subset):
    stage_type_select = stage_select.filter(Measurement.measurement_concept_id.in_(subset))
    p = get_stage_subtype(stage_type_select, path_stage_select, 'aaa_path')
    c = get_stage_subtype(stage_type_select, clin_stage_select, 'zzz_clin')
    i = sa.union(p, c).subquery()
    ranked = (
        sa.select(
            *i.c,
            sa.func.row_number()
            .over(
                partition_by=i.c.modifier_of_event_id,
                order_by=[i.c.stage_type, i.c.stage_date.asc()]
            )
            .label('rn')
        ).subquery()
    )
    return sa.select(*ranked.c).where(ranked.c.rn==1)

t_stage_select = get_overall_for_stage_type(tnm_lookup.t_stage_concepts)
n_stage_select = get_overall_for_stage_type(tnm_lookup.n_stage_concepts)
m_stage_select = get_overall_for_stage_type(tnm_lookup.m_stage_concepts)
group_stage_select = get_overall_for_stage_type(tnm_lookup.group_stage_concepts)

class StageColumns:
    # T, N, M and Group classes preference earliest pathological  
    # stage if it exists else fall back to earliest clinical
    __mv_pk__ = ["stage_id"]
    __table_args__ = {"extend_existing": True}
    person_id = sa.Column(sa.Integer)
    stage_id = sa.Column(primary_key=True)
    stage_date = sa.Column(sa.Date)
    stage_concept_id = sa.Column(sa.Integer)
    stage_label = sa.Column(sa.String)
    modifier_of_event_id = sa.Column(sa.Integer)
    modifier_of_field_concept_id = sa.Column(sa.Integer)

class TStage(MaterializedViewMixin, StageColumns, Base):
    __mv_name__ = 't_stage_mv'
    __mv_select__ = t_stage_select.select()
    __tablename__ = __mv_name__

class NStage(MaterializedViewMixin, StageColumns, Base):
    __mv_name__ = 'n_stage_mv'
    __mv_select__ = n_stage_select.select()
    __tablename__ = __mv_name__
    
class MStage(MaterializedViewMixin, StageColumns, Base):
    __mv_name__ = 'm_stage_mv'
    __mv_select__ = m_stage_select.select()
    __tablename__ = __mv_name__

class GroupStage(MaterializedViewMixin, StageColumns, Base):
    __mv_name__ = 'group_stage_mv'
    __mv_select__ = group_stage_select.select()
    __tablename__ = __mv_name__

all_stage_select = stage_select.filter(Measurement.measurement_concept_id.in_(tnm_lookup.all_concepts)).subquery()

condition_stage_select = (
    sa.select(
        Condition_Occurrence.condition_start_date, 
        Condition_Occurrence.condition_occurrence_id,
        Condition_Occurrence.condition_source_value,
        Condition_Occurrence.condition_concept_id,
        condition_concept.concept_name.label('condition_concept'),
        Episode_Event.episode_id.label('condition_episode'),
        *all_stage_select.c
    )
    .join(all_stage_select, all_stage_select.c.modifier_of_event_id==Condition_Occurrence.condition_occurrence_id)#, isouter=True)
    .join(
        Episode_Event, 
        sa.and_(
            Condition_Occurrence.condition_occurrence_id==Episode_Event.event_id,
            Episode_Event.episode_event_field_concept_id==ModifierFields.condition_occurrence_id.value
            )
    )
    .join(condition_concept, condition_concept.concept_id==Condition_Occurrence.condition_concept_id)
)
        
class StageModifier(MaterializedViewMixin, StageColumns, Base):
    # this class contains all stage modifier records
    # no matter the sub-type, nor does it preference 
    # clin / path stage etc - just a dump of all, along
    # with the identifier to link to condition record
    # where present
    __mv_name__ = 'stage_modifier_mv'
    __mv_select__ = condition_stage_select.select()
    __tablename__ = __mv_name__
    condition_start_date = sa.Column(sa.Date)
    condition_occurrence_id = sa.Column(sa.Integer)
    condition_source_value = sa.Column(sa.String)
    condition_concept_id = sa.Column(sa.Integer)
    condition_concept = sa.Column(sa.String)
    condition_episode = sa.Column(sa.Integer)


def get_eav_modifier_query(modifier_concept_id, target_cols=[Measurement.value_as_concept_id], join_col=Measurement.value_as_concept_id):
    return (
        sa.select(
            Measurement.person_id,
            Measurement.modifier_of_event_id,
            Measurement.modifier_of_field_concept_id,
            Measurement.measurement_id, 
            Measurement.measurement_date, 
            modifier_concept.concept_name,
            modifier_concept.concept_id,
            *target_cols
        )
        .join(modifier_concept, modifier_concept.concept_id==join_col, isouter=True)
        .filter(Measurement.measurement_concept_id==modifier_concept_id)
        .subquery()
    )

def earliest_eav_modifier(starting_query):
    ranked = (
        sa.select(
            *starting_query.c,
            sa.func.row_number()
            .over(
                partition_by=starting_query.c.modifier_of_event_id,
                order_by=starting_query.c.measurement_date.asc()
            )
            .label('rn')
        ).subquery()
    )
    return sa.select(*ranked.c).where(ranked.c.rn==1)

laterality_select = earliest_eav_modifier(
    get_eav_modifier_query(ModifierConcepts.laterality.value)
)

size_select = earliest_eav_modifier(
    get_eav_modifier_query(
        ModifierConcepts.tumor_size.value, 
        [Measurement.value_as_number, Measurement.unit_concept_id], 
        Measurement.unit_concept_id
    )
)

grade_select = earliest_eav_modifier(
    get_eav_modifier_query(ModifierConcepts.grade.value)
)

class MeasModCols:
    __mv_pk__ = ["measurement_id"]
    __table_args__ = {"extend_existing": True}
    
    person_id = sa.Column(sa.Integer)
    measurement_id = sa.Column(primary_key=True)
    measurement_date = sa.Column(sa.Date)
    concept_name = sa.Column(sa.String)
    concept_id = sa.Column(sa.Integer)
    modifier_of_event_id = sa.Column(sa.Integer)
    modifier_of_field_concept_id = sa.Column(sa.Integer)
    
class SizeModifier(MaterializedViewMixin, MeasModCols, Base):
    __mv_name__ = 'size_modifier_mv'
    __mv_select__ = size_select.select()
    __tablename__ = __mv_name__
    value_as_number = sa.Column(sa.Integer)
    unit_concept_id = sa.Column(sa.Integer)
    
class GradeModifier(MaterializedViewMixin, MeasModCols, Base):
    __mv_name__ = 'grade_modifier_mv'
    __mv_select__ = grade_select.select()
    __tablename__ = __mv_name__
    value_as_concept_id = sa.Column(sa.Integer)

class LatModifier(MaterializedViewMixin, MeasModCols, Base):
    __mv_name__ = 'lat_modifier_mv'
    __mv_select__ = laterality_select.select()
    __tablename__ = __mv_name__
    value_as_concept_id = sa.Column(sa.Integer)

mets_select = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        Condition_Occurrence.person_id, 
        Condition_Occurrence.condition_occurrence_id,
        Condition_Occurrence.condition_start_date, 
        Condition_Occurrence.condition_source_value,
        Condition_Occurrence.condition_concept_id,
        condition_concept.concept_name.label('condition_concept'),
        Episode_Event.episode_id.label('condition_episode'),
        Measurement.measurement_id.label('mets_id'),
        Measurement.measurement_date.label('mets_date'),
        Measurement.measurement_concept_id.label('mets_concept_id'),
        Measurement.modifier_of_event_id,
        Measurement.modifier_of_field_concept_id,
        modifier_concept.concept_name.label('mets_label')
    )
    .join(
        Measurement, 
        sa.and_(
            Measurement.modifier_of_event_id==Condition_Occurrence.condition_occurrence_id,
            Measurement.modifier_of_field_concept_id==ModifierFields.condition_occurrence_id.value
            )#, isouter=True)
    )
    .join(
        Episode_Event, 
        sa.and_(
            Condition_Occurrence.condition_occurrence_id==Episode_Event.event_id,
            Episode_Event.episode_event_field_concept_id==ModifierFields.condition_occurrence_id.value
            )
    )
    .join(modifier_concept, modifier_concept.concept_id==Measurement.measurement_concept_id, isouter=True)
    .join(condition_concept, condition_concept.concept_id==Condition_Occurrence.condition_concept_id)
    .filter(Measurement.measurement_concept_id.in_(mets_lookup.all_concepts))
)

class MetsModifier(MaterializedViewMixin, Base):
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __mv_name__ = 'mets_modifier_mv'
    __mv_select__ = mets_select.select()
    __tablename__ = __mv_name__
    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    condition_occurrence_id = sa.Column(sa.Integer)
    condition_start_date = sa.Column(sa.Date)
    condition_source_value = sa.Column(sa.String)
    condition_concept_id = sa.Column(sa.Integer)
    condition_concept = sa.Column(sa.String)
    condition_episode = sa.Column(sa.Integer)
    mets_id = sa.Column(sa.Integer)
    mets_date = sa.Column(sa.Date)
    mets_concept_id = sa.Column(sa.Integer)
    mets_label = sa.Column(sa.String)


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
    	TStage.stage_id.label('t_stage_id'),
    	TStage.stage_date.label('t_stage_date'),
    	TStage.stage_concept_id.label('t_stage_concept_id'),
    	TStage.stage_label.label('t_stage_label'),
    	NStage.stage_id.label('n_stage_id'),
    	NStage.stage_date.label('n_stage_date'),
    	NStage.stage_concept_id.label('n_stage_concept_id'),
    	NStage.stage_label.label('n_stage_label'),
    	MStage.stage_id.label('m_stage_id'),
    	MStage.stage_date.label('m_stage_date'),
    	MStage.stage_concept_id.label('m_stage_concept_id'),
    	MStage.stage_label.label('m_stage_label'),
    	GroupStage.stage_id.label('group_stage_id'),
    	GroupStage.stage_date.label('group_stage_date'),
    	GroupStage.stage_concept_id.label('group_stage_concept_id'),
    	GroupStage.stage_label.label('group_stage_label'),
        GradeModifier.measurement_id.label('grade_id'),
    	GradeModifier.measurement_date.label('grade_date'),
    	GradeModifier.concept_name.label('grade_label'),
    	GradeModifier.concept_id.label('grade_concept_id'),
    	SizeModifier.measurement_id.label('size_id'),
    	SizeModifier.measurement_date.label('size_date'),
    	SizeModifier.value_as_number.label('size_value'),
    	SizeModifier.concept_name.label('size_label'),
    	SizeModifier.concept_id.label('size_concept_id'),
    	LatModifier.measurement_id.label('laterality_id'),
    	LatModifier.measurement_date.label('laterality_date'),
    	LatModifier.concept_name.label('laterality_label'),
    	LatModifier.concept_id.label('laterality_concept_id'),
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
        TStage, 
        sa.and_(
            TStage.modifier_of_field_concept_id==ModifierFields.condition_occurrence_id.value,
            Condition_Occurrence.condition_occurrence_id==TStage.modifier_of_event_id
        ),
        isouter=True
    )
    .join(
        NStage, 
        sa.and_(
            NStage.modifier_of_field_concept_id==ModifierFields.condition_occurrence_id.value,
            Condition_Occurrence.condition_occurrence_id==NStage.modifier_of_event_id
        ),
        isouter=True
    )
    .join(
        MStage, 
        sa.and_(
            MStage.modifier_of_field_concept_id==ModifierFields.condition_occurrence_id.value,
            Condition_Occurrence.condition_occurrence_id==MStage.modifier_of_event_id
        ),
        isouter=True
    )
    .join(
        GroupStage, 
        sa.and_(
            GroupStage.modifier_of_field_concept_id==ModifierFields.condition_occurrence_id.value,
            Condition_Occurrence.condition_occurrence_id==GroupStage.modifier_of_event_id
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
    t_stage_id = sa.Column(sa.Integer)
    t_stage_date = sa.Column(sa.Date)
    t_stage_concept_id = sa.Column(sa.Integer)
    t_stage_label = sa.Column(sa.String)
    n_stage_id = sa.Column(sa.Integer)
    n_stage_date = sa.Column(sa.Date)
    n_stage_concept_id = sa.Column(sa.Integer)
    n_stage_label = sa.Column(sa.String)
    m_stage_id = sa.Column(sa.Integer)
    m_stage_date = sa.Column(sa.Date)
    m_stage_concept_id = sa.Column(sa.Integer)
    m_stage_label = sa.Column(sa.String)
    group_stage_id = sa.Column(sa.Integer)
    group_stage_date = sa.Column(sa.Date)
    group_stage_concept_id = sa.Column(sa.Integer)
    group_stage_label = sa.Column(sa.String)
