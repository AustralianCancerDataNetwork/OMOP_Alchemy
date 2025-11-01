import sqlalchemy as sa
import sqlalchemy.orm as so

from .MaterializedViewMixin import MaterializedViewMixin
from .ModifiedProcedure import ModifiedProcedure
from ...concept_enumerators import ModifierFields, TreatmentEpisode
from ...vocab_lookups import radiotherapy_procedures
from ....db import Base
from ....model.vocabulary import Concept, Concept_Ancestor
from ....model.clinical import Condition_Occurrence, Person, Observation, Procedure_Occurrence, Measurement, Modifiable_Table, Drug_Exposure, Procedure_Occurrence
from ....model.onco_ext import Episode, Episode_Event
# note: as per cycle, we will only see RT procs that have been added to an episode

rt_proc_concept = so.aliased(Concept, name='rt_proc_concept')
fraction_concept = so.aliased(Concept, name='fraction_concept')

fraction_join = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        ModifiedProcedure.person_id,
        ModifiedProcedure.procedure_datetime,
        ModifiedProcedure.procedure_occurrence_id,
        ModifiedProcedure.procedure_concept_id,
        ModifiedProcedure.procedure_concept,
        ModifiedProcedure.intent_concept,
        ModifiedProcedure.intent_concept_id,
        ModifiedProcedure.intent_datetime,
        Episode.episode_id.label('fraction_id'),
        Episode.episode_number.label('fraction_number'),
        Episode.episode_parent_id.label('course_id'),
        fraction_concept.concept_name.label('fraction_name')
    )
    .join(
        Episode_Event, 
        sa.and_(
            Episode_Event.event_id==ModifiedProcedure.procedure_occurrence_id,
            Episode_Event.episode_event_field_concept_id==ModifierFields.procedure_occurrence_id.value
        )
    )
    .join(Episode, Episode.episode_id==Episode_Event.episode_id)
    .join(fraction_concept, fraction_concept.concept_id==Episode.episode_object_concept_id)
    .filter(ModifiedProcedure.procedure_concept_id.in_(radiotherapy_procedures.all_concepts))
)

class Fraction(MaterializedViewMixin, Base):
    __mv_name__ = 'fraction_mv'
    __mv_select__ = fraction_join.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    procedure_occurrence_id = sa.Column(sa.Integer)
    procedure_datetime = sa.Column(sa.DateTime)
    procedure_concept_id = sa.Column(sa.Integer)
    procedure_concept = sa.Column(sa.String)
    intent_datetime = sa.Column(sa.DateTime)
    intent_concept_id = sa.Column(sa.Integer)
    intent_concept = sa.Column(sa.String)
    fraction_id = sa.Column(sa.Integer)
    fraction_number = sa.Column(sa.Integer)
    course_id = sa.Column(sa.Integer)
    fraction_name = sa.Column(sa.String)


# note: this will only pull in procedure events that have been explicitly 
# linked to episodes via care plan

course_concept = so.aliased(Concept, name='course_concept')

frac_summary_join = (
    sa.select(
        Fraction.person_id,
        Fraction.fraction_id,
        Fraction.fraction_number,
        Fraction.course_id,
        Fraction.fraction_name,
        sa.func.min(Fraction.procedure_datetime).label('first_exposure_date'),
        sa.func.max(Fraction.procedure_datetime).label('last_exposure_date'),
        sa.func.count(Fraction.procedure_occurrence_id).label('fraction_count'),
    )
    .group_by(Fraction.person_id, Fraction.fraction_id, Fraction.fraction_number, Fraction.course_id, Fraction.fraction_name)
    .subquery()
)

course_join = (
    sa.select(
        *frac_summary_join.c,
        sa.func.row_number().over().label('mv_id'),
        Episode.episode_number.label('course_number'),
        Episode.episode_parent_id.label('condition_episode_id'),
        ModifiedProcedure.procedure_occurrence_id.label('course_prescription_id'),
        ModifiedProcedure.procedure_concept_id.label('course_concept_id'),
        ModifiedProcedure.procedure_concept.label('course_concept'),
        ModifiedProcedure.intent_concept,
        ModifiedProcedure.intent_concept_id,
        ModifiedProcedure.intent_datetime
    )
    .join(
        Episode, 
        sa.and_(
            Episode.episode_id == frac_summary_join.c.course_id, 
            Episode.episode_concept_id==TreatmentEpisode.treatment_regimen.value
        ),
        isouter=True
    )
    .join(
        Episode_Event, 
        sa.and_(
            Episode_Event.episode_id == Episode.episode_id, 
            Episode_Event.episode_event_field_concept_id == ModifierFields.procedure_occurrence_id.value
        ),
        isouter=True
    )
    .join(ModifiedProcedure, ModifiedProcedure.procedure_occurrence_id==Episode_Event.event_id, isouter=True)
)

class RTCourse(MaterializedViewMixin, Base):
    __mv_name__ = 'rt_course_mv'
    __mv_select__ = course_join.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    fraction_id = sa.Column(sa.Integer)
    fraction_number = sa.Column(sa.Integer)
    fraction_count = sa.Column(sa.Integer)
    fraction_name = sa.Column(sa.String)
    first_exposure_date = sa.Column(sa.Date)
    last_exposure_date = sa.Column(sa.Date)
    course_id = sa.Column(sa.Integer)
    course_number = sa.Column(sa.Integer)
    condition_episode_id = sa.Column(sa.Integer)
    course_prescription_id = sa.Column(sa.Integer)
    course_concept_id = sa.Column(sa.Integer)
    course_concept = sa.Column(sa.String)
    intent_datetime = sa.Column(sa.DateTime)
    intent_concept_id = sa.Column(sa.Integer)
    intent_concept = sa.Column(sa.String)

course_summary_join = (
    sa.select(
        RTCourse.person_id,
        RTCourse.course_concept,
        RTCourse.course_id,
        RTCourse.course_number,
        RTCourse.condition_episode_id,
        RTCourse.intent_concept_id.label('rt_intent_concept_id'),
        RTCourse.intent_concept.label('rt_intent_concept'),
        sa.func.min(RTCourse.first_exposure_date).label('course_start_date'),
        sa.func.max(RTCourse.last_exposure_date).label('course_end_date'),
        sa.func.sum(RTCourse.fraction_count).label('fraction_count'),
        sa.func.count(RTCourse.course_id).label('course_count'),
    )
    .group_by(RTCourse.person_id, RTCourse.course_concept, RTCourse.course_id, RTCourse.course_number, RTCourse.condition_episode_id, RTCourse.intent_concept_id, RTCourse.intent_concept)
    .subquery()
)