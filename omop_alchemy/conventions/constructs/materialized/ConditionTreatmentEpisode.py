import sqlalchemy as sa
import sqlalchemy.orm as so

from .OverarchingEpisode import OverarchingCondition
from .RadiotherapyEpisode import course_summary_join
from .SACTEpisode import regimen_summary_join
from .MaterializedViewMixin import MaterializedViewMixin
from ...concept_enumerators import ModifierFields, TreatmentEpisode
from ....db import Base
from ....model.vocabulary import Concept, Concept_Ancestor
from ....model.clinical import Condition_Occurrence, Person, Observation, Procedure_Occurrence, Measurement, Modifiable_Table, Drug_Exposure, Procedure_Occurrence
from ....model.onco_ext import Episode, Episode_Event

condition_concept = so.aliased(Concept, name='condition_concept')

condition_treatment_join = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        Condition_Occurrence.person_id,
        Condition_Occurrence.condition_occurrence_id,
        Condition_Occurrence.condition_start_date,
        Condition_Occurrence.condition_end_date,
        Episode_Event.episode_id.label('condition_episode_id'),
        *regimen_summary_join.c,
        *course_summary_join.c,
    )
    .join(
        Episode_Event, 
        sa.and_(
            Episode_Event.event_id==Condition_Occurrence.condition_occurrence_id,
            Episode_Event.episode_event_field_concept_id==ModifierFields.condition_occurrence_id.value
        ),
        isouter=True
    )
    .join(regimen_summary_join, regimen_summary_join.c.condition_episode_id==Episode_Event.episode_id, isouter=True)
    .join(course_summary_join, course_summary_join.c.condition_episode_id==Episode_Event.episode_id, isouter=True)
    .subquery()
)

class ConditionTreatmentEpisode(MaterializedViewMixin, Base):
    __mv_name__ = 'cond_treat_mv'
    __mv_select__ = condition_treatment_join.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    condition_episode_id = sa.Column(sa.Integer)
    condition_occurrence_id = sa.Column(sa.Integer)
    condition_start_date = sa.Column(sa.Date)
    condition_end_date = sa.Column(sa.Date)
    course_concept = sa.Column(sa.String)
    course_id = sa.Column(sa.Integer)
    course_number = sa.Column(sa.Integer)
    course_start_date = sa.Column(sa.Date)
    course_end_date = sa.Column(sa.Date)
    rt_intent_concept_id = sa.Column(sa.Integer)
    rt_intent_concept = sa.Column(sa.Integer)
    fraction_count = sa.Column(sa.Integer)
    course_count = sa.Column(sa.Integer)
    regimen_concept = sa.Column(sa.String)
    regimen_id = sa.Column(sa.Integer)
    regimen_number = sa.Column(sa.Integer)
    regimen_start_date = sa.Column(sa.Date)
    regimen_end_date = sa.Column(sa.Date)
    sact_intent_concept_id = sa.Column(sa.Integer)
    sact_intent_concept = sa.Column(sa.Integer)
    exposure_count = sa.Column(sa.Integer)
    regimen_count = sa.Column(sa.Integer)