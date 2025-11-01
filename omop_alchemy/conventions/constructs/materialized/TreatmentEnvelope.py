import sqlalchemy as sa
import sqlalchemy.orm as so

from ....db import Base
from ....conventions.concept_enumerators import CancerProcedureTypes
from ....model.clinical import Procedure_Occurrence, Observation, Person
from ....model.vocabulary import Concept, Concept_Ancestor
from .MaterializedViewMixin import MaterializedViewMixin
from .ConditionTreatmentEpisode import OverarchingCondition
from .Surgeries import SurgicalProcedure
from .SACTEpisode import SACTRegimen
from .RadiotherapyEpisode import RTCourse
from .ModifiedCondition import ModifiedCondition

first_surg = (
    sa.select(
        SurgicalProcedure.overarching_episode_id, 
        sa.func.min(SurgicalProcedure.surgery_datetime).label('first_surgery'), 
    )
    .group_by(        
        SurgicalProcedure.overarching_episode_id,        
    )
    .subquery(name='first_surg')
)

sact_window = (
    sa.select(
        SACTRegimen.condition_episode_id, 
        sa.func.min(SACTRegimen.first_exposure_date).label('first_sact_exposure'), 
        sa.func.max(SACTRegimen.last_exposure_date).label('last_sact_exposure') 
    )
    .group_by(        
        SACTRegimen.condition_episode_id
    )
    .subquery(name='sact_window')
)

rt_window = (
    sa.select(
        RTCourse.condition_episode_id, 
        sa.func.min(RTCourse.first_exposure_date).label('first_rt_exposure'), 
        sa.func.max(RTCourse.last_exposure_date).label('last_rt_exposure') 
    )
    .group_by(        
        RTCourse.condition_episode_id
    )
    .subquery(name='rt_window')
)

treatment_window = (
    sa.select(
        ModifiedCondition.person_id, 
        ModifiedCondition.condition_episode, 
        ModifiedCondition.condition_start_date,
        first_surg.c.first_surgery,
        sact_window.c.first_sact_exposure,
        sact_window.c.last_sact_exposure,
        rt_window.c.first_rt_exposure,
        rt_window.c.last_rt_exposure,
        sa.case(
            (
                (sact_window.c.first_sact_exposure.isnot(None)&rt_window.c.first_rt_exposure.isnot(None)),
                sa.or_(
                    sa.and_(
                        sact_window.c.first_sact_exposure > rt_window.c.first_rt_exposure,
                        sact_window.c.first_sact_exposure < rt_window.c.last_rt_exposure
                    ),
                    sa.and_(
                        rt_window.c.first_rt_exposure > sact_window.c.first_sact_exposure,
                        rt_window.c.first_rt_exposure < sact_window.c.last_sact_exposure
                    )
                )
            ),
                else_=None
        ).label('concurrent_chemort')
    )
    .join(first_surg, first_surg.c.overarching_episode_id==ModifiedCondition.condition_episode, isouter=True)
    .join(rt_window, rt_window.c.condition_episode_id==ModifiedCondition.condition_episode, isouter=True)
    .join(sact_window, sact_window.c.condition_episode_id==ModifiedCondition.condition_episode, isouter=True)
    .distinct()
    .subquery()
)

treatment_envelope = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        Person.person_id,
        treatment_window.c.condition_episode, 
        treatment_window.c.condition_start_date,
        treatment_window.c.concurrent_chemort,
        sa.func.least(treatment_window.c.first_surgery, treatment_window.c.first_sact_exposure, treatment_window.c.first_rt_exposure).label('earliest_treatment'),
        sa.func.greatest(treatment_window.c.last_sact_exposure, treatment_window.c.last_rt_exposure).label('latest_treatment'),
        Person.death_datetime
    )
    .join(treatment_window, treatment_window.c.person_id==Person.person_id)
    .subquery()
)

treatment_envelope_with_scalars = (
    sa.select(
        *treatment_envelope.c,
        sa.case(
            (
                (treatment_envelope.c.death_datetime.isnot(None) & treatment_envelope.c.latest_treatment.isnot(None)),
                sa.func.extract('epoch', treatment_envelope.c.death_datetime - treatment_envelope.c.latest_treatment)/86400
            ),
            else_=None
        ).label('treatment_days_before_death'),
        sa.case(
            (
                (treatment_envelope.c.earliest_treatment.isnot(None) & treatment_envelope.c.condition_start_date.isnot(None)),
                sa.func.extract('epoch', treatment_envelope.c.earliest_treatment - treatment_envelope.c.condition_start_date)/86400
            ),
            else_=None
        ).label('days_from_dx_to_treatment')
    )
)
            

class TreatmentEnvelope(MaterializedViewMixin, Base):
    __mv_name__ = 'treatment_envelope_mv'
    __mv_select__ = treatment_envelope_with_scalars.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    condition_episode = sa.Column(sa.Integer)
    condition_start_date = sa.Column(sa.Date)
    earliest_treatment = sa.Column(sa.Date)
    latest_treatment = sa.Column(sa.Date)
    death_datetime = sa.Column(sa.DateTime)
    treatment_days_before_death = sa.Column(sa.Float)
    days_from_dx_to_treatment = sa.Column(sa.Float)
    concurrent_chemort = sa.Column(sa.Boolean)