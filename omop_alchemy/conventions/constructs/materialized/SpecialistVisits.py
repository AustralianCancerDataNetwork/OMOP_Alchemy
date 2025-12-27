import sqlalchemy as sa
import sqlalchemy.orm as so

from ...concept_enumerators import ProviderSpecialty, CancerConsultTypes, CancerObservations, DiseaseEpisodeConcepts
from ....model.clinical import Visit_Occurrence, Observation, Person
from ....model.health_system import Provider
from ....model.vocabulary import Concept
from ....model.onco_ext import Episode
from ....db import Base

from .TreatmentEnvelope import treatment_window, treatment_envelope, TreatmentEnvelope
from .MaterializedViewMixin import MaterializedViewMixin
from .ForceDxLink import DXRelevantObs



provider_concept = so.aliased(Concept, name='provider_concept')


episode_start_prior = sa.case(
    (
        # if visit within the 6 months after an episode start or 3 months before an episode start, 
        # we link the visit to all eps for the purpose of calculating measures
        (sa.func.abs(sa.func.extract('epoch', Visit_Occurrence.visit_start_datetime - Episode.episode_start_datetime)/86400) < 180) | 
        (sa.func.abs(sa.func.extract('epoch', Episode.episode_start_datetime - Visit_Occurrence.visit_start_datetime)/86400) < 90),
        1
    ),
    (
        # otherwise, within all episodes prior, pick the closest
        (Visit_Occurrence.visit_start_datetime > Episode.episode_start_datetime),
        2
    ),
    # otherwise just pick the closest in absolute terms
    else_=3
)

diff_days = sa.func.extract('epoch', Visit_Occurrence.visit_start_datetime - Episode.episode_start_datetime)/86400


visits_by_specialty = (
    sa.select(
        *Visit_Occurrence.__table__.columns,
        Provider.provider_id,
        provider_concept.concept_name.label('provider_specialty'),
        provider_concept.concept_id.label('provider_specialty_concept_id'),
        Episode.episode_id, 
        Episode.episode_start_datetime,
        episode_start_prior.label('episode_prior'),
        diff_days.label('diff_days'),
        sa.func.row_number()
        .over(
            partition_by=Visit_Occurrence.visit_occurrence_id,
            order_by=[episode_start_prior, diff_days]
        ).label('rank')
    )
    .join(Episode, Episode.person_id==Visit_Occurrence.person_id)
    .join(Provider, Visit_Occurrence.provider_id==Provider.provider_id)
    .join(provider_concept, provider_concept.concept_id==Provider.specialty_concept_id)
    .filter(Episode.episode_concept_id==DiseaseEpisodeConcepts.episode_of_care.value)
    .subquery("visits")
)


class VisitsBySpecialty(MaterializedViewMixin, Base):
    __mv_name__ = 'visits_by_specialty_mv'
    __mv_select__ = visits_by_specialty.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    visit_occurrence_id = sa.Column(sa.Integer)
    episode_id = sa.Column(sa.Integer)
    visit_start_datetime = sa.Column(sa.DateTime)
    provider_specialty = sa.Column(sa.String)
    provider_id = sa.Column(sa.Integer)
    provider_specialty_concept_id = sa.Column(sa.Integer)


visit_link_to_episode = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        *visits_by_specialty.c
    )
    .where(
        sa.or_(
            visits_by_specialty.c.episode_prior==1,
            visits_by_specialty.c.rank==1
        )
    )
)


class DXRelevantVisit(MaterializedViewMixin, Base):
    __mv_pk__ = ["mv_id"]
    __mv_name__ = 'dx_vis_mv'
    __mv_select__ = visit_link_to_episode.select()
    __tablename__ = __mv_name__
    __table_args__ = {"extend_existing": True}
    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    visit_occurrence_id = sa.Column(sa.Integer)
    visit_start_datetime = sa.Column(sa.Date)
    provider_specialty_concept_id = sa.Column(sa.Integer)
    provider_specialty = sa.Column(sa.String)
    episode_id = sa.Column(sa.Integer)
    episode_start_datetime = sa.Column(sa.Date)
    episode_prior = sa.Column(sa.Integer)
    diff_days = sa.Column(sa.Integer)
    rank = sa.Column(sa.Integer)


specialist_visit = (
    sa.select(
        DXRelevantVisit.person_id,
        DXRelevantVisit.episode_id,
        sa.func.min(DXRelevantVisit.visit_start_datetime).label('first_specialist_visit')
    )
    .filter(DXRelevantVisit.provider_specialty_concept_id.in_([ProviderSpecialty.radonc.value, ProviderSpecialty.medonc.value, ProviderSpecialty.haematologist.value]))
    .group_by(DXRelevantVisit.person_id, DXRelevantVisit.episode_id,)
    .subquery()
)

pall_care_visit = (
    sa.select(
        DXRelevantVisit.person_id,
        DXRelevantVisit.episode_id,
        sa.func.min(DXRelevantVisit.visit_start_datetime).label('first_pall_care_visit')
    )
    .filter(DXRelevantVisit.provider_specialty_concept_id.in_([ProviderSpecialty.pall_care.value]))
    .group_by(DXRelevantVisit.person_id, DXRelevantVisit.episode_id,)
    .subquery()
)

first_specialist = (
    sa.select(
        DXRelevantObs.person_id,
        DXRelevantObs.episode_id,
        sa.func.min(DXRelevantObs.observation_date).label('first_specialist_consult'),
        sa.func.max(DXRelevantObs.observation_date).label('last_specialist_consult')
    )
    .filter(DXRelevantObs.concept_id.in_([CancerObservations.medonc.value, CancerObservations.clinonc.value]))
    .group_by(DXRelevantObs.person_id, DXRelevantObs.episode_id,)
    .subquery(name='first_specialist')
)

gp_referral = (
    sa.select(
        DXRelevantObs.person_id,
        DXRelevantObs.episode_id,
        sa.func.min(DXRelevantObs.observation_date).label('initial_gp_referral')
    )
    .filter(DXRelevantObs.concept_id.in_([CancerObservations.oncology_referral.value]))
    .group_by(DXRelevantObs.person_id, DXRelevantObs.episode_id,)
    .subquery(name='gp_referral')
)

pall_care_referral = (
    sa.select(
        DXRelevantObs.person_id,
        DXRelevantObs.episode_id,
        sa.func.min(DXRelevantObs.observation_date).label('first_pall_care_referral')
    )
    .filter(DXRelevantObs.concept_id.in_([CancerObservations.pall_care_referral.value]))
    .group_by(DXRelevantObs.person_id, DXRelevantObs.episode_id,)
    .subquery(name='pall_care_referral')
)

specialist_trajectory = (
    sa.select(
        Episode.person_id, 
        Episode.episode_id, 
        Episode.episode_start_datetime,
        first_specialist.c.first_specialist_consult, 
        first_specialist.c.last_specialist_consult, 
        specialist_visit.c.first_specialist_visit, 
        gp_referral.c.initial_gp_referral,
        pall_care_referral.c.first_pall_care_referral,
        pall_care_visit.c.first_pall_care_visit,
    )
    .join(first_specialist, first_specialist.c.episode_id==Episode.episode_id, isouter=True)
    .join(gp_referral, gp_referral.c.episode_id==Episode.episode_id, isouter=True)
    .join(pall_care_referral, gp_referral.c.episode_id==Episode.episode_id, isouter=True)
    .join(specialist_visit, specialist_visit.c.episode_id==Episode.episode_id, isouter=True)
    .join(pall_care_visit, pall_care_visit.c.episode_id==Episode.episode_id, isouter=True)
    .subquery()
)


class SpecialistConsult(MaterializedViewMixin, Base):
    __mv_name__ = 'specialist_consult_mv'
    __mv_select__ = specialist_trajectory.select()
    __mv_pk__ = ["episode_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    episode_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    episode_start_datetime = sa.Column(sa.DateTime)
    first_specialist_consult = sa.Column(sa.DateTime)
    last_specialist_consult = sa.Column(sa.DateTime)
    initial_gp_referral = sa.Column(sa.DateTime)
    first_pall_care_referral = sa.Column(sa.DateTime)
    first_specialist_visit = sa.Column(sa.DateTime)
    first_pall_care_visit = sa.Column(sa.DateTime)


# class Visits_By_Specialty(MaterializedViewMixin, Base):
#     __mv_name__ = 'visits_by_specialty_mv'
#     __mv_select__ = visits_by_specialty.select()
#     __mv_pk__ = ["mv_id"]
#     __table_args__ = {"extend_existing": True}
#     __tablename__ = __mv_name__

#     mv_id = sa.Column(primary_key=True)
#     person_id = sa.Column(sa.Integer)
#     visit_occurrence_id = sa.Column(sa.Integer)
#     visit_start_datetime = sa.Column(sa.DateTime)
#     provider_specialty = sa.Column(sa.String)




treatment_and_consult_windows = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        specialist_trajectory.c.person_id,
        specialist_trajectory.c.episode_id,
        specialist_trajectory.c.episode_start_datetime,
        specialist_trajectory.c.initial_gp_referral,
        sa.func.least(specialist_trajectory.c.first_specialist_consult, specialist_trajectory.c.first_specialist_visit).label('first_specialist'),
        sa.func.least(specialist_trajectory.c.first_pall_care_referral, specialist_trajectory.c.first_pall_care_visit).label('first_pall_care'),
        sa.func.least(specialist_trajectory.c.first_pall_care_referral, specialist_trajectory.c.first_pall_care_visit, TreatmentEnvelope.earliest_treatment).label('first_pall_care_or_treatment'),
        TreatmentEnvelope.earliest_treatment
    )
    .join(TreatmentEnvelope, TreatmentEnvelope.condition_episode==specialist_trajectory.c.episode_id, isouter=True)
)

treatment_and_consults_with_scalars = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        treatment_and_consult_windows.c.person_id,
        treatment_and_consult_windows.c.episode_id,
        treatment_and_consult_windows.c.episode_start_datetime,
        treatment_and_consult_windows.c.initial_gp_referral,
        sa.func.min(
            sa.case(
            (
                (treatment_and_consult_windows.c.first_specialist.isnot(None) & treatment_and_consult_windows.c.initial_gp_referral.isnot(None)),
                sa.func.extract('epoch', treatment_and_consult_windows.c.first_specialist - treatment_and_consult_windows.c.initial_gp_referral)/86400
            ),
            else_=None
            )
        ).label('referral_to_specialist'),
        sa.func.min(
            sa.case(
                (
                    (treatment_and_consult_windows.c.first_pall_care_or_treatment.isnot(None) & treatment_and_consult_windows.c.initial_gp_referral.isnot(None)),
                    sa.func.extract('epoch', treatment_and_consult_windows.c.first_pall_care_or_treatment - treatment_and_consult_windows.c.initial_gp_referral)/86400
                ),
                else_=None
            )    
        ).label('referral_to_tx')
    )
    .group_by(treatment_and_consult_windows.c.person_id, treatment_and_consult_windows.c.episode_id, treatment_and_consult_windows.c.episode_start_datetime, treatment_and_consult_windows.c.initial_gp_referral)
)

# class Specialist_Consult(MaterializedViewMixin, Base):
#     __mv_name__ = 'specialist_consult_mv'
#     __mv_select__ = specialist_trajectory.select()
#     __mv_pk__ = ["mv_id"]
#     __table_args__ = {"extend_existing": True}
#     __tablename__ = __mv_name__

#     mv_id = sa.Column(primary_key=True)
#     person_id = sa.Column(sa.Integer)
#     first_specialist_consult = sa.Column(sa.DateTime)
#     last_specialist_consult = sa.Column(sa.DateTime)
#     initial_gp_referral = sa.Column(sa.DateTime)
#     first_pall_care_referral = sa.Column(sa.DateTime)
#     first_specialist_care_visit = sa.Column(sa.DateTime)
#     first_pall_care_visit = sa.Column(sa.DateTime)






class ConsultWindow(MaterializedViewMixin, Base):
    __mv_name__ = 'consult_window_mv'
    __mv_select__ = treatment_and_consults_with_scalars.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__
    
    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    episode_id = sa.Column(sa.Integer)
    episode_start_datetime = sa.Column(sa.DateTime)
    initial_gp_referral = sa.Column(sa.Date)
    referral_to_tx = sa.Column(sa.Integer)
    referral_to_specialist = sa.Column(sa.Integer)
