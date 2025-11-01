import sqlalchemy as sa
import sqlalchemy.orm as so

from ...concept_enumerators import ProviderSpecialty, CancerConsultTypes
from ....model.clinical import Visit_Occurrence, Observation, Person
from ....model.health_system import Provider
from ....model.vocabulary import Concept
from ....db import Base

from .TreatmentEnvelope import treatment_window, treatment_envelope, TreatmentEnvelope
from .MaterializedViewMixin import MaterializedViewMixin

oncologist_consults = so.aliased(Observation, flat=True)

specialist_visit = (
    sa.select(
        Visit_Occurrence.person_id,
        sa.func.min(Visit_Occurrence.visit_start_datetime).label('first_specialist_visit')
    )
    .join(Provider, Visit_Occurrence.provider_id==Provider.provider_id)
    .filter(Provider.specialty_concept_id.in_([ProviderSpecialty.radonc.value, ProviderSpecialty.medonc.value, ProviderSpecialty.haematologist.value]))
    .group_by(Visit_Occurrence.person_id)
    .subquery()
)

pall_care_visit = (
    sa.select(
        Visit_Occurrence.person_id,
        sa.func.min(Visit_Occurrence.visit_start_datetime).label('first_pall_care_visit')
    )
    .join(Provider, Visit_Occurrence.provider_id==Provider.provider_id)
    .filter(Provider.specialty_concept_id.in_([ProviderSpecialty.pall_care.value]))
    .group_by(Visit_Occurrence.person_id)
    .subquery()
)

first_specialist = (
    sa.select(
        oncologist_consults.person_id,
        sa.func.min(oncologist_consults.observation_datetime).label('first_specialist_consult'),
        sa.func.max(oncologist_consults.observation_datetime).label('last_specialist_consult')
    )
    .filter(oncologist_consults.observation_concept_id.in_([CancerConsultTypes.medonc.value, CancerConsultTypes.clinonc.value]))
    .group_by(oncologist_consults.person_id)
    .subquery(name='first_specialist')
)

gp_referral = (
    sa.select(
        oncologist_consults.person_id,
        sa.func.min(oncologist_consults.observation_datetime).label('initial_gp_referral')
    )
    .filter(oncologist_consults.observation_concept_id.in_([CancerConsultTypes.oncology_referral.value]))
    .group_by(oncologist_consults.person_id)
    .subquery(name='gp_referral')
)

pall_care_referral = (
    sa.select(
        oncologist_consults.person_id,
        sa.func.min(oncologist_consults.observation_datetime).label('first_pall_care_referral')
    )
    .filter(oncologist_consults.observation_concept_id.in_([CancerConsultTypes.pall_care_referral.value]))
    .group_by(oncologist_consults.person_id,
    )
    .subquery(name='pall_care_referral')
)

specialist_trajectory = (
    sa.select(
        Person.person_id, 
        first_specialist.c.first_specialist_consult, 
        first_specialist.c.last_specialist_consult, 
        specialist_visit.c.first_specialist_visit, 
        gp_referral.c.initial_gp_referral,
        pall_care_referral.c.first_pall_care_referral,
        pall_care_visit.c.first_pall_care_visit,
    )
    .join(first_specialist, first_specialist.c.person_id==Person.person_id, isouter=True)
    .join(gp_referral, gp_referral.c.person_id==Person.person_id, isouter=True)
    .join(pall_care_referral, gp_referral.c.person_id==Person.person_id, isouter=True)
    .join(specialist_visit, specialist_visit.c.person_id==Person.person_id, isouter=True)
    .join(pall_care_visit, pall_care_visit.c.person_id==Person.person_id, isouter=True)
    .subquery()
)


visits_by_specialty = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        Visit_Occurrence.person_id,
        Visit_Occurrence.visit_occurrence_id,
        Visit_Occurrence.visit_start_datetime,
        Concept.concept_name.label('provider_specialty')
    )
    .join(Provider, Visit_Occurrence.provider_id==Provider.provider_id)
    .join(Concept, Concept.concept_id==Provider.specialty_concept_id)
    .subquery("visits")
)



treatment_and_consult_windows = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        specialist_trajectory.c.person_id,
        specialist_trajectory.c.initial_gp_referral,
        sa.func.least(specialist_trajectory.c.first_specialist_consult, specialist_trajectory.c.first_specialist_visit).label('first_specialist'),
        sa.func.least(specialist_trajectory.c.first_pall_care_referral, specialist_trajectory.c.first_pall_care_visit).label('first_pall_care'),
        sa.func.least(specialist_trajectory.c.first_pall_care_referral, specialist_trajectory.c.first_pall_care_visit, TreatmentEnvelope.earliest_treatment).label('first_pall_care_or_treatment'),
        TreatmentEnvelope.earliest_treatment
    )
    .join(TreatmentEnvelope, TreatmentEnvelope.person_id==specialist_trajectory.c.person_id, isouter=True)
)

treatment_and_consults_with_scalars = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        treatment_and_consult_windows.c.person_id,
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
    .group_by(treatment_and_consult_windows.c.person_id, treatment_and_consult_windows.c.initial_gp_referral)
)

class Specialist_Consult(MaterializedViewMixin, Base):
    __mv_name__ = 'specialist_consult_mv'
    __mv_select__ = specialist_trajectory.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    first_specialist_consult = sa.Column(sa.DateTime)
    last_specialist_consult = sa.Column(sa.DateTime)
    initial_gp_referral = sa.Column(sa.DateTime)
    first_pall_care_referral = sa.Column(sa.DateTime)
    first_specialist_care_visit = sa.Column(sa.DateTime)
    first_pall_care_visit = sa.Column(sa.DateTime)


class VisitsBySpecialty(MaterializedViewMixin, Base):
    __mv_name__ = 'visits_by_specialty_mv'
    __mv_select__ = visits_by_specialty.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    visit_occurrence_id = sa.Column(sa.Integer)
    visit_start_datetime = sa.Column(sa.DateTime)
    provider_specialty = sa.Column(sa.String)

class ConsultWindow(MaterializedViewMixin, Base):
    __mv_name__ = 'consult_window_mv'
    __mv_select__ = treatment_and_consults_with_scalars.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__
    
    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    initial_gp_referral = sa.Column(sa.Date)
    referral_to_tx = sa.Column(sa.Integer)
    referral_to_specialist = sa.Column(sa.Integer)
