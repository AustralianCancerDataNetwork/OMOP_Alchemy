from ....model.vocabulary import Concept
from ....model.health_system import Provider, Location
from ....model.clinical import Visit_Occurrence, Drug_Exposure, Procedure_Occurrence, Observation, Condition_Occurrence, Person
from ...concept_enumerators import CancerConsultTypes, ProviderSpecialty
from ..mappers.timeline_mappers import dx_treatment_window
from .episode_event_subqueries import radiation_therapy_start, systemic_therapy_start
from .surgical_subqueries import surgical_procedure
from .alias_definitions import oncologist_consults

import sqlalchemy as sa

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


treatment_and_consult_windows = (
    sa.select(
        specialist_trajectory.c.person_id,
        specialist_trajectory.c.initial_gp_referral,
        sa.func.least(specialist_trajectory.c.first_specialist_consult, specialist_trajectory.c.first_specialist_visit).label('first_specialist'),
        sa.func.least(specialist_trajectory.c.first_pall_care_referral, specialist_trajectory.c.first_pall_care_visit).label('first_pall_care'),
        sa.func.least(radiation_therapy_start.c.rt_start, systemic_therapy_start.c.sact_start, surgical_procedure.c.procedure_datetime).label('first_treatment')
    )
    .join(radiation_therapy_start, radiation_therapy_start.c.person_id==specialist_trajectory.c.person_id, isouter=True)
    .join(systemic_therapy_start, systemic_therapy_start.c.person_id==specialist_trajectory.c.person_id, isouter=True)
    .join(surgical_procedure, surgical_procedure.c.person_id==specialist_trajectory.c.person_id, isouter=True)
    .subquery()
)

visits_by_specialty = (
    sa.select(
        Visit_Occurrence.person_id,
        Visit_Occurrence.visit_occurrence_id,
        Visit_Occurrence.visit_start_datetime,
        Concept.concept_name.label('provider_specialty')
    )
    .join(Provider, Visit_Occurrence.provider_id==Provider.provider_id)
    .join(Concept, Concept.concept_id==Provider.specialty_concept_id)
    .subquery("visits")
)