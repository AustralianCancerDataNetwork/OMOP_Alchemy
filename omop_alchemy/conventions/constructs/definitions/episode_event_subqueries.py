import sqlalchemy as sa
import sqlalchemy.orm as so

from ....db import Base
from ....model.onco_ext import Episode, Episode_Event
from ....model.clinical import Person, Modifiable_Table, Condition_Occurrence, Drug_Exposure, Procedure_Occurrence, Observation
from ....model.vocabulary import Concept, Concept_Ancestor
from ....conventions.concept_enumerators import ModifierFields, TreatmentEpisode, DiseaseEpisodeConcepts
from .alias_definitions import radiation_therapy, systemic_therapy

sact_episode_events = (
        sa.select(Episode_Event)
        .where(Episode_Event.episode_event_field_concept_id==ModifierFields.drug_exposure_id.value)
        .subquery()
    )


"""
    TODO: this needs updating because it currently fails to actually filter down to 
    TODO: RT-specific events, however it is not an issue, due to the fact that RT events are
    TODO: the only ones mapped to episodes - this will break otherwise
"""

rt_episode_events = (
        sa.select(Episode_Event)
        .where(Episode_Event.event_polymorphic.of_type(radiation_therapy))
        .subquery()
    )


regimen_subquery = (
    sa.select(Episode)
    .where(Episode.episode_concept_id==TreatmentEpisode.treatment_regimen.value)
    .subquery()
)

diagnosis_subquery = (
    sa.select(Episode)
    .where(Episode.episode_concept_id==DiseaseEpisodeConcepts.episode_of_care.value)
    .subquery()
)



Regimen = so.with_polymorphic(Episode, [], selectable=regimen_subquery)
Diagnosis = so.with_polymorphic(Episode, [], selectable=diagnosis_subquery)




# select all tx episodes that have at least one drug administration event 
# and find the start of chemo administration for that episode
# todo: should this filter on anti-cancer therapies?
# no - this does not need to filter on anti-cancer therapies only, because we only
# pull in drugs that have been attached to an episode

systemic_therapy_subquery = (
    sa.select(
        Episode.person_id,
        Episode.episode_id.label('sact_episode_id'),
        Episode.episode_parent_id,
        systemic_therapy.drug_exposure_start_date
    )
    .join(
         Episode_Event,
         Episode.episode_id==Episode_Event.episode_id
     )
    .join_from(
        Episode_Event,
        Episode_Event.event_polymorphic.of_type(systemic_therapy)
    )
    .subquery()
)

radiation_therapy_subquery = (
    sa.select(
        Episode.person_id,
        Episode.episode_id.label('rad_episode_id'),
        Episode.episode_parent_id,
        radiation_therapy.procedure_datetime
    )
    .join(
         Episode_Event,
         Episode.episode_id==Episode_Event.episode_id
     )
    .join_from(
        Episode_Event,
        Episode_Event.event_polymorphic.of_type(radiation_therapy)
    )
    .subquery()
)

systemic_therapy_start = (
    sa.select(
        systemic_therapy_subquery.c.person_id,
        systemic_therapy_subquery.c.sact_episode_id,
        systemic_therapy_subquery.c.episode_parent_id,
        sa.func.min(systemic_therapy_subquery.c.drug_exposure_start_date).label('sact_start')
    )
    .group_by(        
        systemic_therapy_subquery.c.person_id,        
        systemic_therapy_subquery.c.episode_parent_id,
        systemic_therapy_subquery.c.sact_episode_id
    )
    .subquery()
)

radiation_therapy_start = (
    sa.select(
        radiation_therapy_subquery.c.person_id,
        radiation_therapy_subquery.c.rad_episode_id,
        radiation_therapy_subquery.c.episode_parent_id,
        sa.func.min(radiation_therapy_subquery.c.procedure_datetime).label('rt_start')
    )
    .group_by(        
        radiation_therapy_subquery.c.person_id,        
        radiation_therapy_subquery.c.episode_parent_id,
        radiation_therapy_subquery.c.rad_episode_id
    )
    .subquery()
)