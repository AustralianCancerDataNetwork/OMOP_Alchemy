from ...db import Base
from ...conventions.concept_enumerators import DemographyConcepts
from ...model.clinical import Person, Observation
from ...model.vocabulary import Concept

import sqlalchemy as sa
import sqlalchemy.orm as so

person_postcode = (
    sa.select(
        Observation.person_id,
        Observation.value_as_number.label('post_code')
    )
    .filter(Observation.observation_concept_id==DemographyConcepts.postcode.value)
    .subquery()
)

person_cob = (
    sa.select(
        Observation.person_id,
        Concept.concept_name.label('country_of_birth')
    )
    .join(Concept, Concept.concept_id==Observation.value_as_concept_id)
    .filter(Observation.observation_concept_id==DemographyConcepts.cob.value)
    .subquery()
)

person_lang = (
    sa.select(
        Observation.person_id,
        Concept.concept_name.label('language_spoken')
    )
    .join(Concept, Concept.concept_id==Observation.value_as_concept_id)
    .filter(Observation.observation_concept_id==DemographyConcepts.language_spoken.value)
    .subquery()
)
