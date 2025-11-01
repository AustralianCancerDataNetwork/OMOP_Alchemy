import sqlalchemy as sa
import sqlalchemy.orm as so

from ....db import Base
from ....conventions.concept_enumerators import DemographyConcepts
from ....model.clinical import Person, Observation
from ....model.vocabulary import Concept

from ..definitions.demography_subqueries import person_postcode, person_cob, person_lang

demographics_join = (
    sa.select(
        Person.person_id, 
        Person.year_of_birth,
        Person.death_datetime,
        Person.person_source_value.label('mrn'),
        Concept.concept_name.label('gender'),
        person_lang.c.language_spoken,
        person_cob.c.country_of_birth,
        person_postcode.c.post_code
    )
    .join(Concept, Concept.concept_id==Person.gender_concept_id)
    .join(person_lang, person_lang.c.person_id==Person.person_id, isouter=True)
    .join(person_cob, person_cob.c.person_id==Person.person_id, isouter=True)
    .join(person_postcode, person_postcode.c.person_id==Person.person_id, isouter=True)
).subquery()

class Person_Demography(Base):
    __table__ = demographics_join
    person_id = demographics_join.c.person_id
    mrn = demographics_join.c.mrn
    year_of_birth = demographics_join.c.year_of_birth
    death_datetime = demographics_join.c.death_datetime
    language_spoken = demographics_join.c.language_spoken
    country_of_birth = demographics_join.c.country_of_birth
    person_postcode = demographics_join.c.post_code
