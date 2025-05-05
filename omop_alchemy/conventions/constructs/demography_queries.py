

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

demographics_join = (
    sa.select(
        Person.person_id, 
        Person.year_of_birth,
        Person.death_datetime,
        Concept.concept_name.label('gender'),
        person_lang.c.language_spoken,
        person_cob.c.country_of_birth,
        person_postcode.c.post_code
    )
    .join(Concept, Concept.concept_id==Person.gender_concept_id)
    .join(person_lang, person_lang.c.person_id==Person.person_id)
    .join(person_cob, person_cob.c.person_id==Person.person_id)
    .join(person_postcode, person_postcode.c.person_id==Person.person_id)
).subquery()

class PersonDemography(Base):
    __table__ = demographics_join
    person_id = demographics_join.c.person_id
    year_of_birth = demographics_join.c.year_of_birth
    death_datetime = demographics_join.c.death_datetime
    language_spoken = demographics_join.c.language_spoken
    country_of_birth = demographics_join.c.country_of_birth
    person_postcode = demographics_join.c.post_code
