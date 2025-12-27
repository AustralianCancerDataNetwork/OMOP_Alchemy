from ....db import Base
from ....conventions.concept_enumerators import DemographyConcepts
from ....model.clinical import Person, Observation
from ....model.vocabulary import Concept
from ....model.onco_ext import Episode
from .MaterializedViewMixin import MaterializedViewMixin

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

# demographics join to all episodes
demographics_join = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        Person.person_id, 
        Person.year_of_birth,
        Person.death_datetime,
        Person.gender_concept_id,
        Person.person_source_value.label('mrn'),
        Episode.episode_id,
        Episode.episode_start_datetime,
        Concept.concept_name.label('sex'),
        person_lang.c.language_spoken,
        person_cob.c.country_of_birth,
        person_postcode.c.post_code
    )
    .join(Episode, Episode.person_id==Person.person_id)
    .join(Concept, Concept.concept_id==Person.gender_concept_id)
    .join(person_lang, person_lang.c.person_id==Person.person_id, isouter=True)
    .join(person_cob, person_cob.c.person_id==Person.person_id, isouter=True)
    .join(person_postcode, person_postcode.c.person_id==Person.person_id, isouter=True)
)

class PersonDemography(MaterializedViewMixin, Base):
    __mv_name__ = 'person_demography_mv'
    __mv_select__ = demographics_join.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    episode_id = sa.Column(sa.Integer)
    episode_start_datetime = sa.Column(sa.DateTime)
    gender_concept_id = sa.Column(sa.Integer)
    mrn = sa.Column(sa.String)
    sex = sa.Column(sa.String)
    year_of_birth = sa.Column(sa.Integer)
    death_datetime = sa.Column(sa.DateTime)
    language_spoken = sa.Column(sa.String)
    country_of_birth = sa.Column(sa.String)
    post_code = sa.Column(sa.Integer)