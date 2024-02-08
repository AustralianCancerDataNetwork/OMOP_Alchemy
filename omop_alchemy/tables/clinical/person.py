from datetime import datetime
from typing import Optional, List
from sqlalchemy.ext.hybrid import hybrid_property
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...conventions import gender_lookup, ethnicity_lookup, race_lookup
from ..concept_links import Concept_Links
from ...db import Base

class Person(Base):#, Concept_Links):
    __tablename__ = 'person'

#    testing out @validates decorator instead and can't do this easily if these are added dynamically after all...
#    labels = {'gender': False, 'ethnicity': False, 'race': False, 'gender_source': False, 'ethnicity_source': False, 'race_source': False}

    # identifier
    person_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    # temporal
    birth_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    death_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    # strings
    person_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    gender_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    race_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    ethnicity_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    # numeric
    year_of_birth: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer)
    month_of_birth: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer)
    day_of_birth: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer)
    # fks
    location_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('location.location_id'))
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('provider.provider_id'))
    care_site_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('care_site.care_site_id'))
    # concept fks
    gender_source_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    ethnicity_source_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    race_source_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    gender_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    ethnicity_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    race_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    # relationships
    location: so.Mapped[Optional['Location']] = so.relationship(foreign_keys=[location_id])
    provider: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    care_site: so.Mapped[Optional['Care_Site']] = so.relationship(foreign_keys=[care_site_id])
    conditions: so.Mapped[List['Condition_Occurrence']] = so.relationship(back_populates="person", lazy="selectin")
    episodes: so.Mapped[List['Episode']] = so.relationship(back_populates="person_object", lazy="selectin")
    # concept_relationships
    gender: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[gender_concept_id])
    ethnicity: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[ethnicity_concept_id])
    race: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[race_concept_id])
    gender_source: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[gender_source_concept_id])
    ethnicity_source: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[ethnicity_source_concept_id])
    race_source: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[race_source_concept_id])

    # don't love how repetitive this is, but haven't worked out how to add functions dynamically that
    # include decorators, so it'll do for now...
    @so.validates('gender_concept_id')
    def validate_gender(self, key, gender_concept_id):
        if gender_concept_id and gender_concept_id not in gender_lookup:
            raise ValueError("failed validation - gender concept")
    
    @so.validates('race_concept_id')
    def validate_race(self, key, race_concept_id):
        if race_concept_id and race_concept_id not in race_lookup:
            raise ValueError("failed validation - race concept")
    
    @so.validates('ethnicity_concept_id')
    def validate_ethnicity(self, key, ethnicity_concept_id):
        if ethnicity_concept_id and ethnicity_concept_id not in ethnicity_lookup:
            raise ValueError("failed validation - ethnicity concept")
    
    @so.validates('year_of_birth')
    def validate_yob(self, key, year_of_birth):
        if year_of_birth and (year_of_birth < 1900 or year_of_birth > datetime.now().year):
            raise ValueError("failed validation - year of birth out of range")
        
    @so.validates('month_of_birth')
    def validate_mob(self, key, month_of_birth):
        if month_of_birth and (month_of_birth < 1 or month_of_birth > 12):
            raise ValueError("failed validation - month of birth out of range")
        
    @so.validates('day_of_birth')
    def validate_day_of_birth(self, key, day_of_birth):
        if day_of_birth:
            try:
                datetime.date(year=self.year_of_birth, month=self.month_of_birth, day=self.day_of_birth)  
            except:
                raise ValueError(f'failed validation - invalid date of birth combination yyyy-mm-dd {self.year_of_birth}-{self.month_of_birth}-{self.day_of_birth}')

    def __repr__(self):
        age = self.age
        y = age['age_years']
        d = age['age_days']
        return f'Person: person_id = {self.person_id}, age={y} years and {d}'

    def age_calc(self, age_at):
        if self.dob is None:
            return {}
        age = (age_at - self.dob).days
        return {'age_total': age, 'age_years': age // 365, 'age_days': age % 365}

    @property
    def dob(self):
        if self.birth_datetime:
            return self.birth_datetime
        if self.year_of_birth is None:
            return None
        day = self.day_of_birth or 1
        month = self.month_of_birth or 1
        return datetime(self.year_of_birth, month, day)


    @property
    def age(self, age_at=None):
        age_at = age_at or datetime.now()
        age_at = min(age_at, self.death_datetime) if self.death_datetime is not None else age_at
        return self.age_calc(age_at)

