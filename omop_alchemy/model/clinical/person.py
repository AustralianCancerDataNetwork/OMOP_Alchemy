from datetime import datetime, date
from typing import Optional, List
from sqlalchemy.ext.hybrid import hybrid_property
import sqlalchemy as sa
import sqlalchemy.orm as so

from ..concept_links import Concept_Links
from ...db import Base

class Person(Base):#, Concept_Links):
    __tablename__ = 'person'
    validators = {}
#    testing out @validates decorator instead and can't do this easily if these are added dynamically after all...
#    labels = {'gender': False, 'ethnicity': False, 'race': False, 'gender_source': False, 'ethnicity_source': False, 'race_source': False}

    # identifier
    person_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    # temporal
    birth_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime, nullable=True)
    death_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime, nullable=True)
    # strings
    person_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    gender_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    race_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    ethnicity_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    # numeric
    year_of_birth: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    month_of_birth: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    day_of_birth: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    # fks
    location_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('location.location_id', name='pr_fk_1'), nullable=True)
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('provider.provider_id', name='pr_fk_2'), nullable=True)
    care_site_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('care_site.care_site_id', name='pr_fk_3'), nullable=True)
    # concept fks
    gender_source_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='pr_fk_4'), nullable=True)
    ethnicity_source_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='pr_fk_5'), nullable=True)
    race_source_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='pr_fk_6'), nullable=True)
    gender_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='pr_fk_7'), default=0)
    ethnicity_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='pr_fk_8'), default=0)
    race_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='pr_fk_9'), default=0)
    # relationships
    location: so.Mapped[Optional['Location']] = so.relationship(foreign_keys=[location_id])
    provider: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    care_site: so.Mapped[Optional['Care_Site']] = so.relationship(foreign_keys=[care_site_id])
    conditions: so.Mapped[List['Condition_Occurrence']] = so.relationship(back_populates="person", lazy="selectin")
    observations: so.Mapped[List['Observation']] = so.relationship(back_populates="person", lazy="selectin")
    episodes: so.Mapped[List['Episode']] = so.relationship(back_populates="person_object", lazy="selectin")
    # concept_relationships
    gender: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[gender_concept_id])
    ethnicity: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[ethnicity_concept_id])
    race: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[race_concept_id])
    gender_source: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[gender_source_concept_id])
    ethnicity_source: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[ethnicity_source_concept_id])
    race_source: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[race_source_concept_id])

    @classmethod
    def set_validators(cls):
        # putting this here so that we can defer the import until after the models have all been instantiated, otherwise 
        # it tries to query the concepts from non-existent tables - there may be a better way?
        from ...conventions.vocab_lookups import gender_lookup, ethnicity_lookup, race_lookup
        cls.validators = {'gender': gender_lookup, 'ethnicity': ethnicity_lookup, 'race': race_lookup}

    # don't love how repetitive this is, but haven't worked out how to add functions dynamically that
    # include decorators, so it'll do for now...
    @so.validates('gender_concept_id')
    def validate_gender(self, key, gender_concept_id):
        if gender_concept_id and gender_concept_id not in self.validators['gender']:
            raise ValueError("failed validation - gender concept")
        return gender_concept_id
    
    @so.validates('race_concept_id')
    def validate_race(self, key, race_concept_id):
        if race_concept_id and race_concept_id not in self.validators['race']:
            raise ValueError("failed validation - race concept")
        return race_concept_id
    
    @so.validates('ethnicity_concept_id')
    def validate_ethnicity(self, key, ethnicity_concept_id):
        if ethnicity_concept_id and ethnicity_concept_id not in self.validators['ethnicity']:
            raise ValueError("failed validation - ethnicity concept")
        return ethnicity_concept_id
    
    @so.validates('year_of_birth')
    def validate_yob(self, key, year_of_birth):
        if year_of_birth and (year_of_birth < 1900 or year_of_birth > datetime.now().year):
            raise ValueError("failed validation - year of birth out of range")
        return year_of_birth
        
    @so.validates('month_of_birth')
    def validate_mob(self, key, month_of_birth):
        if month_of_birth and (month_of_birth < 1 or month_of_birth > 12):
            raise ValueError("failed validation - month of birth out of range")
        return month_of_birth
        
    @so.validates('day_of_birth')
    def validate_day_of_birth(self, key, day_of_birth):
        # this is run before actually storing to attributes, so can't check for like 30th Feb using this form
        if day_of_birth and (day_of_birth < 1 or day_of_birth > 31):
            raise ValueError(f'failed validation - day of birth out of range')
        return day_of_birth

    def __repr__(self):
        age = self.age
        p = f'Person: person_id = {self.person_id}'
        if age != {}:
            y = age['age_years']
            d = age['age_days']
            p += f'; age={y} years and {d} days'
        if self.gender:
            p += f'; {self.gender.concept_name}'
        return p

    def age_calc(self, age_at):
        if self.dob is None:
            return {}
        age = (age_at - self.dob).days
        return {'age_total': age, 'age_years': age // 365, 'age_days': age % 365}

    @property
    def dob(self):
        if self.birth_datetime:
            return self.birth_datetime.date()
        if self.year_of_birth is None:
            return None
        day = self.day_of_birth or 1
        month = self.month_of_birth or 1
        return date(self.year_of_birth, month, day)

    @property
    def age(self, age_at=None):
        age_at = age_at or date.today()
        age_at = min(age_at, self.death_datetime.date()) if self.death_datetime is not None else age_at
        return self.age_calc(age_at)
    
    @hybrid_property
    def gender_label(self):
        if self.gender:
            return self.gender.concept_name

    @hybrid_property
    def primary_dx_eps(self):
        return [e for e in self.episodes if e.is_overarching]

    @hybrid_property
    def current_cond(self):
        if self.conditions:
            return sorted(self.conditions, key=lambda x: x.event_date)[-1]

    @hybrid_property
    def age_current_dx(self):
        cc = self.current_cond
        if cc:
            return self.age_calc(age_at = cc.event_date)