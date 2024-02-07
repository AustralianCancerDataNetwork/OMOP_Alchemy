from datetime import datetime
from typing import Optional, List
from sqlalchemy.ext.hybrid import hybrid_property
import sqlalchemy as sa
import sqlalchemy.orm as so

from .concept_links import Concept_Links
from ...db import Base

class Person(Base, Concept_Links):
    __tablename__ = 'person'

    labels = {'gender': False, 'ethnicity': False, 'race': False, 'gender_source': False, 'ethnicity_source': False, 'race_source': False}

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
    # relationships
    location: so.Mapped[Optional['Location']] = so.relationship(foreign_keys=[location_id])
    provider: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    care_site: so.Mapped[Optional['Care_Site']] = so.relationship(foreign_keys=[care_site_id])
    conditions: so.Mapped[List['Condition_Occurrence']] = so.relationship(back_populates="person", lazy="selectin")
    episodes: so.Mapped[List['Episode']] = so.relationship(back_populates="person_object", lazy="selectin")

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

