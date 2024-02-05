from datetime import datetime
from typing import Optional, List
from sqlalchemy.ext.hybrid import hybrid_property
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Person(Base):
    __tablename__ = 'person'
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
    # concept_relationships
    gender: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[gender_concept_id])
    ethnicity: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[ethnicity_concept_id])
    race: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[race_concept_id])
    gender_source: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[gender_source_concept_id])
    ethnicity_source: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[ethnicity_source_concept_id])
    race_source: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[race_source_concept_id])

    conditions: so.Mapped[List['Condition_Occurrence']] = so.relationship(
        back_populates="person", lazy="selectin"
    )

    def __repr__(self):
        return f'Person: person_id = {self.person_id}'
    
    def get_approximate_dob(self):
        if self.year_of_birth is None:
            return None
        day = self.day_of_birth or 1
        month = self.month_of_birth or 1
        return datetime(self.year_of_birth, month, day)

    @hybrid_property
    def gender_label(self):
        if self.gender:
            return self.gender.concept_name
        
    @gender_label.expression
    def _gender_label_expression(cls) -> sa.ColumnElement[Optional[str]]:
        return sa.cast("SQLColumnExpression[Optional[str]]", cls.gender.concept_name)

    def age_calc(self, age_at, selected_dob):
        if selected_dob is None:
            return {}
        age = (age_at - selected_dob).days
        years = age // 365
        days = age % 365
        return {'age_total': age, 'age_years': years, 'age_days': days}

    @hybrid_property
    def age(self, age_at=None):
        
        if age_at is None:
            age_at = datetime.now()

        if self.death_datetime is not None:
            age_at = min(age_at, self.death_datetime)

        selected_dob = self.birth_datetime or self.get_approximate_dob()

        return self.age_calc(age_at, selected_dob)
    


