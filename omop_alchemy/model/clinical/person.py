import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.declarative import declared_attr
from typing import Optional, TYPE_CHECKING, List
from datetime import date
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.orm.exc import DetachedInstanceError

from omop_alchemy.cdm.base import (
    Base, 
    cdm_table,
    CDMTableBase, 
    required_concept_fk,
    optional_concept_fk,
    required_int,
    optional_int,
    HealthSystemContext,
    ReferenceContextMixin,
    DomainValidationMixin,
    ExpectedDomain,
)

from ..vocabulary import Concept
from ..health_system import Location, Provider, Care_Site

@cdm_table
class Person(CDMTableBase,Base,HealthSystemContext):
    __tablename__ = "person"

    person_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    
    year_of_birth: so.Mapped[int] = required_int()
    month_of_birth: so.Mapped[Optional[int]] = optional_int()
    day_of_birth: so.Mapped[Optional[int]] = optional_int()
    birth_datetime: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime, nullable=True)
    
    gender_concept_id: so.Mapped[int] = required_concept_fk()
    race_concept_id: so.Mapped[int] = required_concept_fk()
    ethnicity_concept_id: so.Mapped[int] = required_concept_fk()
    gender_source_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()
    race_source_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()
    ethnicity_source_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()

    location_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("location.location_id"), nullable=True, index=True)
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("provider.provider_id"), nullable=True, index=True)
    care_site_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("care_site.care_site_id"), nullable=True, index=True)
    
    person_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    gender_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    race_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    ethnicity_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)

    def __repr__(self) -> str:
        return f"<Person {self.person_id}>"

class PersonContext(ReferenceContextMixin):
    gender: so.Mapped["Concept"] = ReferenceContextMixin._reference_relationship(target="Concept",local_fk="gender_concept_id",remote_pk="concept_id")  # type: ignore[assignment]
    race: so.Mapped["Concept"] = ReferenceContextMixin._reference_relationship(target="Concept",local_fk="race_concept_id",remote_pk="concept_id")  # type: ignore[assignment]
    ethnicity: so.Mapped["Concept"] = ReferenceContextMixin._reference_relationship(target="Concept",local_fk="ethnicity_concept_id",remote_pk="concept_id")  # type: ignore[assignment]
    location: so.Mapped["Location"] = ReferenceContextMixin._reference_relationship(target="Location",local_fk="location_id",remote_pk="location_id")  # type: ignore[assignment]   
    provider: so.Mapped["Provider"] = ReferenceContextMixin._reference_relationship(target="Provider",local_fk="provider_id",remote_pk="provider_id")  # type: ignore[assignment]
    care_site: so.Mapped["Care_Site"] = ReferenceContextMixin._reference_relationship(target="Care_Site",local_fk="care_site_id",remote_pk="care_site_id")  # type: ignore[assignment]

class PersonView(Person, PersonContext, DomainValidationMixin):
    """
    Rich, navigable Person mapping.

    Use when:
    - cohort logic
    - analytics
    - inspection / debugging

    Avoid in ETL loops.
    """
    __tablename__ = "person"
    __mapper_args__ = {"concrete": False}
    __expected_domains__ = {
        "gender_concept_id": ExpectedDomain("Gender"),
        "race_concept_id": ExpectedDomain("Race"),
        "ethnicity_concept_id": ExpectedDomain("Ethnicity"),
    }

    @hybrid_method
    def age_at(self, on_date: date) -> Optional[int]: # type: ignore
        if not self.year_of_birth:
            return None
        return on_date.year - self.year_of_birth

    @age_at.expression
    def age_at(cls, on_date):
        return sa.func.extract("year", on_date) - cls.year_of_birth
    
    @property
    def age(self) -> Optional[int]:
        return self.age_at(date.today())
    
    @property
    def gender_code(self) -> Optional[str]:
        """
        Attempt relationship access (may lazy load) to get human-readable gender code.
        """
        try:
            gender = self.gender  # type: ignore[attr-defined]
            if gender is not None:
                return gender.concept_name[:1].upper()
        except DetachedInstanceError:
            pass
        except Exception:
            pass
        return None
 
    def __repr__(self) -> str:
        """
        Compact, safe representation:
        <Person 12345: M(50)>
        <Person 67890: F(?)>
        <Person 99999: ?(?)>
        """
        pid = getattr(self, "person_id", "?")
        gender_code = self.gender_code or "?"
        age = self.age
        age_str = str(age) if age is not None else "?"
        return f"<Person {pid}: {gender_code}({age_str})>"