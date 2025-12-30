import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional
from datetime import date
from omop_alchemy.cdm.base import (
    Base,
    CDMTableBase,
    cdm_table,
    optional_concept_fk,
    ReferenceContextMixin,
    DomainValidationMixin,
    ExpectedDomain,
)

@cdm_table
class Death(CDMTableBase, Base):
    __tablename__ = "death"
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("person.person_id"), primary_key=True)
    death_date: so.Mapped[date] = so.mapped_column(nullable=False)
    death_datetime: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime, nullable=True)
    death_type_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()
    cause_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()
    cause_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    cause_source_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()


class DeathContext(ReferenceContextMixin):
    person: so.Mapped["Person"] = ReferenceContextMixin._reference_relationship(target="Person",local_fk="person_id",remote_pk="person_id")  # type: ignore[assignment]
    death_type_concept: so.Mapped["Concept"] = ReferenceContextMixin._reference_relationship(target="Concept", local_fk="death_type_concept_id", remote_pk="concept_id")  # type: ignore[assignment]
    cause_concept: so.Mapped["Concept"] = ReferenceContextMixin._reference_relationship(target="Concept", local_fk="cause_concept_id", remote_pk="concept_id")  # type: ignore[assignment]


class DeathView(Death, DeathContext, DomainValidationMixin):
    __tablename__ = "death"
    __mapper_args__ = {"concrete": False}
    __expected_domains__ = {
        "death_type_concept_id": ExpectedDomain("Gender"),
        "cause_concept_id": ExpectedDomain("Race"),
    }
