from __future__ import annotations

from datetime import date, datetime
from typing import Optional, List, TYPE_CHECKING, Type, Any
from dataclasses import dataclass
import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import get_model_by_tablename


"""
OMOP CDM structural mixins.

These mixins encode *structural semantics* of the OMOP Common Data Model
as defined in the Table-Level and Field-Level CSV specifications.

They intentionally do NOT encode clinical or analytical meaning.
"""

class PersonScoped:
    """
    Mixin for tables scoped to a person.
    """
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("person.person_id"), nullable=False, index=True)

class ConceptTyped:
    """
    Mixin for tables whose primary meaning is encoded by a concept_id.
    Subclasses MUST define <something>_concept_id.

    Not currently used TBC if there is a use-case to retain that supports EAV queries?
    """

    @so.declared_attr
    def concept_id(cls: Any) -> so.Mapped[int]:
        raise NotImplementedError(
            f"{cls.__name__} must define its own <x>_concept_id column"
        )

class ValueMixin:
    value_as_number: so.Mapped[Optional[float]] = so.mapped_column(sa.Float, nullable=True)
    value_as_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("concept.concept_id"), nullable=True)

    __table_args__ = (
        sa.CheckConstraint(
            "value_as_number IS NOT NULL OR value_as_concept_id IS NOT NULL",
            name="ck_value_present",
        ),
    )

    @so.validates("value_as_number", "value_as_concept_id")
    def _validate_value(self, key, value):
        other = (
            self.value_as_concept_id
            if key == "value_as_number"
            else self.value_as_number
        )

        if value is None and other is None:
            raise ValueError(
                "At least one of value_as_number or value_as_concept_id must be set"
            )
        return value

class DatedEvent:
    """
    Mixin for tables with start/end date and datetime pairs.
    """
    start_date: so.Mapped[date] = so.mapped_column(nullable=False)
    start_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(nullable=True)

    end_date: so.Mapped[Optional[date]] = so.mapped_column(nullable=True)
    end_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(nullable=True)


class HealthSystemContext:
    """
    Mixin for tables attributed to a provider.
    """
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("provider.provider_id"),index=True,nullable=True)
    visit_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("visit_occurrence.visit_occurrence_id"),index=True,nullable=True)
    visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("visit_detail.visit_detail_id"),index=True,nullable=True)

class FactTable:
    """
    Marker mixin for OMOP fact tables.

    Used for introspection, tooling, and documentation only.
    """
    pass

class ReferenceTable:
    """Marker mixin for OMOP reference tables."""
    pass

class SourceAttribution:
    """
    Mixin for *_source_value and *_source_concept_id patterns.
    """
    source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String, nullable=True)
    source_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("concept.concept_id"), nullable=True)     

class UnitConcept:
    """
    Mixin for unit_concept_id.
    """
    unit_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("concept.concept_id"), index=True, nullable=True)
