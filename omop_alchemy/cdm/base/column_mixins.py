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
    `PersonScoped`

    Used for tables that are fundamentally scoped to a person.

    Encodes the standard `person_id` foreign key and indexing pattern.
    """
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("person.person_id"), nullable=False)

class ConceptTyped:
    """
    `ConceptTyped`

    Mixin for tables whose primary meaning is encoded by a concept_id.
    
    Subclasses MUST define *something_concept_id*.

    Not currently used, but intention to build out logic for generic EAV queries & timeline projections.
    """

    @so.declared_attr
    def concept_id(cls: Any) -> so.Mapped[int]:
        raise NotImplementedError(
            f"{cls.__name__} must define its own <x>_concept_id column"
        )

class ValueMixin:
    """
    `ValueMixin`

    Encodes the OMOP pattern where a value may be represented *either numerically or categorically*.

    Structural guarantees:

    - at least one value must be present
    - enforced via a check constraint
    - validated at assignment time

    This helps when building generic tooling that needs to handle values flexibly but then normalise for analysis.

    Examples
    --------
    >>> from omop_alchemy.cdm.model.clinical.measurement import Measurement
    >>> m = Measurement()
    >>> m.value_as_number = 42.0
    >>> m.value_as_concept_id = None  # OK
    >>> m.value_as_number = None  # Raises ValueError

    """
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
    `DatedEvent`

    Mixin for tables with start/end date and datetime pairs.

    This mixin does **not** enforce temporal logic (e.g. start ≤ end); it only defines the shape,
    but this may be integrated with the event timeline machinery in future.

    """
    start_date: so.Mapped[date] = so.mapped_column(nullable=False)
    start_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(nullable=True)

    end_date: so.Mapped[Optional[date]] = so.mapped_column(nullable=True)
    end_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(nullable=True)


class HealthSystemContext:
    """
    `HealthSystemContext`

    Encodes attribution to providers and visits.

    Used across many clinical event tables to provide consistent join points into the health system structure.
    """
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("provider.provider_id"), nullable=True)
    visit_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("visit_occurrence.visit_occurrence_id"), nullable=True)
    visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("visit_detail.visit_detail_id"), nullable=True)

class FactTable:
    """
    `FactTable`

    Marker mixin for OMOP fact tables.

    Used for introspection, tooling, and documentation only.
    """
    pass

class ReferenceTable:
    """
    `ReferenceTable`

    Marker mixin for OMOP reference tables.
    """
    pass

class SourceAttribution:
    """
    `SourceAttribution`

    Mixin for *_source_value and *_source_concept_id patterns.
    """
    source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String, nullable=True)
    source_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("concept.concept_id"), nullable=True)     

class UnitConcept:
    """
    `UnitConcept`
    
    Mixin for unit_concept_id.
    """
    unit_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("concept.concept_id"), nullable=True)
