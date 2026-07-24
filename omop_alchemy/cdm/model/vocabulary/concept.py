import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.declarative import declared_attr
from enum import StrEnum, nonmember
from typing import Optional, TYPE_CHECKING, List
from datetime import date
if TYPE_CHECKING:
    from .domain import Domain
    from .vocabulary import Vocabulary
    from .concept_class import Concept_Class
    from .concept_ancestor import Concept_Ancestor
    from .concept_relationship import Concept_Relationship

from orm_loader.helpers import Base
from omop_alchemy.cdm.base import (
    ReferenceTable,
    cdm_table,
    CDMTableBase,
    ReferenceContext,
    merge_table_args,
    omop_index,
    omop_primary_key_index_name,
    omop_table_options,
)


class StandardConceptFlag(StrEnum):
    """Allowed non-null values of ``concept.standard_concept`` (OMOP CDM v5.4)."""

    STANDARD = "S"
    CLASSIFICATION = "C"

    # Precomputed membership set for the hot Python-side check (Concept.is_standard) --
    # re-deriving this from the enum on every call is measurably expensive (~10x).
    values = nonmember(frozenset({STANDARD, CLASSIFICATION}))


class InvalidReasonFlag(StrEnum):
    """Allowed non-null values of ``concept.invalid_reason`` (OMOP CDM v5.4)."""

    DELETED = "D"
    UPDATED = "U"


def normalised_flag_expr(
    column: sa.SQLColumnExpression[Optional[str]],
) -> sa.SQLColumnExpression[Optional[str]]:
    """Return a canonical OMOP flag expression.

    OMOP CDM v5.4 allows only ``NULL``/``'S'``/``'C'`` for ``standard_concept``
    and ``NULL``/``'D'``/``'U'`` for ``invalid_reason``. Some real-world loads
    contain blank or whitespace-only strings instead of ``NULL``; those are
    normalised here defensively so callers do not need to reimplement the same
    tolerance logic.

    Non-empty non-canonical values are left unchanged so downstream validation
    can still detect them as bad data rather than silently treating them as a
    valid state.
    """
    return sa.func.nullif(sa.func.trim(column), "")


@cdm_table
class Concept(
    ReferenceTable,
    CDMTableBase,
    Base
):
    __tablename__ = "concept"
    __table_args__ = merge_table_args(
        omop_index(__tablename__, "concept_code"),
        omop_index(__tablename__, "vocabulary_id"),
        omop_index(__tablename__, "domain_id"),
        omop_index(__tablename__, "concept_class_id"),
        # Has to be wrapped in func.lower() as that is the common query
        # as it prevents captialisation mismatches between query and data.
        omop_index(
            __tablename__,
            sa.func.lower(sa.column("concept_name")),
            name="ix_concept_concept_name_lower",
        ),
        omop_table_options(cluster_on=omop_primary_key_index_name("concept")),
    )
    concept_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    concept_name: so.Mapped[str] = so.mapped_column(sa.String(255), nullable=False)
    domain_id: so.Mapped[str] = so.mapped_column(sa.ForeignKey("domain.domain_id"), nullable=False)
    vocabulary_id: so.Mapped[str] = so.mapped_column(sa.ForeignKey("vocabulary.vocabulary_id"), nullable=False)
    concept_class_id: so.Mapped[str] = so.mapped_column(sa.ForeignKey("concept_class.concept_class_id"), nullable=False)
    standard_concept: so.Mapped[Optional[str]] = so.mapped_column(sa.String(1), nullable=True)
    concept_code: so.Mapped[str] = so.mapped_column(sa.String(50), nullable=False)
    valid_start_date: so.Mapped[date] = so.mapped_column(sa.Date(), nullable=False)
    valid_end_date: so.Mapped[date] = so.mapped_column(sa.Date(), nullable=False)
    invalid_reason: so.Mapped[Optional[str]] = so.mapped_column(sa.String(1), nullable=True)

    @property
    def is_standard(self) -> bool:
        value = self.standard_concept.strip() if self.standard_concept is not None else ""
        return bool(value) and value in StandardConceptFlag.values

    @classmethod
    def is_standard_expr(cls) -> sa.SQLColumnExpression[bool]:
        """SQL-side counterpart to :attr:`is_standard`, for use in query filters."""
        return normalised_flag_expr(cls.standard_concept).in_(StandardConceptFlag.values)

    @property
    def is_valid(self) -> bool:
        value = self.invalid_reason.strip() if self.invalid_reason is not None else ""
        return not value

    @classmethod
    def is_valid_expr(cls) -> sa.SQLColumnExpression[bool]:
        """SQL-side counterpart to :attr:`is_valid`, for use in query filters."""
        return normalised_flag_expr(cls.invalid_reason).is_(None)

class ConceptContext(ReferenceContext):
    """
    Navigational relationships for Concept.

    This mixin defines read-only ORM relationships that resolve
    foreign keys into reference tables and hierarchy navigation.
    """
    
    domain: so.Mapped["Domain"] = ReferenceContext._reference_relationship(target="Domain",local_fk="domain_id",remote_pk="domain_id") # type: ignore[assignment]
    vocabulary: so.Mapped["Vocabulary"] = ReferenceContext._reference_relationship(target="Vocabulary",local_fk="vocabulary_id",remote_pk="vocabulary_id") # type: ignore[assignment]
    concept_class: so.Mapped["Concept_Class"] = ReferenceContext._reference_relationship(target="Concept_Class",local_fk="concept_class_id",remote_pk="concept_class_id") # type: ignore[assignment]

    @declared_attr
    def outgoing_relationships(cls) -> so.Mapped[List["Concept_Relationship"]]:
        return so.relationship(
            "Concept_Relationship",
            primaryjoin=f"{cls.__name__}.concept_id == Concept_Relationship.concept_id_1", # type: ignore
            foreign_keys="Concept_Relationship.concept_id_1",
            viewonly=True,
            lazy="select",
        )

    @declared_attr
    def incoming_relationships(cls) -> so.Mapped[List["Concept_Relationship"]]:
        return so.relationship(
            "Concept_Relationship",
            primaryjoin=f"{cls.__name__}.concept_id == Concept_Relationship.concept_id_2", # type: ignore
            foreign_keys="Concept_Relationship.concept_id_2",
            viewonly=True,
            lazy="select",
        )

    @declared_attr
    def ancestors(cls) -> so.Mapped[List["Concept_Ancestor"]]:
        return so.relationship(
            "Concept_Ancestor",
            primaryjoin=f"{cls.__name__}.concept_id == Concept_Ancestor.descendant_concept_id", # type: ignore
            foreign_keys="Concept_Ancestor.descendant_concept_id",
            viewonly=True,
            lazy="select",
        )

    @declared_attr
    def descendants(cls) -> so.Mapped[List["Concept_Ancestor"]]:
        return so.relationship(
            "Concept_Ancestor",
            primaryjoin=f"{cls.__name__}.concept_id == Concept_Ancestor.ancestor_concept_id", # type: ignore
            foreign_keys="Concept_Ancestor.ancestor_concept_id",
            viewonly=True,
            lazy="select",
        )

class ConceptView(Concept, ConceptContext):
    """
    Rich, navigable Concept mapping.

    Use when:
    - traversing vocabulary relationships
    - exploring hierarchies
    - semantic inspection

    Avoid in tight loops or ETL paths.
    """
    __tablename__ = "concept"
    __mapper_args__ = {"concrete": False}
