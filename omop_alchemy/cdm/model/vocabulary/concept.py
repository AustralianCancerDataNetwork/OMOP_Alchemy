import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.declarative import declared_attr
from typing import Optional, TYPE_CHECKING, List
from datetime import date
if TYPE_CHECKING:
    from .domain import Domain
    from .vocabulary import Vocabulary
    from .concept_class import Concept_Class
    from .concept_ancestor import Concept_Ancestor
    from .concept_relationship import Concept_Relationship

from omop_alchemy.cdm.base import ReferenceTable, Base, cdm_table, CDMTableBase, ReferenceContextMixin

@cdm_table
class Concept(
    ReferenceTable,
    CDMTableBase,
    Base
):
    __tablename__ = "concept"
    concept_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    concept_name: so.Mapped[str] = so.mapped_column(sa.String(255), nullable=False)
    domain_id: so.Mapped[str] = so.mapped_column(sa.ForeignKey("domain.domain_id"), nullable=False, index=True)
    vocabulary_id: so.Mapped[str] = so.mapped_column(sa.ForeignKey("vocabulary.vocabulary_id"), nullable=False, index=True)
    concept_class_id: so.Mapped[str] = so.mapped_column(sa.ForeignKey("concept_class.concept_class_id"), nullable=False, index=True)
    standard_concept: so.Mapped[Optional[str]] = so.mapped_column(sa.String(1))
    concept_code: so.Mapped[str] = so.mapped_column(sa.String(50), nullable=False)
    valid_start_date: so.Mapped[date] = so.mapped_column(nullable=False)
    valid_end_date: so.Mapped[date] = so.mapped_column(nullable=False)
    invalid_reason: so.Mapped[Optional[str]] = so.mapped_column(sa.String(1))

class ConceptContext(ReferenceContextMixin):
    """
    Navigational relationships for Concept.

    This mixin defines read-only ORM relationships that resolve
    foreign keys into reference tables and hierarchy navigation.
    """
    
    domain: so.Mapped["Domain"] = ReferenceContextMixin._reference_relationship(target="Domain",local_fk="domain_id",remote_pk="domain_id") # type: ignore[assignment]
    vocabulary: so.Mapped["Vocabulary"] = ReferenceContextMixin._reference_relationship(target="Vocabulary",local_fk="vocabulary_id",remote_pk="vocabulary_id") # type: ignore[assignment]
    concept_class: so.Mapped["Concept_Class"] = ReferenceContextMixin._reference_relationship(target="Concept_Class",local_fk="concept_class_id",remote_pk="concept_class_id") # type: ignore[assignment]

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


    @property
    def is_standard(self) -> bool:
        return self.standard_concept == "S"

    @property
    def is_valid(self) -> bool:
        return self.invalid_reason is None