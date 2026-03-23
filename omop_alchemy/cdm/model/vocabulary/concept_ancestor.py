import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base
from omop_alchemy.cdm.base import (
    ReferenceTable,
    cdm_table,
    CDMTableBase,
    merge_table_args,
    omop_index,
)

@cdm_table
class Concept_Ancestor(Base, ReferenceTable, CDMTableBase):
    __tablename__ = "concept_ancestor"
    __table_args__ = merge_table_args(
        omop_index("idx_concept_ancestor_id_1", "ancestor_concept_id", cluster=True),
        omop_index("idx_concept_ancestor_id_2", "descendant_concept_id"),
    )
    ancestor_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("concept.concept_id"),primary_key=True)
    descendant_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("concept.concept_id"),primary_key=True)
    min_levels_of_separation: so.Mapped[int] = so.mapped_column(nullable=False)
    max_levels_of_separation: so.Mapped[int] = so.mapped_column(nullable=False)

    def __repr__(self):
        return f"<ConceptAncestor Ancestor: {self.ancestor_concept_id} Descendant: {self.descendant_concept_id}>"
