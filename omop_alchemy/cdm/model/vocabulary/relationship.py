import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base
from omop_alchemy.cdm.base import ReferenceTable, cdm_table, CDMTableBase

@cdm_table
class Relationship(Base, ReferenceTable, CDMTableBase):
    __tablename__ = "relationship"
    relationship_id: so.Mapped[str] = so.mapped_column(sa.String(20), primary_key=True)
    relationship_name: so.Mapped[str] = so.mapped_column(sa.String(255), nullable=False)
    is_hierarchical: so.Mapped[str] = so.mapped_column(sa.String(1), nullable=False)
    defines_ancestry: so.Mapped[str] = so.mapped_column(sa.String(1), nullable=False)
    reverse_relationship_id: so.Mapped[str] = so.mapped_column(sa.ForeignKey("relationship.relationship_id"),nullable=False)
    relationship_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("concept.concept_id"),nullable=False,)

    def __repr__(self):
        return f"<Relationship {self.relationship_id}>"