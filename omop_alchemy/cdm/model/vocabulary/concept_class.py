import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base
from omop_alchemy.cdm.base import ReferenceTable, cdm_table, CDMTableBase

@cdm_table
class Concept_Class(Base, ReferenceTable, CDMTableBase):
    __tablename__ = "concept_class"
    concept_class_id: so.Mapped[str] = so.mapped_column(sa.String(20), primary_key=True)
    concept_class_name: so.Mapped[str] = so.mapped_column(sa.String(255), nullable=False)
    concept_class_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("concept.concept_id"),nullable=False,)

    def __repr__(self):
        return f"<ConceptClass {self.concept_class_id}>"
