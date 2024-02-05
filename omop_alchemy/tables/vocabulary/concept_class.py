import sqlalchemy as sa
import sqlalchemy.orm as so
from ...db import Base


class Concept_Class(Base): 
    __tablename__ = 'concept_class'
    concept_class_id: so.Mapped[str] = so.mapped_column(sa.String(20), index=True, unique=True, primary_key=True)
    concept_class_name: so.Mapped[str] = so.mapped_column(sa.String(255))
    concept_class_concept_id: so.Mapped[int] = so.mapped_column(sa.BigInteger, sa.ForeignKey('concept.concept_id'))

    concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[concept_class_concept_id])

    def __repr__(self):
        return f'<ConceptClass {self.concept_class_id} - {self.concept_class_name}>'
