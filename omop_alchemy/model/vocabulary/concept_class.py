import sqlalchemy as sa
import sqlalchemy.orm as so
from ...db import Base

class Concept_Class(Base): 
    __tablename__ = 'concept_class'
    concept_class_id: so.Mapped[str] = so.mapped_column(sa.String(20), index=True, unique=True, primary_key=True)
    concept_class_name: so.Mapped[str] = so.mapped_column(sa.String(255))
    concept_class_concept_id: so.Mapped[int] = so.mapped_column(sa.BigInteger, sa.ForeignKey('concept.concept_id', name='cc_fk_1'))

#    concept: so.Mapped['Concept'] = so.relationship('Concept', primaryjoin='Concept.concept_id==Concept_Class.concept_class_concept_id', post_update=True)

   # concept_class_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[concept_class_concept_id])
    concept_class_concept: so.Mapped['Concept'] = so.relationship('Concept', primaryjoin='Concept_Class.concept_class_concept_id==Concept.concept_id')

    def __repr__(self):
        return f'<ConceptClass {self.concept_class_id} - {self.concept_class_name}>'
