import sqlalchemy as sa
import sqlalchemy.orm as so
from ...db import Base

class Vocabulary(Base): 
    __tablename__ = 'vocabulary'
    vocabulary_id : so.Mapped[str] = so.mapped_column(sa.String(20), primary_key=True)
    vocabulary_name: so.Mapped[str] = so.mapped_column(sa.String(255))
    vocabulary_reference: so.Mapped[str] = so.mapped_column(sa.String(255))
    vocabulary_version: so.Mapped[str] = so.mapped_column(sa.String(255))
    vocabulary_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    
    concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[vocabulary_concept_id])

