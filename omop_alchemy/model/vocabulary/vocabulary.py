import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional

from ...db import Base

class Vocabulary(Base): 
    __tablename__ = 'vocabulary'
    vocabulary_id : so.Mapped[str] = so.mapped_column(sa.String(20), primary_key=True)
    vocabulary_name: so.Mapped[str] = so.mapped_column(sa.String(255))
    vocabulary_reference: so.Mapped[Optional[str]] = so.mapped_column(sa.String(255), nullable=True)
    vocabulary_version: so.Mapped[Optional[str]] = so.mapped_column(sa.String(255), nullable=True)
    vocabulary_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='v_fk_1'))
    
    concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[vocabulary_concept_id])

