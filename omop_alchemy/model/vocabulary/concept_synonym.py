from datetime import datetime
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Concept_Synonym(Base):
    __tablename__ = 'concept_synonym'
    concept_synonym_id: so.Mapped[int] = so.mapped_column(index=True, unique=True, primary_key=True)
    concept_synonym_name: so.Mapped[str] = so.mapped_column(sa.String(1000))
    language_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='cs_fk_1'), primary_key=True)
