from datetime import datetime
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Concept_Synonym(Base):
    __tablename__ = 'concept_synonym'
    concept_synonym_id: so.Mapped[int] = so.mapped_column(index=True, unique=True, primary_key=True)
