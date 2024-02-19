from datetime import datetime
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Source_To_Concept_Map(Base):
    __tablename__ = 'source_to_concept_map'
    source_to_concept_map_id: so.Mapped[int] = so.mapped_column(index=True, unique=True, primary_key=True)
