from datetime import datetime
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Drug_Strength(Base):
    __tablename__ = 'drug_strength'
    drug_strength_id: so.Mapped[int] = so.mapped_column(index=True, unique=True, primary_key=True)
