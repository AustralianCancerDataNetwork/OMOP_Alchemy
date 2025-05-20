from datetime import datetime
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Cohort(Base):
    __tablename__ = 'cohort'
    cohort_id: so.Mapped[int] = so.mapped_column(index=True, unique=True, primary_key=True)
