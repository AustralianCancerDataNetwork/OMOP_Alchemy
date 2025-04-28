from datetime import datetime
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Cohort_Definition(Base):
    __tablename__ = 'cohort_definition'
    cohort_definition_id: so.Mapped[int] = so.mapped_column(index=True, unique=True, primary_key=True)
