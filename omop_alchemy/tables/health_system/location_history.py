from datetime import date
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Location_History(Base):
    __tablename__ = 'location_history'
    # identifier
    location_history_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True) 
    # temporal
    start_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    # strings
    # numeric
    entity_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer)
    # fks
    location_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('location.location_id'))
    domain_id: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    # concept fks
    relationship_type_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    # relationships
    location: so.Mapped[Optional['Location']] = so.relationship(foreign_keys=[location_id])
    # concept relationships
    relationship_type_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[relationship_type_concept_id])