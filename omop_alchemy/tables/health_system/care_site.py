from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Care_Site(Base):
    __tablename__ = 'care_site'

    # identifier    
    care_site_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True) 
    # temporal
    # strings
    care_site_name: so.Mapped[Optional[str]] = so.mapped_column(sa.String(255))
    care_site_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    place_of_service_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    # numeric
    # fks
    location_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('location.location_id'))
    # concept fks
    place_of_service_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    # relationships
    location: so.Mapped[Optional['Location']] = so.relationship(foreign_keys=[location_id])    
    # concept relationships
    place_of_service_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[place_of_service_concept_id])