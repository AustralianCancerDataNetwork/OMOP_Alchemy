from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so
from decimal import Decimal
from ...db import Base

class Location(Base):
    __tablename__ = 'location'
    # identifier
    location_id: so.Mapped[int] = so.mapped_column(primary_key=True, unique=True, autoincrement=True) 
    # temporal
    # strings
    address_1: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    address_2: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    city: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    state: so.Mapped[Optional[str]] = so.mapped_column(sa.String(2), nullable=True)
    zip_code: so.Mapped[Optional[str]] = so.mapped_column(sa.String(9), nullable=True) # note column name change for python compatibility
    county: so.Mapped[Optional[str]] = so.mapped_column(sa.String(80), nullable=True)
    country_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(100), nullable=True)
    location_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    # numeric
    latitude: so.Mapped[Optional[Decimal]] = so.mapped_column(sa.Numeric, nullable=True)
    longitude: so.Mapped[Optional[Decimal]] = so.mapped_column(sa.Numeric, nullable=True)
    # fks
    # concept fks
    country_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='lc_fk_1'), nullable=True)
    # relationships
    # concept relationships

# note column is added externally to the class to avoid name collision with python keyword
Location.zip = so.mapped_column(sa.String(9), nullable=True)
