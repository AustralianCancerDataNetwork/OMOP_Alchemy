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
    address_1: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    address_2: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    city: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    # todo: can we make this string length configurable? srsly states aren't always 2 chars guys
    state: so.Mapped[Optional[str]] = so.mapped_column(sa.String(2))
    zip_code: so.Mapped[Optional[str]] = so.mapped_column(sa.String(9)) # note column name change for python compatibility
    county: so.Mapped[Optional[str]] = so.mapped_column(sa.String(20))
    country: so.Mapped[Optional[str]] = so.mapped_column(sa.String(100))
    location_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    # numeric
    latitude: so.Mapped[Optional[Decimal]] = so.mapped_column(sa.Numeric)
    longitude: so.Mapped[Optional[Decimal]] = so.mapped_column(sa.Numeric)
    # fks
    # concept fks
    # relationships
    # concept relationships

