from datetime import datetime, date
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so
from decimal import Decimal

from ...db import Base

class Specimen(Base):
    __tablename__ = 'specimen'
    # identifier
    specimen_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True) 
    # temporal
    specimen_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    specimen_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    # strings
    specimen_source_id: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    specimen_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    unit_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    anatomic_site_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    disease_status_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    # numeric    
    quantity: so.Mapped[Optional[Decimal]] = so.mapped_column(sa.Numeric)
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id', name='s_fk_1'))
    # concept fks
    specimen_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='s_fk_2'))
    specimen_type_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='s_fk_3'))
    unit_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='s_fk_4'))
    anatomic_site_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='s_fk_5'))
    disease_status_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='s_fk_6'))
    # relationships
    person: so.Mapped[Optional['Person']] = so.relationship(foreign_keys=[person_id])
    # concept_relationships
    specimen_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[specimen_concept_id])
    specimen_type_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[specimen_type_concept_id])
    unit_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[unit_concept_id])
    anatomic_site_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[anatomic_site_concept_id])
    disease_status_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[disease_status_concept_id])