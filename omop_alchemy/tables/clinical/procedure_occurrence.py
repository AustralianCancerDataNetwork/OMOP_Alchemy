from datetime import datetime, date
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Procedure_Occurrence(Base):
    __tablename__ = 'procedure_occurrence'
    # identifier
    procedure_occurrence_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True) 

    # temporal
    procedure_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    procedure_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    # strings
    procedure_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    modifier_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    # numeric
    quantity: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer)
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id'))
    provider_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('provider.provider_id'))
    visit_occurrence_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id'))
    visit_detail_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('visit_detail.visit_detail_id'))
    # concept fks
    procedure_type_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    modifier_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    procedure_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    procedure_source_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    # relationships
    person: so.Mapped[Optional['Person']] = so.relationship(foreign_keys=[person_id])
    provider: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    visit_occurrence: so.Mapped[Optional['Visit_Occurrence']] = so.relationship(foreign_keys=[visit_occurrence_id])
    visit_detail: so.Mapped[Optional['Visit_Detail']] = so.relationship(foreign_keys=[visit_detail_id])
    # concept_relationships
    procedure_type_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[procedure_type_concept_id])
    modifier_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[modifier_concept_id])
    procedure_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[procedure_concept_id])
    procedure_source_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[procedure_source_concept_id])