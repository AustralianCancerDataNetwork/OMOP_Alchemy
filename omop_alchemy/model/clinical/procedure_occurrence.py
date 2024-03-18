from datetime import datetime, date
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.hybrid import hybrid_property

from .modifiable_table import Modifiable_Table
from ...db import Base

class Procedure_Occurrence(Modifiable_Table):
    __tablename__ = 'procedure_occurrence'
    # identifier
    procedure_occurrence_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('modifiable_table.modifier_id', name='po_fk_1'), primary_key=True, autoincrement=True) 

    # temporal
    procedure_date: so.Mapped[date] = so.mapped_column(sa.Date)
    procedure_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime, nullable=True)
    # strings
    procedure_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    modifier_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    # numeric
    quantity: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id', name='po_fk_2'))
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('provider.provider_id', name='po_fk_3'), nullable=True)
    visit_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id', name='po_fk_4'), nullable=True)
    visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_detail.visit_detail_id', name='po_fk_5'), nullable=True)
    # concept fks
    procedure_type_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='po_fk_6'))
    modifier_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='po_fk_7'), nullable=True)
    procedure_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='po_fk_8'))
    procedure_source_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='po_fk_9'), nullable=True)
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

    __mapper_args__ = {
        "polymorphic_identity": "procedure",
        'inherit_condition': (procedure_occurrence_id == Modifiable_Table.modifier_id)
    }

    @hybrid_property
    def event_date(self):
        return  self.procedure_datetime.date() if self.procedure_datetime is not None else self.procedure_date
