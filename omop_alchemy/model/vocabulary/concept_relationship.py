from datetime import date
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so
from ...db import Base

class Concept_Relationship(Base): 
    __tablename__ = 'concept_relationship'

    concept_id_1: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='cr_fk_1'), primary_key=True)
    concept_id_2: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='cr_fk_2'), primary_key=True)
    relationship_id: so.Mapped[str] = so.mapped_column(sa.String(20), sa.ForeignKey('relationship.relationship_id', name='cr_fk_3'), primary_key=True)

    valid_start_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    valid_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    invalid_reason: so.Mapped[Optional[str]]  = so.mapped_column(sa.String(1), nullable=True)

    concept_1: so.Mapped['Concept'] = so.relationship('Concept', primaryjoin='Concept_Relationship.concept_id_1==Concept.concept_id', back_populates='concept_relationships')
    concept_2: so.Mapped['Concept'] = so.relationship('Concept', primaryjoin='Concept_Relationship.concept_id_2==Concept.concept_id')
    relationship: so.Mapped['Relationship'] = so.relationship(foreign_keys=[relationship_id])

    def __repr__(self):
        return f'<Relationship {self.concept_1.concept_name} ({self.relationship.relationship_id}) {self.concept_2.concept_name}>'