import sqlalchemy as sa
import sqlalchemy.orm as so
from datetime import date
from typing import Optional, List

from ...db import Base

class Concept(Base): 
    __tablename__ = 'concept'
    concept_id: so.Mapped[int] = so.mapped_column(sa.Integer, index=True, unique=True, primary_key=True)
    concept_name: so.Mapped[str] = so.mapped_column(sa.String(255))
    concept_code: so.Mapped[str] = so.mapped_column(sa.String(50))

    domain_id: so.Mapped[str] = so.mapped_column(sa.String(20), sa.ForeignKey('domain.domain_id', name='c_fk_1'))
    vocabulary_id: so.Mapped[str] = so.mapped_column(sa.String(20), sa.ForeignKey('vocabulary.vocabulary_id', name='c_fk_2'))
    concept_class_id: so.Mapped[str] = so.mapped_column(sa.String(20), sa.ForeignKey('concept_class.concept_class_id', name='c_fk_3'))
    
    domain: so.Mapped['Domain'] = so.relationship('Domain', primaryjoin='Concept.domain_id==Domain.domain_id', post_update=True, cascade="all")
    vocabulary: so.Mapped['Vocabulary'] = so.relationship('Vocabulary', primaryjoin='Concept.vocabulary_id==Vocabulary.vocabulary_id', post_update=True, cascade="all")#foreign_keys=[vocabulary_id], back_populates='vocabulary_concept')
    concept_class: so.Mapped['Concept_Class'] = so.relationship('Concept_Class', primaryjoin='Concept.concept_class_id==Concept_Class.concept_class_id', post_update=True, cascade="all")#foreign_keys=[concept_class_id])

    concept_relationships: so.Mapped[List['Concept_Relationship']] = so.relationship('Concept_Relationship', back_populates='concept_1', primaryjoin='Concept.concept_id==Concept_Relationship.concept_id_1')

    standard_concept: so.Mapped[Optional[str]]  = so.mapped_column(sa.String(1), nullable=True)
    valid_start_date: so.Mapped[date]  = so.mapped_column(sa.Date)
    valid_end_date: so.Mapped[date]  = so.mapped_column(sa.Date)
    invalid_reason: so.Mapped[Optional[str]]  = so.mapped_column(sa.String(1), nullable=True)

    def __repr__(self):
        return f'<Concept {self.concept_id} - {self.concept_code} ({self.concept_name})>'
