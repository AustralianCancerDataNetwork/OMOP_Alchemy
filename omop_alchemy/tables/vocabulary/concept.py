import sqlalchemy as sa
import sqlalchemy.orm as so
from datetime import date

from ...db import Base

class Concept(Base): 
    __tablename__ = 'concept'
    concept_id: so.Mapped[int] = so.mapped_column(index=True, unique=True, primary_key=True)
    concept_name: so.Mapped[str] = so.mapped_column(sa.String(255))
    concept_code: so.Mapped[str] = so.mapped_column(sa.String(50))

    domain_id: so.Mapped[str] = so.mapped_column(sa.String(20), sa.ForeignKey('domain.domain_id', name='c_fk_1'))
    vocabulary_id: so.Mapped[str] = so.mapped_column(sa.String(20), sa.ForeignKey('vocabulary.vocabulary_id', name='c_fk_2'))
    concept_class_id: so.Mapped[str] = so.mapped_column(sa.String(20), sa.ForeignKey('concept_class.concept_class_id', name='c_fk_3'))
    
    domain: so.Mapped['Domain'] = so.relationship(foreign_keys=[domain_id])
    vocabulary: so.Mapped['Vocabulary'] = so.relationship(foreign_keys=[vocabulary_id])
    concept_class: so.Mapped['Concept_Class'] = so.relationship(foreign_keys=[concept_class_id])

    standard_concept: so.Mapped[str]  = so.mapped_column(sa.String(1))
    valid_start_date: so.Mapped[date]  = so.mapped_column(sa.Date)
    valid_end_date: so.Mapped[date]  = so.mapped_column(sa.Date)
    invalid_reason: so.Mapped[str]  = so.mapped_column(sa.String(1))

    def __repr__(self):
        return f'<Concept {self.concept_id} - {self.concept_code} ({self.concept_name})>'
