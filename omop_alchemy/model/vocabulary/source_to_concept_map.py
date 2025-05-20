from datetime import datetime, date
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Source_To_Concept_Map(Base):
    __tablename__ = 'source_to_concept_map'
    source_to_concept_map_id: so.Mapped[int] = so.mapped_column(index=True, unique=True, primary_key=True)
    
    source_code: so.Mapped[str] = so.mapped_column(sa.String(50))
    source_code_description: so.Mapped[Optional[str]] = so.mapped_column(sa.String(255), nullable=True)
    source_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='sc_fk_1'))
    source_vocabulary_id: so.Mapped[str] = so.mapped_column(sa.String(20), sa.ForeignKey('vocabulary.vocabulary_id', name='sc_fk_2'))
    target_vocabulary_id: so.Mapped[str] = so.mapped_column(sa.String(20), sa.ForeignKey('vocabulary.vocabulary_id', name='sc_fk_3'))
    target_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='sc_fk_4'))
    valid_start_date: so.Mapped[date]  = so.mapped_column(sa.Date)
    valid_end_date: so.Mapped[date]  = so.mapped_column(sa.Date)

    invalid_reason: so.Mapped[Optional[str]]  = so.mapped_column(sa.String(1), nullable=True)

    def __repr__(self):
        return f'<Concept {self.concept_id} - {self.concept_code} ({self.concept_name})>'
