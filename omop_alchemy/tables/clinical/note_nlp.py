from datetime import datetime, date
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Note_NLP(Base):
    __tablename__ = 'note_nlp'
    # identifier
    note_nlp_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True) 
    # temporal
    nlp_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    nlp_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    # strings
    snippet: so.Mapped[Optional[str]] = so.mapped_column(sa.String(250))
    offset: so.Mapped[Optional[str]] = so.mapped_column(sa.String(250))
    lexical_variant: so.Mapped[Optional[str]] = so.mapped_column(sa.String(250))
    nlp_system: so.Mapped[Optional[str]] = so.mapped_column(sa.String(250))
    term_temporal: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    term_modifiers: so.Mapped[Optional[str]] = so.mapped_column(sa.String(2000))
    term_exists: so.Mapped[Optional[str]] = so.mapped_column(sa.String(1))
    # numeric
    # fks    
    note_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('note.note_id'))
    # concept fks
    section_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='n_fk_1'))
    note_nlp_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='n_fk_2'))
    note_nlp_source_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='n_fk_3'))
    # relationships
    note: so.Mapped[Optional['Note']] = so.relationship(foreign_keys=[note_id])
    # concept_relationships
    section: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[section_concept_id])
    note_nlp: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[note_nlp_concept_id])
    note_nlp_source: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[note_nlp_source_concept_id])




