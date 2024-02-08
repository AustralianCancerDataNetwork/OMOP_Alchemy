from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Concept_Ancestor(Base): 
    __tablename__ = 'concept_ancestor'

    ancestor_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'), primary_key=True)
    descendant_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'), primary_key=True)

    ancestor: so.Mapped['Concept'] = so.relationship(foreign_keys=[ancestor_concept_id])
    descendant: so.Mapped['Concept'] = so.relationship(foreign_keys=[descendant_concept_id])
    min_levels_of_separation: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer)
    max_levels_of_separation: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer)

    def __repr__(self):
        return f'<Ancestry {self.ancestor.concept_name} ({self.min_levels_of_separation} - {self.max_levels_of_separation}) {self.descendant.concept_name}>'

