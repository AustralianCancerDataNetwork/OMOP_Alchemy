import sqlalchemy as sa
import sqlalchemy.orm as so
from ...db import Base


class Relationship(Base): 
    __tablename__ = 'relationship'
    relationship_id: so.Mapped[str] = so.mapped_column(sa.String(20), primary_key=True)
    relationship_name: so.Mapped[str] = so.mapped_column(sa.String(255))
    is_hierarchical: so.Mapped[str]  = so.mapped_column(sa.String(1))
    defines_ancestry: so.Mapped[str]  = so.mapped_column(sa.String(1))
    reverse_relationship_id: so.Mapped[str] = so.mapped_column(sa.ForeignKey('relationship.relationship_id', name='r_fk_2'))
    relationship_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='r_fk_1'))

    concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[relationship_concept_id])
    reverse: so.Mapped['Relationship'] = so.relationship(foreign_keys=[reverse_relationship_id])

    def __repr__(self):
        return f'<Relationship {self.relationship_id} - {self.relationship_name}>'
    

