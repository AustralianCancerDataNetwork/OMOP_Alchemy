import sqlalchemy as sa
import sqlalchemy.orm as so
from ...db import Base

class Domain(Base): 
    __tablename__ = 'domain'
    domain_id: so.Mapped[str] = so.mapped_column(sa.String(20), primary_key=True)
    domain_name: so.Mapped[str] = so.mapped_column(sa.String(255))
    domain_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))

    concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[domain_concept_id])

    def __repr__(self):
        return f'<Domain {self.domain_id} - {self.domain_name}>'