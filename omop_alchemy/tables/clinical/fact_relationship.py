from datetime import datetime
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Fact_Relationship(Base):
    __tablename__ = 'fact_relationship'
    # identifier
    fact_relationship_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True) 
    # temporal
    # strings
    # numeric
    fact_id_1: so.Mapped[int] = so.mapped_column(sa.Integer)
    fact_id_2: so.Mapped[int] = so.mapped_column(sa.Integer)
    # fks
    # concept fks
    domain_concept_id_1: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    domain_concept_id_2: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    relationship_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    # relationships
    # concept_relationships
    relationship: so.Mapped['Concept'] = so.relationship(foreign_keys=[relationship_concept_id])
    domain_1: so.Mapped['Concept'] = so.relationship(foreign_keys=[domain_concept_id_1])
    domain_2: so.Mapped['Concept'] = so.relationship(foreign_keys=[domain_concept_id_2])
