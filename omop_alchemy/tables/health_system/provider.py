from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Provider(Base):
    __tablename__ = 'provider'
    # identifier
    provider_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True) 
    # temporal
    # strings
    provider_name: so.Mapped[Optional[str]] = so.mapped_column(sa.String(255))
    npi: so.Mapped[Optional[str]] = so.mapped_column(sa.String(20))
    dea: so.Mapped[Optional[str]] = so.mapped_column(sa.String(20))
    provider_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    specialty_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    gender_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    # numeric
    year_of_birth: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer)
    # fks
    care_site_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('care_site.care_site_id'))
    # concept fks
    gender_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    specialty_source_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    gender_source_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    specialty_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    # relationships
    care_site: so.Mapped[Optional['Care_Site']] = so.relationship(foreign_keys=[care_site_id])
    # concept relationships
    gender_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[gender_concept_id])
    specialty_source_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[specialty_source_concept_id])
    gender_source_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[gender_source_concept_id])
    specialty_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[specialty_concept_id])

