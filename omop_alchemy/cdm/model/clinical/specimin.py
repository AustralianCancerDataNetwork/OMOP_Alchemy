import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional
from datetime import date
from orm_loader.helpers import Base
from omop_alchemy.cdm.base import (
    CDMTableBase,
    cdm_table,
    required_concept_fk,
    optional_concept_fk,
)

@cdm_table
class Specimen(CDMTableBase, Base):
    __tablename__ = "specimen"

    specimen_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("person.person_id"), nullable=False)

    specimen_concept_id: so.Mapped[int] = required_concept_fk()
    specimen_type_concept_id: so.Mapped[int] = required_concept_fk()

    specimen_date: so.Mapped[date] = so.mapped_column(nullable=False)
    specimen_datetime: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime, nullable=True)

    quantity: so.Mapped[Optional[float]] = so.mapped_column(nullable=True)
    unit_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()

    anatomic_site_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()
    disease_status_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()

    specimen_source_id: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    specimen_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)

    unit_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    anatomic_site_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    disease_status_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
