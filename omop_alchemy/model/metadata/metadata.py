import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional
from datetime import date

from omop_alchemy.cdm.base import (
    Base,
    cdm_table,
    CDMTableBase,
    required_concept_fk,
    optional_concept_fk,
)

@cdm_table
class Metadata(CDMTableBase, Base):
    __tablename__ = "metadata"

    metadata_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    metadata_concept_id: so.Mapped[int] = required_concept_fk()
    metadata_type_concept_id: so.Mapped[int] = required_concept_fk()
    name: so.Mapped[str] = so.mapped_column(sa.String(250), nullable=False)
    value_as_string: so.Mapped[Optional[str]] = so.mapped_column(sa.String(250), nullable=True)
    value_as_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()
    value_as_number: so.Mapped[Optional[float]] = so.mapped_column(sa.Float, nullable=True)
    metadata_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date,nullable=True)
    metadata_datetime: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime,nullable=True)

    def __repr__(self) -> str:
        return f"<Metadata {self.metadata_id}: {self.name}>"
