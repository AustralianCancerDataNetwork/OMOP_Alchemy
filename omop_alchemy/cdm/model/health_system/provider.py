import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional
from orm_loader.helpers import Base
from omop_alchemy.cdm.base import (
    cdm_table,
    CDMTableBase,
    optional_concept_fk,
)

@cdm_table
class Provider(CDMTableBase, Base):
    __tablename__ = "provider"

    provider_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    provider_name: so.Mapped[Optional[str]] = so.mapped_column(sa.String(255), nullable=True)
    npi: so.Mapped[Optional[str]] = so.mapped_column(sa.String(20), nullable=True)
    dea: so.Mapped[Optional[str]] = so.mapped_column(sa.String(20), nullable=True)
    specialty_concept_id: so.Mapped[Optional[int]] = optional_concept_fk(index=True)
    care_site_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("care_site.care_site_id"),nullable=True,index=True,)
    year_of_birth: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    gender_concept_id: so.Mapped[Optional[int]] = optional_concept_fk(index=True)
    provider_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    specialty_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    specialty_source_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()
    gender_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    gender_source_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()

    def __repr__(self) -> str:
        return f"<Provider {self.provider_id}>"
