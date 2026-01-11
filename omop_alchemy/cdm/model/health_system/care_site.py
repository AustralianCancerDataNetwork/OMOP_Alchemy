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
class Care_Site(CDMTableBase, Base):
    __tablename__ = "care_site"

    care_site_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    care_site_name: so.Mapped[Optional[str]] = so.mapped_column(sa.String(255), nullable=True)
    place_of_service_concept_id: so.Mapped[Optional[int]] = optional_concept_fk(index=True)
    location_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("location.location_id"),nullable=True,index=True)
    care_site_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    place_of_service_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)

    def __repr__(self) -> str:
        return f"<CareSite {self.care_site_id}>"
